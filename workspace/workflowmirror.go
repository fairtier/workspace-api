package workspace

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/yaml.v3"
)

// Workflows as files: the workflow twin of TransformationMirror. Every
// workflow is rendered to workflows/<slug>.yaml in the box's transformations
// repo — next to the transformations/<slug>.yaml execution configs, in the
// one repo a person (and an agent, through write_repo_file) may edit — and
// the adopt pass (workflowmirror_adopt.go) pulls edits, additions and
// deletions made there back into the database.
//
// Unlike the other two mirrors, a step names its target by a stable,
// human-readable key rather than an id: the target's own file slug, i.e. the
// file name of pipelines/<slug>.yaml or transformations/<slug>.yaml without
// ".yaml". A person writing a workflow by hand can read those off the repos;
// the database keeps ids and the mirror translates in both directions.

// workflowsDir is where the mirror keeps workflow definitions inside
// transformationsRepo.
const workflowsDir = "workflows/"

// WorkflowDefinitionRender is the workflow mirror's bookkeeping for one
// rendered workflows/<slug>.yaml: the path and Gitea blob sha of the mirror's
// last write or adoption. Same cache semantics as
// TransformationDefinitionRender.
type WorkflowDefinitionRender struct {
	WorkflowID WorkflowID
	Path       string
	BlobSHA    string
	// RefusedBlobSHA is the last repo blob the adopt pass refused; it
	// suppresses repeat notifications for the same commit.
	RefusedBlobSHA string
}

// WorkflowDefinitionRenderStore persists workflow definition-render
// bookkeeping. Rows are removed by the workflow-delete FK cascade.
type WorkflowDefinitionRenderStore interface {
	UpsertWorkflowDefinitionRender(ctx context.Context, r *WorkflowDefinitionRender) error
	GetWorkflowDefinitionRenders(ctx context.Context, customerSlug string) (map[WorkflowID]WorkflowDefinitionRender, error)
	MarkWorkflowDefinitionRefused(ctx context.Context, id WorkflowID, refusedSHA string) error
}

// WorkflowImporter creates a workflow row (steps included) with a
// caller-supplied id: the id a repo file carries, or one minted for a file
// written without one.
type WorkflowImporter interface {
	ImportWorkflow(ctx context.Context, w *Workflow) error
}

// WorkflowPipelineSource is what the workflow mirror needs from pipelines:
// the list to compute their file slugs, and the lookup the shared validator
// uses.
type WorkflowPipelineSource interface {
	PipelineReader
	ListPipelinesByCustomer(ctx context.Context, customerSlug string) ([]Pipeline, error)
}

// WorkflowTransformationSource is the transformations counterpart of
// WorkflowPipelineSource.
type WorkflowTransformationSource interface {
	TransformationReader
	ListTransformationsByCustomer(ctx context.Context, customerSlug string) ([]Transformation, error)
}

// WorkflowMirror renders a customer's workflows into the box's
// transformations repo and adopts repo edits back.
type WorkflowMirror struct {
	Workspaces      Resolver
	Credentials     BoxGitCredentialStore
	Workflows       WorkflowRepository
	Pipelines       WorkflowPipelineSource
	Transformations WorkflowTransformationSource
	// DefinitionRenders keeps the per-file blob-sha bookkeeping. Optional:
	// nil disables drift detection and the adopt pass, never the converge.
	DefinitionRenders WorkflowDefinitionRenderStore
	// Notifications raises the in-app notifications for overwritten,
	// adopted, refused and deleted out-of-band edits. Optional.
	Notifications Notifier
	// Importer, when set, marks the repo as the source of truth for
	// workflows — the box: the adopt pass then CREATES workflows for new
	// files and DELETES workflows whose file was removed. Unset, only edits
	// to already-rendered files are adopted.
	Importer WorkflowImporter
	// NewClient builds a Gitea client for a box.
	NewClient func(baseURL, username, token string) RepoFileClient
	Logger    *slog.Logger

	// skips dedupes refusal reports for files that have no render row to
	// carry a refused sha (new files, renames, duplicates).
	skips importSkips
}

// workflowFile is the YAML shape of one workflow definition. The id is the
// stable identity (file names follow the display name and move on rename);
// it may be left out of a hand-written new file, and the adopt pass mints
// one.
type workflowFile struct {
	ID          string `yaml:"id,omitempty"`
	Name        string `yaml:"name"`
	Description string `yaml:"description,omitempty"`
	// Schedule is a five-field cron expression (UTC); empty = manual only.
	Schedule string `yaml:"schedule,omitempty"`
	// Enabled defaults to true when left out, as CreateWorkflow does.
	Enabled *bool              `yaml:"enabled,omitempty"`
	Steps   []workflowFileStep `yaml:"steps"`
}

// workflowFileStep names exactly one target by its file slug.
type workflowFileStep struct {
	Pipeline       string `yaml:"pipeline,omitempty"`
	Transformation string `yaml:"transformation,omitempty"`
}

// WorkflowMirrorer is what WorkflowService needs from the mirror.
// SyncCustomerAfterDelete is SyncCustomer told which workflow was just
// deleted, so its file is collected even when someone wrote it by hand.
type WorkflowMirrorer interface {
	SyncCustomer(ctx context.Context, customerSlug string, author *CommitAuthor) error
	SyncCustomerAfterDelete(ctx context.Context, customerSlug string, author *CommitAuthor, deleted *Workflow) error
}

// SyncCustomer converges the box repo's workflows/ directory to the
// customer's current workflows. Out-of-scope customers are skipped silently;
// a concurrent commit is retried once with a fresh tree, never forced.
func (m *WorkflowMirror) SyncCustomer(ctx context.Context, customerSlug string, author *CommitAuthor) error {
	return m.sync(ctx, customerSlug, author, nil)
}

// SyncCustomerAfterDelete is SyncCustomer after a workflow delete.
func (m *WorkflowMirror) SyncCustomerAfterDelete(ctx context.Context, customerSlug string, author *CommitAuthor, deleted *Workflow) error {
	return m.sync(ctx, customerSlug, author, []Workflow{*deleted})
}

func (m *WorkflowMirror) sync(ctx context.Context, customerSlug string, author *CommitAuthor, deleted []Workflow) (err error) {
	ctx, span := tracer.Start(ctx, "WorkflowMirror.SyncCustomer", trace.WithAttributes(
		attrSlug.String(customerSlug),
		attrRepoPlane.String(planeWorkflows),
	))
	started, inScope := time.Now(), false
	defer func() { recordSync(ctx, span, planeWorkflows, started, err, inScope) }()

	client, _, ok, err := boxMirrorClientFor(ctx, m.Workspaces, m.Credentials, m.NewClient, customerSlug, m.Logger)
	if err != nil || !ok {
		return err
	}
	inScope = true

	// Targets first, workflows second: a step can only reference a target
	// that already existed when the workflow was read.
	keys, err := m.loadTargetKeys(ctx, customerSlug)
	if err != nil {
		return err
	}
	workflows, err := m.Workflows.ListWorkflowsByCustomer(ctx, customerSlug)
	if err != nil {
		return fmt.Errorf("list workflows: %w", err)
	}
	desired, err := renderWorkflowFiles(workflows, keys)
	if err != nil {
		return err
	}
	defRenders, err := m.definitionRenderRows(ctx, customerSlug)
	if err != nil {
		return err
	}
	ours := newOwnedWorkflows(append(slices.Clone(workflows), deleted...))
	span.SetAttributes(attribute.Int("workspace.repo.definition_files", len(desired)))

	if err := m.converge(ctx, client, customerSlug, desired, defRenders, ours, author); errors.Is(err, ErrRepoFileChanged) {
		span.AddEvent("workspace.repo.converge_retry")
		return m.converge(ctx, client, customerSlug, desired, defRenders, ours, author)
	} else if err != nil {
		return err
	}
	return nil
}

// converge writes every desired file and collects stale ones. Only
// workflows/*.yaml is touched; the dbt project around it never is.
func (m *WorkflowMirror) converge(ctx context.Context, client RepoFileClient, customerSlug string, desired map[string]workflowDefinitionFile, defRenders map[WorkflowID]WorkflowDefinitionRender, ours ownedWorkflows, author *CommitAuthor) error {
	tree, err := client.ListTree(ctx, transformationsRepo)
	if err != nil {
		return fmt.Errorf("list %s tree: %w", transformationsRepo, err)
	}
	existing := workflowTreeYAML(tree)
	for _, filePath := range sortedPaths(desired) {
		if err := m.upsertFile(ctx, client, customerSlug, filePath, desired[filePath], existing, defRenders, author); err != nil {
			return err
		}
	}
	return m.deleteStale(ctx, client, existing, desired, ours, author)
}

// ownedWorkflows identifies the files a converge may collect: those of the
// workflows this database has (a stale copy after a rename) or has just
// deleted — by id, or by name for a hand-written file that never carried an
// id (the adopt pass imported it under a minted one).
type ownedWorkflows struct {
	ids, names map[string]bool
}

func newOwnedWorkflows(workflows []Workflow) ownedWorkflows {
	o := ownedWorkflows{ids: map[string]bool{}, names: map[string]bool{}}
	for _, w := range workflows {
		o.ids[string(w.ID)] = true
		o.names[w.Name] = true
	}
	return o
}

func (o ownedWorkflows) owns(id, name string) bool {
	if id != "" {
		return o.ids[id]
	}
	return o.names[name]
}

// deleteStale removes the workflow files no longer desired — but only those
// ownedWorkflows claims. Unlike the header test of deleteStaleRendered, this
// also collects a hand-written file once its workflow is renamed or deleted
// in the Console, and it never touches a file the adopt pass has not taken
// (an id this database does not know, or no id and a name no workflow has):
// that may be someone's new workflow, and the only copy of it.
func (m *WorkflowMirror) deleteStale(ctx context.Context, client RepoFileClient, existing map[string]string, desired map[string]workflowDefinitionFile, ours ownedWorkflows, author *CommitAuthor) error {
	for _, filePath := range sortedPaths(existing) {
		if _, ok := desired[filePath]; ok {
			continue
		}
		content, _, err := client.GetContents(ctx, transformationsRepo, filePath)
		if err != nil {
			return fmt.Errorf("get %s: %w", filePath, err)
		}
		var f struct {
			ID   string `yaml:"id"`
			Name string `yaml:"name"`
		}
		if yaml.Unmarshal([]byte(content), &f) != nil || !ours.owns(f.ID, strings.TrimSpace(f.Name)) {
			continue // not provably ours to delete
		}
		msg := fmt.Sprintf("Delete %s via FairTier Console", filePath)
		if err := client.DeleteContents(ctx, transformationsRepo, filePath, existing[filePath], msg, author); err != nil {
			return fmt.Errorf("delete %s: %w", filePath, err)
		}
		recordCommit(ctx, planeWorkflows, "delete", filePath)
	}
	return nil
}

// workflowTreeYAML returns the managed workflows/*.yaml paths (path → blob
// sha).
func workflowTreeYAML(tree []RepoFileEntry) map[string]string {
	files := make(map[string]string)
	for _, e := range tree {
		if strings.HasPrefix(e.Path, workflowsDir) && strings.HasSuffix(e.Path, ".yaml") {
			files[e.Path] = e.SHA
		}
	}
	return files
}

// upsertFile creates or updates one rendered file, overwriting an
// out-of-band edit with a notification — the transformation mirror's
// save-path contract: an explicit save is newer intent.
func (m *WorkflowMirror) upsertFile(ctx context.Context, client RepoFileClient, customerSlug, filePath string, df workflowDefinitionFile, existing map[string]string, defRenders map[WorkflowID]WorkflowDefinitionRender, author *CommitAuthor) error {
	treeSHA, inTree := existing[filePath]
	if !inTree {
		return m.putFile(ctx, client, filePath, df, "", author)
	}
	current, sha, err := client.GetContents(ctx, transformationsRepo, filePath)
	if err != nil {
		return fmt.Errorf("get %s: %w", filePath, err)
	}
	row, tracked := defRenders[df.workflowID]
	ownPath := tracked && row.Path == filePath
	if current == df.content {
		if !ownPath || row.BlobSHA != treeSHA {
			m.recordDefinitionRender(ctx, df.workflowID, filePath, treeSHA)
		}
		return nil
	}
	if err := m.putFile(ctx, client, filePath, df, sha, author); err != nil {
		return err
	}
	if ownPath && row.BlobSHA != treeSHA {
		m.notifyDrift(ctx, customerSlug, filePath)
	}
	return nil
}

// putFile writes one rendered file (sha "" creates it) and records the render.
func (m *WorkflowMirror) putFile(ctx context.Context, client RepoFileClient, filePath string, df workflowDefinitionFile, sha string, author *CommitAuthor) error {
	msg := fmt.Sprintf("Render %s via FairTier Console", filePath)
	newSHA, err := client.PutContents(ctx, transformationsRepo, filePath, df.content, sha, msg, author)
	if err != nil {
		return fmt.Errorf("put %s: %w", filePath, err)
	}
	recordCommit(ctx, planeWorkflows, "upsert", filePath)
	m.recordDefinitionRender(ctx, df.workflowID, filePath, newSHA)
	return nil
}

// notifyDrift reports an out-of-band edit the converge just overwrote.
func (m *WorkflowMirror) notifyDrift(ctx context.Context, customerSlug, filePath string) {
	trace.SpanFromContext(ctx).AddEvent("workspace.repo.drift_overwritten", trace.WithAttributes(attrRepoPath.String(filePath)))
	m.notify(ctx, customerSlug, "Workflow file changed outside the Console",
		fmt.Sprintf("%s in your workspace repo was edited outside the Console; the Console version has been reapplied. The overwritten change remains in the file's git history.", filePath),
		"/workflows")
}

func (m *WorkflowMirror) definitionRenderRows(ctx context.Context, customerSlug string) (map[WorkflowID]WorkflowDefinitionRender, error) {
	if m.DefinitionRenders == nil {
		return nil, nil
	}
	rows, err := m.DefinitionRenders.GetWorkflowDefinitionRenders(ctx, customerSlug)
	if err != nil {
		return nil, fmt.Errorf("get definition renders: %w", err)
	}
	return rows, nil
}

// recordDefinitionRender best-effort stamps the row after a write or an
// adoption.
func (m *WorkflowMirror) recordDefinitionRender(ctx context.Context, id WorkflowID, filePath, blobSHA string) {
	if m.DefinitionRenders == nil {
		return
	}
	err := m.DefinitionRenders.UpsertWorkflowDefinitionRender(ctx, &WorkflowDefinitionRender{
		WorkflowID: id, Path: filePath, BlobSHA: blobSHA,
	})
	if err != nil && m.Logger != nil {
		m.Logger.WarnContext(ctx, "record workflow render", "workflow", id, "err", err)
	}
}

// notify raises one best-effort in-app notification.
func (m *WorkflowMirror) notify(ctx context.Context, customerSlug, title, body, link string) {
	if m.Notifications == nil {
		return
	}
	n := Notification{CustomerSlug: customerSlug, Type: "info", Title: title, Body: body, Link: link}
	if err := m.Notifications.Notify(ctx, n); err != nil && m.Logger != nil {
		m.Logger.WarnContext(ctx, "notify workflow file", "customer", customerSlug, "title", title, "err", err)
	}
}

// --- Target keys ---

// workflowTargetKeys translates between step targets' ids and the file slugs
// a workflow file names them by.
type workflowTargetKeys struct {
	pipelineSlug       map[PipelineID]string
	transformationSlug map[TransformationID]string
	pipelineBySlug     map[string]PipelineID
	transformBySlug    map[string]TransformationID
}

func (m *WorkflowMirror) loadTargetKeys(ctx context.Context, customerSlug string) (*workflowTargetKeys, error) {
	pipelines, err := m.Pipelines.ListPipelinesByCustomer(ctx, customerSlug)
	if err != nil {
		return nil, fmt.Errorf("list pipelines: %w", err)
	}
	transformations, err := m.Transformations.ListTransformationsByCustomer(ctx, customerSlug)
	if err != nil {
		return nil, fmt.Errorf("list transformations: %w", err)
	}
	k := &workflowTargetKeys{
		pipelineSlug:       pipelineFileSlugs(pipelines),
		transformationSlug: transformationFileSlugs(transformations),
		pipelineBySlug:     make(map[string]PipelineID, len(pipelines)),
		transformBySlug:    make(map[string]TransformationID, len(transformations)),
	}
	for id, slug := range k.pipelineSlug {
		k.pipelineBySlug[slug] = id
	}
	for id, slug := range k.transformationSlug {
		k.transformBySlug[slug] = id
	}
	return k, nil
}

// pipelineKey is the key a rendered file names a pipeline step by. A target
// the listing did not include (created between the two reads) falls back to
// its id, which resolve accepts too.
func (k *workflowTargetKeys) pipelineKey(id PipelineID) string {
	if slug, ok := k.pipelineSlug[id]; ok {
		return slug
	}
	return string(id)
}

func (k *workflowTargetKeys) transformationKey(id TransformationID) string {
	if slug, ok := k.transformationSlug[id]; ok {
		return slug
	}
	return string(id)
}

// resolve maps a file step to a WorkflowStep, or returns the user-facing
// reason it cannot. A key is a target's file slug; a target's id is accepted
// as well, so a file stays valid if its target is renamed under it.
func (k *workflowTargetKeys) resolve(i int, s workflowFileStep) (WorkflowStep, string) {
	switch {
	case s.Pipeline != "" && s.Transformation != "":
		return WorkflowStep{}, fmt.Sprintf("step %d names both a pipeline and a transformation; a step runs exactly one", i+1)
	case s.Pipeline != "":
		if id, ok := k.pipelineBySlug[s.Pipeline]; ok {
			return NewWorkflowStep(StepKindPipeline, string(id)), ""
		}
		if _, ok := k.pipelineSlug[PipelineID(s.Pipeline)]; ok {
			return NewWorkflowStep(StepKindPipeline, s.Pipeline), ""
		}
		return WorkflowStep{}, fmt.Sprintf("step %d names pipeline %q, which does not exist in this workspace (use the file name of pipelines/<name>.yaml without .yaml)", i+1, s.Pipeline)
	case s.Transformation != "":
		if id, ok := k.transformBySlug[s.Transformation]; ok {
			return NewWorkflowStep(StepKindTransformation, string(id)), ""
		}
		if _, ok := k.transformationSlug[TransformationID(s.Transformation)]; ok {
			return NewWorkflowStep(StepKindTransformation, s.Transformation), ""
		}
		return WorkflowStep{}, fmt.Sprintf("step %d names transformation %q, which does not exist in this workspace (use the file name of transformations/<name>.yaml without .yaml)", i+1, s.Transformation)
	default:
		return WorkflowStep{}, fmt.Sprintf("step %d names neither a pipeline nor a transformation", i+1)
	}
}

// --- Rendering ---

// workflowDefinitionFile is one desired workflows/<slug>.yaml.
type workflowDefinitionFile struct {
	workflowID WorkflowID
	content    string
}

func renderWorkflowFiles(workflows []Workflow, keys *workflowTargetKeys) (map[string]workflowDefinitionFile, error) {
	slugs := workflowFileSlugs(workflows)
	files := make(map[string]workflowDefinitionFile, len(workflows))
	for i := range workflows {
		w := &workflows[i]
		content, err := renderWorkflowFile(w, keys)
		if err != nil {
			return nil, fmt.Errorf("render workflow %s: %w", w.ID, err)
		}
		files[workflowsDir+slugs[w.ID]+".yaml"] = workflowDefinitionFile{workflowID: w.ID, content: content}
	}
	return files, nil
}

func renderWorkflowFile(w *Workflow, keys *workflowTargetKeys) (string, error) {
	enabled := w.Enabled
	f := workflowFile{
		ID:          string(w.ID),
		Name:        w.Name,
		Description: w.Description,
		Schedule:    w.Schedule,
		Enabled:     &enabled,
		Steps:       make([]workflowFileStep, 0, len(w.Steps)),
	}
	for _, s := range w.Steps {
		if s.Kind == StepKindPipeline {
			f.Steps = append(f.Steps, workflowFileStep{Pipeline: keys.pipelineKey(s.PipelineID)})
		} else {
			f.Steps = append(f.Steps, workflowFileStep{Transformation: keys.transformationKey(s.TransformationID)})
		}
	}
	out, err := yaml.Marshal(f)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}
	return pipelineFileHeader + string(out), nil
}

// workflowFileSlugs assigns each workflow its file slug, suffixing every
// member of a name-collision group with -<id[:8]> (the other mirrors' rule).
// Names are unique per workspace, so a collision needs two names that slug
// alike ("Nightly" and "nightly").
func workflowFileSlugs(workflows []Workflow) map[WorkflowID]string {
	base := func(w Workflow) string {
		if slug := nameFileSlug(w.Name); slug != "" {
			return slug
		}
		return shortID(string(w.ID))
	}
	count := make(map[string]int, len(workflows))
	for _, w := range workflows {
		count[base(w)]++
	}
	slugs := make(map[WorkflowID]string, len(workflows))
	for _, w := range workflows {
		slug := base(w)
		if count[slug] > 1 {
			slug += "-" + shortID(string(w.ID))
		}
		slugs[w.ID] = slug
	}
	return slugs
}

// --- Parsing ---

// yamlTypeNoise strips the Go type names yaml.v3 puts in its errors ("field
// x not found in type workspace.workflowFile"), which mean nothing to the
// person reading the notification.
var yamlTypeNoise = regexp.MustCompile(` in type \S+`)

// parseWorkflowFile strictly parses a workflow file: an unknown key (a
// misspelt "pipline:") is an error rather than a silently dropped step. It
// returns the user-facing reason on failure.
func parseWorkflowFile(content string) (*workflowFile, string) {
	dec := yaml.NewDecoder(strings.NewReader(content))
	dec.KnownFields(true)
	var f workflowFile
	if err := dec.Decode(&f); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, "the file is empty"
		}
		return nil, "the file is not a valid workflow definition (" + yamlTypeNoise.ReplaceAllString(err.Error(), "") + ")"
	}
	return &f, ""
}

// workflowFromFile resolves and validates a parsed file into the workflow to
// store, with the same validator the API uses. It returns nil and a
// user-facing reason when the file cannot be taken; err is reserved for
// failures that should abort the pass.
func (m *WorkflowMirror) workflowFromFile(ctx context.Context, f *workflowFile, id WorkflowID, customerSlug string, keys *workflowTargetKeys) (*Workflow, string, error) {
	w := &Workflow{
		ID:           id,
		CustomerSlug: customerSlug,
		Name:         f.Name,
		Description:  f.Description,
		Schedule:     f.Schedule,
		Enabled:      f.Enabled == nil || *f.Enabled,
	}
	for i, s := range f.Steps {
		step, reason := keys.resolve(i, s)
		if reason != "" {
			return nil, reason, nil
		}
		w.Steps = append(w.Steps, step)
	}
	err := ValidateWorkflowDefinition(ctx, m.Pipelines, m.Transformations, customerSlug, w)
	var inv *ErrInvalidWorkflow
	if errors.As(err, &inv) {
		return nil, "it is not a valid workflow: " + fileFieldReason(inv), nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("validate workflow file: %w", err)
	}
	return w, "", nil
}

// fileFieldReason rephrases a validation error in the file's terms: the API
// field "steps[2].ref_id" is the file's step 3.
var stepFieldRe = regexp.MustCompile(`^steps\[(\d+)\]`)

func fileFieldReason(inv *ErrInvalidWorkflow) string {
	if m := stepFieldRe.FindStringSubmatch(inv.Field); m != nil {
		var n int
		_, _ = fmt.Sscanf(m[1], "%d", &n)
		return fmt.Sprintf("step %d: %s", n+1, inv.Msg)
	}
	return inv.Field + ": " + inv.Msg
}

// RepoSyncer is the converge shape PipelineMirrorer and TransformationMirrorer
// share.
type RepoSyncer interface {
	SyncCustomer(ctx context.Context, customerSlug string, author *CommitAuthor) error
}

// ThenSyncWorkflows wraps a pipeline or transformation mirror so that every
// successful converge of it re-renders the workflow files too. A workflow
// file names its steps by the targets' file slugs, so a renamed target moves
// its key, and a deleted one takes its step with it (FK cascade) — both
// change workflow files without any workflow being saved.
//
// The workflow converge is best-effort: its failure is logged and the
// primary save still succeeds, because nothing the user saved is lost — the
// workflow rows are already right, and the next converge rewrites the files.
type ThenSyncWorkflows struct {
	Primary   RepoSyncer
	Workflows RepoSyncer
	Logger    *slog.Logger
}

// SyncCustomer converges Primary and, when that succeeds, Workflows.
func (t *ThenSyncWorkflows) SyncCustomer(ctx context.Context, customerSlug string, author *CommitAuthor) error {
	if err := t.Primary.SyncCustomer(ctx, customerSlug, author); err != nil {
		return err
	}
	if err := t.Workflows.SyncCustomer(ctx, customerSlug, author); err != nil && t.Logger != nil {
		t.Logger.WarnContext(ctx, "re-render workflow files after a target change; the next converge retries", "customer", customerSlug, "err", err)
	}
	return nil
}

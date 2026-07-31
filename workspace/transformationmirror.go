package workspace

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"gopkg.in/yaml.v3"
)

// transformationsRepo is the box Gitea repo holding the customer's dbt
// project (seeded by the box's deploy-time seed job). The mirror manages only
// the transformations/ directory inside it — the dbt code itself (models/,
// macros/, dbt_project.yml, …) is customer-owned and never touched.
const transformationsRepo = "transformations"

// TransformationDefinitionRender is the transformation mirror's bookkeeping
// for one rendered transformations/<slug>.yaml: the path and Gitea blob sha
// of the mirror's last write. Same cache semantics as
// PipelineDefinitionRender: a lost row costs one missed drift signal, never
// correctness.
type TransformationDefinitionRender struct {
	TransformationID TransformationID
	Path             string
	BlobSHA          string
	// RefusedBlobSHA is the last repo blob the adopt pass refused to take
	// into the central cache (unparseable, foreign id, foreign trigger
	// pipeline). It suppresses repeat notifications for the same refused
	// commit; a successful render or adoption clears it.
	RefusedBlobSHA string
}

// TransformationDefinitionRenderStore persists transformation
// definition-render bookkeeping. Rows are removed by the
// transformation-delete FK cascade; the mirror only ever upserts.
type TransformationDefinitionRenderStore interface {
	UpsertTransformationDefinitionRender(ctx context.Context, r *TransformationDefinitionRender) error
	// GetTransformationDefinitionRenders returns all rows for a customer's
	// transformations, keyed by transformation id (one batch read per
	// converge).
	GetTransformationDefinitionRenders(ctx context.Context, customerSlug string) (map[TransformationID]TransformationDefinitionRender, error)
	// MarkTransformationDefinitionRefused stamps the refused blob sha
	// without touching the last-render bookkeeping (adopt pass).
	MarkTransformationDefinitionRefused(ctx context.Context, id TransformationID, refusedSHA string) error
}

// TransformationMirror renders a customer's transformation execution configs
// (which repo/ref to run, schedule, selector — never the dbt code) into the
// box's transformations repo as transformations/<slug>.yaml
// (control-plane/workspace-split Phase 2F, the transformations twin of
// PipelineMirror). Git credentials for connected external repos are NOT
// rendered — they stay central-only and keep flowing to the worker via the
// poll.
type TransformationMirror struct {
	Workspaces      Resolver
	Credentials     BoxGitCredentialStore
	Transformations TransformationRepository
	// Pipelines validates trigger_after_pipeline references on adoption, so
	// a repo edit can't chain a transformation onto another tenant's
	// pipeline. Required for the adopt pass; the rendering converge never
	// uses it.
	Pipelines PipelineReader
	// DefinitionRenders keeps per-file blob-sha bookkeeping — the drift
	// signal behind overwrite-and-notify and the adopt pass. Optional: nil
	// disables drift detection and adoption, never the converge itself.
	DefinitionRenders TransformationDefinitionRenderStore
	// Notifications, when set, raises the in-app notifications for
	// overwritten, adopted, and refused out-of-band edits. Optional.
	Notifications Notifier
	// NewClient builds a Gitea client for a box (same factory shape as
	// PipelineMirror.NewClient).
	NewClient func(baseURL, username, token string) RepoFileClient
	Logger    *slog.Logger
}

// transformationFile is the rendered YAML shape of one transformation
// execution config. The id is the stable key (file names follow the display
// name and can move on rename); git credentials are deliberately absent.
type transformationFile struct {
	ID      string `yaml:"id"`
	Name    string `yaml:"name"`
	RepoURL string `yaml:"repo_url,omitempty"`
	RepoRef string `yaml:"repo_ref"`
	// Schedule is a cron expression; empty means trigger-only.
	Schedule             string `yaml:"schedule,omitempty"`
	TriggerAfterPipeline string `yaml:"trigger_after_pipeline,omitempty"`
	DBTSelector          string `yaml:"dbt_selector,omitempty"`
	Enabled              bool   `yaml:"enabled"`
}

// SyncCustomer converges the box's transformations repo to the customer's
// current transformation set. Customers outside the mirror's scope (shared
// substrate, box not provisioned, no deposited credential yet) are skipped
// silently. A concurrent commit (stale sha) is retried once with a fresh
// tree; conflicting writes are never forced.
//
// author is the acting Console user when the sync was triggered by a save,
// nil for platform-initiated syncs; it becomes the git author of every
// commit this converge makes.
func (m *TransformationMirror) SyncCustomer(ctx context.Context, customerSlug string, author *CommitAuthor) error {
	client, _, ok, err := boxMirrorClientFor(ctx, m.Workspaces, m.Credentials, m.NewClient, customerSlug)
	if err != nil || !ok {
		return err
	}

	transformations, err := m.Transformations.ListTransformationsByCustomer(ctx, customerSlug)
	if err != nil {
		return fmt.Errorf("list transformations: %w", err)
	}
	desired, err := renderTransformationFiles(transformations)
	if err != nil {
		return err
	}
	defRenders, err := m.definitionRenderRows(ctx, customerSlug)
	if err != nil {
		return err
	}

	if err := m.converge(ctx, client, customerSlug, desired, defRenders, author); errors.Is(err, ErrRepoFileChanged) {
		// Something else committed between our tree read and write. One
		// fresh-tree retry; a second conflict is surfaced, never overridden.
		// defRenders is deliberately NOT re-read: files the first attempt
		// already converged compare content-equal on the retry and adopt the
		// new sha silently, so no double drift notification.
		return m.converge(ctx, client, customerSlug, desired, defRenders, author)
	} else if err != nil {
		return err
	}
	return nil
}

// converge diffs the repo's transformations/ directory against desired and
// applies the minimum set of put/delete commits. Files outside
// transformations/*.yaml (the dbt project itself) are never touched.
func (m *TransformationMirror) converge(ctx context.Context, client RepoFileClient, customerSlug string, desired map[string]transformationDefinitionFile, defRenders map[TransformationID]TransformationDefinitionRender, author *CommitAuthor) error {
	tree, err := client.ListTree(ctx, transformationsRepo)
	if err != nil {
		return fmt.Errorf("list %s tree: %w", transformationsRepo, err)
	}
	existing := transformationTreeYAML(tree)

	for _, filePath := range sortedPaths(desired) {
		if err := m.upsertFile(ctx, client, customerSlug, filePath, desired[filePath], existing, defRenders, author); err != nil {
			return err
		}
	}
	return deleteStale(ctx, client, transformationsRepo, existing, desired, author)
}

// transformationTreeYAML returns the managed transformations/*.yaml paths
// (path → blob sha). Everything else in the repo is the customer's dbt
// project and stays unmanaged.
func transformationTreeYAML(tree []RepoFileEntry) map[string]string {
	files := make(map[string]string)
	for _, e := range tree {
		if strings.HasPrefix(e.Path, "transformations/") && strings.HasSuffix(e.Path, ".yaml") {
			files[e.Path] = e.SHA
		}
	}
	return files
}

// upsertFile creates filePath when absent, or updates it when its content
// differs from the desired render — overwriting drift with a notification,
// exactly like the pipeline mirror's save-path converge (an explicit Console
// save is newer intent than an out-of-band edit).
func (m *TransformationMirror) upsertFile(ctx context.Context, client RepoFileClient, customerSlug, filePath string, df transformationDefinitionFile, existing map[string]string, defRenders map[TransformationID]TransformationDefinitionRender, author *CommitAuthor) error {
	msg := fmt.Sprintf("Render %s via FairTier Console", filePath)
	treeSHA, inTree := existing[filePath]
	if !inTree {
		newSHA, err := client.PutContents(ctx, transformationsRepo, filePath, df.content, "", msg, author)
		if err != nil {
			return fmt.Errorf("put %s: %w", filePath, err)
		}
		m.recordDefinitionRender(ctx, df.transformationID, filePath, newSHA)
		return nil
	}
	current, sha, err := client.GetContents(ctx, transformationsRepo, filePath)
	if err != nil {
		return fmt.Errorf("get %s: %w", filePath, err)
	}
	row, tracked := defRenders[df.transformationID]
	if current == df.content {
		m.adoptInSyncRender(ctx, df.transformationID, filePath, treeSHA, row, tracked)
		return nil
	}
	drifted := tracked && row.Path == filePath && row.BlobSHA != treeSHA
	newSHA, err := client.PutContents(ctx, transformationsRepo, filePath, df.content, sha, msg, author)
	if err != nil {
		return fmt.Errorf("put %s: %w", filePath, err)
	}
	m.recordDefinitionRender(ctx, df.transformationID, filePath, newSHA)
	if drifted {
		m.notifyDefinitionDrift(ctx, customerSlug, filePath)
	}
	return nil
}

// adoptInSyncRender stamps the drift row when the repo file already equals
// the rendered content but the bookkeeping does not yet reflect it (same
// contract as the pipeline mirror's twin: nothing was overwritten, so there
// is nothing to report).
func (m *TransformationMirror) adoptInSyncRender(ctx context.Context, id TransformationID, filePath, treeSHA string, row TransformationDefinitionRender, tracked bool) {
	if !tracked || row.BlobSHA != treeSHA || row.Path != filePath {
		m.recordDefinitionRender(ctx, id, filePath, treeSHA)
	}
}

// definitionRenderRows loads the drift bookkeeping for a customer; nil when
// the store is not wired (drift detection off).
func (m *TransformationMirror) definitionRenderRows(ctx context.Context, customerSlug string) (map[TransformationID]TransformationDefinitionRender, error) {
	if m.DefinitionRenders == nil {
		return nil, nil
	}
	rows, err := m.DefinitionRenders.GetTransformationDefinitionRenders(ctx, customerSlug)
	if err != nil {
		return nil, fmt.Errorf("get definition renders: %w", err)
	}
	return rows, nil
}

// recordDefinitionRender best-effort stamps the row after a write (a lost
// write costs one missed drift signal, never correctness).
func (m *TransformationMirror) recordDefinitionRender(ctx context.Context, id TransformationID, filePath, blobSHA string) {
	if m.DefinitionRenders == nil {
		return
	}
	err := m.DefinitionRenders.UpsertTransformationDefinitionRender(ctx, &TransformationDefinitionRender{
		TransformationID: id,
		Path:             filePath,
		BlobSHA:          blobSHA,
	})
	if err != nil && m.Logger != nil {
		m.Logger.WarnContext(ctx, "record transformation render", "transformation", id, "err", err)
	}
}

// notifyDefinitionDrift raises the in-app notification for an out-of-band
// repo edit the converge just overwrote. Best-effort.
func (m *TransformationMirror) notifyDefinitionDrift(ctx context.Context, customerSlug, filePath string) {
	if m.Notifications == nil {
		return
	}
	n := Notification{
		CustomerSlug: customerSlug,
		Type:         "info",
		Title:        "Transformation file changed outside the Console",
		Body:         fmt.Sprintf("%s in your workspace repo was edited outside the Console; the Console version has been reapplied. The overwritten change remains in the file's git history.", filePath),
		Link:         "transformations",
	}
	if err := m.Notifications.Notify(ctx, n); err != nil && m.Logger != nil {
		m.Logger.WarnContext(ctx, "notify transformation drift", "customer", customerSlug, "err", err)
	}
}

// transformationDefinitionFile is one desired transformations/<slug>.yaml:
// the rendered content plus the owning transformation (for render
// bookkeeping and drift attribution).
type transformationDefinitionFile struct {
	transformationID TransformationID
	content          string
}

// renderTransformationFiles maps transformations to path → desired file.
func renderTransformationFiles(transformations []Transformation) (map[string]transformationDefinitionFile, error) {
	slugs := transformationFileSlugs(transformations)
	files := make(map[string]transformationDefinitionFile, len(transformations))
	for _, t := range transformations {
		content, err := renderTransformationFile(&t)
		if err != nil {
			return nil, fmt.Errorf("render transformation %s: %w", t.ID, err)
		}
		files["transformations/"+slugs[t.ID]+".yaml"] = transformationDefinitionFile{transformationID: t.ID, content: content}
	}
	return files, nil
}

func renderTransformationFile(t *Transformation) (string, error) {
	f := transformationFile{
		ID:                   string(t.ID),
		Name:                 t.Name,
		RepoURL:              t.RepoURL,
		RepoRef:              t.RepoRef,
		Schedule:             t.Schedule,
		TriggerAfterPipeline: string(t.TriggerAfterPipelineID),
		DBTSelector:          t.DBTSelector,
		Enabled:              t.Enabled,
	}
	out, err := yaml.Marshal(f)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}
	return pipelineFileHeader + string(out), nil
}

// transformationFileSlugs assigns each transformation its file slug. Every
// member of a name-collision group gets an -<id[:8]> suffix (not just the
// later one), so which transformation was created first never changes an
// existing path.
func transformationFileSlugs(transformations []Transformation) map[TransformationID]string {
	baseCount := make(map[string]int, len(transformations))
	for _, t := range transformations {
		baseCount[transformationFileSlug(t)]++
	}

	slugs := make(map[TransformationID]string, len(transformations))
	for _, t := range transformations {
		slug := transformationFileSlug(t)
		if baseCount[slug] > 1 {
			slug += "-" + shortID(string(t.ID))
		}
		slugs[t.ID] = slug
	}
	return slugs
}

func transformationFileSlug(t Transformation) string {
	if slug := nameFileSlug(t.Name); slug != "" {
		return slug
	}
	return shortID(string(t.ID))
}

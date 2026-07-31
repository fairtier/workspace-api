package workspace

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"gopkg.in/yaml.v3"
)

// Adopt-on-drift (control-plane/workspace-split Phase 2B): the box's
// pipelines repo is the source of truth, so out-of-band commits are pulled
// INTO the central cache instead of being overwritten. AdoptCustomer is
// strictly read-only toward the repo — it writes only central rows — which
// keeps it safe to run anywhere (the platform worker) without the age or
// OAuth material the rendering converge needs.
//
// Semantics per foreign definition commit (blob sha differs from the last
// recorded render):
//   - parses cleanly, same id, same source type → the central row adopts the
//     repo state; the user is notified "updated from git".
//   - anything else → the file is left alone, the user is notified once (the
//     refused sha is recorded), and the central cache keeps its state. The
//     next Console save still overwrites — an explicit save is newer intent.
//
// Foreign .age commits flip the pipeline to externally-managed credentials
// (see upsertCredentialFile — same flag, shared with the save-path converge).
// New files and deletions are NOT adopted: the Console remains the only
// create/delete path (git-centric gaps #4 decision).

// AdoptCustomer runs one adopt pass for a customer. Out-of-scope customers
// (shared substrate, no deposited credential) are skipped silently.
func (m *PipelineMirror) AdoptCustomer(ctx context.Context, customerSlug string) error {
	client, _, ok, err := m.clientFor(ctx, customerSlug)
	if err != nil || !ok {
		return err
	}
	if m.DefinitionRenders == nil {
		return nil // no bookkeeping — cannot tell foreign commits apart
	}

	tree, err := client.ListTree(ctx, pipelinesRepo)
	if err != nil {
		return fmt.Errorf("list %s tree: %w", pipelinesRepo, err)
	}
	yamlTree, ageTree := partitionPipelineTree(tree)

	pipelines, err := m.Pipelines.ListPipelinesByCustomer(ctx, customerSlug)
	if err != nil {
		return fmt.Errorf("list pipelines: %w", err)
	}
	defRenders, err := m.DefinitionRenders.GetPipelineDefinitionRenders(ctx, customerSlug)
	if err != nil {
		return fmt.Errorf("get definition renders: %w", err)
	}

	for i := range pipelines {
		p := &pipelines[i]
		if err := m.adoptDefinition(ctx, client, p, defRenders, yamlTree); err != nil {
			return err
		}
	}
	m.flagForeignCredentialFiles(ctx, customerSlug, pipelines, ageTree)
	return nil
}

// adoptDefinition inspects one pipeline's rendered definition for a foreign
// commit and adopts or refuses it.
func (m *PipelineMirror) adoptDefinition(ctx context.Context, client RepoFileClient, p *Pipeline, defRenders map[PipelineID]PipelineDefinitionRender, yamlTree map[string]string) error {
	row, tracked := defRenders[p.ID]
	if !tracked {
		return nil // never rendered — the next save-converge bootstraps it
	}
	treeSHA, inTree := yamlTree[row.Path]
	if !inTree || treeSHA == row.BlobSHA {
		return nil // deleted files are healed by the next save-converge
	}
	if treeSHA == row.RefusedBlobSHA {
		return nil // already refused and reported this exact commit
	}

	content, _, err := client.GetContents(ctx, pipelinesRepo, row.Path)
	if err != nil {
		return fmt.Errorf("get %s: %w", row.Path, err)
	}
	adopted, refuseReason := parseAdoptablePipeline(content, p)
	if adopted == nil {
		m.refuseAdoption(ctx, p, row.Path, treeSHA, refuseReason)
		return nil
	}

	adopted.UpdatedAt = time.Now()
	if err := m.Pipelines.UpdatePipeline(ctx, adopted); err != nil {
		return fmt.Errorf("adopt %s: %w", row.Path, err)
	}
	m.recordDefinitionRender(ctx, p.ID, row.Path, treeSHA)
	m.notifyAdopted(ctx, p.CustomerSlug, row.Path, p.ID)
	return nil
}

// parseAdoptablePipeline strictly parses a foreign definition edit against
// the current row. It returns the pipeline to store, or nil with a
// user-facing reason the edit cannot be adopted.
func parseAdoptablePipeline(content string, current *Pipeline) (*Pipeline, string) {
	var f pipelineFile
	if err := yaml.Unmarshal([]byte(content), &f); err != nil {
		return nil, "the file is not valid YAML"
	}
	if f.ID != string(current.ID) {
		return nil, "the file's id field does not match this pipeline"
	}
	if f.SourceType != current.SourceType {
		// Includes file_upload pipelines, whose files are rendered in their
		// rewritten "filesystem" form — their identity lives in the Console.
		return nil, "the file changes the pipeline's source type"
	}
	adopted, err := pipelineFromFile(current.ID, f)
	if err != nil {
		return nil, "the file's source_config cannot be read"
	}
	if err := ValidateSourceConfig(adopted.SourceType, adopted.SourceConfig); err != nil {
		return nil, "the file's source_config is not valid for this source type"
	}
	adopted.CustomerSlug = current.CustomerSlug
	adopted.SourceCredentials = current.SourceCredentials
	adopted.CreatedAt = current.CreatedAt
	adopted.CredentialsExternal = current.CredentialsExternal
	return adopted, ""
}

// refuseAdoption records the refused sha (once-per-commit notification
// guard) and tells the user their edit could not be taken over.
func (m *PipelineMirror) refuseAdoption(ctx context.Context, p *Pipeline, filePath, treeSHA, reason string) {
	if err := m.DefinitionRenders.MarkPipelineDefinitionRefused(ctx, p.ID, treeSHA); err != nil {
		if m.Logger != nil {
			m.Logger.WarnContext(ctx, "mark definition refused", "pipeline", p.ID, "err", err)
		}
		return
	}
	if m.Notifications == nil {
		return
	}
	n := Notification{
		CustomerSlug: p.CustomerSlug,
		Type:         "info",
		Title:        "Pipeline file edit could not be applied",
		Body:         fmt.Sprintf("%s was edited outside the Console, but %s. The Console keeps its current configuration; fix the file or save the pipeline in the Console to overwrite it.", filePath, reason),
		Link:         "/pipelines?pipeline=" + string(p.ID),
	}
	if err := m.Notifications.Notify(ctx, n); err != nil && m.Logger != nil {
		m.Logger.WarnContext(ctx, "notify refused adoption", "pipeline", p.ID, "err", err)
	}
}

// notifyAdopted raises the "updated from git" notification. Best-effort.
func (m *PipelineMirror) notifyAdopted(ctx context.Context, customerSlug, filePath string, id PipelineID) {
	if m.Notifications == nil {
		return
	}
	n := Notification{
		CustomerSlug: customerSlug,
		Type:         "info",
		Title:        "Pipeline updated from your repo",
		Body:         fmt.Sprintf("%s was edited outside the Console; the change has been applied to the pipeline.", filePath),
		Link:         "/pipelines?pipeline=" + string(id),
	}
	if err := m.Notifications.Notify(ctx, n); err != nil && m.Logger != nil {
		m.Logger.WarnContext(ctx, "notify adopted definition", "pipeline", id, "err", err)
	}
}

// flagForeignCredentialFiles marks pipelines whose .age file carries a
// commit the mirror did not make (same detection as the save-path converge,
// without any repo write). Requires the credential render bookkeeping and
// the ownership store; missing either is a no-op.
func (m *PipelineMirror) flagForeignCredentialFiles(ctx context.Context, customerSlug string, pipelines []Pipeline, ageTree map[string]string) {
	if m.Renders == nil || m.Ownership == nil {
		return
	}
	renders, err := m.Renders.GetPipelineCredentialRenders(ctx, customerSlug)
	if err != nil {
		if m.Logger != nil {
			m.Logger.WarnContext(ctx, "get credential renders", "customer", customerSlug, "err", err)
		}
		return
	}
	slugs := pipelineFileSlugs(pipelines)
	for _, p := range pipelines {
		if p.CredentialsExternal {
			continue
		}
		row, ok := renders[p.ID]
		if !ok {
			continue
		}
		filePath := "pipelines/" + slugs[p.ID] + ".credentials.age"
		treeSHA, inTree := ageTree[filePath]
		if !inTree || treeSHA == row.BlobSHA {
			continue
		}
		m.markCredentialsExternal(ctx, filePath, p.ID)
	}
}

// WorkspaceLister enumerates the workspaces in adopt scope (VM substrate).
type WorkspaceLister interface {
	ListVMWorkspaceSlugs(ctx context.Context) ([]string, error)
}

// AdoptSweeper periodically runs the adopt passes across all VM workspaces —
// the pull half of git-as-source-of-truth, catching edits made directly in
// a box's Gitea between Console saves. Each mirror is optional (nil = that
// plane's git-primary flag is off), so pipelines and transformations can
// roll out independently.
type AdoptSweeper struct {
	Mirror          *PipelineMirror
	Transformations *TransformationMirror
	Workspaces      WorkspaceLister
	Logger          *slog.Logger
}

// Run sweeps immediately, then on every tick until ctx is done — same shape
// as StuckRunSweeper.Run, so a box adopts direct Gitea edits right after boot
// instead of a full interval later. Errors are logged, never fatal.
func (s *AdoptSweeper) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		s.sweep(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *AdoptSweeper) sweep(ctx context.Context) {
	slugs, err := s.Workspaces.ListVMWorkspaceSlugs(ctx)
	if err != nil {
		s.Logger.WarnContext(ctx, "adopt sweep: list workspaces", "err", err)
		return
	}
	for _, slug := range slugs {
		if s.Mirror != nil {
			if err := s.Mirror.AdoptCustomer(ctx, slug); err != nil {
				s.Logger.WarnContext(ctx, "adopt sweep", "customer", slug, "err", err)
			}
		}
		if s.Transformations != nil {
			if err := s.Transformations.AdoptCustomer(ctx, slug); err != nil {
				s.Logger.WarnContext(ctx, "transformation adopt sweep", "customer", slug, "err", err)
			}
		}
	}
}

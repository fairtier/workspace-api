package workspace

import (
	"context"
	"fmt"
	"log/slog"
	"path"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/fairtier/workspace-api/core"
)

// DraftFile is one LLM-generated file, path relative to the target repo root.
type DraftFile struct {
	Path    string
	Content string
}

// TransformationDraft is the LLM-produced draft of a dbt transformation: the
// non-sensitive config fields plus starter model files for the hosted repo.
// repo_url, git_credentials and trigger_after_pipeline_id are never drafted.
type TransformationDraft struct {
	Name        string
	Schedule    string
	DBTSelector string
	Files       []DraftFile
	Notes       string
}

// RillDraft is the LLM-produced draft of Rill project files (metrics views,
// explore dashboards, optional models).
type RillDraft struct {
	Files []DraftFile
	Notes string
}

// SqlDraft is the LLM-produced draft of one read-only DuckDB query. Never
// executed server-side on the user's behalf — it lands in the editor.
type SqlDraft struct {
	SQL   string
	Notes string
}

// ErrorExplanation is the LLM-produced diagnosis of one failure. All fields
// are prose except SuggestedSnippet, which may carry a corrected SQL/config
// fragment (or be empty when the model is not confident).
type ErrorExplanation struct {
	Explanation      string
	LikelyCause      string
	SuggestedFix     string
	SuggestedSnippet string
}

// TransformationDrafter turns a natural-language prompt into a draft dbt
// transformation via an LLM. Implementations live outside the domain (the llm
// package) so the domain stays free of any LLM SDK dependency.
type TransformationDrafter interface {
	DraftTransformation(ctx context.Context, prompt string) (*TransformationDraft, error)
}

// RillDrafter turns a natural-language prompt into draft Rill project files.
// existingPaths lets the model reference real models/sources in the repo.
type RillDrafter interface {
	DraftRillDashboard(ctx context.Context, prompt string, existingPaths []string) (*RillDraft, error)
}

// SqlDrafter turns a natural-language prompt (plus the editor's current SQL
// and a schema listing) into a draft DuckDB query.
type SqlDrafter interface {
	DraftSql(ctx context.Context, prompt, currentSQL, schemaContext string) (*SqlDraft, error)
}

// ErrorExplainer turns server-assembled failure context into a structured
// explanation. The context is assembled HERE, never client-supplied for run
// targets, so nothing a browser sends can smuggle another tenant's data — or
// instructions — into the prompt as trusted context.
type ErrorExplainer interface {
	ExplainError(ctx context.Context, contextText string) (*ErrorExplanation, error)
}

// TableRef names one warehouse table.
type TableRef struct {
	Namespace string
	Name      string
}

// ColumnSchema is one column of a described table.
type ColumnSchema struct {
	Name     string
	Type     string
	Nullable bool
}

// SchemaSource lists and describes the caller's warehouse tables, and
// validates SQL without executing it. Implemented in the server package over
// the tenant's own DuckFlight engine binding.
type SchemaSource interface {
	Tables(ctx context.Context, callerID core.UserID) ([]TableRef, error)
	Columns(ctx context.Context, callerID core.UserID, ref TableRef) ([]ColumnSchema, error)
	// Explain runs EXPLAIN <sql> as a cheap validity check; the returned error
	// carries the engine's own message (nil = the statement binds).
	Explain(ctx context.Context, callerID core.UserID, sql string) error
}

// PipelineLookup is the slice of PipelineService that ExplainError needs:
// tenant-scoped resolution of a pipeline and its recent runs.
type PipelineLookup interface {
	GetPipeline(ctx context.Context, callerID core.UserID, id PipelineID) (*Pipeline, []PipelineRun, error)
}

// TransformationLookup mirrors PipelineLookup for transformations.
type TransformationLookup interface {
	GetTransformation(ctx context.Context, callerID core.UserID, id TransformationID) (*Transformation, []TransformationRun, error)
}

// draftFileLimits bound what an LLM draft may produce; anything outside is a
// validation error, exactly like a malformed manual request.
const (
	maxDraftFiles    = 8
	maxDraftFileSize = 64 * 1024
)

// AssistService drafts dbt transformations and Rill dashboards from natural
// language. It mirrors PipelineAssistService: tenant-scoped, rate-limited,
// with all model output validated server-side (which is also what catches
// schema drift from providers without strict structured output).
type AssistService struct {
	// Transformations performs the dbt LLM call. When nil, DraftTransformation
	// returns ErrDraftNotConfigured.
	Transformations TransformationDrafter
	// Rill performs the Rill LLM call. When nil, DraftRillDashboard returns
	// ErrDraftNotConfigured.
	Rill RillDrafter
	// Sql performs the SQL LLM call. When nil, DraftSql returns
	// ErrDraftNotConfigured.
	Sql SqlDrafter
	// Explainer performs the error-explanation LLM call. When nil, the
	// Explain* methods return ErrDraftNotConfigured.
	Explainer ErrorExplainer
	// Schema provides the warehouse schema context for DraftSql (required for
	// it) and best-effort SQL validation. Nil disables DraftSql.
	Schema SchemaSource
	// PipelineRuns resolves the trusted context for ExplainPipelineRun.
	PipelineRuns PipelineLookup
	// TransformationRuns resolves the trusted context for
	// ExplainTransformationRun.
	TransformationRuns TransformationLookup
	// Customers resolves the caller's tenant; the draft RPCs are tenant-scoped
	// so only provisioned customers can use them.
	Workspaces Resolver
	// Limiter optionally rate-limits draft requests per caller. Shared with
	// PipelineAssistService — the guarded resource is per-user LLM spend.
	Limiter RateLimiter
	Logger  *slog.Logger
}

// DraftTransformation turns prompt into a validated TransformationDraft for
// the caller's tenant. Drafted files are constrained to models/ with .sql/.yml
// extensions.
func (s *AssistService) DraftTransformation(ctx context.Context, callerID core.UserID, prompt string) (_ *TransformationDraft, err error) {
	defer func() { recordDraft(ctx, "transformation", err) }()

	if s.Transformations == nil {
		return nil, ErrDraftNotConfigured
	}
	if err := s.gate(ctx, callerID, prompt); err != nil {
		return nil, err
	}

	draft, err := s.Transformations.DraftTransformation(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("draft transformation: %w", err)
	}

	if strings.TrimSpace(draft.Name) == "" {
		return nil, &ErrInvalidSourceConfig{Field: "name", Msg: "draft is missing a name"}
	}
	if err := validateDraftFiles(draft.Files, []string{"models/"}, []string{".sql", ".yml"}); err != nil {
		return nil, err
	}
	return draft, nil
}

// DraftRillDashboard turns prompt into a validated RillDraft for the caller's
// tenant. Drafted files are constrained to models/, metrics/ and dashboards/;
// every .yaml file must parse as YAML.
func (s *AssistService) DraftRillDashboard(ctx context.Context, callerID core.UserID, prompt string, existingPaths []string) (_ *RillDraft, err error) {
	defer func() { recordDraft(ctx, "rill_dashboard", err) }()

	if s.Rill == nil {
		return nil, ErrDraftNotConfigured
	}
	if err := s.gate(ctx, callerID, prompt); err != nil {
		return nil, err
	}

	draft, err := s.Rill.DraftRillDashboard(ctx, prompt, existingPaths)
	if err != nil {
		return nil, fmt.Errorf("draft rill dashboard: %w", err)
	}

	if len(draft.Files) == 0 {
		return nil, &ErrInvalidSourceConfig{Field: "files", Msg: "draft produced no files"}
	}
	if err := validateDraftFiles(draft.Files, []string{"models/", "metrics/", "dashboards/"}, []string{".sql", ".yaml"}); err != nil {
		return nil, err
	}
	for _, f := range draft.Files {
		if !strings.HasSuffix(f.Path, ".yaml") {
			continue
		}
		var doc any
		if err := yaml.Unmarshal([]byte(f.Content), &doc); err != nil {
			return nil, &ErrInvalidSourceConfig{
				Field: "files." + f.Path,
				Msg:   fmt.Sprintf("drafted %s is not valid YAML: %v", f.Path, err),
			}
		}
	}
	return draft, nil
}

// gate runs the shared pre-LLM checks: non-empty prompt, tenant scoping, rate
// limit — the same sequence as PipelineAssistService.DraftPipeline.
func (s *AssistService) gate(ctx context.Context, callerID core.UserID, prompt string) error {
	if strings.TrimSpace(prompt) == "" {
		return &ErrInvalidSourceConfig{Field: "prompt", Msg: "prompt is required"}
	}
	return s.scope(ctx, callerID)
}

// scope is gate without the prompt check, for the Explain* methods whose
// input is a target reference rather than free text: tenant scoping, then the
// shared rate limit (one budget across every assist surface — the guarded
// resource is per-user LLM spend).
func (s *AssistService) scope(ctx context.Context, callerID core.UserID) error {
	if _, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID); err != nil {
		return fmt.Errorf("get customer: %w", err)
	}
	if s.Limiter != nil && !s.Limiter.Allow(string(callerID)) {
		return ErrDraftRateLimited
	}
	return nil
}

// validateDraftFiles checks LLM-drafted files: clean relative paths under an
// allowed root, an allowed extension, and content within size limits. This is
// a validation surface, not a security boundary — drafts are only ever shown
// to the user for review — but it keeps a drifting model from producing
// nonsense paths that a later commit step would have to reject anyway.
func validateDraftFiles(files []DraftFile, allowedRoots, allowedExts []string) error {
	if len(files) > maxDraftFiles {
		return &ErrInvalidSourceConfig{Field: "files", Msg: fmt.Sprintf("draft produced %d files (max %d)", len(files), maxDraftFiles)}
	}
	for _, f := range files {
		if err := validateDraftFile(f, allowedRoots, allowedExts); err != nil {
			return err
		}
	}
	return nil
}

// validateDraftFile validates a single drafted file's path, extension, and content.
func validateDraftFile(f DraftFile, allowedRoots, allowedExts []string) error {
	if f.Path == "" {
		return &ErrInvalidSourceConfig{Field: "files", Msg: "drafted file has an empty path"}
	}
	clean := path.Clean(f.Path)
	if clean != f.Path || strings.HasPrefix(clean, "/") || strings.HasPrefix(clean, "..") {
		return &ErrInvalidSourceConfig{Field: "files." + f.Path, Msg: fmt.Sprintf("drafted file path %q is not a clean relative path", f.Path)}
	}
	if !draftPathUnderRoot(clean, allowedRoots) {
		return &ErrInvalidSourceConfig{Field: "files." + f.Path, Msg: fmt.Sprintf("drafted file path %q is outside the allowed directories (%s)", f.Path, strings.Join(allowedRoots, ", "))}
	}
	if !draftPathHasAllowedExt(clean, allowedExts) {
		return &ErrInvalidSourceConfig{Field: "files." + f.Path, Msg: fmt.Sprintf("drafted file %q has a disallowed extension (allowed: %s)", f.Path, strings.Join(allowedExts, ", "))}
	}
	if f.Content == "" {
		return &ErrInvalidSourceConfig{Field: "files." + f.Path, Msg: fmt.Sprintf("drafted file %q is empty", f.Path)}
	}
	if len(f.Content) > maxDraftFileSize {
		return &ErrInvalidSourceConfig{Field: "files." + f.Path, Msg: fmt.Sprintf("drafted file %q exceeds %d bytes", f.Path, maxDraftFileSize)}
	}
	return nil
}

// draftPathUnderRoot reports whether clean sits under one of the allowed roots.
func draftPathUnderRoot(clean string, allowedRoots []string) bool {
	for _, root := range allowedRoots {
		if strings.HasPrefix(clean, root) && len(clean) > len(root) {
			return true
		}
	}
	return false
}

// draftPathHasAllowedExt reports whether clean ends with an allowed extension.
func draftPathHasAllowedExt(clean string, allowedExts []string) bool {
	for _, ext := range allowedExts {
		if strings.HasSuffix(clean, ext) {
			return true
		}
	}
	return false
}

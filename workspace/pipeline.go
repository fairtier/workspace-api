package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/fairtier/workspace-api/core"
)

// PipelineID is a strongly-typed identifier for pipelines (UUID).
type PipelineID string

func (id PipelineID) String() string { return string(id) }

// Pipeline represents a dlt data-loading pipeline configuration.
type Pipeline struct {
	ID                PipelineID
	CustomerSlug      string
	Name              string
	SourceType        string
	SourceConfig      json.RawMessage
	SourceCredentials json.RawMessage
	DatasetName       string
	Schedule          string
	WriteDisposition  string
	MergeStrategy     string
	Enabled           bool
	// CredentialsExternal marks the pipeline's .credentials.age file as
	// managed outside the Console (an out-of-band edit in the box repo the
	// central plane cannot decrypt). The mirror leaves the file alone until
	// the next Console credential edit reclaims ownership (Phase 2B).
	CredentialsExternal bool
	CreatedAt           time.Time
	UpdatedAt           time.Time

	// Transient fields set by GetEnabledPipelines.
	TriggerNow   bool
	PendingRunID string
	LastRunAt    *time.Time // Last successful run time (nil if never run)

	// Transient fields set by ListPipelinesByCustomer / GetPipeline: the most
	// recent run regardless of status, for the list view.
	LastRunTime   *time.Time
	LastRunStatus string
}

// PipelineRun records the result of a single pipeline execution.
type PipelineRun struct {
	ID           string
	PipelineID   PipelineID
	Status       string // "pending", "running", "success", "failed"
	StartedAt    *time.Time
	CompletedAt  *time.Time
	RowsLoaded   int64
	ErrorMessage string
	CreatedAt    time.Time
}

// PipelineRepository persists pipeline configurations and run results.
type PipelineRepository interface {
	CreatePipeline(ctx context.Context, p *Pipeline) error
	GetPipeline(ctx context.Context, id PipelineID) (*Pipeline, error)
	ListPipelinesByCustomer(ctx context.Context, customerSlug string) ([]Pipeline, error)
	UpdatePipeline(ctx context.Context, p *Pipeline) error
	DeletePipeline(ctx context.Context, id PipelineID) error

	// GetEnabledPipelines returns the pipelines the worker should act on: every
	// enabled pipeline, plus any disabled one with a pending (manually
	// triggered) run (worker-facing).
	GetEnabledPipelines(ctx context.Context, customerSlug string) ([]Pipeline, error)

	CreatePipelineRun(ctx context.Context, run *PipelineRun) error
	UpdatePipelineRun(ctx context.Context, run *PipelineRun) error
	ListRecentRuns(ctx context.Context, pipelineID PipelineID, limit int) ([]PipelineRun, error)

	// GetPendingRun returns the oldest queued run for a pipeline, or (nil, nil)
	// when none is pending. Used to keep manual triggers idempotent.
	GetPendingRun(ctx context.Context, pipelineID PipelineID) (*PipelineRun, error)
}

// PipelineMirrorer mirrors a customer's pipeline definitions into the box's
// pipelines repo (pipelines-as-files Phase 1). Implemented by PipelineMirror.
// author attributes the resulting commits to the acting Console user; nil
// keeps the platform attribution (platform-initiated syncs).
type PipelineMirrorer interface {
	SyncCustomer(ctx context.Context, customerSlug string, author *CommitAuthor) error
}

// PipelineVersioner reads a pipeline's history out of the mirrored box repo
// (git-centric gaps #2). Implemented by PipelineMirror.
type PipelineVersioner interface {
	ListVersions(ctx context.Context, customerSlug string, id PipelineID) ([]PipelineVersion, error)
	VersionAt(ctx context.Context, customerSlug string, id PipelineID, sha string) (*Pipeline, error)
}

// PipelineCredentialOwnershipStore flips a pipeline's credential ownership
// between the Console and the box repo (Phase 2B adopt-on-drift): marking a
// pipeline external stops the mirror re-rendering its .age file; the render
// row delete is the reclaim half — with the row gone, the next converge
// overwrites the foreign file with freshly provided Console credentials.
type PipelineCredentialOwnershipStore interface {
	SetPipelineCredentialsExternal(ctx context.Context, id PipelineID, external bool) error
	DeletePipelineCredentialRender(ctx context.Context, id PipelineID) error
}

// PipelineService orchestrates pipeline CRUD and run reporting.
type PipelineService struct {
	Workspaces Resolver
	Pipelines  PipelineRepository
	// Notifications, when set, raises an in-app notification on each completed
	// run reported by the worker. Optional (nil = no notifications).
	Notifications Notifier
	// Alerts, when set, sends an email on a failed run. Optional (nil = disabled).
	Alerts PipelineAlerter
	// Mirror, when set, writes definitions to the box's pipelines repo on the
	// request path: create/update/delete converge the repo synchronously, and
	// a failed commit hard-fails the save with the row compensated back. The
	// repo is the source of truth; the row is a cache over it.
	//
	// The legacy async dual-write (row-is-truth, best-effort mirror behind a
	// PIPELINES_GIT_PRIMARY lever) was retired with the pipelines-as-files
	// Phase 2.5 cleanup — with it went the dispatcher whose retry was
	// in-memory, and the "a restart drops pending mirror work" hole that a
	// durable outbox was once meant to close. A save now either commits or
	// fails in front of the user.
	Mirror PipelineMirrorer
	// Users resolves the acting user so mirrored commits carry them as git
	// author. Optional: nil keeps the plain platform attribution.
	Users UserReader
	// Versions surfaces the mirrored repo's per-pipeline history for the
	// Console (implemented by PipelineMirror). Optional: nil means version
	// history is unavailable (mirror not wired).
	Versions PipelineVersioner
	// Ownership, when set, lets a Console credential edit reclaim a pipeline
	// whose .age file went externally-managed (Phase 2B). Optional.
	Ownership PipelineCredentialOwnershipStore
	// GoogleOAuth resolves "Sign in with Google" grants into stored refresh
	// tokens on pipeline create/update. Nil disables the OAuth path: a
	// google_sheets pipeline can then only be created with a service account.
	GoogleOAuth GoogleOAuthGrantStore
	// OAuthClients resolves the CUSTOMER's own Google OAuth app, whose
	// client_id/client_secret are injected into google_sheets OAuth credentials
	// at serve time (GetEnabledPipelines) so the worker can refresh access
	// tokens. Per customer rather than one shared app because the pair travels
	// with the credential onto the customer's own machine. Nil = no injection.
	OAuthClients OAuthClientStore
	// Connections resolves workspace-level Connection references
	// (oauth.connection_id) into their stored refresh tokens at serve time,
	// and validates the reference on create/update. Nil = connection
	// references are rejected on save and left unresolved on serve.
	Connections ConnectionStore
	// StripPollCredentials removes source_credentials from the worker-facing
	// GetEnabledPipelines response (pipelines-as-files Phase 3 kill-switch,
	// env POLL_SOURCE_CREDENTIALS=off) — flip only once the fleet's workers
	// decrypt pipelines/<name>.credentials.age from their checkouts. Strips
	// row-backed pipelines ONLY: synthesized file_upload pipelines keep
	// their injected storage credentials (those are not source_credentials
	// rows and are never rendered as .age files).
	StripPollCredentials bool
	Logger               *slog.Logger
}

// CreatePipeline creates a new pipeline for the caller's ws.
func (s *PipelineService) CreatePipeline(ctx context.Context, callerID core.UserID, p *Pipeline) (*Pipeline, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}

	p.CustomerSlug = ws.Slug
	now := time.Now()
	p.CreatedAt = now
	p.UpdatedAt = now
	if p.WriteDisposition == "" {
		p.WriteDisposition = "append"
	}
	p.Enabled = true

	if err := ValidateSourceConfig(p.SourceType, p.SourceConfig); err != nil {
		return nil, err
	}
	if err := ValidateSourceCredentials(p.SourceType, p.SourceConfig, p.SourceCredentials); err != nil {
		return nil, err
	}
	if err := s.swapGoogleOAuthGrant(ctx, ws.Slug, p); err != nil {
		return nil, err
	}

	if err := s.Pipelines.CreatePipeline(ctx, p); err != nil {
		return nil, fmt.Errorf("create pipeline: %w", err)
	}

	if err := s.commitOrCompensate(ctx, callerID, ws.Slug, "create", p.ID, func(ctx context.Context) error {
		return s.Pipelines.DeletePipeline(ctx, p.ID)
	}); err != nil {
		return nil, err
	}
	return p, nil
}

// gitPrimarySyncTimeout bounds the synchronous request-path converge in
// git-first mode. Generous enough for a render + a few contents-API commits,
// short enough that a dead box yields a clear error, not a gateway timeout.
const gitPrimarySyncTimeout = 20 * time.Second

// commitMirror converges the box repo on the request path (git-first mode).
// Out-of-scope customers (shared substrate, no deposited credential) are a
// silent no-op inside SyncCustomer, exactly as in dispatcher mode.
func (s *PipelineService) commitMirror(ctx context.Context, callerID core.UserID, customerSlug string) error {
	if s.Mirror == nil {
		return nil
	}
	author := resolveCommitAuthor(ctx, s.Users, callerID)
	ctx, cancel := context.WithTimeout(ctx, gitPrimarySyncTimeout)
	defer cancel()
	if err := s.Mirror.SyncCustomer(ctx, customerSlug, author); err != nil {
		return fmt.Errorf("commit definitions to the box repo: %w", err)
	}
	return nil
}

// commitOrCompensate is the git-first save tail: converge the repo, and on
// failure roll the cache row back via compensate so the failed save leaves
// no state. A failed compensation is logged, not returned — the periodic
// converge heals a cache row that ran ahead of the repo.
func (s *PipelineService) commitOrCompensate(ctx context.Context, callerID core.UserID, customerSlug, op string, id PipelineID, compensate func(context.Context) error) error {
	err := s.commitMirror(ctx, callerID, customerSlug)
	if err == nil {
		return nil
	}
	if compErr := compensate(ctx); compErr != nil {
		s.Logger.ErrorContext(ctx, "git-first "+op+": failed to compensate cache row after mirror failure; converge will heal", "pipeline", id, "error", compErr)
	}
	return err
}

// swapGoogleOAuthGrant redeems a "Sign in with Google" grant referenced in a
// Google-backed pipeline's credentials, replacing {"oauth":{"grant_id":…}} with
// the stored {"oauth":{"refresh_token":…,"email":…}}. No-op for service-account
// or already-stored credentials. The grant is consumed (one-time use) and its
// tenant is checked against customerSlug.
//
// Serves google_sheets and duckdb/gdrive alike: both carry the credential
// under the same "oauth" member, so one Google sign-in feeds either.
func (s *PipelineService) swapGoogleOAuthGrant(ctx context.Context, customerSlug string, p *Pipeline) error {
	// A connection reference is stored as-is (resolved at serve/render time so
	// it follows the connection's lifecycle), but it must exist, belong to
	// this tenant, and carry the authorization this source actually needs —
	// otherwise the save would only defer the failure to a run.
	if connID, ok := googleConnectionRef(p.SourceType, p.SourceCredentials); ok {
		return s.checkGoogleConnection(ctx, customerSlug, p.SourceType, connID)
	}

	grantID, ok := googleGrantID(p.SourceType, p.SourceCredentials)
	if !ok {
		return nil
	}
	if s.GoogleOAuth == nil {
		return &ErrInvalidSourceCredentials{Field: "oauth", Msg: p.SourceType + ": Sign in with Google is not enabled on this server"}
	}
	grant, err := s.GoogleOAuth.ConsumeGoogleOAuthGrant(ctx, grantID, customerSlug)
	if err != nil {
		if errors.Is(err, ErrOAuthGrantNotFound) {
			return &ErrInvalidSourceCredentials{Field: "oauth", Msg: p.SourceType + ": the Google sign-in expired or was already used; please reconnect"}
		}
		return fmt.Errorf("consume oauth grant: %w", err)
	}
	stored, err := storedGoogleOAuthCreds(p.SourceType, p.SourceCredentials, grant.RefreshToken, grant.Email, grant.ClientID)
	if err != nil {
		return fmt.Errorf("build oauth credentials: %w", err)
	}
	p.SourceCredentials = stored
	return nil
}

// checkGoogleConnection validates a connection-referencing credential at save
// time: the connection exists, belongs to this tenant, and was granted the
// scope this source type needs.
//
// The scope check is the difference between a message on the screen the user
// is looking at and a 403 inside a scheduled run on the box, hours later, in a
// log they have no reason to open. It is also why the connection records what
// its consent granted at all — see Connection.HasGoogleScope for why an
// unrecorded scope set has to read as "unknown" and pass.
func (s *PipelineService) checkGoogleConnection(ctx context.Context, customerSlug, sourceType, connID string) error {
	if s.Connections == nil {
		return &ErrInvalidSourceCredentials{Field: "oauth", Msg: sourceType + ": connections are not enabled on this server"}
	}
	conn, err := s.Connections.GetConnection(ctx, customerSlug, connID)
	if err != nil {
		if errors.Is(err, ErrConnectionNotFound) {
			return &ErrInvalidSourceCredentials{Field: "oauth", Msg: sourceType + ": the referenced Google connection does not exist; reconnect in Integrations"}
		}
		return fmt.Errorf("resolve connection: %w", err)
	}
	if scope := googleScopeRequired(sourceType); scope != "" && !conn.HasGoogleScope(scope) {
		return &ErrInvalidSourceCredentials{Field: "oauth", Msg: sourceType + ": this Google account is not authorized for Google Drive; reconnect it and allow Drive access"}
	}
	return nil
}

// ListPipelines returns all pipelines for the caller's ws.
func (s *PipelineService) ListPipelines(ctx context.Context, callerID core.UserID) ([]Pipeline, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}

	pipelines, err := s.Pipelines.ListPipelinesByCustomer(ctx, ws.Slug)
	if err != nil {
		return nil, fmt.Errorf("list pipelines: %w", err)
	}

	return pipelines, nil
}

// GetPipeline returns a pipeline by ID, verifying ownership.
func (s *PipelineService) GetPipeline(ctx context.Context, callerID core.UserID, id PipelineID) (*Pipeline, []PipelineRun, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, nil, fmt.Errorf("get customer: %w", err)
	}

	pipeline, err := s.Pipelines.GetPipeline(ctx, id)
	if err != nil {
		return nil, nil, fmt.Errorf("get pipeline: %w", err)
	}
	if pipeline.CustomerSlug != ws.Slug {
		return nil, nil, ErrPipelineNotFound
	}

	runs, err := s.Pipelines.ListRecentRuns(ctx, id, 10)
	if err != nil {
		return nil, nil, fmt.Errorf("list recent runs: %w", err)
	}

	return pipeline, runs, nil
}

// UpdateOption tunes a pipeline save.
type UpdateOption func(*updateOptions)

type updateOptions struct{ clearCredentials bool }

// ClearCredentials makes the save DROP the pipeline's stored source
// credentials rather than preserve them.
//
// It exists because empty source credentials on an update already mean "keep
// existing" — the right default for a write-only field the editor cannot show
// back — which leaves no way at all to say "detach". Without an explicit
// signal a pipeline referencing a workspace Connection can never let go of
// it, so the connection's in-use guard can never be satisfied and the
// connection can never be deleted: the customer is stuck with an unusable
// pipeline AND an undeletable connection.
func ClearCredentials() UpdateOption {
	return func(o *updateOptions) { o.clearCredentials = true }
}

func newUpdateOptions(opts []UpdateOption) updateOptions {
	var o updateOptions
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// UpdatePipeline updates a pipeline, verifying ownership.
func (s *PipelineService) UpdatePipeline(ctx context.Context, callerID core.UserID, p *Pipeline, opts ...UpdateOption) (*Pipeline, error) {
	opt := newUpdateOptions(opts)
	if opt.clearCredentials && !isEmptyJSON(p.SourceCredentials) {
		return nil, &ErrInvalidSourceCredentials{Field: "source_credentials", Msg: "cannot clear and set source credentials in the same save"}
	}

	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}

	existing, err := s.Pipelines.GetPipeline(ctx, p.ID)
	if err != nil {
		return nil, fmt.Errorf("get pipeline: %w", err)
	}
	if existing.CustomerSlug != ws.Slug {
		return nil, ErrPipelineNotFound
	}

	p.CustomerSlug = ws.Slug
	p.UpdatedAt = time.Now()

	if err := ValidateSourceConfig(p.SourceType, p.SourceConfig); err != nil {
		return nil, err
	}
	credsProvided := !isEmptyJSON(p.SourceCredentials)
	if err := s.resolveUpdateCredentials(ctx, ws.Slug, p, existing, opt); err != nil {
		return nil, err
	}

	if err := s.Pipelines.UpdatePipeline(ctx, p); err != nil {
		return nil, fmt.Errorf("update pipeline: %w", err)
	}
	return s.finishUpdate(ctx, callerID, ws.Slug, p, existing, credsProvided || opt.clearCredentials)
}

// finishUpdate is the save tail after the cache row is written: reclaim an
// externally-managed .age file when the stored credential changed, then commit
// the repo on the request path.
//
// A clear counts as a change: without reclaiming, the box-owned .age file
// survives the converge (external files are kept, never written) and the
// detached pipeline would keep running against a token the customer believes
// they revoked.
func (s *PipelineService) finishUpdate(ctx context.Context, callerID core.UserID, customerSlug string, p, existing *Pipeline, credentialsTouched bool) (*Pipeline, error) {
	if credentialsTouched && existing.CredentialsExternal {
		s.reclaimCredentials(ctx, p.ID)
	}
	if err := s.commitOrCompensate(ctx, callerID, customerSlug, "update", p.ID, func(ctx context.Context) error {
		return s.Pipelines.UpdatePipeline(ctx, existing)
	}); err != nil {
		return nil, err
	}
	return p, nil
}

// reclaimCredentials returns an externally-managed .age file to Console
// ownership after the user provided fresh credentials: the external flag is
// cleared and the render row deleted, so the next converge re-renders (and
// overwrites) the foreign file. Best-effort — a failed reclaim leaves the
// file external and the next credential edit retries.
func (s *PipelineService) reclaimCredentials(ctx context.Context, id PipelineID) {
	if s.Ownership == nil {
		return
	}
	if err := s.Ownership.SetPipelineCredentialsExternal(ctx, id, false); err != nil {
		s.Logger.WarnContext(ctx, "reclaim credentials: clear external flag", "pipeline", id, "err", err)
		return
	}
	if err := s.Ownership.DeletePipelineCredentialRender(ctx, id); err != nil {
		s.Logger.WarnContext(ctx, "reclaim credentials: drop render row", "pipeline", id, "err", err)
	}
}

// resolveUpdateCredentials validates newly provided source credentials (and
// redeems a Google sign-in grant) or, when the update carries none, preserves
// the existing stored credentials — unless the caller explicitly asked to
// clear them, the only way to detach a pipeline from a workspace Connection.
//
// A cleared pipeline is left credential-less on purpose: its runs then fail
// on missing credentials, which is honest and recoverable, whereas keeping a
// reference the customer asked to drop is neither.
func (s *PipelineService) resolveUpdateCredentials(ctx context.Context, customerSlug string, p, existing *Pipeline, opt updateOptions) error {
	if opt.clearCredentials {
		p.SourceCredentials = nil
		return nil
	}
	if isEmptyJSON(p.SourceCredentials) {
		p.SourceCredentials = existing.SourceCredentials
		return nil
	}
	if err := ValidateSourceCredentials(p.SourceType, p.SourceConfig, p.SourceCredentials); err != nil {
		return err
	}
	return s.swapGoogleOAuthGrant(ctx, customerSlug, p)
}

// DeletePipeline deletes a pipeline, verifying ownership.
func (s *PipelineService) DeletePipeline(ctx context.Context, callerID core.UserID, id PipelineID) error {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return fmt.Errorf("get customer: %w", err)
	}

	existing, err := s.Pipelines.GetPipeline(ctx, id)
	if err != nil {
		return fmt.Errorf("get pipeline: %w", err)
	}
	if existing.CustomerSlug != ws.Slug {
		return ErrPipelineNotFound
	}

	if err := s.Pipelines.DeletePipeline(ctx, id); err != nil {
		return fmt.Errorf("delete pipeline: %w", err)
	}

	// Compensation re-inserts the definition under a new row id; the run
	// history is already gone (FK cascade) — same loss as a successful
	// delete, but the definition survives, matching the repo state the
	// failed commit left behind.
	return s.commitOrCompensate(ctx, callerID, ws.Slug, "delete", id, func(ctx context.Context) error {
		return s.Pipelines.CreatePipeline(ctx, existing)
	})
}

// ListPipelineVersions returns the newest-first history of a pipeline's
// rendered definition file in the box repo, verifying ownership.
func (s *PipelineService) ListPipelineVersions(ctx context.Context, callerID core.UserID, id PipelineID) ([]PipelineVersion, error) {
	if s.Versions == nil {
		return nil, ErrBoxRepoUnavailable
	}
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}
	// Tenancy is enforced inside: the id must resolve within the caller's
	// own pipeline set (a foreign id is ErrPipelineNotFound).
	return s.Versions.ListVersions(ctx, ws.Slug, id)
}

// RestorePipelineVersion applies the state recorded at sha through the
// normal update path — validation, Postgres save (the editor truth), mirror
// re-render as a new forward commit authored by the caller. Never a git
// revert, and credentials are untouched (never part of the rendered file).
func (s *PipelineService) RestorePipelineVersion(ctx context.Context, callerID core.UserID, id PipelineID, sha string) (*Pipeline, error) {
	if s.Versions == nil {
		return nil, ErrBoxRepoUnavailable
	}
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}
	current, err := s.Pipelines.GetPipeline(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get pipeline: %w", err)
	}
	if current.CustomerSlug != ws.Slug {
		return nil, ErrPipelineNotFound
	}
	restored, err := s.Versions.VersionAt(ctx, ws.Slug, id, sha)
	if err != nil {
		return nil, err
	}
	// A restore must not change a pipeline's source type. file_upload
	// pipelines are rendered to the box as their rewritten "filesystem" form
	// (so the worker can load them), so parsing that history back would yield
	// a filesystem pipeline — restoring it would overwrite the file_upload row
	// with a credential-less filesystem source pointed at the upload prefix.
	// The uploaded-files history is managed through the file-drop UI, not git.
	if restored.SourceType != current.SourceType {
		return nil, &ErrInvalidSourceConfig{Field: "source_type", Msg: "this version has a different source type and cannot be restored"}
	}
	return s.UpdatePipeline(ctx, callerID, restored)
}

// TriggerPipeline queues a "pending" run for a pipeline, verifying ownership.
// It is idempotent: if a run is already queued, that run is returned rather
// than stacking a duplicate the worker would later run twice. Works on paused
// (disabled) pipelines too — GetEnabledPipelines surfaces a pending run so the
// worker executes it once.
func (s *PipelineService) TriggerPipeline(ctx context.Context, callerID core.UserID, id PipelineID) (*PipelineRun, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}

	existing, err := s.Pipelines.GetPipeline(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get pipeline: %w", err)
	}
	if existing.CustomerSlug != ws.Slug {
		return nil, ErrPipelineNotFound
	}

	// Idempotency: a run already waiting to be picked up satisfies the trigger.
	if pending, err := s.Pipelines.GetPendingRun(ctx, id); err != nil {
		return nil, fmt.Errorf("get pending run: %w", err)
	} else if pending != nil {
		return pending, nil
	}

	now := time.Now()
	run := &PipelineRun{
		PipelineID: id,
		Status:     "pending",
		CreatedAt:  now,
	}

	if err := s.Pipelines.CreatePipelineRun(ctx, run); err != nil {
		return nil, fmt.Errorf("create pipeline run: %w", err)
	}

	return run, nil
}

// GetEnabledPipelines returns all enabled pipelines for a customer (worker-facing).
//
// file_upload pipelines are served to the worker as plain "filesystem"
// pipelines with the customer's own storage credentials injected — the
// customer-facing type never carries credentials (see domain/filedrop.go).
// file_upload pipelines with no files yet, or belonging to a customer whose
// storage is not provisioned, are omitted: there is nothing to load.
func (s *PipelineService) GetEnabledPipelines(ctx context.Context, customerSlug string) ([]Pipeline, error) {
	pipelines, err := s.Pipelines.GetEnabledPipelines(ctx, customerSlug)
	if err != nil {
		return nil, fmt.Errorf("get enabled pipelines: %w", err)
	}

	var storage *core.S3Config // resolved once, only if a file_upload pipeline exists
	oauth := newOAuthClientResolver(s.OAuthClients, s.Connections, customerSlug)
	out := pipelines[:0]
	for _, p := range pipelines {
		if p.SourceType != SourceTypeFileUpload {
			if s.StripPollCredentials {
				// Phase 3 kill-switch: the worker reads these from its
				// age-encrypted checkout files instead (OAuth client
				// included — the mirror injects it before encrypting).
				p.SourceCredentials = nil
			} else if injected, ok := oauth.inject(ctx, &p); ok {
				p.SourceCredentials = injected
			}
			out = append(out, p)
			continue
		}
		if storage == nil {
			s3, err := s.resolveUploadStorage(ctx, customerSlug)
			if err != nil {
				return nil, err
			}
			storage = &s3
		}
		if storage.Bucket == "" {
			continue
		}
		s.appendFileUploadPipeline(ctx, p, *storage, &out)
	}
	return out, nil
}

// oauthClientResolver looks a customer's own Google OAuth app up at most once
// per batch and injects it into each Google-backed credential (google_sheets,
// and duckdb with the gdrive extension) — first resolving a workspace
// Connection reference (oauth.connection_id) into the connection's refresh
// token, so the served credential shape is identical whether the pipeline
// embeds its token or references a connection. The lookup is lazy because most
// batches contain no Google pipeline at all, and cached because both callers
// iterate every pipeline of one customer.
//
// Shared by the worker poll (GetEnabledPipelines) and the .age render
// (PipelineMirror): the two must agree byte-for-byte, since the render
// fingerprint is taken over the injected plaintext.
type oauthClientResolver struct {
	clients      OAuthClientStore
	connections  ConnectionStore
	customerSlug string
	resolved     bool
	clientID     string
	clientSecret string
	// connCache caches per-connection resolution for the batch; nil value =
	// lookup failed (do not retry within the batch).
	connCache map[string]*googleConnectionCredentials
}

func newOAuthClientResolver(clients OAuthClientStore, connections ConnectionStore, customerSlug string) *oauthClientResolver {
	return &oauthClientResolver{clients: clients, connections: connections, customerSlug: customerSlug}
}

// resolveConnectionCredential expands a connection-referencing credential into
// the embedded-token shape ({"oauth":{refresh_token,email,client_id}}), or
// returns (nil, false) when the credential does not reference a connection or
// the connection cannot be resolved. The connection_id is deliberately absent
// from the output: the worker-facing shape must be byte-identical to an
// embedded credential's.
//
// Only the oauth member is replaced. Sibling fields are preserved, because a
// duckdb credential carries attach_params and its own secret keys alongside
// it — rebuilding the envelope from the connection alone would serve a Drive
// pipeline with the rest of its credentials silently stripped.
func (r *oauthClientResolver) resolveConnectionCredential(ctx context.Context, sourceType string, raw json.RawMessage) (json.RawMessage, bool) {
	connID, ok := googleConnectionRef(sourceType, raw)
	if !ok || r.connections == nil {
		return nil, false
	}
	if r.connCache == nil {
		r.connCache = make(map[string]*googleConnectionCredentials)
	}
	gc, cached := r.connCache[connID]
	if !cached {
		gc = nil
		if conn, err := r.connections.GetConnection(ctx, r.customerSlug, connID); err == nil {
			gc, _ = conn.googleCredentials()
		}
		r.connCache[connID] = gc
	}
	if gc == nil {
		return nil, false
	}
	resolved, err := storedGoogleOAuthCreds(sourceType, raw, gc.RefreshToken, gc.Email, gc.ClientID)
	if err != nil {
		return nil, false
	}
	return resolved, true
}

// load fetches the customer's app once. A missing app is cached as the empty
// pair, which inject treats as "do not inject" — the same outcome as an
// unconfigured store, and correct: without their app there is nothing to
// refresh with.
func (r *oauthClientResolver) load(ctx context.Context) {
	if r.resolved {
		return
	}
	r.resolved = true
	if r.clients == nil {
		return
	}
	cc, err := r.clients.GetOAuthClient(ctx, r.customerSlug, OAuthProviderGoogle)
	if err != nil {
		return
	}
	r.clientID, r.clientSecret = cc.ClientID, cc.ClientSecret
}

// inject returns the credential with a referenced connection expanded and the
// customer's client pair added, or (nil, false) to leave it untouched.
//
// A connection-resolved credential is returned even when the client pair
// cannot be injected (no app connected, or a client-id mismatch): rendering
// the refresh token without its pair fails the run on a missing client_id —
// the same honest signal an embedded credential gives — whereas returning the
// raw connection_id would fail on a missing refresh_token, one step further
// from the cause.
func (r *oauthClientResolver) inject(ctx context.Context, p *Pipeline) (json.RawMessage, bool) {
	if !isGoogleOAuthSourceType(p.SourceType) {
		return nil, false
	}
	raw := p.SourceCredentials
	resolvedFromConnection := false
	if resolved, ok := r.resolveConnectionCredential(ctx, p.SourceType, raw); ok {
		raw = resolved
		resolvedFromConnection = true
	}
	r.load(ctx)
	if r.clientID == "" || r.clientSecret == "" {
		if resolvedFromConnection {
			return serveGoogleOAuthCredential(p.SourceType, raw), true
		}
		return nil, false
	}
	if injected, ok := injectGoogleOAuthClient(p.SourceType, raw, r.clientID, r.clientSecret); ok {
		return injected, true
	}
	if resolvedFromConnection {
		return serveGoogleOAuthCredential(p.SourceType, raw), true
	}
	return nil, false
}

// staleClientID reports whether a stored OAuth credential names a client other
// than the customer's current one — i.e. the connection is dead and needs a
// reconnect. Connection references are resolved first, so a pipeline hanging
// off a stale workspace connection is reported the same way as one with a
// stale embedded token. Returns false when there is nothing to compare
// against, so a customer who has not connected an app yet is not nagged.
func (r *oauthClientResolver) staleClientID(ctx context.Context, p *Pipeline) bool {
	raw := p.SourceCredentials
	if resolved, ok := r.resolveConnectionCredential(ctx, p.SourceType, raw); ok {
		raw = resolved
	}
	storedID, isOAuth := googleOAuthClientID(p.SourceType, raw)
	if !isOAuth {
		return false
	}
	r.load(ctx)
	if r.clientID == "" {
		return false
	}
	return storedID != r.clientID
}

// resolveUploadStorage loads the customer's upload S3 config. A provisioning
// gap is logged and returned as a zero core.S3Config (empty Bucket) rather than an
// error, so file_upload pipelines are skipped without failing the whole call.
func (s *PipelineService) resolveUploadStorage(ctx context.Context, customerSlug string) (core.S3Config, error) {
	ws, err := s.Workspaces.GetWorkspace(ctx, customerSlug)
	if err != nil {
		return core.S3Config{}, fmt.Errorf("get customer: %w", err)
	}
	s3, err := uploadStorage(ws)
	if err != nil {
		if s.Logger != nil {
			s.Logger.WarnContext(ctx, "file_upload pipelines skipped: storage not provisioned", "customer", customerSlug)
		}
		return core.S3Config{}, nil
	}
	return s3, nil
}

// appendFileUploadPipeline resolves a file_upload pipeline and appends it to out
// when it has files to load. Resolution errors are logged and swallowed.
func (s *PipelineService) appendFileUploadPipeline(ctx context.Context, p Pipeline, storage core.S3Config, out *[]Pipeline) {
	ok, err := resolveFileUploadPipeline(&p, storage)
	if err != nil {
		if s.Logger != nil {
			s.Logger.WarnContext(ctx, "file_upload pipeline skipped", "pipeline_id", p.ID, "err", err)
		}
		return
	}
	if ok {
		*out = append(*out, p)
	}
}

// ReportPipelineRun records a run result from the dlt-worker.
//
// A report carrying an id is an upsert on that id: it updates the existing
// row, and creates the row under that same id when there is none. That
// second half is what lets one run keep one identity. On a box the worker
// records every run in this database itself and reports the id it used, so
// an id it has never seen means the local write is the one that did not
// happen — not that a new run should be invented. An id-less report (a
// worker predating the single-identity contract) still inserts, with the
// column default minting the id.
//
// callerSlug, when non-empty, is the tenant bound to the caller's service
// token: the reported pipeline must belong to that ws. Empty skips the
// check — only possible while the internal mux runs in log mode. The
// run-update path is covered too: UpdatePipelineRun matches on run ID AND
// pipeline ID, so a run can't be touched through someone else's pipeline.
// The create-on-missing path cannot reach another tenant's run either: the
// id is a primary key, so colliding with an existing row fails the insert
// rather than overwriting it.
func (s *PipelineService) ReportPipelineRun(ctx context.Context, callerSlug string, run *PipelineRun) error {
	if callerSlug != "" {
		pipeline, err := s.Pipelines.GetPipeline(ctx, run.PipelineID)
		if err != nil {
			return fmt.Errorf("get pipeline: %w", err)
		}
		if pipeline.CustomerSlug != callerSlug {
			return ErrPipelineNotFound
		}
	}
	if run.ID != "" {
		err := s.Pipelines.UpdatePipelineRun(ctx, run)
		if errors.Is(err, ErrPipelineRunNotFound) {
			err = s.Pipelines.CreatePipelineRun(ctx, run)
		}
		if err != nil {
			return fmt.Errorf("record pipeline run: %w", err)
		}
	} else {
		if err := s.Pipelines.CreatePipelineRun(ctx, run); err != nil {
			return fmt.Errorf("create pipeline run: %w", err)
		}
	}
	recordPipelineRun(ctx, run)
	s.notifyRun(ctx, run)
	return nil
}

// recordPipelineRun turns the worker's report into the plane's run metrics.
//
// No span of its own: this already runs inside the internal RPC's span, and a
// worker reporting a result is one database write — the interesting part is
// the aggregate, not any individual call. The status goes on the current span
// too, so a trace of the report shows what was reported without opening the
// row.
//
// Only terminal statuses are counted. "running" is a progress report, and
// counting it would double every run and put a zero-duration sample in the
// histogram for work that has barely started.
func recordPipelineRun(ctx context.Context, run *PipelineRun) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(
		attrPipelineID.String(string(run.PipelineID)),
		attrRunStatus.String(run.Status),
	)
	if run.Status != "success" && run.Status != "failed" {
		return
	}

	status := metric.WithAttributes(attrRunStatus.String(run.Status))
	pipelineRuns.Add(ctx, 1, status)
	if run.RowsLoaded > 0 {
		pipelineRunRows.Add(ctx, run.RowsLoaded, status)
	}
	if seconds, ok := runDurationSeconds(run.StartedAt, run.CompletedAt); ok {
		pipelineRunDuration.Record(ctx, seconds, status)
	}
}

// notifyRun raises a best-effort in-app notification for a completed run and,
// for failures, an out-of-band email alert. It never fails the report:
// notification problems are logged, not propagated.
func (s *PipelineService) notifyRun(ctx context.Context, run *PipelineRun) {
	if !s.shouldNotifyRun(run) {
		return
	}
	pipeline, err := s.Pipelines.GetPipeline(ctx, run.PipelineID)
	if err != nil {
		if s.Logger != nil {
			s.Logger.WarnContext(ctx, "notify run: get pipeline", "pipeline_id", run.PipelineID, "err", err)
		}
		return
	}

	if s.Notifications != nil {
		s.publishRunNotification(ctx, pipeline, run)
	}
	s.alertRunFailure(ctx, pipeline, run)
}

// shouldNotifyRun reports whether a run warrants any notification: a sink is
// configured and the run reached a terminal status.
func (s *PipelineService) shouldNotifyRun(run *PipelineRun) bool {
	if s.Notifications == nil && s.Alerts == nil {
		return false
	}
	return run.Status == "success" || run.Status == "failed"
}

// alertRunFailure sends the best-effort out-of-band email alert for a failed run.
func (s *PipelineService) alertRunFailure(ctx context.Context, pipeline *Pipeline, run *PipelineRun) {
	if s.Alerts == nil || run.Status != "failed" {
		return
	}
	if err := s.Alerts.AlertPipelineFailure(ctx, pipeline.CustomerSlug, pipeline.Name, run.ErrorMessage); err != nil && s.Logger != nil {
		s.Logger.WarnContext(ctx, "notify run: email alert", "pipeline_id", run.PipelineID, "err", err)
	}
}

// publishRunNotification raises the in-app notification for a completed run.
func (s *PipelineService) publishRunNotification(ctx context.Context, pipeline *Pipeline, run *PipelineRun) {
	title := fmt.Sprintf("Pipeline %q succeeded", pipeline.Name)
	body := ""
	if run.Status == "failed" {
		title = fmt.Sprintf("Pipeline %q failed", pipeline.Name)
		body = run.ErrorMessage
	} else if run.RowsLoaded > 0 {
		body = fmt.Sprintf("Loaded %d rows.", run.RowsLoaded)
	}

	// Deep-link to the specific pipeline so the Console opens its run history
	// (and the failing run's full error) on arrival, rather than the bare list.
	n := Notification{
		CustomerSlug: pipeline.CustomerSlug,
		Type:         "pipeline_run",
		Title:        title,
		Body:         body,
		Link:         "/pipelines?pipeline=" + string(run.PipelineID),
	}
	if err := s.Notifications.Notify(ctx, n); err != nil && s.Logger != nil {
		s.Logger.WarnContext(ctx, "notify run: publish", "pipeline_id", run.PipelineID, "err", err)
	}
}

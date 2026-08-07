package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

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
	// Mirror, when set, dual-writes definitions to the box's pipelines repo
	// after each successful create/update/delete. Best-effort by default: a
	// mirror failure never fails the save. With GitPrimary the mirror commit
	// joins the request path instead (see below).
	Mirror PipelineMirrorer
	// GitPrimary flips the source of truth to the box's pipelines repo
	// (control-plane/workspace-split Phase 2A, env PIPELINES_GIT_PRIMARY=on):
	// create/update/delete converge the repo synchronously, and a failed
	// commit hard-fails the save with the central row compensated back — the
	// row is demoted to a cache over the repo. Off = legacy async dual-write.
	GitPrimary bool
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
	// StripPollCredentials removes source_credentials from the worker-facing
	// GetEnabledPipelines response (pipelines-as-files Phase 3 kill-switch,
	// env POLL_SOURCE_CREDENTIALS=off) — flip only once the fleet's workers
	// decrypt pipelines/<name>.credentials.age from their checkouts. Strips
	// row-backed pipelines ONLY: synthesized file_upload pipelines keep
	// their injected storage credentials (those are not source_credentials
	// rows and are never rendered as .age files).
	StripPollCredentials bool
	Logger               *slog.Logger

	// mirrorDispatcher runs box-repo mirror syncs off the request path,
	// serialized and coalesced per customer with bounded retry. Created lazily
	// on first save so a zero-value PipelineService needs no constructor.
	mirrorOnce       sync.Once
	mirrorDispatcher *pipelineMirrorDispatcher
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
	if err := s.swapGoogleSheetsGrant(ctx, ws.Slug, p); err != nil {
		return nil, err
	}

	if err := s.Pipelines.CreatePipeline(ctx, p); err != nil {
		return nil, fmt.Errorf("create pipeline: %w", err)
	}

	if s.GitPrimary {
		if err := s.commitOrCompensate(ctx, callerID, ws.Slug, "create", p.ID, func(ctx context.Context) error {
			return s.Pipelines.DeletePipeline(ctx, p.ID)
		}); err != nil {
			return nil, err
		}
		return p, nil
	}
	s.mirrorPipelines(ctx, callerID, ws.Slug)
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

// mirrorPipelines hands the customer's definitions off to be dual-written to
// the box repo (pipelines-as-files Phase 1), best-effort and OFF the request
// path: a save must never block on — let alone 504 on — a live round-trip to
// an unreachable box, so this returns immediately once Postgres (the source of
// truth) is written. The dispatcher serializes and coalesces converges per
// customer with bounded retry (see pipelineMirrorDispatcher); a failed sync is
// retried, and overlapping saves can never leave the box on a stale snapshot.
func (s *PipelineService) mirrorPipelines(ctx context.Context, callerID core.UserID, customerSlug string) {
	if s.Mirror == nil {
		return
	}
	s.mirrorOnce.Do(func() {
		s.mirrorDispatcher = newPipelineMirrorDispatcher(s.Mirror, s.Users, s.Logger)
	})
	s.mirrorDispatcher.enqueue(ctx, callerID, customerSlug)
}

// swapGoogleSheetsGrant redeems a "Sign in with Google" grant referenced in a
// google_sheets pipeline's credentials, replacing {"oauth":{"grant_id":…}} with
// the stored {"oauth":{"refresh_token":…,"email":…}}. No-op for service-account
// or already-stored credentials. The grant is consumed (one-time use) and its
// tenant is checked against customerSlug.
func (s *PipelineService) swapGoogleSheetsGrant(ctx context.Context, customerSlug string, p *Pipeline) error {
	grantID, ok := googleSheetsGrantID(p.SourceType, p.SourceCredentials)
	if !ok {
		return nil
	}
	if s.GoogleOAuth == nil {
		return &ErrInvalidSourceCredentials{Field: "oauth", Msg: "google_sheets: Sign in with Google is not enabled on this server"}
	}
	grant, err := s.GoogleOAuth.ConsumeGoogleOAuthGrant(ctx, grantID, customerSlug)
	if err != nil {
		if errors.Is(err, ErrOAuthGrantNotFound) {
			return &ErrInvalidSourceCredentials{Field: "oauth", Msg: "google_sheets: the Google sign-in expired or was already used; please reconnect"}
		}
		return fmt.Errorf("consume oauth grant: %w", err)
	}
	stored, err := googleSheetsStoredOAuthCreds(grant.RefreshToken, grant.Email, grant.ClientID)
	if err != nil {
		return fmt.Errorf("build oauth credentials: %w", err)
	}
	p.SourceCredentials = stored
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

// UpdatePipeline updates a pipeline, verifying ownership.
func (s *PipelineService) UpdatePipeline(ctx context.Context, callerID core.UserID, p *Pipeline) (*Pipeline, error) {
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
	if err := s.resolveUpdateCredentials(ctx, ws.Slug, p, existing); err != nil {
		return nil, err
	}

	if err := s.Pipelines.UpdatePipeline(ctx, p); err != nil {
		return nil, fmt.Errorf("update pipeline: %w", err)
	}
	return s.finishUpdate(ctx, callerID, ws.Slug, p, existing, credsProvided)
}

// finishUpdate is the save tail after the cache row is written: reclaim an
// externally-managed .age file when fresh credentials arrived, then mirror
// (synchronously under GitPrimary, async-dispatched otherwise).
func (s *PipelineService) finishUpdate(ctx context.Context, callerID core.UserID, customerSlug string, p, existing *Pipeline, credsProvided bool) (*Pipeline, error) {
	if credsProvided && existing.CredentialsExternal {
		s.reclaimCredentials(ctx, p.ID)
	}
	if s.GitPrimary {
		if err := s.commitOrCompensate(ctx, callerID, customerSlug, "update", p.ID, func(ctx context.Context) error {
			return s.Pipelines.UpdatePipeline(ctx, existing)
		}); err != nil {
			return nil, err
		}
		return p, nil
	}
	s.mirrorPipelines(ctx, callerID, customerSlug)
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
// redeems a Google sign-in grant) or, when the update carries none,
// preserves the existing stored credentials.
func (s *PipelineService) resolveUpdateCredentials(ctx context.Context, customerSlug string, p, existing *Pipeline) error {
	if isEmptyJSON(p.SourceCredentials) {
		p.SourceCredentials = existing.SourceCredentials
		return nil
	}
	if err := ValidateSourceCredentials(p.SourceType, p.SourceConfig, p.SourceCredentials); err != nil {
		return err
	}
	return s.swapGoogleSheetsGrant(ctx, customerSlug, p)
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

	if s.GitPrimary {
		// Compensation re-inserts the definition under a new row id; the run
		// history is already gone (FK cascade) — same loss as a successful
		// delete, but the definition survives, matching the repo state the
		// failed commit left behind.
		return s.commitOrCompensate(ctx, callerID, ws.Slug, "delete", id, func(ctx context.Context) error {
			return s.Pipelines.CreatePipeline(ctx, existing)
		})
	}
	s.mirrorPipelines(ctx, callerID, ws.Slug)
	return nil
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
	oauth := newOAuthClientResolver(s.OAuthClients, customerSlug)
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
// per batch and injects it into each OAuth google_sheets credential. The lookup
// is lazy because most batches contain no Sheets pipeline at all, and cached
// because both callers iterate every pipeline of one customer.
//
// Shared by the worker poll (GetEnabledPipelines) and the .age render
// (PipelineMirror): the two must agree byte-for-byte, since the render
// fingerprint is taken over the injected plaintext.
type oauthClientResolver struct {
	clients      OAuthClientStore
	customerSlug string
	resolved     bool
	clientID     string
	clientSecret string
}

func newOAuthClientResolver(clients OAuthClientStore, customerSlug string) *oauthClientResolver {
	return &oauthClientResolver{clients: clients, customerSlug: customerSlug}
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

// inject returns the credential with the customer's client pair added, or
// (nil, false) to leave it untouched.
func (r *oauthClientResolver) inject(ctx context.Context, p *Pipeline) (json.RawMessage, bool) {
	if p.SourceType != "google_sheets" {
		return nil, false
	}
	r.load(ctx)
	if r.clientID == "" || r.clientSecret == "" {
		return nil, false
	}
	return injectGoogleSheetsOAuthClient(p.SourceType, p.SourceCredentials, r.clientID, r.clientSecret)
}

// staleClientID reports whether a stored OAuth credential names a client other
// than the customer's current one — i.e. the connection is dead and needs a
// reconnect. Returns false when there is nothing to compare against, so a
// customer who has not connected an app yet is not nagged about it.
func (r *oauthClientResolver) staleClientID(ctx context.Context, p *Pipeline) bool {
	storedID, isOAuth := googleSheetsOAuthClientID(p.SourceType, p.SourceCredentials)
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
	s.notifyRun(ctx, run)
	return nil
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

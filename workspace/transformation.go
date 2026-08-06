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

// TransformationID is a strongly-typed identifier for transformations (UUID).
type TransformationID string

func (id TransformationID) String() string { return string(id) }

// Transformation represents a git-backed dbt project execution config. The
// dbt project itself is code in a git repo; this record only says which
// repo/ref to run and when. RepoURL empty
// means the box-hosted Gitea repo, whose URL and read token live on the box
// only — central never stores box credentials.
type Transformation struct {
	ID           TransformationID
	CustomerSlug string
	Name         string
	RepoURL      string
	RepoRef      string
	// GitCredentials is a JSON object like {"username","token"} for a
	// connected repo. Encrypted at rest, only ever sent to the box worker.
	GitCredentials json.RawMessage
	Schedule       string
	// TriggerAfterPipelineID runs the transformation after that pipeline
	// reports a successful run. The chaining happens inside the dlt-worker,
	// which executes both.
	TriggerAfterPipelineID PipelineID
	DBTSelector            string
	Enabled                bool
	CreatedAt              time.Time
	UpdatedAt              time.Time

	// Transient fields set by GetEnabledTransformations.
	TriggerNow   bool
	PendingRunID string
	LastRunAt    *time.Time // Last successful run time (nil if never run)

	// Transient fields set by ListTransformationsByCustomer /
	// GetTransformation: the most recent run regardless of status.
	LastRunTime   *time.Time
	LastRunStatus string
}

// TransformationRun records the result of a single dbt execution, pinned to
// the commit SHA the repo ref resolved to.
type TransformationRun struct {
	ID               string
	TransformationID TransformationID
	Status           string // "pending", "running", "success", "failed"
	StartedAt        *time.Time
	CompletedAt      *time.Time
	CommitSHA        string
	ModelsTotal      int32
	ModelsFailed     int32
	TestsTotal       int32
	TestsFailed      int32
	// ModelResults is a JSON array of per-node results:
	// [{"name","resource_type","status","execution_time","message"}, ...]
	ModelResults json.RawMessage
	ErrorMessage string
	CreatedAt    time.Time
}

// TransformationRepository persists transformation configurations and runs.
type TransformationRepository interface {
	CreateTransformation(ctx context.Context, t *Transformation) error
	GetTransformation(ctx context.Context, id TransformationID) (*Transformation, error)
	ListTransformationsByCustomer(ctx context.Context, customerSlug string) ([]Transformation, error)
	UpdateTransformation(ctx context.Context, t *Transformation) error
	DeleteTransformation(ctx context.Context, id TransformationID) error

	// GetEnabledTransformations returns all enabled transformations for a
	// customer (worker-facing).
	GetEnabledTransformations(ctx context.Context, customerSlug string) ([]Transformation, error)

	CreateTransformationRun(ctx context.Context, run *TransformationRun) error
	UpdateTransformationRun(ctx context.Context, run *TransformationRun) error
	ListRecentTransformationRuns(ctx context.Context, id TransformationID, limit int) ([]TransformationRun, error)
}

// PipelineReader is the narrow pipeline lookup the transformation service
// needs to validate trigger_after_pipeline_id tenant ownership.
type PipelineReader interface {
	GetPipeline(ctx context.Context, id PipelineID) (*Pipeline, error)
}

// TransformationMirrorer mirrors a customer's transformation execution
// configs into the box's transformations repo (control-plane/workspace-split
// Phase 2F). Implemented by TransformationMirror. author attributes the
// resulting commits to the acting Console user; nil keeps the platform
// attribution.
type TransformationMirrorer interface {
	SyncCustomer(ctx context.Context, customerSlug string, author *CommitAuthor) error
}

// TransformationService orchestrates transformation CRUD and run reporting.
type TransformationService struct {
	Workspaces      Resolver
	Transformations TransformationRepository
	Pipelines       PipelineReader
	// Notifications, when set, raises an in-app notification on each
	// completed run reported by the worker. Optional (nil = no notifications).
	Notifications Notifier
	// Mirror, when set, dual-writes execution configs to the box's
	// transformations repo after each successful create/update/delete.
	// Best-effort by default: a mirror failure never fails the save. With
	// GitPrimary the mirror commit joins the request path instead.
	Mirror TransformationMirrorer
	// GitPrimary flips the source of truth to the box's transformations repo
	// (Phase 2F, env TRANSFORMATIONS_GIT_PRIMARY=on): create/update/delete
	// converge the repo synchronously, and a failed commit hard-fails the
	// save with the central row compensated back. Off = async dual-write.
	GitPrimary bool
	// Users resolves the acting user so mirrored commits carry them as git
	// author. Optional: nil keeps the plain platform attribution.
	Users  UserReader
	Logger *slog.Logger

	// mirrorDispatcher runs box-repo mirror syncs off the request path,
	// serialized and coalesced per customer with bounded retry (shared
	// implementation with the pipeline dispatcher). Created lazily on first
	// save so a zero-value TransformationService needs no constructor.
	mirrorOnce       sync.Once
	mirrorDispatcher *pipelineMirrorDispatcher
}

// commitMirror converges the box repo on the request path (git-first mode).
// Out-of-scope customers are a silent no-op inside SyncCustomer.
func (s *TransformationService) commitMirror(ctx context.Context, callerID core.UserID, customerSlug string) error {
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
func (s *TransformationService) commitOrCompensate(ctx context.Context, callerID core.UserID, customerSlug, op string, id TransformationID, compensate func(context.Context) error) error {
	err := s.commitMirror(ctx, callerID, customerSlug)
	if err == nil {
		return nil
	}
	if compErr := compensate(ctx); compErr != nil && s.Logger != nil {
		s.Logger.ErrorContext(ctx, "git-first "+op+": failed to compensate cache row after mirror failure; converge will heal", "transformation", id, "error", compErr)
	}
	return err
}

// mirrorTransformations hands the customer's execution configs off to be
// dual-written to the box repo, best-effort and OFF the request path (same
// contract as PipelineService.mirrorPipelines).
func (s *TransformationService) mirrorTransformations(ctx context.Context, callerID core.UserID, customerSlug string) {
	if s.Mirror == nil {
		return
	}
	s.mirrorOnce.Do(func() {
		s.mirrorDispatcher = newPipelineMirrorDispatcher(s.Mirror, s.Users, s.Logger)
	})
	s.mirrorDispatcher.enqueue(ctx, callerID, customerSlug)
}

// validateTrigger verifies that a non-empty trigger pipeline belongs to the
// caller's ws, so a transformation can't be chained onto another
// tenant's pipeline.
func (s *TransformationService) validateTrigger(ctx context.Context, customerSlug string, id PipelineID) error {
	if id == "" {
		return nil
	}
	pipeline, err := s.Pipelines.GetPipeline(ctx, id)
	if err != nil {
		return fmt.Errorf("get trigger pipeline: %w", err)
	}
	if pipeline.CustomerSlug != customerSlug {
		return ErrPipelineNotFound
	}
	return nil
}

// CreateTransformation creates a new transformation for the caller's ws.
func (s *TransformationService) CreateTransformation(ctx context.Context, callerID core.UserID, t *Transformation) (*Transformation, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}

	t.CustomerSlug = ws.Slug
	now := time.Now()
	t.CreatedAt = now
	t.UpdatedAt = now
	if t.RepoRef == "" {
		t.RepoRef = "main"
	}
	t.Enabled = true

	if err := s.validateTrigger(ctx, ws.Slug, t.TriggerAfterPipelineID); err != nil {
		return nil, err
	}

	if err := s.Transformations.CreateTransformation(ctx, t); err != nil {
		return nil, fmt.Errorf("create transformation: %w", err)
	}

	if s.GitPrimary {
		if err := s.commitOrCompensate(ctx, callerID, ws.Slug, "create", t.ID, func(ctx context.Context) error {
			return s.Transformations.DeleteTransformation(ctx, t.ID)
		}); err != nil {
			return nil, err
		}
		return t, nil
	}
	s.mirrorTransformations(ctx, callerID, ws.Slug)
	return t, nil
}

// ListTransformations returns all transformations for the caller's ws.
func (s *TransformationService) ListTransformations(ctx context.Context, callerID core.UserID) ([]Transformation, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}

	transformations, err := s.Transformations.ListTransformationsByCustomer(ctx, ws.Slug)
	if err != nil {
		return nil, fmt.Errorf("list transformations: %w", err)
	}

	return transformations, nil
}

// GetTransformation returns a transformation by ID, verifying ownership.
func (s *TransformationService) GetTransformation(ctx context.Context, callerID core.UserID, id TransformationID) (*Transformation, []TransformationRun, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, nil, fmt.Errorf("get customer: %w", err)
	}

	t, err := s.Transformations.GetTransformation(ctx, id)
	if err != nil {
		return nil, nil, fmt.Errorf("get transformation: %w", err)
	}
	if t.CustomerSlug != ws.Slug {
		return nil, nil, ErrTransformationNotFound
	}

	runs, err := s.Transformations.ListRecentTransformationRuns(ctx, id, 10)
	if err != nil {
		return nil, nil, fmt.Errorf("list recent runs: %w", err)
	}

	return t, runs, nil
}

// UpdateTransformation updates a transformation, verifying ownership.
func (s *TransformationService) UpdateTransformation(ctx context.Context, callerID core.UserID, t *Transformation) (*Transformation, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}

	existing, err := s.Transformations.GetTransformation(ctx, t.ID)
	if err != nil {
		return nil, fmt.Errorf("get transformation: %w", err)
	}
	if existing.CustomerSlug != ws.Slug {
		return nil, ErrTransformationNotFound
	}

	t.CustomerSlug = ws.Slug
	t.UpdatedAt = time.Now()
	if t.RepoRef == "" {
		t.RepoRef = "main"
	}

	if err := s.validateTrigger(ctx, ws.Slug, t.TriggerAfterPipelineID); err != nil {
		return nil, err
	}

	// Preserve existing credentials unless new ones were actually provided.
	if isEmptyJSON(t.GitCredentials) {
		t.GitCredentials = existing.GitCredentials
	}

	if err := s.Transformations.UpdateTransformation(ctx, t); err != nil {
		return nil, fmt.Errorf("update transformation: %w", err)
	}

	if s.GitPrimary {
		if err := s.commitOrCompensate(ctx, callerID, ws.Slug, "update", t.ID, func(ctx context.Context) error {
			return s.Transformations.UpdateTransformation(ctx, existing)
		}); err != nil {
			return nil, err
		}
		return t, nil
	}
	s.mirrorTransformations(ctx, callerID, ws.Slug)
	return t, nil
}

// DeleteTransformation deletes a transformation, verifying ownership.
func (s *TransformationService) DeleteTransformation(ctx context.Context, callerID core.UserID, id TransformationID) error {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return fmt.Errorf("get customer: %w", err)
	}

	existing, err := s.Transformations.GetTransformation(ctx, id)
	if err != nil {
		return fmt.Errorf("get transformation: %w", err)
	}
	if existing.CustomerSlug != ws.Slug {
		return ErrTransformationNotFound
	}

	if err := s.Transformations.DeleteTransformation(ctx, id); err != nil {
		return fmt.Errorf("delete transformation: %w", err)
	}

	if s.GitPrimary {
		// Compensation re-inserts the config under a new row id; the run
		// history is already gone (FK cascade) — same loss as a successful
		// delete, but the config survives, matching the repo state the
		// failed commit left behind.
		return s.commitOrCompensate(ctx, callerID, ws.Slug, "delete", id, func(ctx context.Context) error {
			return s.Transformations.CreateTransformation(ctx, existing)
		})
	}
	s.mirrorTransformations(ctx, callerID, ws.Slug)
	return nil
}

// TriggerTransformation creates a "pending" run entry, verifying ownership.
// The worker picks it up on its next poll (same mechanism as pipelines).
func (s *TransformationService) TriggerTransformation(ctx context.Context, callerID core.UserID, id TransformationID) (*TransformationRun, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}

	existing, err := s.Transformations.GetTransformation(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get transformation: %w", err)
	}
	if existing.CustomerSlug != ws.Slug {
		return nil, ErrTransformationNotFound
	}

	run := &TransformationRun{
		TransformationID: id,
		Status:           "pending",
		CreatedAt:        time.Now(),
	}

	if err := s.Transformations.CreateTransformationRun(ctx, run); err != nil {
		return nil, fmt.Errorf("create transformation run: %w", err)
	}

	return run, nil
}

// GetEnabledTransformations returns all enabled transformations for a
// customer (worker-facing).
func (s *TransformationService) GetEnabledTransformations(ctx context.Context, customerSlug string) ([]Transformation, error) {
	transformations, err := s.Transformations.GetEnabledTransformations(ctx, customerSlug)
	if err != nil {
		return nil, fmt.Errorf("get enabled transformations: %w", err)
	}
	return transformations, nil
}

// ReportTransformationRun records a run result from the dlt-worker.
//
// Like ReportPipelineRun, a report carrying an id is an upsert on that id:
// one run, one identity, whether the row was created here or by the box
// worker recording the run locally first.
//
// callerSlug, when non-empty, is the tenant bound to the caller's service
// token: the reported transformation must belong to that ws. Empty
// skips the check — only possible while the internal mux runs in log mode.
func (s *TransformationService) ReportTransformationRun(ctx context.Context, callerSlug string, run *TransformationRun) error {
	if callerSlug != "" {
		t, err := s.Transformations.GetTransformation(ctx, run.TransformationID)
		if err != nil {
			return fmt.Errorf("get transformation: %w", err)
		}
		if t.CustomerSlug != callerSlug {
			return ErrTransformationNotFound
		}
	}
	if run.ID != "" {
		err := s.Transformations.UpdateTransformationRun(ctx, run)
		if errors.Is(err, ErrTransformationRunNotFound) {
			err = s.Transformations.CreateTransformationRun(ctx, run)
		}
		if err != nil {
			return fmt.Errorf("record transformation run: %w", err)
		}
	} else {
		if err := s.Transformations.CreateTransformationRun(ctx, run); err != nil {
			return fmt.Errorf("create transformation run: %w", err)
		}
	}
	s.notifyRun(ctx, run)
	return nil
}

// notifyRun raises a best-effort in-app notification for a completed run. It
// never fails the report: notification problems are logged, not propagated.
func (s *TransformationService) notifyRun(ctx context.Context, run *TransformationRun) {
	if s.Notifications == nil {
		return
	}
	if run.Status != "success" && run.Status != "failed" {
		return
	}
	t, err := s.Transformations.GetTransformation(ctx, run.TransformationID)
	if err != nil {
		if s.Logger != nil {
			s.Logger.WarnContext(ctx, "notify run: get transformation", "transformation_id", run.TransformationID, "err", err)
		}
		return
	}

	title, body := transformationRunNotificationText(t.Name, run)

	n := Notification{
		CustomerSlug: t.CustomerSlug,
		Type:         "transformation_run",
		Title:        title,
		Body:         body,
		Link:         "transformations",
	}
	if err := s.Notifications.Notify(ctx, n); err != nil && s.Logger != nil {
		s.Logger.WarnContext(ctx, "notify run: publish", "transformation_id", run.TransformationID, "err", err)
	}
}

// transformationRunNotificationText builds the notification title and body for a
// completed transformation run.
func transformationRunNotificationText(name string, run *TransformationRun) (title, body string) {
	title = fmt.Sprintf("Transformation %q succeeded", name)
	if run.Status == "failed" {
		title = fmt.Sprintf("Transformation %q failed", name)
		body = run.ErrorMessage
		if body == "" && run.ModelsFailed+run.TestsFailed > 0 {
			body = fmt.Sprintf("%d models and %d tests failed.", run.ModelsFailed, run.TestsFailed)
		}
	} else if run.ModelsTotal > 0 {
		body = fmt.Sprintf("Built %d models, %d tests passed.", run.ModelsTotal, run.TestsTotal-run.TestsFailed)
	}
	return title, body
}

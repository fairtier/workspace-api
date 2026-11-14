package workspace

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/fairtier/workspace-api/core"
)

// WorkflowID is a strongly-typed identifier for workflows (UUID).
type WorkflowID string

func (id WorkflowID) String() string { return string(id) }

// WorkflowStepKind says what a workflow step runs.
type WorkflowStepKind string

const (
	StepKindPipeline       WorkflowStepKind = "pipeline"
	StepKindTransformation WorkflowStepKind = "transformation"
)

// Workflow run and step-run statuses. Runs use the same vocabulary as
// pipeline and transformation runs; a step can additionally be skipped,
// which is what every step after the first failure becomes.
const (
	RunStatusPending = "pending"
	RunStatusRunning = "running"
	RunStatusSuccess = "success"
	RunStatusFailed  = "failed"
	RunStatusSkipped = "skipped"
)

// Workflow run triggers.
const (
	WorkflowTriggerManual   = "manual"
	WorkflowTriggerSchedule = "schedule"
)

// MaxWorkflowSteps caps a workflow's length. A workflow is a short, linear
// chain; anything longer is a sign it wants to be two.
const MaxWorkflowSteps = 20

// Workflow is a named, ordered, linear list of steps, each running an
// existing pipeline or transformation, with at most one cron schedule. Steps
// run one after the other and the first failure stops the run.
//
// Like pipelines and transformations, a workflow is rendered to the box's
// git repositories (workflows/<slug>.yaml in the transformations repo, see
// WorkflowMirror) and edits made there are adopted back.
type Workflow struct {
	ID           WorkflowID
	CustomerSlug string
	Name         string
	Description  string
	// Schedule is a cron expression; empty means manual runs only.
	Schedule  string
	Enabled   bool
	Steps     []WorkflowStep
	CreatedAt time.Time
	UpdatedAt time.Time

	// Transient fields set by GetEnabledWorkflows.
	TriggerNow   bool
	PendingRunID string

	// Transient: the most recent run. ListWorkflowsByCustomer/GetWorkflow
	// consider every run; GetEnabledWorkflows only runs that left "pending",
	// which is the worker's cron baseline.
	LastRunAt     *time.Time
	LastRunStatus string

	// Transient failure summary of the most recent run (List/Get only), set
	// when it failed: the one-line summary (ErrorSummary) of the failed
	// step's error (else the run's), and the failed step's position and
	// name when one failed.
	LastRunError           string
	LastFailedStepPosition *int
	LastFailedStepName     string
}

// maxLastRunErrorLen caps Workflow.LastRunError and a failure notification's
// error line.
const maxLastRunErrorLen = 200

// SetLastRun copies the most recent run's status, time and failure summary
// onto w.
func (w *Workflow) SetLastRun(run *WorkflowRun) {
	w.LastRunStatus = run.Status
	w.LastRunAt = &run.CreatedAt
	if run.StartedAt != nil {
		w.LastRunAt = run.StartedAt
	}
	w.LastRunError, w.LastFailedStepPosition, w.LastFailedStepName = "", nil, ""
	if run.Status != RunStatusFailed {
		return
	}
	msg := run.ErrorMessage
	if sr := run.failedStep(); sr != nil {
		pos := sr.Position
		w.LastFailedStepPosition, w.LastFailedStepName = &pos, sr.Name
		msg = cmp.Or(sr.ErrorMessage, msg)
	}
	w.LastRunError = ErrorSummary(msg, maxLastRunErrorLen)
}

// failedStep returns the first failed step run, or nil.
func (r *WorkflowRun) failedStep() *WorkflowStepRun {
	for i := range r.StepRuns {
		if r.StepRuns[i].Status == RunStatusFailed {
			return &r.StepRuns[i]
		}
	}
	return nil
}

// WorkflowStep is one entry in a workflow's step list. Exactly one of
// PipelineID / TransformationID is set, per Kind.
type WorkflowStep struct {
	Position         int
	Kind             WorkflowStepKind
	PipelineID       PipelineID
	TransformationID TransformationID

	// Read-only, filled by the store from the referenced row.
	Name       string
	RefEnabled bool
}

// RefID returns the id of whatever the step references.
func (s WorkflowStep) RefID() string {
	if s.Kind == StepKindPipeline {
		return string(s.PipelineID)
	}
	return string(s.TransformationID)
}

// NewWorkflowStep builds a step referencing refID by kind.
func NewWorkflowStep(kind WorkflowStepKind, refID string) WorkflowStep {
	step := WorkflowStep{Kind: kind}
	switch kind {
	case StepKindPipeline:
		step.PipelineID = PipelineID(refID)
	case StepKindTransformation:
		step.TransformationID = TransformationID(refID)
	}
	return step
}

// WorkflowRun records one execution of a workflow.
type WorkflowRun struct {
	ID           string
	WorkflowID   WorkflowID
	Status       string // "pending", "running", "success", "failed"
	Trigger      string // "manual", "schedule"
	StartedAt    *time.Time
	CompletedAt  *time.Time
	ErrorMessage string
	CreatedAt    time.Time
	StepRuns     []WorkflowStepRun
}

// WorkflowStepRun is a snapshot of one step within one run: kind, ref and
// name are copied at run time, so history survives edits and deletions.
// PipelineRunID / TransformationRunID link to the run the step produced; they
// are plain ids, not foreign keys, because the worker writes those rows on
// its own schedule.
type WorkflowStepRun struct {
	Position            int
	Kind                WorkflowStepKind
	RefID               string
	Name                string
	Status              string // "pending", "running", "success", "failed", "skipped"
	PipelineRunID       string
	TransformationRunID string
	StartedAt           *time.Time
	CompletedAt         *time.Time
	ErrorMessage        string
}

// WorkflowRunCursor is a keyset position in a workflow's newest-first run
// history: the page after it starts strictly below (CreatedAt, ID).
type WorkflowRunCursor struct {
	CreatedAt time.Time
	ID        string
}

// WorkflowRepository persists workflows, their steps, and their runs.
type WorkflowRepository interface {
	// CreateWorkflow inserts the workflow and its steps atomically, setting
	// w.ID. A duplicate name in the workspace is ErrWorkflowAlreadyExists.
	CreateWorkflow(ctx context.Context, w *Workflow) error
	// GetWorkflow returns the workflow with its steps (names resolved).
	GetWorkflow(ctx context.Context, id WorkflowID) (*Workflow, error)
	ListWorkflowsByCustomer(ctx context.Context, customerSlug string) ([]Workflow, error)
	// UpdateWorkflow rewrites the workflow and replaces its steps atomically.
	UpdateWorkflow(ctx context.Context, w *Workflow) error
	// SetWorkflowEnabled sets only the enabled flag (and updated_at).
	SetWorkflowEnabled(ctx context.Context, id WorkflowID, enabled bool, at time.Time) error
	DeleteWorkflow(ctx context.Context, id WorkflowID) error

	// GetEnabledWorkflows returns every enabled workflow plus any disabled
	// one with a pending run, with TriggerNow/PendingRunID and the cron
	// baseline (LastRunAt/LastRunStatus) set (worker-facing).
	GetEnabledWorkflows(ctx context.Context, customerSlug string) ([]Workflow, error)

	// GetPendingWorkflowRun returns the oldest pending run, or (nil, nil).
	GetPendingWorkflowRun(ctx context.Context, id WorkflowID) (*WorkflowRun, error)
	// CreateWorkflowRun inserts a run (no step runs), setting run.ID when
	// empty.
	CreateWorkflowRun(ctx context.Context, run *WorkflowRun) error
	// UpsertWorkflowRun inserts or updates the run by id together with its
	// step runs (upserted by position), atomically. It returns the status the
	// run had before ("" if it did not exist). A run id that already belongs
	// to another workflow is ErrWorkflowRunNotFound.
	UpsertWorkflowRun(ctx context.Context, run *WorkflowRun) (previousStatus string, err error)
	// ListWorkflowRuns returns up to limit runs newest first, strictly older
	// than after when it is non-nil, with their step runs.
	ListWorkflowRuns(ctx context.Context, id WorkflowID, limit int, after *WorkflowRunCursor) ([]WorkflowRun, error)
	// ListRunningWorkflowRuns returns the workspace's runs whose status is
	// "running", with their step runs, for the stuck-run sweep to judge.
	ListRunningWorkflowRuns(ctx context.Context, customerSlug string) ([]WorkflowRun, error)
}

// TransformationReader is the narrow transformation lookup the workflow
// service needs to validate step references.
type TransformationReader interface {
	GetTransformation(ctx context.Context, id TransformationID) (*Transformation, error)
}

// WorkflowService orchestrates workflow CRUD, manual triggers, and run
// reporting. It never executes anything: the box's dlt-worker does.
type WorkflowService struct {
	Workspaces      Resolver
	Workflows       WorkflowRepository
	Pipelines       PipelineReader
	Transformations TransformationReader
	// Notifications, when set, raises an in-app notification when a run
	// becomes failed. Optional (nil = no notifications).
	Notifications Notifier
	// Mirror, when set, renders workflows into the box's transformations
	// repo on the request path: create, update, enable/disable and delete
	// converge the repo synchronously, and a failed commit hard-fails the
	// save with the row compensated back — TransformationService.Mirror's
	// contract.
	Mirror WorkflowMirrorer
	// Users resolves the acting user so mirrored commits carry them as git
	// author. Optional.
	Users  UserReader
	Logger *slog.Logger
}

// commitMirror converges the box repo on the request path. deleted, when
// set, is the workflow the caller just deleted.
func (s *WorkflowService) commitMirror(ctx context.Context, callerID core.UserID, customerSlug string, deleted *Workflow) error {
	if s.Mirror == nil {
		return nil
	}
	author := resolveCommitAuthor(ctx, s.Users, callerID)
	ctx, cancel := context.WithTimeout(ctx, gitPrimarySyncTimeout)
	defer cancel()
	var err error
	if deleted != nil {
		err = s.Mirror.SyncCustomerAfterDelete(ctx, customerSlug, author, deleted)
	} else {
		err = s.Mirror.SyncCustomer(ctx, customerSlug, author)
	}
	if err != nil {
		return fmt.Errorf("commit workflow definitions to the box repo: %w", err)
	}
	return nil
}

// commitOrCompensate is the git-first save tail: converge the repo and, on
// failure, roll the row back so the failed save leaves no state. A failed
// compensation is logged, not returned.
func (s *WorkflowService) commitOrCompensate(ctx context.Context, callerID core.UserID, customerSlug, op string, id WorkflowID, deleted *Workflow, compensate func(context.Context) error) error {
	err := s.commitMirror(ctx, callerID, customerSlug, deleted)
	if err == nil {
		return nil
	}
	if compErr := compensate(ctx); compErr != nil && s.Logger != nil {
		s.Logger.ErrorContext(ctx, "git-first workflow "+op+": failed to compensate row after mirror failure; converge will heal", "workflow_id", id, "error", compErr)
	}
	return err
}

// recentWorkflowRuns is how many runs GetWorkflow returns.
const recentWorkflowRuns = 10

// Page size bounds for ListWorkflowRuns.
const (
	defaultWorkflowRunPage = 20
	maxWorkflowRunPage     = 100
)

// ownedWorkflow resolves the caller's workspace and the workflow, returning
// ErrWorkflowNotFound for another tenant's workflow.
func (s *WorkflowService) ownedWorkflow(ctx context.Context, callerID core.UserID, id WorkflowID) (*Workspace, *Workflow, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, nil, fmt.Errorf("get customer: %w", err)
	}
	w, err := s.Workflows.GetWorkflow(ctx, id)
	if err != nil {
		return nil, nil, fmt.Errorf("get workflow: %w", err)
	}
	if w.CustomerSlug != ws.Slug {
		return nil, nil, ErrWorkflowNotFound
	}
	return ws, w, nil
}

// CreateWorkflow creates a new, enabled workflow in the caller's workspace.
func (s *WorkflowService) CreateWorkflow(ctx context.Context, callerID core.UserID, w *Workflow) (*Workflow, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}
	if err := s.validateWorkflow(ctx, ws.Slug, w); err != nil {
		return nil, err
	}

	w.CustomerSlug = ws.Slug
	now := time.Now()
	w.CreatedAt = now
	w.UpdatedAt = now

	if err := s.Workflows.CreateWorkflow(ctx, w); err != nil {
		return nil, fmt.Errorf("create workflow: %w", err)
	}
	if err := s.commitOrCompensate(ctx, callerID, ws.Slug, "create", w.ID, nil, func(ctx context.Context) error {
		return s.Workflows.DeleteWorkflow(ctx, w.ID)
	}); err != nil {
		return nil, err
	}
	return s.reload(ctx, w)
}

// reload re-reads a just-written workflow so the response carries resolved
// step names. A failed re-read returns what was written.
func (s *WorkflowService) reload(ctx context.Context, w *Workflow) (*Workflow, error) {
	fresh, err := s.Workflows.GetWorkflow(ctx, w.ID)
	if err != nil {
		if s.Logger != nil {
			s.Logger.WarnContext(ctx, "reload workflow after write", "workflow_id", w.ID, "err", err)
		}
		return w, nil
	}
	return fresh, nil
}

// ListWorkflows returns all workflows of the caller's workspace.
func (s *WorkflowService) ListWorkflows(ctx context.Context, callerID core.UserID) ([]Workflow, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}
	workflows, err := s.Workflows.ListWorkflowsByCustomer(ctx, ws.Slug)
	if err != nil {
		return nil, fmt.Errorf("list workflows: %w", err)
	}
	for i := range workflows {
		workflows[i].LastRunError = ErrorSummary(workflows[i].LastRunError, maxLastRunErrorLen)
	}
	return workflows, nil
}

// GetWorkflow returns a workflow with its most recent runs, verifying
// ownership.
func (s *WorkflowService) GetWorkflow(ctx context.Context, callerID core.UserID, id WorkflowID) (*Workflow, []WorkflowRun, error) {
	_, w, err := s.ownedWorkflow(ctx, callerID, id)
	if err != nil {
		return nil, nil, err
	}
	runs, err := s.Workflows.ListWorkflowRuns(ctx, id, recentWorkflowRuns, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("list recent workflow runs: %w", err)
	}
	// The list view's last-run fields, from the newest run.
	if len(runs) > 0 {
		w.SetLastRun(&runs[0])
	}
	return w, runs, nil
}

// UpdateWorkflow replaces a workflow's definition, verifying ownership.
func (s *WorkflowService) UpdateWorkflow(ctx context.Context, callerID core.UserID, w *Workflow) (*Workflow, error) {
	ws, existing, err := s.ownedWorkflow(ctx, callerID, w.ID)
	if err != nil {
		return nil, err
	}
	if err := s.validateWorkflow(ctx, ws.Slug, w); err != nil {
		return nil, err
	}

	w.CustomerSlug = ws.Slug
	w.CreatedAt = existing.CreatedAt
	w.UpdatedAt = time.Now()

	if err := s.Workflows.UpdateWorkflow(ctx, w); err != nil {
		return nil, fmt.Errorf("update workflow: %w", err)
	}
	if err := s.commitOrCompensate(ctx, callerID, ws.Slug, "update", w.ID, nil, func(ctx context.Context) error {
		return s.Workflows.UpdateWorkflow(ctx, existing)
	}); err != nil {
		return nil, err
	}
	return s.reload(ctx, w)
}

// SetWorkflowEnabled turns a workflow's schedule on or off, verifying
// ownership, without touching its definition.
func (s *WorkflowService) SetWorkflowEnabled(ctx context.Context, callerID core.UserID, id WorkflowID, enabled bool) (*Workflow, error) {
	ws, w, err := s.ownedWorkflow(ctx, callerID, id)
	if err != nil {
		return nil, err
	}
	if err := s.Workflows.SetWorkflowEnabled(ctx, id, enabled, time.Now()); err != nil {
		return nil, fmt.Errorf("set workflow enabled: %w", err)
	}
	if err := s.commitOrCompensate(ctx, callerID, ws.Slug, "set enabled", id, nil, func(ctx context.Context) error {
		return s.Workflows.SetWorkflowEnabled(ctx, id, w.Enabled, w.UpdatedAt)
	}); err != nil {
		return nil, err
	}
	return s.reload(ctx, w)
}

// DeleteWorkflow deletes a workflow and its run history, verifying
// ownership.
func (s *WorkflowService) DeleteWorkflow(ctx context.Context, callerID core.UserID, id WorkflowID) error {
	ws, existing, err := s.ownedWorkflow(ctx, callerID, id)
	if err != nil {
		return err
	}
	if err := s.Workflows.DeleteWorkflow(ctx, id); err != nil {
		return fmt.Errorf("delete workflow: %w", err)
	}
	// Compensation re-inserts the definition; the run history is already
	// gone (FK cascade), the same loss as a successful delete. The id is kept
	// when the store can import, so the file the failed commit left behind
	// still names this workflow.
	return s.commitOrCompensate(ctx, callerID, ws.Slug, "delete", id, existing, func(ctx context.Context) error {
		if imp, ok := s.Workflows.(WorkflowImporter); ok {
			return imp.ImportWorkflow(ctx, existing)
		}
		return s.Workflows.CreateWorkflow(ctx, existing)
	})
}

// TriggerWorkflow queues a "pending" manual run, verifying ownership. The
// worker picks it up on its next poll — even when the workflow is disabled:
// disabling stops the schedule, not a run a person just asked for (the
// pipeline rule). A run already pending satisfies the trigger.
func (s *WorkflowService) TriggerWorkflow(ctx context.Context, callerID core.UserID, id WorkflowID) (*WorkflowRun, error) {
	_, w, err := s.ownedWorkflow(ctx, callerID, id)
	if err != nil {
		return nil, err
	}
	// A workflow whose every step was deleted with its target is never
	// served to the worker, so a run queued for it would wait forever.
	if len(w.Steps) == 0 {
		return nil, ErrWorkflowHasNoSteps
	}

	if pending, err := s.Workflows.GetPendingWorkflowRun(ctx, id); err != nil {
		return nil, fmt.Errorf("get pending workflow run: %w", err)
	} else if pending != nil {
		return pending, nil
	}

	run := &WorkflowRun{
		WorkflowID: id,
		Status:     RunStatusPending,
		Trigger:    WorkflowTriggerManual,
		CreatedAt:  time.Now(),
	}
	if err := s.Workflows.CreateWorkflowRun(ctx, run); err != nil {
		return nil, fmt.Errorf("create workflow run: %w", err)
	}
	return run, nil
}

// ListWorkflowRuns pages through a workflow's runs newest first, verifying
// ownership. It returns the page and the token of the next one ("" at the
// end).
func (s *WorkflowService) ListWorkflowRuns(ctx context.Context, callerID core.UserID, id WorkflowID, pageSize int, pageToken string) ([]WorkflowRun, string, error) {
	if _, _, err := s.ownedWorkflow(ctx, callerID, id); err != nil {
		return nil, "", err
	}
	after, err := decodeWorkflowRunCursor(pageToken)
	if err != nil {
		return nil, "", err
	}
	limit := clampPageSize(pageSize)

	// One extra row says whether another page exists without a count query.
	runs, err := s.Workflows.ListWorkflowRuns(ctx, id, limit+1, after)
	if err != nil {
		return nil, "", fmt.Errorf("list workflow runs: %w", err)
	}
	if len(runs) <= limit {
		return runs, "", nil
	}
	runs = runs[:limit]
	last := runs[limit-1]
	return runs, encodeWorkflowRunCursor(WorkflowRunCursor{CreatedAt: last.CreatedAt, ID: last.ID}), nil
}

func clampPageSize(n int) int {
	switch {
	case n <= 0:
		return defaultWorkflowRunPage
	case n > maxWorkflowRunPage:
		return maxWorkflowRunPage
	default:
		return n
	}
}

// GetWorkflowConfigs returns the workflows the worker should act on
// (worker-facing). Omitted:
//   - workflows left without steps (every target deleted): there is nothing
//     to run. A run queued for one is failed here, or it would stay pending
//     forever.
//   - workflows whose latest started run is still "running": the worker must
//     not start a second run beside it. A run that never reports again is
//     failed by the stuck-run sweep, after which the workflow reappears.
func (s *WorkflowService) GetWorkflowConfigs(ctx context.Context, customerSlug string) ([]Workflow, error) {
	workflows, err := s.Workflows.GetEnabledWorkflows(ctx, customerSlug)
	if err != nil {
		return nil, fmt.Errorf("get enabled workflows: %w", err)
	}
	out := workflows[:0]
	for _, w := range workflows {
		switch {
		case len(w.Steps) == 0:
			if w.PendingRunID != "" {
				s.failStrandedRun(ctx, &w)
			}
		case w.LastRunStatus == RunStatusRunning:
			// Withheld until that run finishes.
		default:
			out = append(out, w)
		}
	}
	return out, nil
}

// errNoStepsLeft is the error recorded on a run queued for a workflow that
// then lost every step.
const errNoStepsLeft = "not run: every step of this workflow was removed (its pipelines or transformations were deleted) before the run started"

// failStrandedRun fails the pending run of a workflow without steps.
// Best-effort: an error is logged and the next poll retries.
func (s *WorkflowService) failStrandedRun(ctx context.Context, w *Workflow) {
	now := time.Now()
	run := &WorkflowRun{
		ID: w.PendingRunID, WorkflowID: w.ID, Status: RunStatusFailed, Trigger: WorkflowTriggerManual,
		StartedAt: &now, CompletedAt: &now, ErrorMessage: errNoStepsLeft,
	}
	if err := s.recordFailure(ctx, w, run); err != nil && s.Logger != nil {
		s.Logger.WarnContext(ctx, "fail stranded workflow run", "workflow_id", w.ID, "run_id", run.ID, "err", err)
	}
}

// recordFailure upserts a run the server itself failed and notifies on the
// transition into failed, as a worker report would.
func (s *WorkflowService) recordFailure(ctx context.Context, w *Workflow, run *WorkflowRun) error {
	previous, err := s.Workflows.UpsertWorkflowRun(ctx, run)
	if err != nil {
		return err
	}
	if previous != RunStatusFailed {
		s.notifyFailure(ctx, w, run)
	}
	return nil
}

// WorkflowStuckPolicy decides when a "running" workflow run is dead.
//
// A workflow run is not one long job but a sequence of steps, and the worker
// reports every transition: the run when it starts, each step as "running"
// (with started_at) when it begins and again when it ends. So the age of the
// run says nothing — a healthy workflow of 20 long pipeline steps can run for
// days — while the age of its current step and of its last transition do:
//
//   - a step "running" for longer than StepTimeout is past anything the
//     worker allows one step (StuckRunTimeoutFor, the pipeline sweep's own
//     derivation from the worker's limits); 0 turns this rule off, as a
//     worker without a time limit leaves no age that proves a step dead;
//   - a run with no step "running" whose last transition (run start, or any
//     step's start/end) is older than IdleGrace is dead: the worker moves
//     from one step straight to the next.
type WorkflowStuckPolicy struct {
	StepTimeout time.Duration
	// IdleGrace; 0 = DefaultWorkflowIdleGrace.
	IdleGrace time.Duration
}

// DefaultWorkflowIdleGrace is how long a running workflow run may go with no
// step running and no transition reported. The worker moves straight from
// one step to the next, so this only has to cover a lost report being
// retried and a slow poll; callers add their sweep interval on top.
const DefaultWorkflowIdleGrace = 15 * time.Minute

// errStuckStep and errStuckIdle are the errors recorded on a run failed by
// the stuck-run sweep.
const (
	errStuckStep = "no report from the worker: step %d has been running for %s, longer than the worker allows one step (%s); the run was marked failed (the worker probably restarted or was killed mid-run)"
	errStuckIdle = "no report from the worker for %s since its last step transition, with no step running: the run was marked failed (the worker probably restarted or was killed mid-run)"
)

// SweepStuckRuns fails the workspace's "running" workflow runs that policy
// finds dead: the run and its running step become failed, steps that never
// started become skipped, and the usual failure notification fires once. It
// returns how many runs it failed.
func (s *WorkflowService) SweepStuckRuns(ctx context.Context, customerSlug string, policy WorkflowStuckPolicy) (int, error) {
	runs, err := s.Workflows.ListRunningWorkflowRuns(ctx, customerSlug)
	if err != nil {
		return 0, fmt.Errorf("list running workflow runs: %w", err)
	}
	failed := 0
	now := time.Now()
	for i := range runs {
		run := &runs[i]
		msg, stuck := policy.stuck(run, now)
		if !stuck {
			continue
		}
		w, err := s.Workflows.GetWorkflow(ctx, run.WorkflowID)
		if err != nil {
			return failed, fmt.Errorf("get workflow: %w", err)
		}
		failStuckRun(run, w, msg, now)
		if err := s.recordFailure(ctx, w, run); err != nil {
			return failed, fmt.Errorf("fail stuck workflow run: %w", err)
		}
		failed++
	}
	return failed, nil
}

// stuck reports whether run is dead at now, and the error to record.
func (p WorkflowStuckPolicy) stuck(run *WorkflowRun, now time.Time) (string, bool) {
	last := run.CreatedAt
	if run.StartedAt != nil {
		last = laterOf(last, *run.StartedAt)
	}
	for _, sr := range run.StepRuns {
		if sr.Status == RunStatusRunning {
			return p.stuckStep(sr, last, now)
		}
		if sr.StartedAt != nil {
			last = laterOf(last, *sr.StartedAt)
		}
		if sr.CompletedAt != nil {
			last = laterOf(last, *sr.CompletedAt)
		}
	}
	grace := p.IdleGrace
	if grace <= 0 {
		grace = DefaultWorkflowIdleGrace
	}
	if idle := now.Sub(last); idle > grace {
		return fmt.Sprintf(errStuckIdle, idle.Round(time.Minute)), true
	}
	return "", false
}

// stuckStep judges a run by its running step, timed from its own start (the
// run's last transition before it, if the worker sent none).
func (p WorkflowStuckPolicy) stuckStep(sr WorkflowStepRun, fallback, now time.Time) (string, bool) {
	if p.StepTimeout <= 0 {
		return "", false
	}
	since := fallback
	if sr.StartedAt != nil {
		since = *sr.StartedAt
	}
	if age := now.Sub(since); age > p.StepTimeout {
		return fmt.Sprintf(errStuckStep, sr.Position+1, age.Round(time.Minute), p.StepTimeout), true
	}
	return "", false
}

func laterOf(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// failStuckRun turns a stuck run into its failed form: a running (or
// pending) step fails, and every step the workflow defines that has no
// step run yet is recorded as skipped.
func failStuckRun(run *WorkflowRun, w *Workflow, msg string, now time.Time) {
	run.Status, run.CompletedAt, run.ErrorMessage = RunStatusFailed, &now, msg
	seen := map[int]bool{}
	for i := range run.StepRuns {
		sr := &run.StepRuns[i]
		seen[sr.Position] = true
		switch sr.Status {
		case RunStatusRunning:
			sr.Status, sr.CompletedAt, sr.ErrorMessage = RunStatusFailed, &now, msg
		case RunStatusPending:
			sr.Status = RunStatusSkipped
		}
	}
	for _, st := range w.Steps {
		if !seen[st.Position] {
			run.StepRuns = append(run.StepRuns, WorkflowStepRun{
				Position: st.Position, Kind: st.Kind, RefID: st.RefID(), Name: st.Name, Status: RunStatusSkipped,
			})
		}
	}
}

// ReportWorkflowRun records a workflow run's progress from the dlt-worker: an
// upsert on run.ID, step runs included, so a repeated report is harmless.
//
// callerSlug is the tenant bound to the caller's service token: the workflow
// must belong to it. Empty skips the check, as for ReportTransformationRun.
func (s *WorkflowService) ReportWorkflowRun(ctx context.Context, callerSlug string, run *WorkflowRun) error {
	if err := validateWorkflowRunReport(run); err != nil {
		return err
	}
	w, err := s.Workflows.GetWorkflow(ctx, run.WorkflowID)
	if err != nil {
		return fmt.Errorf("get workflow: %w", err)
	}
	if callerSlug != "" && w.CustomerSlug != callerSlug {
		return ErrWorkflowNotFound
	}

	previous, err := s.Workflows.UpsertWorkflowRun(ctx, run)
	if err != nil {
		return fmt.Errorf("record workflow run: %w", err)
	}
	// Progress is reported after every step, and a report may be repeated:
	// notify on the transition into failed, not on every failed report.
	if run.Status == RunStatusFailed && previous != RunStatusFailed {
		s.notifyFailure(ctx, w, run)
	}
	return nil
}

// notifyFailure raises a best-effort in-app notification for a failed run.
// Successes are not announced: each step's own pipeline/transformation run
// already notifies, and a workflow success would only repeat the last one.
func (s *WorkflowService) notifyFailure(ctx context.Context, w *Workflow, run *WorkflowRun) {
	if s.Notifications == nil {
		return
	}
	n := Notification{
		CustomerSlug: w.CustomerSlug,
		Type:         "workflow_run",
		Title:        fmt.Sprintf("Workflow %q failed", w.Name),
		Body:         workflowFailureBody(run),
		Link:         "/workflows?workflow=" + string(w.ID),
	}
	if err := s.Notifications.Notify(ctx, n); err != nil && s.Logger != nil {
		s.Logger.WarnContext(ctx, "notify workflow run: publish", "workflow_id", w.ID, "err", err)
	}
}

// workflowFailureBody names the failed step when the report says which one.
func workflowFailureBody(run *WorkflowRun) string {
	for _, sr := range run.StepRuns {
		if sr.Status != RunStatusFailed {
			continue
		}
		body := fmt.Sprintf("Step %d (%s %q) failed.", sr.Position+1, sr.Kind, sr.Name)
		if msg := ErrorSummary(cmp.Or(sr.ErrorMessage, run.ErrorMessage), maxLastRunErrorLen); msg != "" {
			body += " " + msg
		}
		return body
	}
	return ErrorSummary(run.ErrorMessage, maxLastRunErrorLen)
}

// Workflow error sentinels.
var (
	ErrWorkflowNotFound      = errors.New("workflow not found")
	ErrWorkflowAlreadyExists = errors.New("workflow already exists")
	ErrWorkflowRunNotFound   = errors.New("workflow run not found")
	ErrWorkflowHasNoSteps    = errors.New("workflow has no steps")
	ErrInvalidPageToken      = errors.New("invalid page token")
)

// ErrInvalidWorkflow rejects a workflow definition or run report. Field names
// the offending input (e.g. "steps[2].ref_id") for a FieldViolation.
type ErrInvalidWorkflow struct {
	Field string
	Msg   string
}

func (e *ErrInvalidWorkflow) Error() string { return e.Field + ": " + e.Msg }

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
// Unlike pipelines and transformations, a workflow is not rendered to the
// box's git repositories: the row is the only copy.
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
	Logger        *slog.Logger
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
	w.Enabled = true

	if err := s.Workflows.CreateWorkflow(ctx, w); err != nil {
		return nil, fmt.Errorf("create workflow: %w", err)
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
	return s.reload(ctx, w)
}

// DeleteWorkflow deletes a workflow and its run history, verifying
// ownership.
func (s *WorkflowService) DeleteWorkflow(ctx context.Context, callerID core.UserID, id WorkflowID) error {
	if _, _, err := s.ownedWorkflow(ctx, callerID, id); err != nil {
		return err
	}
	if err := s.Workflows.DeleteWorkflow(ctx, id); err != nil {
		return fmt.Errorf("delete workflow: %w", err)
	}
	return nil
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
// (worker-facing). Workflows left without steps — every target deleted — are
// omitted: there is nothing to run.
func (s *WorkflowService) GetWorkflowConfigs(ctx context.Context, customerSlug string) ([]Workflow, error) {
	workflows, err := s.Workflows.GetEnabledWorkflows(ctx, customerSlug)
	if err != nil {
		return nil, fmt.Errorf("get enabled workflows: %w", err)
	}
	out := workflows[:0]
	for _, w := range workflows {
		if len(w.Steps) > 0 {
			out = append(out, w)
		}
	}
	return out, nil
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
		if msg := cmp.Or(sr.ErrorMessage, run.ErrorMessage); msg != "" {
			body += " " + msg
		}
		return body
	}
	return run.ErrorMessage
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

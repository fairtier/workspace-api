package workspace

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// maxWorkflowDescription bounds the free-text description.
const maxWorkflowDescription = 2000

// validateWorkflow normalizes and checks a workflow definition before it is
// stored; see ValidateWorkflowDefinition.
func (s *WorkflowService) validateWorkflow(ctx context.Context, customerSlug string, w *Workflow) error {
	return ValidateWorkflowDefinition(ctx, s.Pipelines, s.Transformations, customerSlug, w)
}

// ValidateWorkflowDefinition normalizes and checks a workflow definition
// before it is stored: name, description, schedule, and a 1..MaxWorkflowSteps
// step list of distinct pipelines/transformations that all belong to
// customerSlug. Step positions are reassigned from the list order. It is the
// one validator behind both write paths — the API and a workflow file adopted
// from the repo — so a definition one accepts the other accepts too.
func ValidateWorkflowDefinition(ctx context.Context, pipelines PipelineReader, transformations TransformationReader, customerSlug string, w *Workflow) error {
	w.Name = strings.TrimSpace(w.Name)
	w.Schedule = strings.TrimSpace(w.Schedule)
	if w.Name == "" {
		return &ErrInvalidWorkflow{Field: "name", Msg: "name is required"}
	}
	if len(w.Description) > maxWorkflowDescription {
		return &ErrInvalidWorkflow{Field: "description", Msg: fmt.Sprintf("description is longer than %d characters", maxWorkflowDescription)}
	}
	if err := ValidateCronSchedule(w.Schedule); err != nil {
		return &ErrInvalidWorkflow{Field: "schedule", Msg: err.Error()}
	}
	if err := validateStepShape(w.Steps); err != nil {
		return err
	}
	for i := range w.Steps {
		w.Steps[i].Position = i
		if err := checkStepRef(ctx, pipelines, transformations, customerSlug, i, w.Steps[i]); err != nil {
			return err
		}
	}
	return nil
}

// validateStepShape checks the step list without touching the store: count,
// kinds, id syntax, and no target twice.
func validateStepShape(steps []WorkflowStep) error {
	if len(steps) == 0 {
		return &ErrInvalidWorkflow{Field: "steps", Msg: "a workflow needs at least one step"}
	}
	if len(steps) > MaxWorkflowSteps {
		return &ErrInvalidWorkflow{Field: "steps", Msg: fmt.Sprintf("a workflow has at most %d steps", MaxWorkflowSteps)}
	}
	seen := make(map[string]int, len(steps))
	for i, step := range steps {
		if err := validateStepRef(fmt.Sprintf("steps[%d]", i), step.Kind, step.RefID()); err != nil {
			return err
		}
		key := string(step.Kind) + "/" + step.RefID()
		if first, dup := seen[key]; dup {
			return &ErrInvalidWorkflow{
				Field: fmt.Sprintf("steps[%d].ref_id", i),
				Msg:   fmt.Sprintf("this %s is already step %d", step.Kind, first+1),
			}
		}
		seen[key] = i
	}
	return nil
}

// validateStepRef checks a kind/ref pair's syntax.
func validateStepRef(field string, kind WorkflowStepKind, refID string) error {
	if kind != StepKindPipeline && kind != StepKindTransformation {
		return &ErrInvalidWorkflow{Field: field + ".kind", Msg: `kind must be "pipeline" or "transformation"`}
	}
	if _, err := uuid.Parse(refID); err != nil {
		return &ErrInvalidWorkflow{Field: field + ".ref_id", Msg: "ref_id must be a UUID"}
	}
	return nil
}

// checkStepRef verifies the step's target exists in the caller's workspace.
// Missing and foreign targets read the same, so the answer leaks nothing
// about other tenants.
func checkStepRef(ctx context.Context, pipelines PipelineReader, transformations TransformationReader, customerSlug string, i int, step WorkflowStep) error {
	owner, err := stepRefOwner(ctx, pipelines, transformations, step)
	if errors.Is(err, ErrPipelineNotFound) || errors.Is(err, ErrTransformationNotFound) || (err == nil && owner != customerSlug) {
		return &ErrInvalidWorkflow{
			Field: fmt.Sprintf("steps[%d].ref_id", i),
			Msg:   fmt.Sprintf("%s not found", step.Kind),
		}
	}
	return err
}

// stepRefOwner returns the workspace slug owning the step's target.
func stepRefOwner(ctx context.Context, pipelines PipelineReader, transformations TransformationReader, step WorkflowStep) (string, error) {
	if step.Kind == StepKindPipeline {
		p, err := pipelines.GetPipeline(ctx, step.PipelineID)
		if err != nil {
			return "", err
		}
		return p.CustomerSlug, nil
	}
	t, err := transformations.GetTransformation(ctx, step.TransformationID)
	if err != nil {
		return "", err
	}
	return t.CustomerSlug, nil
}

// reportableRunStatuses are what the worker may report a run as; "pending"
// belongs to TriggerWorkflow alone.
var reportableRunStatuses = map[string]bool{
	RunStatusRunning: true, RunStatusSuccess: true, RunStatusFailed: true,
}

var stepRunStatuses = map[string]bool{
	RunStatusPending: true, RunStatusRunning: true, RunStatusSuccess: true,
	RunStatusFailed: true, RunStatusSkipped: true,
}

// validateWorkflowRunReport checks a worker report before it is stored.
func validateWorkflowRunReport(run *WorkflowRun) error {
	if _, err := uuid.Parse(run.ID); err != nil {
		return &ErrInvalidWorkflow{Field: "run_id", Msg: "run_id must be a UUID"}
	}
	if !reportableRunStatuses[run.Status] {
		return &ErrInvalidWorkflow{Field: "status", Msg: `status must be "running", "success" or "failed"`}
	}
	if run.Trigger != "" && run.Trigger != WorkflowTriggerManual && run.Trigger != WorkflowTriggerSchedule {
		return &ErrInvalidWorkflow{Field: "trigger", Msg: `trigger must be "manual" or "schedule"`}
	}
	if len(run.StepRuns) > MaxWorkflowSteps {
		return &ErrInvalidWorkflow{Field: "step_runs", Msg: fmt.Sprintf("at most %d step runs", MaxWorkflowSteps)}
	}
	seen := make(map[int]bool, len(run.StepRuns))
	for i, sr := range run.StepRuns {
		if err := validateStepRunReport(fmt.Sprintf("step_runs[%d]", i), sr); err != nil {
			return err
		}
		if seen[sr.Position] {
			return &ErrInvalidWorkflow{Field: fmt.Sprintf("step_runs[%d].position", i), Msg: "duplicate position"}
		}
		seen[sr.Position] = true
	}
	return nil
}

func validateStepRunReport(field string, sr WorkflowStepRun) error {
	if sr.Position < 0 || sr.Position >= MaxWorkflowSteps {
		return &ErrInvalidWorkflow{Field: field + ".position", Msg: fmt.Sprintf("position must be 0..%d", MaxWorkflowSteps-1)}
	}
	if err := validateStepRef(field, sr.Kind, sr.RefID); err != nil {
		return err
	}
	if !stepRunStatuses[sr.Status] {
		return &ErrInvalidWorkflow{Field: field + ".status", Msg: "unknown step status"}
	}
	for name, id := range map[string]string{"pipeline_run_id": sr.PipelineRunID, "transformation_run_id": sr.TransformationRunID} {
		if id == "" {
			continue
		}
		if _, err := uuid.Parse(id); err != nil {
			return &ErrInvalidWorkflow{Field: field + "." + name, Msg: name + " must be a UUID"}
		}
	}
	return nil
}

// --- Cron ---

// cronMacros are shorthand schedules the worker's croniter would accept but
// the Console's cron editor rejects (it cannot display them), so they are
// refused here too, with the five-field equivalent as the fix.
var cronMacros = map[string]string{
	"@yearly": "0 0 1 1 *", "@annually": "0 0 1 1 *", "@monthly": "0 0 1 * *", "@weekly": "0 0 * * 0",
	"@daily": "0 0 * * *", "@midnight": "0 0 * * *", "@hourly": "0 * * * *",
}

// cronField describes one of the five standard fields.
type cronField struct {
	name     string
	min, max int
	names    []string // index + min = value, for month/day-of-week names
}

var cronFields = []cronField{
	{name: "minute", min: 0, max: 59},
	{name: "hour", min: 0, max: 23},
	{name: "day of month", min: 1, max: 31},
	{name: "month", min: 1, max: 12, names: []string{"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"}},
	// 7 is Sunday as well as 0, as in croniter.
	{name: "day of week", min: 0, max: 7, names: []string{"sun", "mon", "tue", "wed", "thu", "fri", "sat"}},
}

// ValidateCronSchedule accepts an empty schedule (manual only) or a standard
// five-field cron expression whose fields are "*", values, ranges, lists and
// steps ("*/15", "1-5", "mon,wed,fri"). That is a deliberate subset of what
// the worker's croniter parses, matching what the Console's cron editor
// accepts — @macros such as "@daily" are refused like there.
func ValidateCronSchedule(schedule string) error {
	if schedule == "" {
		return nil
	}
	if strings.HasPrefix(schedule, "@") {
		if eq, ok := cronMacros[strings.ToLower(schedule)]; ok {
			return fmt.Errorf("%s shortcuts are not supported; use %q instead", schedule, eq)
		}
		return fmt.Errorf("%s shortcuts are not supported; use a five-field cron expression", schedule)
	}
	parts := strings.Fields(schedule)
	if len(parts) != len(cronFields) {
		return fmt.Errorf("schedule must be a five-field cron expression, got %d fields", len(parts))
	}
	for i, part := range parts {
		for item := range strings.SplitSeq(part, ",") {
			if err := cronFields[i].validateItem(strings.ToLower(item)); err != nil {
				return fmt.Errorf("schedule %s field %q: %w", cronFields[i].name, part, err)
			}
		}
	}
	return nil
}

// validateItem checks one list item: base ("*", "n", "a-b") with an optional
// "/step".
func (f cronField) validateItem(item string) error {
	base, step, hasStep := strings.Cut(item, "/")
	if hasStep {
		if n, err := strconv.Atoi(step); err != nil || n < 1 {
			return errors.New("step must be a positive number")
		}
	}
	if base == "*" {
		return nil
	}
	lo, hi, isRange := strings.Cut(base, "-")
	a, err := f.value(lo)
	if err != nil {
		return err
	}
	if !isRange {
		return nil
	}
	b, err := f.value(hi)
	if err != nil {
		return err
	}
	if a > b {
		return errors.New("range start is after its end")
	}
	return nil
}

func (f cronField) value(s string) (int, error) {
	for i, n := range f.names {
		if s == n {
			return f.min + i, nil
		}
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("%q is not a number", s)
	}
	if n < f.min || n > f.max {
		return 0, fmt.Errorf("%d is outside %d-%d", n, f.min, f.max)
	}
	return n, nil
}

// --- Run-history page tokens ---

// encodeWorkflowRunCursor renders a cursor as an opaque, URL-safe token.
func encodeWorkflowRunCursor(c WorkflowRunCursor) string {
	raw := c.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + c.ID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeWorkflowRunCursor parses a page token; "" is the first page (nil).
func decodeWorkflowRunCursor(token string) (*WorkflowRunCursor, error) {
	if token == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, ErrInvalidPageToken
	}
	ts, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return nil, ErrInvalidPageToken
	}
	createdAt, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return nil, ErrInvalidPageToken
	}
	if _, err := uuid.Parse(id); err != nil {
		return nil, ErrInvalidPageToken
	}
	return &WorkflowRunCursor{CreatedAt: createdAt, ID: id}, nil
}

package mcpserver

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	workflowv1 "github.com/fairtier/workspace-api/proto/workflow/v1"
)

// workflowModel is the two sentences every workflow tool teaches: what a
// workflow is and where the step ids come from.
const workflowModel = `A workflow is a named, linear list of steps run one after the other on one optional cron schedule; the first failed step stops the run and the remaining steps are recorded as skipped (no branching, no parallelism, no parameters). Each step runs an existing pipeline or transformation, referenced by kind ("pipeline" or "transformation") and its id — find ids with list_pipelines / list_transformations.`

const listWorkflowsDesc = workflowModel + `

Lists every workflow with its steps (in run order), schedule (empty = manual only), whether it is enabled, and its last run's status.`

const getWorkflowDesc = workflowModel + `

Gets one workflow by id with its 10 most recent runs, newest first, each with its step runs: which step failed, its error message, and the pipeline or transformation run it produced (inspect that with get_pipeline / get_transformation).`

const listWorkflowRunsDesc = `Page through one workflow's run history, newest first, step runs included. Pass next_page_token from the previous page to continue; an empty next_page_token means there are no more runs. get_workflow already returns the 10 most recent runs — use this only to look further back.`

const triggerWorkflowDesc = workflowModel + `

Queues a run of one workflow now, outside its schedule (also when the workflow is disabled). Runs asynchronously: this returns the queued run immediately — poll get_workflow to follow it. If a run is already waiting to start, that run is returned instead of queueing a second one. Each step runs its pipeline (fetching from the source, writing tables) or transformation (rebuilding models).`

const createWorkflowDesc = workflowModel + `

Creates a workflow, enabled. Steps are given in run order (1 to 20, no pipeline or transformation twice). schedule is a five-field cron expression or a macro such as @daily; omit it for manual runs only.`

const updateWorkflowDesc = workflowModel + `

Changes one workflow. Only the fields you pass change; the rest are kept. steps, when passed, replaces the whole step list (in run order). Set enabled to false to stop the schedule without losing the definition; set schedule to "" for manual runs only. There is deliberately no delete tool — a person deletes workflows in the Console.`

type workflowStepIn struct {
	Kind  string `json:"kind" jsonschema:"pipeline or transformation"`
	RefID string `json:"ref_id" jsonschema:"the pipeline's or transformation's id, as list_pipelines / list_transformations report it"`
}

type workflowStep struct {
	Kind  string `json:"kind"`
	RefID string `json:"ref_id"`
	Name  string `json:"name,omitempty"`
}

type workflowSummary struct {
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	Description   string         `json:"description,omitempty"`
	Schedule      string         `json:"schedule,omitempty"`
	Enabled       bool           `json:"enabled"`
	Steps         []workflowStep `json:"steps"`
	LastRunStatus string         `json:"last_run_status,omitempty"`
	LastRunAt     string         `json:"last_run_at,omitempty"`
}

type workflowStepRun struct {
	Position            int32  `json:"position"`
	Kind                string `json:"kind"`
	RefID               string `json:"ref_id"`
	Name                string `json:"name,omitempty"`
	Status              string `json:"status"`
	PipelineRunID       string `json:"pipeline_run_id,omitempty"`
	TransformationRunID string `json:"transformation_run_id,omitempty"`
	StartedAt           string `json:"started_at,omitempty"`
	CompletedAt         string `json:"completed_at,omitempty"`
	ErrorMessage        string `json:"error_message,omitempty"`
}

type workflowRun struct {
	ID           string            `json:"id"`
	Status       string            `json:"status"`
	Trigger      string            `json:"trigger,omitempty"`
	CreatedAt    string            `json:"created_at,omitempty"`
	StartedAt    string            `json:"started_at,omitempty"`
	CompletedAt  string            `json:"completed_at,omitempty"`
	ErrorMessage string            `json:"error_message,omitempty"`
	StepRuns     []workflowStepRun `json:"step_runs,omitempty"`
}

type listWorkflowsOut struct {
	Workflows []workflowSummary `json:"workflows"`
}

type getWorkflowOut struct {
	Workflow   workflowSummary `json:"workflow"`
	RecentRuns []workflowRun   `json:"recent_runs"`
}

type listWorkflowRunsIn struct {
	WorkflowID string `json:"workflow_id" jsonschema:"the workflow's id, as list_workflows reports it"`
	PageSize   int32  `json:"page_size,omitempty" jsonschema:"runs per page; default 20, at most 100"`
	PageToken  string `json:"page_token,omitempty" jsonschema:"next_page_token from the previous page; omit for the first page"`
}

type listWorkflowRunsOut struct {
	Runs          []workflowRun `json:"runs"`
	NextPageToken string        `json:"next_page_token,omitempty"`
}

type workflowOut struct {
	Workflow workflowSummary `json:"workflow"`
}

type workflowRunOut struct {
	Run workflowRun `json:"run"`
}

type createWorkflowIn struct {
	Name        string           `json:"name" jsonschema:"unique within the workspace"`
	Description string           `json:"description,omitempty" jsonschema:"what the workflow is for"`
	Schedule    string           `json:"schedule,omitempty" jsonschema:"cron expression (five fields) or a macro such as @daily; omit for manual runs only"`
	Steps       []workflowStepIn `json:"steps" jsonschema:"the steps in run order"`
}

type updateWorkflowIn struct {
	ID          string            `json:"id" jsonschema:"the workflow's id, as list_workflows reports it"`
	Name        *string           `json:"name,omitempty" jsonschema:"new name; omit to keep"`
	Description *string           `json:"description,omitempty" jsonschema:"new description; omit to keep"`
	Schedule    *string           `json:"schedule,omitempty" jsonschema:"new cron expression or macro; \"\" for manual runs only; omit to keep"`
	Enabled     *bool             `json:"enabled,omitempty" jsonschema:"false stops scheduled runs; omit to keep"`
	Steps       *[]workflowStepIn `json:"steps,omitempty" jsonschema:"the complete new step list in run order; omit to keep"`
}

func workflowSummaryOf(w *workflowv1.Workflow) workflowSummary {
	if w == nil {
		return workflowSummary{}
	}
	out := workflowSummary{
		ID: w.Id, Name: w.Name, Description: w.Description, Schedule: w.Schedule, Enabled: w.Enabled,
		Steps: make([]workflowStep, 0, len(w.Steps)), LastRunStatus: w.LastRunStatus, LastRunAt: w.LastRunAt,
	}
	for _, s := range w.Steps {
		out.Steps = append(out.Steps, workflowStep{Kind: s.Kind, RefID: s.RefId, Name: s.Name})
	}
	return out
}

func workflowRunOf(r *workflowv1.WorkflowRun) workflowRun {
	if r == nil {
		return workflowRun{}
	}
	out := workflowRun{
		ID: r.Id, Status: r.Status, Trigger: r.Trigger, CreatedAt: r.CreatedAt, StartedAt: r.StartedAt,
		CompletedAt: r.CompletedAt, ErrorMessage: truncate(redactString(r.ErrorMessage), maxErrorMessageLen),
	}
	for _, sr := range r.StepRuns {
		out.StepRuns = append(out.StepRuns, workflowStepRun{
			Position: sr.Position, Kind: sr.Kind, RefID: sr.RefId, Name: sr.Name, Status: sr.Status,
			PipelineRunID: sr.PipelineRunId, TransformationRunID: sr.TransformationRunId,
			StartedAt: sr.StartedAt, CompletedAt: sr.CompletedAt,
			ErrorMessage: truncate(redactString(sr.ErrorMessage), maxErrorMessageLen),
		})
	}
	return out
}

func workflowRunsOf(runs []*workflowv1.WorkflowRun) []workflowRun {
	out := make([]workflowRun, 0, len(runs))
	for _, r := range runs {
		out = append(out, workflowRunOf(r))
	}
	return out
}

func stepsToPB(steps []workflowStepIn) []*workflowv1.WorkflowStep {
	out := make([]*workflowv1.WorkflowStep, 0, len(steps))
	for _, s := range steps { // the service orders steps by slice position
		out = append(out, &workflowv1.WorkflowStep{Kind: s.Kind, RefId: s.RefID})
	}
	return out
}

func listWorkflows(ctx context.Context, b Backends, _ struct{}) (listWorkflowsOut, error) {
	resp, err := b.Workflows.ListWorkflows(ctx, connect.NewRequest(&workflowv1.ListWorkflowsRequest{}))
	if err != nil {
		return listWorkflowsOut{}, err
	}
	out := listWorkflowsOut{Workflows: make([]workflowSummary, 0, len(resp.Msg.Workflows))}
	for _, w := range resp.Msg.Workflows {
		out.Workflows = append(out.Workflows, workflowSummaryOf(w))
	}
	return out, nil
}

func getWorkflow(ctx context.Context, b Backends, in idIn) (getWorkflowOut, error) {
	resp, err := b.Workflows.GetWorkflow(ctx, connect.NewRequest(&workflowv1.GetWorkflowRequest{Id: in.ID}))
	if err != nil {
		return getWorkflowOut{}, err
	}
	return getWorkflowOut{Workflow: workflowSummaryOf(resp.Msg.Workflow), RecentRuns: workflowRunsOf(resp.Msg.RecentRuns)}, nil
}

func listWorkflowRuns(ctx context.Context, b Backends, in listWorkflowRunsIn) (listWorkflowRunsOut, error) {
	resp, err := b.Workflows.ListWorkflowRuns(ctx, connect.NewRequest(&workflowv1.ListWorkflowRunsRequest{
		WorkflowId: in.WorkflowID, PageSize: in.PageSize, PageToken: in.PageToken,
	}))
	if err != nil {
		return listWorkflowRunsOut{}, err
	}
	return listWorkflowRunsOut{Runs: workflowRunsOf(resp.Msg.Runs), NextPageToken: resp.Msg.NextPageToken}, nil
}

func triggerWorkflow(ctx context.Context, b Backends, in idIn) (workflowRunOut, error) {
	resp, err := b.Workflows.TriggerWorkflow(ctx, connect.NewRequest(&workflowv1.TriggerWorkflowRequest{Id: in.ID}))
	if err != nil {
		return workflowRunOut{}, err
	}
	return workflowRunOut{Run: workflowRunOf(resp.Msg.Run)}, nil
}

func createWorkflow(ctx context.Context, b Backends, in createWorkflowIn) (workflowOut, error) {
	resp, err := b.Workflows.CreateWorkflow(ctx, connect.NewRequest(&workflowv1.CreateWorkflowRequest{
		Name: in.Name, Description: in.Description, Schedule: in.Schedule, Steps: stepsToPB(in.Steps),
	}))
	if err != nil {
		return workflowOut{}, err
	}
	return workflowOut{Workflow: workflowSummaryOf(resp.Msg.Workflow)}, nil
}

// updateWorkflow is a read-modify-write: UpdateWorkflow replaces the whole
// definition, but an agent should be able to flip one flag without
// restating the rest. Two concurrent edits are last-writer-wins, as they
// are in the Console.
func updateWorkflow(ctx context.Context, b Backends, in updateWorkflowIn) (workflowOut, error) {
	if in.ID == "" {
		return workflowOut{}, errors.New("id is required")
	}
	cur, err := b.Workflows.GetWorkflow(ctx, connect.NewRequest(&workflowv1.GetWorkflowRequest{Id: in.ID}))
	if err != nil {
		return workflowOut{}, err
	}
	w := cur.Msg.Workflow
	if w == nil {
		return workflowOut{}, errors.New("workflow not found")
	}
	req := &workflowv1.UpdateWorkflowRequest{
		Id: in.ID, Name: w.Name, Description: w.Description, Schedule: w.Schedule, Enabled: w.Enabled, Steps: w.Steps,
	}
	if in.Name != nil {
		req.Name = *in.Name
	}
	if in.Description != nil {
		req.Description = *in.Description
	}
	if in.Schedule != nil {
		req.Schedule = *in.Schedule
	}
	if in.Enabled != nil {
		req.Enabled = *in.Enabled
	}
	if in.Steps != nil {
		req.Steps = stepsToPB(*in.Steps)
	}
	resp, err := b.Workflows.UpdateWorkflow(ctx, connect.NewRequest(req))
	if err != nil {
		return workflowOut{}, err
	}
	return workflowOut{Workflow: workflowSummaryOf(resp.Msg.Workflow)}, nil
}

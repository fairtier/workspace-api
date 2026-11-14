package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	"github.com/fairtier/workspace-api/core"
	workflowv1 "github.com/fairtier/workspace-api/proto/workflow/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// WorkflowServer implements the ConnectRPC WorkflowService handler.
//
// User-facing RPCs require JWT auth (via auth interceptor on the mux), exactly
// like TransformationService's. Worker-facing RPCs (GetWorkflowConfigs,
// ReportWorkflowRun) are served only by a NewInternalWorkflowServer instance
// on the internal mux, with a tenant-bound service token (see workerAuth).
type WorkflowServer struct {
	Service *workspace.WorkflowService

	worker workerAuth
}

// NewInternalWorkflowServer builds the worker-facing WorkflowServer for the
// internal mux.
func NewInternalWorkflowServer(svc *workspace.WorkflowService) *WorkflowServer {
	return &WorkflowServer{Service: svc, worker: newWorkerAuth()}
}

func requireCaller(ctx context.Context) (core.UserID, error) {
	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return "", connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}
	return callerID, nil
}

func requireID(id string) error {
	if id == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}
	return nil
}

// --- User-facing RPCs ---

func (s *WorkflowServer) CreateWorkflow(ctx context.Context, req *connect.Request[workflowv1.CreateWorkflowRequest]) (*connect.Response[workflowv1.CreateWorkflowResponse], error) {
	callerID, err := requireCaller(ctx)
	if err != nil {
		return nil, err
	}
	w := &workspace.Workflow{
		Name:        req.Msg.Name,
		Description: req.Msg.Description,
		Schedule:    req.Msg.Schedule,
		// Absent means enabled: clients written before the field existed
		// create enabled workflows.
		Enabled: req.Msg.Enabled == nil || *req.Msg.Enabled,
		Steps:   stepsFromPB(req.Msg.Steps),
	}
	result, err := s.Service.CreateWorkflow(ctx, callerID, w)
	if err != nil {
		return nil, domainError(err)
	}
	return connect.NewResponse(&workflowv1.CreateWorkflowResponse{Workflow: workflowToPB(result)}), nil
}

func (s *WorkflowServer) ListWorkflows(ctx context.Context, _ *connect.Request[workflowv1.ListWorkflowsRequest]) (*connect.Response[workflowv1.ListWorkflowsResponse], error) {
	callerID, err := requireCaller(ctx)
	if err != nil {
		return nil, err
	}
	workflows, err := s.Service.ListWorkflows(ctx, callerID)
	if err != nil {
		return nil, domainError(err)
	}
	out := make([]*workflowv1.Workflow, 0, len(workflows))
	for i := range workflows {
		out = append(out, workflowToPB(&workflows[i]))
	}
	return connect.NewResponse(&workflowv1.ListWorkflowsResponse{Workflows: out}), nil
}

func (s *WorkflowServer) GetWorkflow(ctx context.Context, req *connect.Request[workflowv1.GetWorkflowRequest]) (*connect.Response[workflowv1.GetWorkflowResponse], error) {
	callerID, err := requireCaller(ctx)
	if err != nil {
		return nil, err
	}
	if err := requireID(req.Msg.Id); err != nil {
		return nil, err
	}
	w, runs, err := s.Service.GetWorkflow(ctx, callerID, workspace.WorkflowID(req.Msg.Id))
	if err != nil {
		return nil, domainError(err)
	}
	return connect.NewResponse(&workflowv1.GetWorkflowResponse{
		Workflow:   workflowToPB(w),
		RecentRuns: workflowRunsToPB(runs),
	}), nil
}

func (s *WorkflowServer) UpdateWorkflow(ctx context.Context, req *connect.Request[workflowv1.UpdateWorkflowRequest]) (*connect.Response[workflowv1.UpdateWorkflowResponse], error) {
	callerID, err := requireCaller(ctx)
	if err != nil {
		return nil, err
	}
	if err := requireID(req.Msg.Id); err != nil {
		return nil, err
	}
	w := &workspace.Workflow{
		ID:          workspace.WorkflowID(req.Msg.Id),
		Name:        req.Msg.Name,
		Description: req.Msg.Description,
		Schedule:    req.Msg.Schedule,
		Enabled:     req.Msg.Enabled,
		Steps:       stepsFromPB(req.Msg.Steps),
	}
	result, err := s.Service.UpdateWorkflow(ctx, callerID, w)
	if err != nil {
		return nil, domainError(err)
	}
	return connect.NewResponse(&workflowv1.UpdateWorkflowResponse{Workflow: workflowToPB(result)}), nil
}

func (s *WorkflowServer) SetWorkflowEnabled(ctx context.Context, req *connect.Request[workflowv1.SetWorkflowEnabledRequest]) (*connect.Response[workflowv1.SetWorkflowEnabledResponse], error) {
	callerID, err := requireCaller(ctx)
	if err != nil {
		return nil, err
	}
	if err := requireID(req.Msg.Id); err != nil {
		return nil, err
	}
	result, err := s.Service.SetWorkflowEnabled(ctx, callerID, workspace.WorkflowID(req.Msg.Id), req.Msg.Enabled)
	if err != nil {
		return nil, domainError(err)
	}
	return connect.NewResponse(&workflowv1.SetWorkflowEnabledResponse{Workflow: workflowToPB(result)}), nil
}

func (s *WorkflowServer) DeleteWorkflow(ctx context.Context, req *connect.Request[workflowv1.DeleteWorkflowRequest]) (*connect.Response[workflowv1.DeleteWorkflowResponse], error) {
	callerID, err := requireCaller(ctx)
	if err != nil {
		return nil, err
	}
	if err := requireID(req.Msg.Id); err != nil {
		return nil, err
	}
	if err := s.Service.DeleteWorkflow(ctx, callerID, workspace.WorkflowID(req.Msg.Id)); err != nil {
		return nil, domainError(err)
	}
	return connect.NewResponse(&workflowv1.DeleteWorkflowResponse{}), nil
}

func (s *WorkflowServer) TriggerWorkflow(ctx context.Context, req *connect.Request[workflowv1.TriggerWorkflowRequest]) (*connect.Response[workflowv1.TriggerWorkflowResponse], error) {
	callerID, err := requireCaller(ctx)
	if err != nil {
		return nil, err
	}
	if err := requireID(req.Msg.Id); err != nil {
		return nil, err
	}
	run, err := s.Service.TriggerWorkflow(ctx, callerID, workspace.WorkflowID(req.Msg.Id))
	if err != nil {
		return nil, domainError(err)
	}
	return connect.NewResponse(&workflowv1.TriggerWorkflowResponse{Run: workflowRunToPB(run)}), nil
}

func (s *WorkflowServer) ListWorkflowRuns(ctx context.Context, req *connect.Request[workflowv1.ListWorkflowRunsRequest]) (*connect.Response[workflowv1.ListWorkflowRunsResponse], error) {
	callerID, err := requireCaller(ctx)
	if err != nil {
		return nil, err
	}
	if req.Msg.WorkflowId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("workflow_id is required"))
	}
	runs, next, err := s.Service.ListWorkflowRuns(ctx, callerID, workspace.WorkflowID(req.Msg.WorkflowId),
		int(req.Msg.PageSize), req.Msg.PageToken)
	if err != nil {
		return nil, domainError(err)
	}
	return connect.NewResponse(&workflowv1.ListWorkflowRunsResponse{
		Runs:          workflowRunsToPB(runs),
		NextPageToken: next,
	}), nil
}

// --- Worker-facing RPCs (tenant-bound service auth, internal mux only) ---

func (s *WorkflowServer) GetWorkflowConfigs(ctx context.Context, req *connect.Request[workflowv1.GetWorkflowConfigsRequest]) (*connect.Response[workflowv1.GetWorkflowConfigsResponse], error) {
	if req.Msg.CustomerSlug == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("customer_slug is required"))
	}
	callerSlug, err := s.worker.callerSlug(ctx)
	if err != nil {
		return nil, err
	}
	if callerSlug != req.Msg.CustomerSlug {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("token tenant does not match customer_slug"))
	}

	workflows, err := s.Service.GetWorkflowConfigs(ctx, req.Msg.CustomerSlug)
	if err != nil {
		return nil, domainError(err)
	}
	out := make([]*workflowv1.WorkflowConfigItem, 0, len(workflows))
	for i := range workflows {
		out = append(out, workflowConfigToPB(&workflows[i]))
	}
	return connect.NewResponse(&workflowv1.GetWorkflowConfigsResponse{Workflows: out}), nil
}

func (s *WorkflowServer) ReportWorkflowRun(ctx context.Context, req *connect.Request[workflowv1.ReportWorkflowRunRequest]) (*connect.Response[workflowv1.ReportWorkflowRunResponse], error) {
	callerSlug, err := s.worker.callerSlug(ctx)
	if err != nil {
		return nil, err
	}
	run, err := workflowRunFromReport(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := s.Service.ReportWorkflowRun(ctx, callerSlug, run); err != nil {
		return nil, domainError(err)
	}
	return connect.NewResponse(&workflowv1.ReportWorkflowRunResponse{}), nil
}

// --- Conversion helpers ---

func stepsFromPB(steps []*workflowv1.WorkflowStep) []workspace.WorkflowStep {
	out := make([]workspace.WorkflowStep, 0, len(steps))
	for _, s := range steps {
		out = append(out, workspace.NewWorkflowStep(workspace.WorkflowStepKind(s.Kind), s.RefId))
	}
	return out
}

// workflowRunFromReport parses the identifiers and timestamps of a report;
// the service validates the rest.
func workflowRunFromReport(msg *workflowv1.ReportWorkflowRunRequest) (*workspace.WorkflowRun, error) {
	workflowUUID, err := uuid.Parse(msg.WorkflowId)
	if err != nil {
		return nil, fmt.Errorf("workflow_id: %w", err)
	}
	startedAt, err := parseTimePtr(msg.StartedAt)
	if err != nil {
		return nil, err
	}
	completedAt, err := parseTimePtr(msg.CompletedAt)
	if err != nil {
		return nil, err
	}
	run := &workspace.WorkflowRun{
		ID:           msg.RunId,
		WorkflowID:   workspace.WorkflowID(workflowUUID.String()),
		Status:       msg.Status,
		Trigger:      msg.Trigger,
		StartedAt:    startedAt,
		CompletedAt:  completedAt,
		ErrorMessage: msg.ErrorMessage,
	}
	for _, sr := range msg.StepRuns {
		stepRun, err := stepRunFromReport(sr)
		if err != nil {
			return nil, fmt.Errorf("step_runs[%d]: %w", sr.Position, err)
		}
		run.StepRuns = append(run.StepRuns, stepRun)
	}
	return run, nil
}

func stepRunFromReport(sr *workflowv1.WorkflowStepRunReport) (workspace.WorkflowStepRun, error) {
	startedAt, err := parseTimePtr(sr.StartedAt)
	if err != nil {
		return workspace.WorkflowStepRun{}, err
	}
	completedAt, err := parseTimePtr(sr.CompletedAt)
	if err != nil {
		return workspace.WorkflowStepRun{}, err
	}
	return workspace.WorkflowStepRun{
		Position:            int(sr.Position),
		Kind:                workspace.WorkflowStepKind(sr.Kind),
		RefID:               sr.RefId,
		Name:                sr.Name,
		Status:              sr.Status,
		PipelineRunID:       sr.PipelineRunId,
		TransformationRunID: sr.TransformationRunId,
		StartedAt:           startedAt,
		CompletedAt:         completedAt,
		ErrorMessage:        sr.ErrorMessage,
	}, nil
}

func workflowToPB(w *workspace.Workflow) *workflowv1.Workflow {
	steps := make([]*workflowv1.WorkflowStep, 0, len(w.Steps))
	for _, s := range w.Steps {
		steps = append(steps, &workflowv1.WorkflowStep{
			Kind:     string(s.Kind),
			RefId:    s.RefID(),
			Name:     s.Name,
			Position: int32(s.Position),
		})
	}
	return &workflowv1.Workflow{
		Id:                     string(w.ID),
		CustomerSlug:           w.CustomerSlug,
		Name:                   w.Name,
		Description:            w.Description,
		Schedule:               w.Schedule,
		Enabled:                w.Enabled,
		Steps:                  steps,
		CreatedAt:              formatTime(&w.CreatedAt),
		UpdatedAt:              formatTime(&w.UpdatedAt),
		LastRunAt:              formatTime(w.LastRunAt),
		LastRunStatus:          w.LastRunStatus,
		LastRunError:           w.LastRunError,
		LastFailedStepPosition: optionalInt32(w.LastFailedStepPosition),
		LastFailedStepName:     w.LastFailedStepName,
	}
}

// optionalInt32 converts a step position, keeping its absence.
func optionalInt32(p *int) *int32 {
	if p == nil {
		return nil
	}
	v := int32(*p) //nolint:gosec // a step position, < MaxWorkflowSteps
	return &v
}

func workflowConfigToPB(w *workspace.Workflow) *workflowv1.WorkflowConfigItem {
	steps := make([]*workflowv1.WorkflowConfigStep, 0, len(w.Steps))
	for _, s := range w.Steps {
		steps = append(steps, &workflowv1.WorkflowConfigStep{
			Position:   int32(s.Position),
			Kind:       string(s.Kind),
			RefId:      s.RefID(),
			Name:       s.Name,
			RefEnabled: s.RefEnabled,
		})
	}
	return &workflowv1.WorkflowConfigItem{
		Id:            string(w.ID),
		Name:          w.Name,
		Schedule:      w.Schedule,
		Enabled:       w.Enabled,
		Steps:         steps,
		TriggerNow:    w.TriggerNow,
		PendingRunId:  w.PendingRunID,
		LastRunAt:     formatTime(w.LastRunAt),
		LastRunStatus: w.LastRunStatus,
	}
}

func workflowRunsToPB(runs []workspace.WorkflowRun) []*workflowv1.WorkflowRun {
	out := make([]*workflowv1.WorkflowRun, 0, len(runs))
	for i := range runs {
		out = append(out, workflowRunToPB(&runs[i]))
	}
	return out
}

func workflowRunToPB(r *workspace.WorkflowRun) *workflowv1.WorkflowRun {
	stepRuns := make([]*workflowv1.WorkflowStepRun, 0, len(r.StepRuns))
	for _, sr := range r.StepRuns {
		stepRuns = append(stepRuns, &workflowv1.WorkflowStepRun{
			Position:            int32(sr.Position),
			Kind:                string(sr.Kind),
			RefId:               sr.RefID,
			Name:                sr.Name,
			Status:              sr.Status,
			PipelineRunId:       sr.PipelineRunID,
			TransformationRunId: sr.TransformationRunID,
			StartedAt:           formatTime(sr.StartedAt),
			CompletedAt:         formatTime(sr.CompletedAt),
			ErrorMessage:        sr.ErrorMessage,
		})
	}
	return &workflowv1.WorkflowRun{
		Id:           r.ID,
		WorkflowId:   string(r.WorkflowID),
		Status:       r.Status,
		Trigger:      r.Trigger,
		StartedAt:    formatTime(r.StartedAt),
		CompletedAt:  formatTime(r.CompletedAt),
		ErrorMessage: r.ErrorMessage,
		CreatedAt:    formatTime(&r.CreatedAt),
		StepRuns:     stepRuns,
	}
}

// formatTime renders an RFC 3339 UTC timestamp, "" for nil or zero.
func formatTime(t *time.Time) string {
	if t == nil || t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

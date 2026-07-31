package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	transformationv1 "github.com/fairtier/workspace-api/proto/transformation/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// TransformationServer implements the ConnectRPC TransformationService
// handler (git-backed dbt transformations).
//
// User-facing RPCs require JWT auth (via auth interceptor on the mux).
// Worker-facing RPCs (GetTransformationConfigs, ReportTransformationRun) are
// called by the dlt-worker on the internal mux with a tenant-bound Casdoor
// service token, same as the pipeline RPCs — and, same as those, they serve
// only from a NewInternalTransformationServer instance (see workerAuth).
type TransformationServer struct {
	Service *workspace.TransformationService

	worker workerAuth
}

// NewInternalTransformationServer builds the worker-facing
// TransformationServer for the internal mux, in the given INTERNAL_AUTH_MODE.
func NewInternalTransformationServer(svc *workspace.TransformationService, internalAuthMode string) *TransformationServer {
	return &TransformationServer{Service: svc, worker: newWorkerAuth(internalAuthMode)}
}

// --- User-facing RPCs ---

func (s *TransformationServer) CreateTransformation(ctx context.Context, req *connect.Request[transformationv1.CreateTransformationRequest]) (*connect.Response[transformationv1.CreateTransformationResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	if req.Msg.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("name is required"))
	}
	if req.Msg.TriggerAfterPipelineId != "" {
		if _, err := uuid.Parse(req.Msg.TriggerAfterPipelineId); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("trigger_after_pipeline_id: %w", err))
		}
	}

	t := &workspace.Transformation{
		Name:                   req.Msg.Name,
		RepoURL:                req.Msg.RepoUrl,
		RepoRef:                req.Msg.RepoRef,
		GitCredentials:         jsonOrEmpty(req.Msg.GitCredentials),
		Schedule:               req.Msg.Schedule,
		TriggerAfterPipelineID: workspace.PipelineID(req.Msg.TriggerAfterPipelineId),
		DBTSelector:            req.Msg.DbtSelector,
	}

	result, err := s.Service.CreateTransformation(ctx, callerID, t)
	if err != nil {
		return nil, domainError(err)
	}

	return connect.NewResponse(&transformationv1.CreateTransformationResponse{
		Transformation: transformationToPB(result),
	}), nil
}

func (s *TransformationServer) ListTransformations(ctx context.Context, _ *connect.Request[transformationv1.ListTransformationsRequest]) (*connect.Response[transformationv1.ListTransformationsResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	transformations, err := s.Service.ListTransformations(ctx, callerID)
	if err != nil {
		return nil, domainError(err)
	}

	out := make([]*transformationv1.Transformation, 0, len(transformations))
	for _, t := range transformations {
		out = append(out, transformationToPB(&t))
	}

	return connect.NewResponse(&transformationv1.ListTransformationsResponse{
		Transformations: out,
	}), nil
}

func (s *TransformationServer) GetTransformation(ctx context.Context, req *connect.Request[transformationv1.GetTransformationRequest]) (*connect.Response[transformationv1.GetTransformationResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}

	t, runs, err := s.Service.GetTransformation(ctx, callerID, workspace.TransformationID(req.Msg.Id))
	if err != nil {
		return nil, domainError(err)
	}

	runsPB := make([]*transformationv1.TransformationRun, 0, len(runs))
	for _, r := range runs {
		runsPB = append(runsPB, transformationRunToPB(&r))
	}

	// Mirror the list view's last-run fields from the most recent run so the
	// detail page is consistent with the list.
	if len(runs) > 0 {
		latest := runs[0]
		t.LastRunStatus = latest.Status
		if latest.StartedAt != nil {
			t.LastRunTime = latest.StartedAt
		} else {
			t.LastRunTime = &latest.CreatedAt
		}
	}

	return connect.NewResponse(&transformationv1.GetTransformationResponse{
		Transformation: transformationToPB(t),
		RecentRuns:     runsPB,
	}), nil
}

func (s *TransformationServer) UpdateTransformation(ctx context.Context, req *connect.Request[transformationv1.UpdateTransformationRequest]) (*connect.Response[transformationv1.UpdateTransformationResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}
	if req.Msg.TriggerAfterPipelineId != "" {
		if _, err := uuid.Parse(req.Msg.TriggerAfterPipelineId); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("trigger_after_pipeline_id: %w", err))
		}
	}

	t := &workspace.Transformation{
		ID:                     workspace.TransformationID(req.Msg.Id),
		Name:                   req.Msg.Name,
		RepoURL:                req.Msg.RepoUrl,
		RepoRef:                req.Msg.RepoRef,
		GitCredentials:         jsonOrEmpty(req.Msg.GitCredentials),
		Schedule:               req.Msg.Schedule,
		TriggerAfterPipelineID: workspace.PipelineID(req.Msg.TriggerAfterPipelineId),
		DBTSelector:            req.Msg.DbtSelector,
		Enabled:                req.Msg.Enabled,
	}

	result, err := s.Service.UpdateTransformation(ctx, callerID, t)
	if err != nil {
		return nil, domainError(err)
	}

	return connect.NewResponse(&transformationv1.UpdateTransformationResponse{
		Transformation: transformationToPB(result),
	}), nil
}

func (s *TransformationServer) DeleteTransformation(ctx context.Context, req *connect.Request[transformationv1.DeleteTransformationRequest]) (*connect.Response[transformationv1.DeleteTransformationResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}

	if err := s.Service.DeleteTransformation(ctx, callerID, workspace.TransformationID(req.Msg.Id)); err != nil {
		return nil, domainError(err)
	}

	return connect.NewResponse(&transformationv1.DeleteTransformationResponse{}), nil
}

func (s *TransformationServer) TriggerTransformation(ctx context.Context, req *connect.Request[transformationv1.TriggerTransformationRequest]) (*connect.Response[transformationv1.TriggerTransformationResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}

	run, err := s.Service.TriggerTransformation(ctx, callerID, workspace.TransformationID(req.Msg.Id))
	if err != nil {
		return nil, domainError(err)
	}

	return connect.NewResponse(&transformationv1.TriggerTransformationResponse{
		Run: transformationRunToPB(run),
	}), nil
}

// --- Worker-facing RPCs (tenant-bound service auth, internal mux only) ---

func (s *TransformationServer) GetTransformationConfigs(ctx context.Context, req *connect.Request[transformationv1.GetTransformationConfigsRequest]) (*connect.Response[transformationv1.GetTransformationConfigsResponse], error) {
	if req.Msg.CustomerSlug == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("customer_slug is required"))
	}
	callerSlug, err := s.worker.callerSlug(ctx)
	if err != nil {
		return nil, err
	}
	if callerSlug != "" && callerSlug != req.Msg.CustomerSlug {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("token tenant does not match customer_slug"))
	}

	transformations, err := s.Service.GetEnabledTransformations(ctx, req.Msg.CustomerSlug)
	if err != nil {
		return nil, domainError(err)
	}

	out := make([]*transformationv1.TransformationConfigItem, 0, len(transformations))
	for _, t := range transformations {
		var lastRunAt string
		if t.LastRunAt != nil {
			lastRunAt = t.LastRunAt.UTC().Format(time.RFC3339)
		}
		out = append(out, &transformationv1.TransformationConfigItem{
			Id:                     string(t.ID),
			Name:                   t.Name,
			RepoUrl:                t.RepoURL,
			RepoRef:                t.RepoRef,
			GitCredentials:         string(t.GitCredentials),
			Schedule:               t.Schedule,
			TriggerAfterPipelineId: string(t.TriggerAfterPipelineID),
			DbtSelector:            t.DBTSelector,
			Enabled:                t.Enabled,
			TriggerNow:             t.TriggerNow,
			PendingRunId:           t.PendingRunID,
			LastRunAt:              lastRunAt,
		})
	}

	return connect.NewResponse(&transformationv1.GetTransformationConfigsResponse{
		Transformations: out,
	}), nil
}

func (s *TransformationServer) ReportTransformationRun(ctx context.Context, req *connect.Request[transformationv1.ReportTransformationRunRequest]) (*connect.Response[transformationv1.ReportTransformationRunResponse], error) {
	callerSlug, err := s.worker.callerSlug(ctx)
	if err != nil {
		return nil, err
	}
	if req.Msg.TransformationId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("transformation_id is required"))
	}
	transformationUUID, err := uuid.Parse(req.Msg.TransformationId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("transformation_id: %w", err))
	}
	if req.Msg.Status == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("status is required"))
	}
	var runID string
	if req.Msg.RunId != "" {
		parsed, err := uuid.Parse(req.Msg.RunId)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("run_id: %w", err))
		}
		runID = parsed.String()
	}

	startedAt, err := parseTimePtr(req.Msg.StartedAt)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	completedAt, err := parseTimePtr(req.Msg.CompletedAt)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	run := &workspace.TransformationRun{
		ID:               runID,
		TransformationID: workspace.TransformationID(transformationUUID.String()),
		Status:           req.Msg.Status,
		StartedAt:        startedAt,
		CompletedAt:      completedAt,
		CommitSHA:        req.Msg.CommitSha,
		ModelsTotal:      req.Msg.ModelsTotal,
		ModelsFailed:     req.Msg.ModelsFailed,
		TestsTotal:       req.Msg.TestsTotal,
		TestsFailed:      req.Msg.TestsFailed,
		ModelResults:     jsonArrayOrEmptyPB(req.Msg.ModelResults),
		ErrorMessage:     req.Msg.ErrorMessage,
	}

	if err := s.Service.ReportTransformationRun(ctx, callerSlug, run); err != nil {
		return nil, domainError(err)
	}

	return connect.NewResponse(&transformationv1.ReportTransformationRunResponse{}), nil
}

// --- Helpers ---

func transformationToPB(t *workspace.Transformation) *transformationv1.Transformation {
	pb := &transformationv1.Transformation{
		Id:                     string(t.ID),
		CustomerSlug:           t.CustomerSlug,
		Name:                   t.Name,
		RepoUrl:                t.RepoURL,
		RepoRef:                t.RepoRef,
		Schedule:               t.Schedule,
		TriggerAfterPipelineId: string(t.TriggerAfterPipelineID),
		DbtSelector:            t.DBTSelector,
		Enabled:                t.Enabled,
		CreatedAt:              t.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:              t.UpdatedAt.UTC().Format(time.RFC3339),
		LastRunStatus:          t.LastRunStatus,
	}
	if t.LastRunTime != nil {
		pb.LastRunAt = t.LastRunTime.UTC().Format(time.RFC3339)
	}
	return pb
}

func transformationRunToPB(r *workspace.TransformationRun) *transformationv1.TransformationRun {
	pb := &transformationv1.TransformationRun{
		Id:               r.ID,
		TransformationId: string(r.TransformationID),
		Status:           r.Status,
		CommitSha:        r.CommitSHA,
		ModelsTotal:      r.ModelsTotal,
		ModelsFailed:     r.ModelsFailed,
		TestsTotal:       r.TestsTotal,
		TestsFailed:      r.TestsFailed,
		ModelResults:     string(r.ModelResults),
		ErrorMessage:     r.ErrorMessage,
		CreatedAt:        r.CreatedAt.UTC().Format(time.RFC3339),
	}
	if r.StartedAt != nil {
		pb.StartedAt = r.StartedAt.UTC().Format(time.RFC3339)
	}
	if r.CompletedAt != nil {
		pb.CompletedAt = r.CompletedAt.UTC().Format(time.RFC3339)
	}
	return pb
}

// jsonArrayOrEmptyPB returns the raw JSON bytes from a string, defaulting to "[]".
func jsonArrayOrEmptyPB(s string) []byte {
	if s == "" {
		return []byte("[]")
	}
	return []byte(s)
}

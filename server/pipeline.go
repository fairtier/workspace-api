package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	pipelinev1 "github.com/fairtier/workspace-api/proto/pipeline/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// PipelineServer implements the ConnectRPC PipelineService handler.
//
// User-facing RPCs require JWT auth (via auth interceptor on the mux).
// Worker-facing RPCs (GetPipelineConfigs, ReportPipelineRun) are called by the
// dlt-worker on the internal mux with a tenant-bound Casdoor service token
// (see NewInternalAuthInterceptor); the handlers bind the token's tenant to
// the requested customer. A PipelineServer built with a plain struct literal
// serves the user-facing half only — use NewInternalPipelineServer for the
// internal mux (see workerAuth).
type PipelineServer struct {
	Service *workspace.PipelineService
	// FileDrop backs the file_upload RPCs (ListUploadedFiles,
	// DeleteUploadedFile). Nil returns UNIMPLEMENTED, keeping the internal
	// mux (which never serves these) honest.
	FileDrop *workspace.FileDropService

	worker workerAuth
}

// NewInternalPipelineServer builds the worker-facing PipelineServer for the
// internal mux, in the given INTERNAL_AUTH_MODE. No FileDrop: the worker only
// polls configs and reports runs.
func NewInternalPipelineServer(svc *workspace.PipelineService, internalAuthMode string) *PipelineServer {
	return &PipelineServer{Service: svc, worker: newWorkerAuth(internalAuthMode)}
}

// --- User-facing RPCs ---

func (s *PipelineServer) CreatePipeline(ctx context.Context, req *connect.Request[pipelinev1.CreatePipelineRequest]) (*connect.Response[pipelinev1.CreatePipelineResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	if req.Msg.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("name is required"))
	}
	if req.Msg.SourceType == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("source_type is required"))
	}
	if req.Msg.DatasetName == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("dataset_name is required"))
	}

	p := &workspace.Pipeline{
		Name:              req.Msg.Name,
		SourceType:        req.Msg.SourceType,
		SourceConfig:      jsonOrEmpty(req.Msg.SourceConfig),
		SourceCredentials: jsonOrEmpty(req.Msg.SourceCredentials),
		DatasetName:       req.Msg.DatasetName,
		Schedule:          req.Msg.Schedule,
		WriteDisposition:  req.Msg.WriteDisposition,
		MergeStrategy:     req.Msg.MergeStrategy,
	}

	result, err := s.Service.CreatePipeline(ctx, callerID, p)
	if err != nil {
		return nil, domainError(err)
	}

	return connect.NewResponse(&pipelinev1.CreatePipelineResponse{
		Pipeline: pipelineToPB(result),
	}), nil
}

func (s *PipelineServer) ListPipelines(ctx context.Context, _ *connect.Request[pipelinev1.ListPipelinesRequest]) (*connect.Response[pipelinev1.ListPipelinesResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	pipelines, err := s.Service.ListPipelines(ctx, callerID)
	if err != nil {
		return nil, domainError(err)
	}

	out := make([]*pipelinev1.Pipeline, 0, len(pipelines))
	for _, p := range pipelines {
		out = append(out, pipelineToPB(&p))
	}

	return connect.NewResponse(&pipelinev1.ListPipelinesResponse{
		Pipelines: out,
	}), nil
}

func (s *PipelineServer) GetPipeline(ctx context.Context, req *connect.Request[pipelinev1.GetPipelineRequest]) (*connect.Response[pipelinev1.GetPipelineResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}

	pipeline, runs, err := s.Service.GetPipeline(ctx, callerID, workspace.PipelineID(req.Msg.Id))
	if err != nil {
		return nil, domainError(err)
	}

	runsPB := make([]*pipelinev1.PipelineRun, 0, len(runs))
	for _, r := range runs {
		runsPB = append(runsPB, pipelineRunToPB(&r))
	}

	// Mirror the list view's last-run fields from the most recent run so the
	// detail page is consistent with the list.
	if len(runs) > 0 {
		latest := runs[0]
		pipeline.LastRunStatus = latest.Status
		if latest.StartedAt != nil {
			pipeline.LastRunTime = latest.StartedAt
		} else {
			pipeline.LastRunTime = &latest.CreatedAt
		}
	}

	return connect.NewResponse(&pipelinev1.GetPipelineResponse{
		Pipeline:   pipelineToPB(pipeline),
		RecentRuns: runsPB,
	}), nil
}

func (s *PipelineServer) UpdatePipeline(ctx context.Context, req *connect.Request[pipelinev1.UpdatePipelineRequest]) (*connect.Response[pipelinev1.UpdatePipelineResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}

	p := &workspace.Pipeline{
		ID:                workspace.PipelineID(req.Msg.Id),
		Name:              req.Msg.Name,
		SourceType:        req.Msg.SourceType,
		SourceConfig:      jsonOrEmpty(req.Msg.SourceConfig),
		SourceCredentials: jsonOrEmpty(req.Msg.SourceCredentials),
		DatasetName:       req.Msg.DatasetName,
		Schedule:          req.Msg.Schedule,
		WriteDisposition:  req.Msg.WriteDisposition,
		MergeStrategy:     req.Msg.MergeStrategy,
		Enabled:           req.Msg.Enabled,
	}

	result, err := s.Service.UpdatePipeline(ctx, callerID, p)
	if err != nil {
		return nil, domainError(err)
	}

	return connect.NewResponse(&pipelinev1.UpdatePipelineResponse{
		Pipeline: pipelineToPB(result),
	}), nil
}

func (s *PipelineServer) DeletePipeline(ctx context.Context, req *connect.Request[pipelinev1.DeletePipelineRequest]) (*connect.Response[pipelinev1.DeletePipelineResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}

	if err := s.Service.DeletePipeline(ctx, callerID, workspace.PipelineID(req.Msg.Id)); err != nil {
		return nil, domainError(err)
	}

	return connect.NewResponse(&pipelinev1.DeletePipelineResponse{}), nil
}

func (s *PipelineServer) TriggerPipeline(ctx context.Context, req *connect.Request[pipelinev1.TriggerPipelineRequest]) (*connect.Response[pipelinev1.TriggerPipelineResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}

	run, err := s.Service.TriggerPipeline(ctx, callerID, workspace.PipelineID(req.Msg.Id))
	if err != nil {
		return nil, domainError(err)
	}

	return connect.NewResponse(&pipelinev1.TriggerPipelineResponse{
		Run: pipelineRunToPB(run),
	}), nil
}

// ListPipelineVersions returns the pipeline's rendered-file history from the
// box repo (git-centric gaps #2).
func (s *PipelineServer) ListPipelineVersions(ctx context.Context, req *connect.Request[pipelinev1.ListPipelineVersionsRequest]) (*connect.Response[pipelinev1.ListPipelineVersionsResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}
	if req.Msg.PipelineId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("pipeline_id is required"))
	}

	versions, err := s.Service.ListPipelineVersions(ctx, callerID, workspace.PipelineID(req.Msg.PipelineId))
	if err != nil {
		return nil, domainError(err)
	}
	out := make([]*pipelinev1.PipelineVersion, 0, len(versions))
	for _, v := range versions {
		out = append(out, &pipelinev1.PipelineVersion{
			Sha:         v.SHA,
			Message:     v.Message,
			AuthorName:  v.AuthorName,
			AuthorEmail: v.AuthorEmail,
			Date:        v.Date,
		})
	}
	return connect.NewResponse(&pipelinev1.ListPipelineVersionsResponse{Versions: out}), nil
}

// RestorePipelineVersion applies a historical definition through the normal
// update path.
func (s *PipelineServer) RestorePipelineVersion(ctx context.Context, req *connect.Request[pipelinev1.RestorePipelineVersionRequest]) (*connect.Response[pipelinev1.RestorePipelineVersionResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}
	if req.Msg.PipelineId == "" || req.Msg.Sha == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("pipeline_id and sha are required"))
	}

	restored, err := s.Service.RestorePipelineVersion(ctx, callerID, workspace.PipelineID(req.Msg.PipelineId), req.Msg.Sha)
	if err != nil {
		return nil, domainError(err)
	}
	return connect.NewResponse(&pipelinev1.RestorePipelineVersionResponse{
		Pipeline: pipelineToPB(restored),
	}), nil
}

// --- File drop RPCs (user-facing) ---

func (s *PipelineServer) ListUploadedFiles(ctx context.Context, req *connect.Request[pipelinev1.ListUploadedFilesRequest]) (*connect.Response[pipelinev1.ListUploadedFilesResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}
	if s.FileDrop == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("file drop is not available on this endpoint"))
	}
	if req.Msg.PipelineId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("pipeline_id is required"))
	}

	files, err := s.FileDrop.List(ctx, callerID, workspace.PipelineID(req.Msg.PipelineId))
	if err != nil {
		return nil, domainError(err)
	}

	out := make([]*pipelinev1.UploadedFile, 0, len(files))
	for _, f := range files {
		out = append(out, &pipelinev1.UploadedFile{
			Name:       f.Name,
			File:       f.File,
			SizeBytes:  f.SizeBytes,
			UploadedAt: f.UploadedAt,
			Missing:    f.Missing,
		})
	}
	return connect.NewResponse(&pipelinev1.ListUploadedFilesResponse{Files: out}), nil
}

func (s *PipelineServer) DeleteUploadedFile(ctx context.Context, req *connect.Request[pipelinev1.DeleteUploadedFileRequest]) (*connect.Response[pipelinev1.DeleteUploadedFileResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}
	if s.FileDrop == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("file drop is not available on this endpoint"))
	}
	if req.Msg.PipelineId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("pipeline_id is required"))
	}
	if req.Msg.File == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("file is required"))
	}

	if err := s.FileDrop.Delete(ctx, callerID, workspace.PipelineID(req.Msg.PipelineId), req.Msg.File); err != nil {
		return nil, domainError(err)
	}
	return connect.NewResponse(&pipelinev1.DeleteUploadedFileResponse{}), nil
}

// --- Worker-facing RPCs (tenant-bound service auth, internal mux only) ---

func (s *PipelineServer) GetPipelineConfigs(ctx context.Context, req *connect.Request[pipelinev1.GetPipelineConfigsRequest]) (*connect.Response[pipelinev1.GetPipelineConfigsResponse], error) {
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

	pipelines, err := s.Service.GetEnabledPipelines(ctx, req.Msg.CustomerSlug)
	if err != nil {
		return nil, domainError(err)
	}

	out := make([]*pipelinev1.PipelineConfigItem, 0, len(pipelines))
	for _, p := range pipelines {
		var lastRunAt string
		if p.LastRunAt != nil {
			lastRunAt = p.LastRunAt.UTC().Format(time.RFC3339)
		}
		out = append(out, &pipelinev1.PipelineConfigItem{
			Id:                string(p.ID),
			Name:              p.Name,
			SourceType:        p.SourceType,
			SourceConfig:      string(p.SourceConfig),
			SourceCredentials: string(p.SourceCredentials),
			DatasetName:       p.DatasetName,
			Schedule:          p.Schedule,
			WriteDisposition:  p.WriteDisposition,
			MergeStrategy:     p.MergeStrategy,
			Enabled:           p.Enabled,
			TriggerNow:        p.TriggerNow,
			PendingRunId:      p.PendingRunID,
			LastRunAt:         lastRunAt,
		})
	}

	return connect.NewResponse(&pipelinev1.GetPipelineConfigsResponse{
		Pipelines: out,
	}), nil
}

func (s *PipelineServer) ReportPipelineRun(ctx context.Context, req *connect.Request[pipelinev1.ReportPipelineRunRequest]) (*connect.Response[pipelinev1.ReportPipelineRunResponse], error) {
	callerSlug, err := s.worker.callerSlug(ctx)
	if err != nil {
		return nil, err
	}
	if req.Msg.PipelineId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("pipeline_id is required"))
	}
	pipelineUUID, err := uuid.Parse(req.Msg.PipelineId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("pipeline_id: %w", err))
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

	run := &workspace.PipelineRun{
		ID:           runID,
		PipelineID:   workspace.PipelineID(pipelineUUID.String()),
		Status:       req.Msg.Status,
		StartedAt:    startedAt,
		CompletedAt:  completedAt,
		RowsLoaded:   req.Msg.RowsLoaded,
		ErrorMessage: req.Msg.ErrorMessage,
	}

	if err := s.Service.ReportPipelineRun(ctx, callerSlug, run); err != nil {
		return nil, domainError(err)
	}

	return connect.NewResponse(&pipelinev1.ReportPipelineRunResponse{}), nil
}

// --- Helpers ---

func pipelineToPB(p *workspace.Pipeline) *pipelinev1.Pipeline {
	pb := &pipelinev1.Pipeline{
		Id:               string(p.ID),
		CustomerSlug:     p.CustomerSlug,
		Name:             p.Name,
		SourceType:       p.SourceType,
		SourceConfig:     string(p.SourceConfig),
		DatasetName:      p.DatasetName,
		Schedule:         p.Schedule,
		WriteDisposition: p.WriteDisposition,
		MergeStrategy:    p.MergeStrategy,
		Enabled:          p.Enabled,
		CreatedAt:        p.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:        p.UpdatedAt.UTC().Format(time.RFC3339),
		LastRunStatus:    p.LastRunStatus,
	}
	if p.LastRunTime != nil {
		pb.LastRunAt = p.LastRunTime.UTC().Format(time.RFC3339)
	}
	return pb
}

func pipelineRunToPB(r *workspace.PipelineRun) *pipelinev1.PipelineRun {
	pb := &pipelinev1.PipelineRun{
		Id:           r.ID,
		PipelineId:   string(r.PipelineID),
		Status:       r.Status,
		RowsLoaded:   r.RowsLoaded,
		ErrorMessage: r.ErrorMessage,
		CreatedAt:    r.CreatedAt.UTC().Format(time.RFC3339),
	}
	if r.StartedAt != nil {
		pb.StartedAt = r.StartedAt.UTC().Format(time.RFC3339)
	}
	if r.CompletedAt != nil {
		pb.CompletedAt = r.CompletedAt.UTC().Format(time.RFC3339)
	}
	return pb
}

// jsonOrEmpty returns the raw JSON bytes from a string, defaulting to "{}".
func jsonOrEmpty(s string) json.RawMessage {
	if s == "" {
		return json.RawMessage("{}")
	}
	return json.RawMessage(s)
}

// parseTimePtr parses an RFC 3339 timestamp string, returning nil for empty strings.
// Returns an error for non-empty strings that fail to parse.
func parseTimePtr(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil, fmt.Errorf("invalid RFC 3339 timestamp %q: %w", s, err)
	}
	return &t, nil
}

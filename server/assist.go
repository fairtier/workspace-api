package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	"github.com/fairtier/workspace-api/core"
	assistv1 "github.com/fairtier/workspace-api/proto/assist/v1"
	transformationv1 "github.com/fairtier/workspace-api/proto/transformation/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// AssistServer implements the ConnectRPC AssistService handler: single-shot
// LLM drafts for dbt transformations and Rill dashboards, mirroring
// PipelineAssistServer.
type AssistServer struct {
	Service *workspace.AssistService
}

// DraftTransformation turns a natural-language prompt into a pre-validated
// draft of a CreateTransformation request plus starter dbt model files. The
// draft never includes credentials or repo URLs (hosted-repo mode).
func (s *AssistServer) DraftTransformation(ctx context.Context, req *connect.Request[assistv1.DraftTransformationRequest]) (*connect.Response[assistv1.DraftTransformationResponse], error) {
	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	draft, err := s.Service.DraftTransformation(ctx, callerID, req.Msg.Prompt)
	if err != nil {
		return nil, draftError(err)
	}

	return connect.NewResponse(&assistv1.DraftTransformationResponse{
		Draft: &transformationv1.CreateTransformationRequest{
			Name:           draft.Name,
			RepoUrl:        "", // hosted-repo mode — the model never drafts repos
			GitCredentials: "", // never drafted — the user supplies secrets
			Schedule:       draft.Schedule,
			DbtSelector:    draft.DBTSelector,
		},
		Files: draftFiles(draft.Files),
		Notes: draft.Notes,
	}), nil
}

// DraftRillDashboard turns a natural-language prompt into draft Rill project
// files, YAML-validated server-side and opened as unsaved buffers in the
// Console editor.
func (s *AssistServer) DraftRillDashboard(ctx context.Context, req *connect.Request[assistv1.DraftRillDashboardRequest]) (*connect.Response[assistv1.DraftRillDashboardResponse], error) {
	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	draft, err := s.Service.DraftRillDashboard(ctx, callerID, req.Msg.Prompt, req.Msg.ExistingPaths)
	if err != nil {
		return nil, draftError(err)
	}

	return connect.NewResponse(&assistv1.DraftRillDashboardResponse{
		Files: draftFiles(draft.Files),
		Notes: draft.Notes,
	}), nil
}

// DraftSql turns a natural-language request into one read-only DuckDB query,
// drafted against the caller's own schema and inserted into the editor —
// never executed here.
func (s *AssistServer) DraftSql(ctx context.Context, req *connect.Request[assistv1.DraftSqlRequest]) (*connect.Response[assistv1.DraftSqlResponse], error) {
	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	draft, err := s.Service.DraftSql(ctx, callerID, req.Msg.Prompt, req.Msg.CurrentSql)
	if err != nil {
		return nil, draftError(err)
	}

	return connect.NewResponse(&assistv1.DraftSqlResponse{
		Sql:            draft.SQL,
		Notes:          draft.Notes,
		NoRelevantData: draft.NoRelevantData,
	}), nil
}

// ExplainError explains one failure. Run targets are resolved by id
// server-side (trusted context); the SQL target is client-supplied.
func (s *AssistServer) ExplainError(ctx context.Context, req *connect.Request[assistv1.ExplainErrorRequest]) (*connect.Response[assistv1.ExplainErrorResponse], error) {
	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	var ex *workspace.ErrorExplanation
	var err error
	switch target := req.Msg.Target.(type) {
	case *assistv1.ExplainErrorRequest_PipelineRun:
		ex, err = s.Service.ExplainPipelineRun(ctx, callerID,
			workspace.PipelineID(target.PipelineRun.PipelineId), target.PipelineRun.RunId)
	case *assistv1.ExplainErrorRequest_TransformationRun:
		ex, err = s.Service.ExplainTransformationRun(ctx, callerID,
			workspace.TransformationID(target.TransformationRun.TransformationId), target.TransformationRun.RunId)
	case *assistv1.ExplainErrorRequest_Sql:
		ex, err = s.Service.ExplainSqlError(ctx, callerID, target.Sql.Sql, target.Sql.ErrorMessage)
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("target is required"))
	}
	if err != nil {
		return nil, draftError(err)
	}

	return connect.NewResponse(&assistv1.ExplainErrorResponse{
		Explanation:      ex.Explanation,
		LikelyCause:      ex.LikelyCause,
		SuggestedFix:     ex.SuggestedFix,
		SuggestedSnippet: ex.SuggestedSnippet,
	}), nil
}

func draftFiles(files []workspace.DraftFile) []*assistv1.DraftFile {
	out := make([]*assistv1.DraftFile, 0, len(files))
	for _, f := range files {
		out = append(out, &assistv1.DraftFile{Path: f.Path, Content: f.Content})
	}
	return out
}

package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	"github.com/fairtier/workspace-api/core"
	pipelinev1 "github.com/fairtier/workspace-api/proto/pipeline/v1"
	pipelineassistv1 "github.com/fairtier/workspace-api/proto/pipeline_assist/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// PipelineAssistServer implements the ConnectRPC PipelineAssistService handler.
type PipelineAssistServer struct {
	Service *workspace.PipelineAssistService
}

// DraftPipeline turns a natural-language prompt into a pre-validated draft of a
// CreatePipeline request. The draft never includes credentials.
func (s *PipelineAssistServer) DraftPipeline(ctx context.Context, req *connect.Request[pipelineassistv1.DraftPipelineRequest]) (*connect.Response[pipelineassistv1.DraftPipelineResponse], error) {
	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	draft, err := s.Service.DraftPipeline(ctx, callerID, req.Msg.Prompt)
	if err != nil {
		return nil, draftError(err)
	}

	return connect.NewResponse(&pipelineassistv1.DraftPipelineResponse{
		Draft: &pipelinev1.CreatePipelineRequest{
			Name:              draft.Name,
			SourceType:        draft.SourceType,
			SourceConfig:      string(draft.SourceConfig),
			SourceCredentials: "", // never drafted — the user supplies secrets
			DatasetName:       draft.DatasetName,
			Schedule:          draft.Schedule,
			WriteDisposition:  draft.WriteDisposition,
			MergeStrategy:     draft.MergeStrategy,
		},
		Notes: draft.Notes,
	}), nil
}

// draftError maps drafting-specific domain errors onto Connect codes, falling
// back to the shared domainError mapping (which carries ValidationErrors detail
// for an invalid source_config).
func draftError(err error) error {
	// A Connect error passes through unchanged: the schema source (DraftSql's
	// table listing) fails with already-mapped engine errors, and re-wrapping
	// them would bury "the query engine is not enabled" under CodeInternal.
	var cerr *connect.Error
	if errors.As(err, &cerr) {
		return cerr
	}
	switch {
	case errors.Is(err, workspace.ErrDraftNotConfigured):
		return connect.NewError(connect.CodeUnimplemented, err)
	case errors.Is(err, workspace.ErrDraftRateLimited):
		return connect.NewError(connect.CodeResourceExhausted, err)
	default:
		return domainError(err)
	}
}

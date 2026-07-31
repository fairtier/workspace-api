package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"

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
	callerID := UserIDFromContext(ctx)
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
	callerID := UserIDFromContext(ctx)
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

func draftFiles(files []workspace.DraftFile) []*assistv1.DraftFile {
	out := make([]*assistv1.DraftFile, 0, len(files))
	for _, f := range files {
		out = append(out, &assistv1.DraftFile{Path: f.Path, Content: f.Content})
	}
	return out
}

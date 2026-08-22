package server

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"

	"github.com/fairtier/workspace-api/core"
	demov1 "github.com/fairtier/workspace-api/proto/demo/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// DemoServer implements the ConnectRPC DemoService handler: the one-click
// "NYC Taxi Pulse" starter demo. All RPCs are user-facing (JWT auth on the
// public mux).
type DemoServer struct {
	Service *workspace.DemoService
}

func (s *DemoServer) GetDemoStatus(ctx context.Context, _ *connect.Request[demov1.GetDemoStatusRequest]) (*connect.Response[demov1.GetDemoStatusResponse], error) {
	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}
	status, err := s.Service.GetDemoStatus(ctx, callerID)
	if err != nil {
		return nil, domainError(err)
	}
	return connect.NewResponse(&demov1.GetDemoStatusResponse{Status: demoStatusToPB(status)}), nil
}

func (s *DemoServer) LoadDemoProject(ctx context.Context, req *connect.Request[demov1.LoadDemoProjectRequest]) (*connect.Response[demov1.LoadDemoProjectResponse], error) {
	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}
	status, err := s.Service.LoadDemoProject(ctx, callerID, req.Msg.Tier)
	if err != nil {
		return nil, domainError(err)
	}
	return connect.NewResponse(&demov1.LoadDemoProjectResponse{Status: demoStatusToPB(status)}), nil
}

func (s *DemoServer) RemoveDemoProject(ctx context.Context, _ *connect.Request[demov1.RemoveDemoProjectRequest]) (*connect.Response[demov1.RemoveDemoProjectResponse], error) {
	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}
	if err := s.Service.RemoveDemoProject(ctx, callerID); err != nil {
		return nil, domainError(err)
	}
	// Warehouse tables are intentionally left in place (see the proto doc).
	return connect.NewResponse(&demov1.RemoveDemoProjectResponse{RemovedWarehouse: false}), nil
}

func demoStatusToPB(s *workspace.DemoStatus) *demov1.DemoStatus {
	pb := &demov1.DemoStatus{
		Available:        s.Available,
		Loaded:           s.Loaded,
		Loading:          s.Loading,
		Tier:             s.Tier,
		TripsPipelineId:  s.TripsPipelineID,
		ZonesPipelineId:  s.ZonesPipelineID,
		TransformationId: s.TransformationID,
	}
	if s.LoadedAt != nil {
		pb.LoadedAt = s.LoadedAt.UTC().Format(time.RFC3339)
	}
	return pb
}

package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	"github.com/fairtier/workspace-api/core"
	lakekeeperuserv1 "github.com/fairtier/workspace-api/proto/lakekeeper_user/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// LakekeeperUserServer implements the ConnectRPC LakekeeperUserService handler.
type LakekeeperUserServer struct {
	Service *workspace.LakekeeperUserService
}

func (s *LakekeeperUserServer) AddUser(ctx context.Context, req *connect.Request[lakekeeperuserv1.AddUserRequest]) (*connect.Response[lakekeeperuserv1.AddUserResponse], error) {
	if req.Msg.Name == "" || req.Msg.Role == "" || req.Msg.WarehouseName == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("name, role, and warehouse_name are required"))
	}

	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	result, err := s.Service.AddUser(ctx, callerID, req.Msg.Name, req.Msg.Role, req.Msg.WarehouseName)
	if err != nil {
		return nil, domainError(err)
	}

	return connect.NewResponse(&lakekeeperuserv1.AddUserResponse{
		User: &lakekeeperuserv1.LakekeeperUser{
			Id:        result.ID,
			Name:      result.Name,
			Role:      result.Role,
			Warehouse: result.Warehouse,
		},
		ClientId:     result.ClientID,
		ClientSecret: result.ClientSecret,
	}), nil
}

func (s *LakekeeperUserServer) RemoveUser(ctx context.Context, req *connect.Request[lakekeeperuserv1.RemoveUserRequest]) (*connect.Response[lakekeeperuserv1.RemoveUserResponse], error) {
	if req.Msg.UserId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id is required"))
	}

	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	if err := s.Service.RemoveUser(ctx, callerID, req.Msg.UserId); err != nil {
		return nil, domainError(err)
	}

	return connect.NewResponse(&lakekeeperuserv1.RemoveUserResponse{}), nil
}

func (s *LakekeeperUserServer) ListUsers(ctx context.Context, _ *connect.Request[lakekeeperuserv1.ListUsersRequest]) (*connect.Response[lakekeeperuserv1.ListUsersResponse], error) {
	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	results, err := s.Service.ListUsers(ctx, callerID)
	if err != nil {
		return nil, domainError(err)
	}

	users := make([]*lakekeeperuserv1.LakekeeperUser, 0, len(results))
	for _, r := range results {
		users = append(users, &lakekeeperuserv1.LakekeeperUser{
			Id:        r.ID,
			Name:      r.Name,
			Role:      r.Role,
			Warehouse: r.Warehouse,
		})
	}

	return connect.NewResponse(&lakekeeperuserv1.ListUsersResponse{
		Users: users,
	}), nil
}

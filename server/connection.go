package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	connectionv1 "github.com/fairtier/workspace-api/proto/connection/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// ConnectionServer implements the ConnectRPC ConnectionService handler: the
// Console's workspace-level Connections ("connect Google once"), consumed by
// pipelines (by reference) and by the box query engine (as minted tokens).
//
// The tenant is resolved from the caller's JWT on every call, so a user can
// only ever read or write their own workspace's connections.
type ConnectionServer struct {
	Workspaces workspace.Resolver
	Service    *workspace.ConnectionService
	// Mirror, when set, re-renders the tenant's pipelines repo after a
	// connection delete so .age files referencing it stop carrying a live
	// token on the next converge. Best-effort, same contract as the other
	// mirror call sites.
	Mirror workspace.PipelineMirrorer
	Logger *slog.Logger
}

func (s *ConnectionServer) ListConnections(ctx context.Context, _ *connect.Request[connectionv1.ListConnectionsRequest]) (*connect.Response[connectionv1.ListConnectionsResponse], error) {
	slug, err := s.resolve(ctx)
	if err != nil {
		return nil, err
	}
	conns, err := s.Service.ListConnections(ctx, slug)
	if err != nil {
		return nil, connectionErr(err)
	}
	out := &connectionv1.ListConnectionsResponse{}
	for i := range conns {
		out.Connections = append(out.Connections, connectionProto(&conns[i]))
	}
	return connect.NewResponse(out), nil
}

func (s *ConnectionServer) CreateConnection(ctx context.Context, req *connect.Request[connectionv1.CreateConnectionRequest]) (*connect.Response[connectionv1.CreateConnectionResponse], error) {
	slug, err := s.resolve(ctx)
	if err != nil {
		return nil, err
	}

	grantID := req.Msg.GetGoogleGrantId()
	if grantID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("a connection source is required (google_grant_id)"))
	}
	c, err := s.Service.CreateGoogleConnection(ctx, slug, grantID, req.Msg.Name)
	if err != nil {
		return nil, connectionErr(err)
	}
	if s.Logger != nil {
		// Id and name only; the credentials never reach a log line.
		s.Logger.InfoContext(ctx, "connection created", "slug", slug, "connection", c.ID, "type", c.Type)
	}
	return connect.NewResponse(&connectionv1.CreateConnectionResponse{Connection: connectionProto(c)}), nil
}

func (s *ConnectionServer) DeleteConnection(ctx context.Context, req *connect.Request[connectionv1.DeleteConnectionRequest]) (*connect.Response[connectionv1.DeleteConnectionResponse], error) {
	slug, err := s.resolve(ctx)
	if err != nil {
		return nil, err
	}
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}
	if err := s.Service.DeleteConnection(ctx, slug, req.Msg.Id); err != nil {
		return nil, connectionErr(err)
	}
	if s.Logger != nil {
		s.Logger.InfoContext(ctx, "connection deleted", "slug", slug, "connection", req.Msg.Id)
	}
	// Best-effort converge so any pipeline .age file that referenced the
	// connection is re-rendered without a resolvable token.
	if s.Mirror != nil {
		mctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if err := s.Mirror.SyncCustomer(mctx, slug, nil); err != nil && s.Logger != nil {
			s.Logger.WarnContext(ctx, "post-delete pipeline mirror sync", "slug", slug, "err", err)
		}
	}
	return connect.NewResponse(&connectionv1.DeleteConnectionResponse{}), nil
}

// resolve binds the caller to their workspace.
func (s *ConnectionServer) resolve(ctx context.Context) (string, error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return "", connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return "", connect.NewError(connect.CodePermissionDenied, errors.New("no workspace for this user"))
	}
	return ws.Slug, nil
}

// connectionErr maps domain sentinels onto Connect codes.
func connectionErr(err error) error {
	switch {
	case errors.Is(err, workspace.ErrConnectionNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, workspace.ErrConnectionAlreadyExists):
		return connect.NewError(connect.CodeAlreadyExists, err)
	case errors.Is(err, workspace.ErrConnectionInUse):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, workspace.ErrOAuthGrantNotFound):
		return connect.NewError(connect.CodeInvalidArgument, errors.New("the Google sign-in expired or was already used; please reconnect"))
	case errors.Is(err, workspace.ErrConnectionsNotConfigured):
		return connect.NewError(connect.CodeUnimplemented, err)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}

func connectionProto(c *workspace.Connection) *connectionv1.Connection {
	out := &connectionv1.Connection{
		Id:     c.ID,
		Type:   c.Type,
		Name:   c.Name,
		Status: c.Status,
		Email:  c.GoogleEmail(),
	}
	if !c.CreatedAt.IsZero() {
		out.CreatedAt = c.CreatedAt.UTC().Format(time.RFC3339)
	}
	return out
}

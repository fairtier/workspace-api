package server

import (
	"context"
	"database/sql"
	"log/slog"

	"connectrpc.com/connect"

	workspacehealthv1 "github.com/fairtier/workspace-api/proto/workspace_health/v1"
	"github.com/fairtier/workspace-api/version"
)

// HealthServer implements the workspace plane's HealthService handler.
//
// The plain HTTP /healthz and /readyz endpoints stay the probe surface for
// container orchestrators; this is the Connect-level equivalent, so a Console
// or an operator tool can ask the same question over the same transport as
// every other workspace RPC and get the build revision back with it.
type HealthServer struct {
	DB     *sql.DB
	Logger *slog.Logger
}

// Check reports database reachability. A failed ping is an unhealthy payload,
// not an RPC error: callers need to distinguish "the plane answered and is
// degraded" from "the plane did not answer". The ping error itself only goes
// to the log — this endpoint is unauthenticated, and pgx connect errors spell
// out the DB user, database name, and host.
func (s *HealthServer) Check(ctx context.Context, _ *connect.Request[workspacehealthv1.CheckRequest]) (*connect.Response[workspacehealthv1.CheckResponse], error) {
	if err := s.DB.PingContext(ctx); err != nil {
		if s.Logger != nil {
			s.Logger.ErrorContext(ctx, "health check: database ping failed", "err", err)
		}
		return connect.NewResponse(&workspacehealthv1.CheckResponse{
			Healthy: false,
			Message: "database ping failed",
			Version: version.Binary(),
		}), nil
	}

	return connect.NewResponse(&workspacehealthv1.CheckResponse{
		Healthy: true,
		Message: "database connection healthy",
		Version: version.Binary(),
	}), nil
}

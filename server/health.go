package server

import (
	"context"
	"database/sql"
	"fmt"

	"connectrpc.com/connect"

	healthv1 "github.com/fairtier/workspace-api/proto/health/v1"
)

// HealthServer implements the HealthService ConnectRPC handler.
type HealthServer struct {
	DB *sql.DB
}

func (s *HealthServer) Check(ctx context.Context, _ *connect.Request[healthv1.CheckRequest]) (*connect.Response[healthv1.CheckResponse], error) {
	if err := s.DB.PingContext(ctx); err != nil {
		return connect.NewResponse(&healthv1.CheckResponse{
			Healthy: false,
			Message: fmt.Sprintf("database ping failed: %v", err),
		}), nil
	}

	return connect.NewResponse(&healthv1.CheckResponse{
		Healthy: true,
		Message: "database connection healthy",
	}), nil
}

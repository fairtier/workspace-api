package server

import (
	"context"
	"net/http"
	"testing"

	"connectrpc.com/connect"

	pipelinev1 "github.com/fairtier/workspace-api/proto/pipeline/v1"
	transformationv1 "github.com/fairtier/workspace-api/proto/transformation/v1"
)

// The worker RPCs take the tenant as a request field, so on the public mux —
// where the caller is an end user of an arbitrary tenant and no internal
// caller is ever set — serving them would hand the requested tenant's
// object-storage keys and dbt git credentials to anyone with a valid JWT.
// A public-mux handler must therefore refuse them outright, whatever the
// request says.
func TestWorkerRPCsDeniedOnPublicMux(t *testing.T) {
	ctx := context.Background()

	t.Run("GetPipelineConfigs", func(t *testing.T) {
		srv := &PipelineServer{Service: newTestPipelineService()}
		_, err := srv.GetPipelineConfigs(ctx, connect.NewRequest(&pipelinev1.GetPipelineConfigsRequest{
			CustomerSlug: "victim",
		}))
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("GetPipelineConfigs() error = %v, want PermissionDenied", err)
		}
	})

	t.Run("ReportPipelineRun", func(t *testing.T) {
		srv := &PipelineServer{Service: newTestPipelineService()}
		_, err := srv.ReportPipelineRun(ctx, connect.NewRequest(&pipelinev1.ReportPipelineRunRequest{
			PipelineId: "5f5cf1cd-4b0e-4a3f-9a1e-1f6a4a1a0001",
			Status:     "success",
		}))
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("ReportPipelineRun() error = %v, want PermissionDenied", err)
		}
	})

	t.Run("GetTransformationConfigs", func(t *testing.T) {
		srv := &TransformationServer{}
		_, err := srv.GetTransformationConfigs(ctx, connect.NewRequest(&transformationv1.GetTransformationConfigsRequest{
			CustomerSlug: "victim",
		}))
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("GetTransformationConfigs() error = %v, want PermissionDenied", err)
		}
	})

	t.Run("ReportTransformationRun", func(t *testing.T) {
		srv := &TransformationServer{}
		_, err := srv.ReportTransformationRun(ctx, connect.NewRequest(&transformationv1.ReportTransformationRunRequest{
			TransformationId: "5f5cf1cd-4b0e-4a3f-9a1e-1f6a4a1a0002",
			Status:           "success",
		}))
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("ReportTransformationRun() error = %v, want PermissionDenied", err)
		}
	})
}

// Mounting a public-mux handler on the internal mux would answer every worker
// poll with PermissionDenied. Registration rejects it at startup instead.
func TestRegisterWorkspaceInternalRejectsPublicHandlers(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("RegisterWorkspaceInternal accepted a public-mux handler")
		}
	}()
	RegisterWorkspaceInternal(http.NewServeMux(), WorkspaceInternalServers{
		Pipelines:       &PipelineServer{Service: newTestPipelineService()},
		Transformations: NewInternalTransformationServer(nil),
	}, connect.WithInterceptors())
}

// And the mirror image: an internal handler on the public mux would put the
// worker RPCs back in reach of end-user JWTs — the leak this gate exists to
// close.
func TestRegisterWorkspacePlaneRejectsInternalHandlers(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("RegisterWorkspacePlane accepted an internal-mux handler")
		}
	}()
	RegisterWorkspacePlane(http.NewServeMux(), WorkspacePlaneServers{
		Pipelines:       NewInternalPipelineServer(newTestPipelineService(), nil),
		Transformations: &TransformationServer{},
	}, connect.WithInterceptors())
}

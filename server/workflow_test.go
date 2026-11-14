package server

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"connectrpc.com/connect"

	workflowv1 "github.com/fairtier/workspace-api/proto/workflow/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// The workflow worker RPCs follow the pipeline/transformation rule: a
// public-mux handler refuses them whatever the request says.
func TestWorkflowWorkerRPCsDeniedOnPublicMux(t *testing.T) {
	srv := &WorkflowServer{}
	ctx := withBoxCaller("workspace", "victim")

	_, err := srv.GetWorkflowConfigs(ctx, connect.NewRequest(&workflowv1.GetWorkflowConfigsRequest{CustomerSlug: "victim"}))
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("GetWorkflowConfigs() error = %v, want PermissionDenied", err)
	}
	_, err = srv.ReportWorkflowRun(ctx, connect.NewRequest(&workflowv1.ReportWorkflowRunRequest{
		RunId: "5f5cf1cd-4b0e-4a3f-9a1e-1f6a4a1a0003", WorkflowId: "5f5cf1cd-4b0e-4a3f-9a1e-1f6a4a1a0004", Status: "running",
	}))
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("ReportWorkflowRun() error = %v, want PermissionDenied", err)
	}
}

// On the internal mux the token's tenant must match the requested one.
func TestWorkflowConfigsRejectForeignTenant(t *testing.T) {
	srv := NewInternalWorkflowServer(&workspace.WorkflowService{})
	_, err := srv.GetWorkflowConfigs(withBoxCaller("workspace", "acme"),
		connect.NewRequest(&workflowv1.GetWorkflowConfigsRequest{CustomerSlug: "victim"}))
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("GetWorkflowConfigs() error = %v, want PermissionDenied", err)
	}
}

func TestReportWorkflowRunRejectsMalformedInput(t *testing.T) {
	srv := NewInternalWorkflowServer(&workspace.WorkflowService{})
	ctx := withBoxCaller("workspace", "acme")
	cases := map[string]*workflowv1.ReportWorkflowRunRequest{
		"workflow id": {RunId: "5f5cf1cd-4b0e-4a3f-9a1e-1f6a4a1a0003", WorkflowId: "nope", Status: "running"},
		"timestamp":   {RunId: "5f5cf1cd-4b0e-4a3f-9a1e-1f6a4a1a0003", WorkflowId: "5f5cf1cd-4b0e-4a3f-9a1e-1f6a4a1a0004", Status: "running", StartedAt: "yesterday"},
		"step timestamp": {
			RunId: "5f5cf1cd-4b0e-4a3f-9a1e-1f6a4a1a0003", WorkflowId: "5f5cf1cd-4b0e-4a3f-9a1e-1f6a4a1a0004", Status: "running",
			StepRuns: []*workflowv1.WorkflowStepRunReport{{CompletedAt: "soon"}},
		},
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := srv.ReportWorkflowRun(ctx, connect.NewRequest(req))
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("error = %v, want InvalidArgument", err)
			}
		})
	}
}

func TestWorkflowUserRPCsRequireAuthentication(t *testing.T) {
	srv := &WorkflowServer{}
	_, err := srv.ListWorkflows(context.Background(), connect.NewRequest(&workflowv1.ListWorkflowsRequest{}))
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("ListWorkflows() error = %v, want Unauthenticated", err)
	}
}

func TestRegisterWorkspaceInternalRejectsPublicWorkflowHandler(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("RegisterWorkspaceInternal accepted a public-mux WorkflowServer")
		}
	}()
	RegisterWorkspaceInternal(http.NewServeMux(), WorkspaceInternalServers{
		Pipelines:       NewInternalPipelineServer(newTestPipelineService(), nil),
		Transformations: NewInternalTransformationServer(nil),
		Workflows:       &WorkflowServer{},
	}, connect.WithInterceptors())
}

func TestRegisterWorkspacePlaneRejectsInternalWorkflowHandler(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("RegisterWorkspacePlane accepted an internal-mux WorkflowServer")
		}
	}()
	RegisterWorkspacePlane(http.NewServeMux(), WorkspacePlaneServers{
		Health:          &HealthServer{},
		Pipelines:       &PipelineServer{Service: newTestPipelineService()},
		Transformations: &TransformationServer{},
		Workflows:       NewInternalWorkflowServer(nil),
	}, connect.WithInterceptors())
}

func TestWorkflowInvalidMapsToFieldViolation(t *testing.T) {
	err := domainError(&workspace.ErrInvalidWorkflow{Field: "steps[0].ref_id", Msg: "pipeline not found"})
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", connect.CodeOf(err))
	}
	var cerr *connect.Error
	if !errors.As(err, &cerr) || len(cerr.Details()) != 1 {
		t.Fatalf("want one ValidationErrors detail, got %v", err)
	}
}

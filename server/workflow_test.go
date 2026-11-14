package server

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"connectrpc.com/connect"

	"github.com/fairtier/workspace-api/core"
	pipelinev1 "github.com/fairtier/workspace-api/proto/pipeline/v1"
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

// The Console reads FieldViolations by field name: an empty name and too many
// steps must name "name" and "steps" through the real handler.
func TestCreateWorkflowValidationFieldViolations(t *testing.T) {
	s := &WorkflowServer{Service: &workspace.WorkflowService{
		Workspaces: &workspace.StaticResolver{Workspace: workspace.Workspace{Slug: "acme", CasdoorOrg: "customer-acme"}},
	}}
	ctx := core.ContextWithUserID(context.Background(), "customer-acme/alice")
	tooMany := make([]*workflowv1.WorkflowStep, workspace.MaxWorkflowSteps+1)
	for i := range tooMany {
		tooMany[i] = &workflowv1.WorkflowStep{Kind: "pipeline", RefId: "11111111-1111-4111-8111-111111111111"}
	}
	for want, req := range map[string]*workflowv1.CreateWorkflowRequest{
		"name":  {Name: "", Steps: tooMany[:1]},
		"steps": {Name: "x", Steps: tooMany},
	} {
		_, err := s.CreateWorkflow(ctx, connect.NewRequest(req))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("%s: code = %v (%v)", want, connect.CodeOf(err), err)
		}
		var cerr *connect.Error
		if !errors.As(err, &cerr) || len(cerr.Details()) != 1 {
			t.Fatalf("%s: details missing: %v", want, err)
		}
		v, derr := cerr.Details()[0].Value()
		ve, ok := v.(*pipelinev1.ValidationErrors)
		if derr != nil || !ok || len(ve.Violations) != 1 || ve.Violations[0].Field != want {
			t.Errorf("%s: violation = %v (%v)", want, v, derr)
		}
	}
}

// createStore keeps what CreateWorkflow stored; every other method panics
// (the embedded nil interface), so the test notices an unexpected call.
type createStore struct {
	workspace.WorkflowRepository
	stored *workspace.Workflow
}

func (c *createStore) CreateWorkflow(_ context.Context, w *workspace.Workflow) error {
	w.ID = "00000000-0000-4000-8000-000000000001"
	cp := *w
	c.stored = &cp
	return nil
}

func (c *createStore) GetWorkflow(context.Context, workspace.WorkflowID) (*workspace.Workflow, error) {
	cp := *c.stored
	return &cp, nil
}

type acmePipelines struct{}

func (acmePipelines) GetPipeline(_ context.Context, id workspace.PipelineID) (*workspace.Pipeline, error) {
	return &workspace.Pipeline{ID: id, CustomerSlug: "acme"}, nil
}

// enabled is optional on create: absent keeps the old "created enabled",
// false creates the workflow paused.
func TestCreateWorkflowEnabledPresence(t *testing.T) {
	ctx := core.ContextWithUserID(context.Background(), "customer-acme/alice")
	paused := false
	for name, tc := range map[string]struct {
		enabled *bool
		want    bool
	}{
		"absent": {nil, true},
		"false":  {&paused, false},
	} {
		store := &createStore{}
		s := &WorkflowServer{Service: &workspace.WorkflowService{
			Workspaces: &workspace.StaticResolver{Workspace: workspace.Workspace{Slug: "acme", CasdoorOrg: "customer-acme"}},
			Workflows:  store,
			Pipelines:  acmePipelines{},
		}}
		resp, err := s.CreateWorkflow(ctx, connect.NewRequest(&workflowv1.CreateWorkflowRequest{
			Name: "nightly", Enabled: tc.enabled,
			Steps: []*workflowv1.WorkflowStep{{Kind: "pipeline", RefId: "11111111-1111-4111-8111-111111111111"}},
		}))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if store.stored.Enabled != tc.want || resp.Msg.Workflow.Enabled != tc.want {
			t.Errorf("%s: stored enabled = %v, response %v, want %v", name, store.stored.Enabled, resp.Msg.Workflow.Enabled, tc.want)
		}
	}
}

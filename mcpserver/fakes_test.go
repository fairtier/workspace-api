package mcpserver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"github.com/MicahParks/jwkset"
	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"

	"github.com/fairtier/workspace-api/core"
	boxrepov1 "github.com/fairtier/workspace-api/proto/boxrepo/v1"
	notificationv1 "github.com/fairtier/workspace-api/proto/notification/v1"
	pipelinev1 "github.com/fairtier/workspace-api/proto/pipeline/v1"
	queryv1 "github.com/fairtier/workspace-api/proto/query/v1"
	transformationv1 "github.com/fairtier/workspace-api/proto/transformation/v1"
	workflowv1 "github.com/fairtier/workspace-api/proto/workflow/v1"
)

// fakeWorkspace stands in for every backend and records what reached it, and
// as whom. Each method insists on a caller in the context, as the real
// handlers do.
type fakeWorkspace struct {
	mu      sync.Mutex
	calls   []string
	callers []core.UserID
	queries []string
	puts    []*boxrepov1.PutFileRequest
	// workflows is the fake's workflow store, keyed by id; updates records
	// every UpdateWorkflow request as received.
	workflows map[string]*workflowv1.Workflow
	updates   []*workflowv1.UpdateWorkflowRequest
}

func (f *fakeWorkspace) record(ctx context.Context, call string) error {
	caller := core.UserIDFromContext(ctx)
	if caller == "" {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
	f.callers = append(f.callers, caller)
	return nil
}

func (f *fakeWorkspace) called(call string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if c == call {
			return true
		}
	}
	return false
}

func (f *fakeWorkspace) backends() Backends {
	return Backends{Query: f, Pipelines: f, Transformations: f, Workflows: f, Repos: f, Notifications: f}
}

func (f *fakeWorkspace) ExecuteQuery(ctx context.Context, req *connect.Request[queryv1.ExecuteQueryRequest]) (*connect.Response[queryv1.ExecuteQueryResponse], error) {
	if err := f.record(ctx, "ExecuteQuery"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.queries = append(f.queries, req.Msg.Sql)
	f.mu.Unlock()
	return connect.NewResponse(&queryv1.ExecuteQueryResponse{
		Columns:  []*queryv1.ColumnInfo{{Name: "n", Type: "BIGINT"}, {Name: "s", Type: "VARCHAR"}},
		Rows:     []string{`[1,"a"]`, `[2,null]`},
		RowCount: 2,
	}), nil
}

func (f *fakeWorkspace) ListTables(ctx context.Context, _ *connect.Request[queryv1.ListTablesRequest]) (*connect.Response[queryv1.ListTablesResponse], error) {
	if err := f.record(ctx, "ListTables"); err != nil {
		return nil, err
	}
	return connect.NewResponse(&queryv1.ListTablesResponse{Tables: []*queryv1.TableRef{{Namespace: "sales", Name: "orders"}}}), nil
}

func (f *fakeWorkspace) DescribeTable(ctx context.Context, _ *connect.Request[queryv1.DescribeTableRequest]) (*connect.Response[queryv1.DescribeTableResponse], error) {
	if err := f.record(ctx, "DescribeTable"); err != nil {
		return nil, err
	}
	return connect.NewResponse(&queryv1.DescribeTableResponse{Columns: []*queryv1.ColumnSchema{{Name: "id", Type: "BIGINT"}}}), nil
}

func (f *fakeWorkspace) ListPipelines(ctx context.Context, _ *connect.Request[pipelinev1.ListPipelinesRequest]) (*connect.Response[pipelinev1.ListPipelinesResponse], error) {
	if err := f.record(ctx, "ListPipelines"); err != nil {
		return nil, err
	}
	return connect.NewResponse(&pipelinev1.ListPipelinesResponse{Pipelines: []*pipelinev1.Pipeline{
		{Id: "p1", Name: "orders", SourceType: "rest_api", DatasetName: "sales", Enabled: true, Schedule: "@daily", LastRunStatus: "failed", LastRunAt: "2026-01-02T03:04:05Z"},
		{Id: "p2", Name: "customers", SourceType: "sql_database", DatasetName: "crm", Enabled: true, Schedule: "@hourly"},
	}}), nil
}

func (f *fakeWorkspace) GetPipeline(ctx context.Context, req *connect.Request[pipelinev1.GetPipelineRequest]) (*connect.Response[pipelinev1.GetPipelineResponse], error) {
	if err := f.record(ctx, "GetPipeline"); err != nil {
		return nil, err
	}
	if req.Msg.Id != "p1" {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("pipeline not found"))
	}
	return connect.NewResponse(&pipelinev1.GetPipelineResponse{
		Pipeline: &pipelinev1.Pipeline{
			Id: "p1", Name: "orders", SourceType: "rest_api", DatasetName: "sales",
			SourceConfig: `{"base_url":"https://user:hunter2@api.example.test","headers":{"Authorization":"Bearer sk-live-123"},"resources":[{"name":"orders","endpoint":"/orders"}]}`,
		},
		RecentRuns:     []*pipelinev1.PipelineRun{{Id: "r1", Status: "failed", ErrorMessage: "connect postgresql://app:s3cret@db.example.test/x: refused"}},
		HasCredentials: true,
	}), nil
}

func (f *fakeWorkspace) TriggerPipeline(ctx context.Context, req *connect.Request[pipelinev1.TriggerPipelineRequest]) (*connect.Response[pipelinev1.TriggerPipelineResponse], error) {
	if err := f.record(ctx, "TriggerPipeline"); err != nil {
		return nil, err
	}
	return connect.NewResponse(&pipelinev1.TriggerPipelineResponse{Run: &pipelinev1.PipelineRun{Id: "r2", PipelineId: req.Msg.Id, Status: "pending"}}), nil
}

func (f *fakeWorkspace) ListTransformations(ctx context.Context, _ *connect.Request[transformationv1.ListTransformationsRequest]) (*connect.Response[transformationv1.ListTransformationsResponse], error) {
	if err := f.record(ctx, "ListTransformations"); err != nil {
		return nil, err
	}
	return connect.NewResponse(&transformationv1.ListTransformationsResponse{Transformations: []*transformationv1.Transformation{{Id: "t1", Name: "marts", Enabled: true, LastRunStatus: "success", LastRunAt: "2026-01-02T04:00:00Z"}}}), nil
}

func (f *fakeWorkspace) GetTransformation(ctx context.Context, _ *connect.Request[transformationv1.GetTransformationRequest]) (*connect.Response[transformationv1.GetTransformationResponse], error) {
	if err := f.record(ctx, "GetTransformation"); err != nil {
		return nil, err
	}
	return connect.NewResponse(&transformationv1.GetTransformationResponse{Transformation: &transformationv1.Transformation{Id: "t1", Name: "marts"}}), nil
}

func (f *fakeWorkspace) TriggerTransformation(ctx context.Context, _ *connect.Request[transformationv1.TriggerTransformationRequest]) (*connect.Response[transformationv1.TriggerTransformationResponse], error) {
	if err := f.record(ctx, "TriggerTransformation"); err != nil {
		return nil, err
	}
	return connect.NewResponse(&transformationv1.TriggerTransformationResponse{Run: &transformationv1.TransformationRun{Id: "tr1", Status: "pending"}}), nil
}

func (f *fakeWorkspace) ListFiles(ctx context.Context, _ *connect.Request[boxrepov1.ListFilesRequest]) (*connect.Response[boxrepov1.ListFilesResponse], error) {
	if err := f.record(ctx, "ListFiles"); err != nil {
		return nil, err
	}
	return connect.NewResponse(&boxrepov1.ListFilesResponse{Files: []*boxrepov1.FileEntry{
		{Path: "models/orders.sql", Sha: "a1"},
		{Path: "profiles.yml", Sha: "a2"},
		{Path: "keys/deploy.pem", Sha: "a3"},
		{Path: "dbt_project.yml", Sha: "a4"},
	}}), nil
}

func (f *fakeWorkspace) GetFile(ctx context.Context, req *connect.Request[boxrepov1.GetFileRequest]) (*connect.Response[boxrepov1.GetFileResponse], error) {
	if err := f.record(ctx, "GetFile"); err != nil {
		return nil, err
	}
	return connect.NewResponse(&boxrepov1.GetFileResponse{Content: "select 1 -- " + req.Msg.Path, Sha: "a1"}), nil
}

func (f *fakeWorkspace) PutFile(ctx context.Context, req *connect.Request[boxrepov1.PutFileRequest]) (*connect.Response[boxrepov1.PutFileResponse], error) {
	if err := f.record(ctx, "PutFile"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.puts = append(f.puts, req.Msg)
	f.mu.Unlock()
	return connect.NewResponse(&boxrepov1.PutFileResponse{Sha: "b2"}), nil
}

func (f *fakeWorkspace) ListNotifications(ctx context.Context, _ *connect.Request[notificationv1.ListNotificationsRequest]) (*connect.Response[notificationv1.ListNotificationsResponse], error) {
	if err := f.record(ctx, "ListNotifications"); err != nil {
		return nil, err
	}
	return connect.NewResponse(&notificationv1.ListNotificationsResponse{UnreadCount: 1, Notifications: []*notificationv1.Notification{{Id: "n1", Title: "Pipeline failed"}}}), nil
}

func (f *fakeWorkspace) ListWorkflows(ctx context.Context, _ *connect.Request[workflowv1.ListWorkflowsRequest]) (*connect.Response[workflowv1.ListWorkflowsResponse], error) {
	if err := f.record(ctx, "ListWorkflows"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []*workflowv1.Workflow{{
		Id: "w1", Name: "nightly", Schedule: "@daily", Enabled: true, LastRunStatus: "failed", LastRunAt: "2026-01-02T05:00:00Z",
		Steps: []*workflowv1.WorkflowStep{{Kind: "pipeline", RefId: "p1", Name: "orders"}, {Kind: "transformation", RefId: "t1", Name: "marts", Position: 1}},
	}}
	for _, w := range f.workflows {
		out = append(out, w)
	}
	return connect.NewResponse(&workflowv1.ListWorkflowsResponse{Workflows: out}), nil
}

func (f *fakeWorkspace) GetWorkflow(ctx context.Context, req *connect.Request[workflowv1.GetWorkflowRequest]) (*connect.Response[workflowv1.GetWorkflowResponse], error) {
	if err := f.record(ctx, "GetWorkflow"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if w, ok := f.workflows[req.Msg.Id]; ok {
		return connect.NewResponse(&workflowv1.GetWorkflowResponse{Workflow: w}), nil
	}
	if req.Msg.Id != "w1" {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("workflow not found"))
	}
	return connect.NewResponse(&workflowv1.GetWorkflowResponse{
		Workflow: &workflowv1.Workflow{
			Id: "w1", Name: "nightly", Schedule: "@daily", Enabled: true,
			Steps: []*workflowv1.WorkflowStep{{Kind: "pipeline", RefId: "p1", Name: "orders"}, {Kind: "transformation", RefId: "t1", Name: "marts", Position: 1}},
		},
		RecentRuns: []*workflowv1.WorkflowRun{{
			Id: "wr1", WorkflowId: "w1", Status: "failed", Trigger: "schedule", ErrorMessage: "step 0 failed",
			StepRuns: []*workflowv1.WorkflowStepRun{
				{Position: 0, Kind: "pipeline", RefId: "p1", Name: "orders", Status: "failed", PipelineRunId: "r1", ErrorMessage: "connect postgresql://app:s3cret@db.example.test/x: refused"},
				{Position: 1, Kind: "transformation", RefId: "t1", Name: "marts", Status: "skipped"},
			},
		}},
	}), nil
}

func (f *fakeWorkspace) ListWorkflowRuns(ctx context.Context, req *connect.Request[workflowv1.ListWorkflowRunsRequest]) (*connect.Response[workflowv1.ListWorkflowRunsResponse], error) {
	if err := f.record(ctx, "ListWorkflowRuns"); err != nil {
		return nil, err
	}
	// Two pages of one run each.
	if req.Msg.PageToken == "" {
		return connect.NewResponse(&workflowv1.ListWorkflowRunsResponse{Runs: []*workflowv1.WorkflowRun{{Id: "wr2", Status: "success"}}, NextPageToken: "page2"}), nil
	}
	return connect.NewResponse(&workflowv1.ListWorkflowRunsResponse{Runs: []*workflowv1.WorkflowRun{{Id: "wr1", Status: "failed"}}}), nil
}

func (f *fakeWorkspace) TriggerWorkflow(ctx context.Context, req *connect.Request[workflowv1.TriggerWorkflowRequest]) (*connect.Response[workflowv1.TriggerWorkflowResponse], error) {
	if err := f.record(ctx, "TriggerWorkflow"); err != nil {
		return nil, err
	}
	return connect.NewResponse(&workflowv1.TriggerWorkflowResponse{Run: &workflowv1.WorkflowRun{Id: "wr3", WorkflowId: req.Msg.Id, Status: "pending", Trigger: "manual"}}), nil
}

func (f *fakeWorkspace) CreateWorkflow(ctx context.Context, req *connect.Request[workflowv1.CreateWorkflowRequest]) (*connect.Response[workflowv1.CreateWorkflowResponse], error) {
	if err := f.record(ctx, "CreateWorkflow"); err != nil {
		return nil, err
	}
	w := &workflowv1.Workflow{Id: "w2", Name: req.Msg.Name, Description: req.Msg.Description, Schedule: req.Msg.Schedule, Enabled: true, Steps: req.Msg.Steps}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.workflows == nil {
		f.workflows = map[string]*workflowv1.Workflow{}
	}
	f.workflows[w.Id] = w
	return connect.NewResponse(&workflowv1.CreateWorkflowResponse{Workflow: w}), nil
}

func (f *fakeWorkspace) UpdateWorkflow(ctx context.Context, req *connect.Request[workflowv1.UpdateWorkflowRequest]) (*connect.Response[workflowv1.UpdateWorkflowResponse], error) {
	if err := f.record(ctx, "UpdateWorkflow"); err != nil {
		return nil, err
	}
	w := &workflowv1.Workflow{Id: req.Msg.Id, Name: req.Msg.Name, Description: req.Msg.Description, Schedule: req.Msg.Schedule, Enabled: req.Msg.Enabled, Steps: req.Msg.Steps}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates = append(f.updates, req.Msg)
	if f.workflows == nil {
		f.workflows = map[string]*workflowv1.Workflow{}
	}
	f.workflows[w.Id] = w
	return connect.NewResponse(&workflowv1.UpdateWorkflowResponse{Workflow: w}), nil
}

// testJWKS returns a Keyfunc over a fresh ed25519 key and a signer for it (a
// twin of core's and server's helpers of the same name).
func testJWKS(t *testing.T) (keyfunc.Keyfunc, func(claims jwt.MapClaims) string) {
	t.Helper()
	ctx := context.Background()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	jwk, err := jwkset.NewJWKFromKey(pub, jwkset.JWKOptions{
		Metadata: jwkset.JWKMetadataOptions{KID: "test-key", ALG: jwkset.AlgEdDSA, USE: jwkset.UseSig},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := jwkset.NewMemoryStorage()
	if err := store.KeyWrite(ctx, jwk); err != nil {
		t.Fatal(err)
	}
	jwks, err := keyfunc.New(keyfunc.Options{Ctx: ctx, Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	return jwks, func(claims jwt.MapClaims) string {
		token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
		token.Header["kid"] = "test-key"
		s, err := token.SignedString(priv)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
}

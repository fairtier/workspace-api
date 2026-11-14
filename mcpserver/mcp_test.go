package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/golang-jwt/jwt/v5"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

const (
	testIssuer  = "https://auth.example.test"
	testConsole = "https://console.example.test"
)

type testEnv struct {
	srv    *httptest.Server
	fake   *fakeWorkspace
	pats   *workspace.AccessTokenService
	signIn func(sub string) string
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	jwks, sign := testJWKS(t)
	pats := &workspace.AccessTokenService{
		Workspaces: &workspace.StaticResolver{Workspace: workspace.Workspace{Slug: "acme", CasdoorOrg: "customer-acme"}},
		Tokens:     &workspace.MemoryAccessTokenStore{},
	}
	fake := &fakeWorkspace{}
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	Register(mux, fake.backends(), Options{
		ResourceURL:          srv.URL + Path,
		AuthorizationServers: []string{testIssuer},
		AllowedOrigins:       []string{testConsole},
		Verifier:             NewTokenVerifier(pats, core.UserAuth{JWKS: jwks, Issuer: testIssuer}),
		Version:              "test",
	})
	return &testEnv{
		srv: srv, fake: fake, pats: pats,
		signIn: func(sub string) string {
			return sign(jwt.MapClaims{"sub": sub, "iss": testIssuer, "exp": time.Now().Add(time.Hour).Unix(), "email": "alice@example.test", "preferred_username": "alice"})
		},
	}
}

func (e *testEnv) mintPAT(t *testing.T, scopes ...string) (string, *workspace.AccessToken) {
	t.Helper()
	tok, secret, err := e.pats.CreateAccessToken(context.Background(), "customer-acme/alice", workspace.CreateAccessTokenInput{Name: "agent", Scopes: scopes})
	if err != nil {
		t.Fatal(err)
	}
	return secret, tok
}

type bearerTransport struct {
	token  string
	header http.Header
}

func (b bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	for k, v := range b.header {
		r.Header[k] = v
	}
	return http.DefaultTransport.RoundTrip(r)
}

func (e *testEnv) connect(t *testing.T, token string) *mcp.ClientSession {
	t.Helper()
	client := mcp.NewClient(&mcp.Implementation{Name: "test-agent", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{
		Endpoint:             e.srv.URL + Path,
		HTTPClient:           &http.Client{Transport: bearerTransport{token: token}},
		DisableStandaloneSSE: true,
		MaxRetries:           -1,
	}, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func toolNames(t *testing.T, s *mcp.ClientSession) []string {
	t.Helper()
	res, err := s.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	var names []string
	for _, tool := range res.Tools {
		names = append(names, tool.Name)
	}
	return names
}

func call(t *testing.T, s *mcp.ClientSession, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	res, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("tools/call %s: %v", name, err)
	}
	return res
}

func text(res *mcp.CallToolResult) string {
	var b strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}

var (
	readTools = []string{
		"workspace_status", "list_tables", "describe_table", "run_query", "list_pipelines", "get_pipeline",
		"list_transformations", "get_transformation", "list_workflows", "get_workflow", "list_workflow_runs",
		"list_repo_files", "read_repo_file", "list_notifications",
	}
	writeTools = []string{"trigger_pipeline", "trigger_transformation", "trigger_workflow", "create_workflow", "update_workflow", "write_repo_file"}
)

func TestMCP_ToolsListDependsOnScope(t *testing.T) {
	env := newTestEnv(t)
	readPAT, _ := env.mintPAT(t, workspace.ScopeWorkspaceRead)
	writePAT, _ := env.mintPAT(t, workspace.ScopeWorkspaceWrite)

	got := toolNames(t, env.connect(t, readPAT))
	slices.Sort(got)
	want := slices.Clone(readTools)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("read token sees %v, want %v", got, want)
	}

	if len(got) != 14 {
		t.Errorf("read token sees %d tools, want 14", len(got))
	}
	if slices.Contains(got, "delete_workflow") {
		t.Error("a delete_workflow tool exists")
	}

	got = toolNames(t, env.connect(t, writePAT))
	slices.Sort(got)
	want = append(slices.Clone(readTools), writeTools...)
	slices.Sort(want)
	if !slices.Equal(got, want) || len(got) != 20 {
		t.Errorf("write token sees %v, want %v", got, want)
	}

	// A sign-in token is a full member: every tool.
	if got := toolNames(t, env.connect(t, env.signIn("customer-acme/alice"))); len(got) != len(readTools)+len(writeTools) {
		t.Errorf("sign-in token sees %v", got)
	}
}

func TestMCP_ToolAnnotations(t *testing.T) {
	env := newTestEnv(t)
	res, err := env.connect(t, env.signIn("customer-acme/alice")).ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range res.Tools {
		write := slices.Contains(writeTools, tool.Name)
		if tool.Annotations == nil || tool.Annotations.ReadOnlyHint == write {
			t.Errorf("%s: readOnlyHint wrong (write=%v): %+v", tool.Name, write, tool.Annotations)
		}
		if tool.Description == "" || tool.InputSchema == nil {
			t.Errorf("%s: missing description or input schema", tool.Name)
		}
	}
}

func TestMCP_WriteToolWithReadTokenFails(t *testing.T) {
	env := newTestEnv(t)
	readPAT, _ := env.mintPAT(t, workspace.ScopeWorkspaceRead)
	s := env.connect(t, readPAT)

	for name, args := range map[string]map[string]any{
		"trigger_pipeline": {"id": "p1"},
		"trigger_workflow": {"id": "w1"},
		"create_workflow":  {"name": "x", "steps": []any{map[string]any{"kind": "pipeline", "ref_id": "p1"}}},
		"update_workflow":  {"id": "w1", "enabled": false},
	} {
		res, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
		if err == nil && !res.IsError {
			t.Fatalf("%s with a read token succeeded: %s", name, text(res))
		}
	}
	for _, c := range []string{"TriggerPipeline", "TriggerWorkflow", "CreateWorkflow", "UpdateWorkflow", "GetWorkflow"} {
		if env.fake.called(c) {
			t.Fatalf("the backend was reached: %s", c)
		}
	}
}

func structured[T any](t *testing.T, res *mcp.CallToolResult) T {
	t.Helper()
	var out T
	raw, _ := json.Marshal(res.StructuredContent)
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("%v (%s)", err, raw)
	}
	return out
}

func TestMCP_WorkflowHappyPath(t *testing.T) {
	env := newTestEnv(t)
	writePAT, _ := env.mintPAT(t, workspace.ScopeWorkspaceWrite)
	s := env.connect(t, writePAT)

	res := call(t, s, "create_workflow", map[string]any{
		"name": "daily", "schedule": "0 0 * * *",
		"steps": []any{map[string]any{"kind": "pipeline", "ref_id": "p1"}, map[string]any{"kind": "transformation", "ref_id": "t1"}},
	})
	if res.IsError {
		t.Fatalf("create_workflow: %s", text(res))
	}
	created := structured[workflowOut](t, res).Workflow
	if created.ID != "w2" || !created.Enabled || len(created.Steps) != 2 || created.Steps[1].Kind != "transformation" {
		t.Fatalf("created = %+v", created)
	}

	// Only the flag: SetWorkflowEnabled, no full rewrite.
	res = call(t, s, "update_workflow", map[string]any{"id": "w2", "enabled": false})
	if res.IsError || structured[workflowOut](t, res).Workflow.Enabled {
		t.Fatalf("update_workflow: %s", text(res))
	}
	if !env.fake.called("SetWorkflowEnabled") || env.fake.called("UpdateWorkflow") {
		t.Error("enabled-only update did not use SetWorkflowEnabled")
	}
	// Anything else is a read-modify-write that keeps the omitted fields.
	res = call(t, s, "update_workflow", map[string]any{"id": "w2", "description": "nightly load"})
	if res.IsError {
		t.Fatalf("update_workflow: %s", text(res))
	}
	upd := env.fake.updates[len(env.fake.updates)-1]
	if upd.Enabled || upd.Name != "daily" || upd.Description != "nightly load" || upd.Schedule != "0 0 * * *" || len(upd.Steps) != 2 {
		t.Errorf("update request = %+v", upd)
	}
	// An explicit "" clears the schedule; steps replace the list.
	res = call(t, s, "update_workflow", map[string]any{"id": "w2", "schedule": "", "steps": []any{map[string]any{"kind": "pipeline", "ref_id": "p1"}}})
	if res.IsError {
		t.Fatalf("update_workflow: %s", text(res))
	}
	upd = env.fake.updates[len(env.fake.updates)-1]
	if upd.Schedule != "" || upd.Enabled || len(upd.Steps) != 1 {
		t.Errorf("update request = %+v", upd)
	}

	run := structured[workflowRunOut](t, call(t, s, "trigger_workflow", map[string]any{"id": "w2"})).Run
	if run.Status != "pending" || run.Trigger != "manual" {
		t.Errorf("trigger_workflow run = %+v", run)
	}

	got := structured[getWorkflowOut](t, call(t, s, "get_workflow", map[string]any{"id": "w1"}))
	if len(got.RecentRuns) != 1 || len(got.RecentRuns[0].StepRuns) != 2 || got.RecentRuns[0].StepRuns[1].Status != "skipped" {
		t.Fatalf("get_workflow = %+v", got)
	}
	if strings.Contains(got.RecentRuns[0].StepRuns[0].ErrorMessage, "s3cret") {
		t.Error("get_workflow leaked a step run's credential")
	}

	page := structured[listWorkflowRunsOut](t, call(t, s, "list_workflow_runs", map[string]any{"workflow_id": "w1"}))
	if len(page.Runs) != 1 || page.NextPageToken != "page2" {
		t.Fatalf("page 1 = %+v", page)
	}
	page = structured[listWorkflowRunsOut](t, call(t, s, "list_workflow_runs", map[string]any{"workflow_id": "w1", "page_token": page.NextPageToken}))
	if len(page.Runs) != 1 || page.Runs[0].ID != "wr1" || page.NextPageToken != "" {
		t.Fatalf("page 2 = %+v", page)
	}

	if res := call(t, s, "update_workflow", map[string]any{"id": "nope", "enabled": true}); !res.IsError || !strings.Contains(text(res), "not found") {
		t.Errorf("update of a missing workflow: %s", text(res))
	}
}

func TestMCP_WorkspaceStatus(t *testing.T) {
	env := newTestEnv(t)
	readPAT, _ := env.mintPAT(t, workspace.ScopeWorkspaceRead)
	s := env.connect(t, readPAT)

	res := call(t, s, "workspace_status", map[string]any{})
	if res.IsError {
		t.Fatalf("workspace_status: %s", text(res))
	}
	if raw := text(res); strings.Contains(raw, "s3cret") || strings.Contains(raw, "hunter2") {
		t.Fatalf("workspace_status leaked a credential: %s", raw)
	}
	out := structured[workspaceStatusOut](t, res)
	if len(out.Pipelines) != 2 || len(out.Transformations) != 1 || len(out.Workflows) != 1 || out.Now == "" {
		t.Fatalf("status = %+v", out)
	}
	if p := out.Pipelines[0]; p.LastRunStatus != "failed" || !strings.Contains(p.LastError, "refused") || !strings.Contains(p.LastError, "[redacted]") {
		t.Errorf("failed pipeline = %+v", p)
	}
	// The step's own error is dlt-shaped: its cause, not its preamble.
	if w := out.Workflows[0]; !strings.HasPrefix(w.LastError, `step 1 (pipeline "orders") failed: connect `) || !strings.HasSuffix(w.LastError, "refused") {
		t.Errorf("failed workflow = %+v", w)
	}
	if out.Transformations[0].LastError != "" {
		t.Errorf("a successful transformation carries an error: %+v", out.Transformations[0])
	}
	// Failed pipeline, never-run scheduled pipeline, failed workflow.
	if len(out.Attention) != 3 || !strings.Contains(out.Attention[1], "never run") {
		t.Errorf("attention = %q", out.Attention)
	}
	// Only failed items cost a get call.
	if env.fake.called("GetTransformation") {
		t.Error("a successful transformation was fetched")
	}
	if out.AgentSQL.EnforcedBy != "unknown" {
		t.Errorf("agent_sql = %+v, want unknown from a backend that reports nothing", out.AgentSQL)
	}
}

// workspace_status says what keeps run_query read-only, with the reason
// redacted like any other error text.
func TestMCP_WorkspaceStatusReportsSQLSafety(t *testing.T) {
	env := newTestEnv(t)
	env.fake.safety = SQLSafety{
		EnforcedBy: "guard_only",
		Mode:       "guard_only",
		Detail:     "the query engine has no read-only session: dial postgres://agent:hunter2@engine:5432\nsecond line",
	}
	readPAT, _ := env.mintPAT(t, workspace.ScopeWorkspaceRead)
	res := call(t, env.connect(t, readPAT), "workspace_status", map[string]any{})
	if res.IsError {
		t.Fatalf("workspace_status: %s", text(res))
	}
	got := structured[workspaceStatusOut](t, res).AgentSQL
	if got.EnforcedBy != "guard_only" || !strings.Contains(got.Detail, "no read-only session") {
		t.Errorf("agent_sql = %+v", got)
	}
	if strings.Contains(text(res), "hunter2") {
		t.Errorf("agent_sql leaked a credential: %+v", got)
	}
}

// A statement the lexical guard lets through but the engine's read-only
// session refuses comes back as a tool error carrying the engine's reason.
func TestMCP_RunQueryEngineRefusal(t *testing.T) {
	env := newTestEnv(t)
	env.fake.queryErr = connect.NewError(connect.CodePermissionDenied,
		errors.New(`the query engine's read-only session refused this statement: table function "read_blob" is not allowed`))
	readPAT, _ := env.mintPAT(t, workspace.ScopeWorkspaceRead)
	res := call(t, env.connect(t, readPAT), "run_query", map[string]any{"sql": "SELECT 1"})
	if !res.IsError {
		t.Fatalf("run_query succeeded: %s", text(res))
	}
	if msg := text(res); !strings.Contains(msg, "read-only session refused") || !strings.Contains(msg, "read_blob") {
		t.Errorf("tool error = %q, want the engine's refusal and reason", msg)
	}
}

// The scope check inside a write tool holds even if the tool were somehow
// listed for a read token.
func TestCallContext_WriteScope(t *testing.T) {
	read := &auth.TokenInfo{Scopes: []string{workspace.ScopeWorkspaceRead}, Extra: map[string]any{callerKey: Caller{UserID: "alice"}}}
	req := &mcp.CallToolRequest{Extra: &mcp.RequestExtra{TokenInfo: read}}
	if _, err := callContext(context.Background(), req, true); !errors.Is(err, errWriteScope) {
		t.Errorf("write tool with read scope: %v, want errWriteScope", err)
	}
	ctx, err := callContext(context.Background(), req, false)
	if err != nil || core.UserIDFromContext(ctx) != "alice" {
		t.Errorf("read tool: ctx caller %q, err %v", core.UserIDFromContext(ctx), err)
	}
	if _, err := callContext(context.Background(), &mcp.CallToolRequest{Extra: &mcp.RequestExtra{}}, false); err == nil {
		t.Error("a call with no token info was authorized")
	}
}

func TestMCP_WriteToolsReachBackendAsCaller(t *testing.T) {
	env := newTestEnv(t)
	writePAT, tok := env.mintPAT(t, workspace.ScopeWorkspaceWrite)
	s := env.connect(t, writePAT)

	if res := call(t, s, "trigger_pipeline", map[string]any{"id": "p1"}); res.IsError {
		t.Fatalf("trigger_pipeline: %s", text(res))
	}
	if res := call(t, s, "trigger_transformation", map[string]any{"id": "t1"}); res.IsError {
		t.Fatalf("trigger_transformation: %s", text(res))
	}
	res := call(t, s, "write_repo_file", map[string]any{
		"repo": "transformations", "path": "models/new.sql", "content": "select 1", "message": "add model",
	})
	if res.IsError {
		t.Fatalf("write_repo_file: %s", text(res))
	}
	for _, c := range env.fake.callers {
		if c != "customer-acme/alice" {
			t.Errorf("backend saw caller %q", c)
		}
	}
	msg := env.fake.puts[0].Message
	if !strings.HasPrefix(msg, "add model\n\nVia FairTier MCP, access token "+tok.Prefix) {
		t.Errorf("commit message %q does not name the token", msg)
	}
	if strings.Contains(msg, writePAT) {
		t.Fatal("commit message carries the secret")
	}
}

func TestMCP_RunQuery(t *testing.T) {
	env := newTestEnv(t)
	readPAT, _ := env.mintPAT(t, workspace.ScopeWorkspaceRead)
	s := env.connect(t, readPAT)

	res := call(t, s, "run_query", map[string]any{"sql": "SELECT n, s FROM sales.orders", "max_rows": 5000})
	if res.IsError {
		t.Fatalf("run_query: %s", text(res))
	}
	var out runQueryOut
	raw, _ := json.Marshal(res.StructuredContent)
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if out.RowCount != 2 || len(out.Columns) != 2 || out.Rows[0][1] != "a" {
		t.Errorf("result = %+v", out)
	}

	for _, sql := range []string{"DELETE FROM sales.orders", "SELECT 1; DROP TABLE sales.orders", "SELECT * FROM read_text('/proc/self/environ')"} {
		res := call(t, s, "run_query", map[string]any{"sql": sql})
		if !res.IsError || !strings.Contains(text(res), "read-only") {
			t.Errorf("%q: IsError=%v %s", sql, res.IsError, text(res))
		}
	}
	if n := len(env.fake.queries); n != 1 {
		t.Errorf("%d queries reached the engine, want only the allowed one", n)
	}
}

func TestMCP_SecretHygiene(t *testing.T) {
	env := newTestEnv(t)
	s := env.connect(t, env.signIn("customer-acme/alice"))

	got := text(call(t, s, "get_pipeline", map[string]any{"id": "p1"}))
	for _, secret := range []string{"hunter2", "sk-live-123", "s3cret"} {
		if strings.Contains(got, secret) {
			t.Errorf("get_pipeline leaked %q: %s", secret, got)
		}
	}
	if !strings.Contains(got, "/orders") {
		t.Errorf("get_pipeline dropped non-secret config: %s", got)
	}

	files := text(call(t, s, "list_repo_files", map[string]any{"repo": "transformations"}))
	if strings.Contains(files, "profiles.yml") || strings.Contains(files, "deploy.pem") || !strings.Contains(files, "models/orders.sql") {
		t.Errorf("list_repo_files = %s", files)
	}

	for _, p := range []string{"profiles.yml", ".dlt/secrets.toml", "keys/deploy.pem", "x/.env.local"} {
		if res := call(t, s, "read_repo_file", map[string]any{"repo": "transformations", "path": p}); !res.IsError {
			t.Errorf("read_repo_file %s succeeded", p)
		}
		if res := call(t, s, "write_repo_file", map[string]any{"repo": "transformations", "path": p, "content": "x", "message": "m"}); !res.IsError {
			t.Errorf("write_repo_file %s succeeded", p)
		}
	}
	if env.fake.called("GetFile") || env.fake.called("PutFile") {
		t.Error("a secrets path reached the repository backend")
	}
}

func TestMCP_AllReadToolsAnswer(t *testing.T) {
	env := newTestEnv(t)
	readPAT, _ := env.mintPAT(t, workspace.ScopeWorkspaceRead)
	s := env.connect(t, readPAT)
	for name, args := range map[string]map[string]any{
		"list_tables":          {},
		"describe_table":       {"namespace": "sales", "name": "orders"},
		"list_pipelines":       {},
		"get_pipeline":         {"id": "p1"},
		"list_transformations": {},
		"get_transformation":   {"id": "t1"},
		"list_repo_files":      {"repo": "transformations"},
		"read_repo_file":       {"repo": "transformations", "path": "models/orders.sql"},
		"list_notifications":   {},
		"workspace_status":     {},
		"list_workflows":       {},
		"get_workflow":         {"id": "w1"},
		"list_workflow_runs":   {"workflow_id": "w1"},
	} {
		if res := call(t, s, name, args); res.IsError || res.StructuredContent == nil {
			t.Errorf("%s: IsError=%v %s", name, res.IsError, text(res))
		}
	}
	// A backend error comes back as a tool error, not a protocol failure.
	if res := call(t, s, "get_pipeline", map[string]any{"id": "nope"}); !res.IsError || !strings.Contains(text(res), "not found") {
		t.Errorf("missing pipeline: %s", text(res))
	}
}

func post(t *testing.T, url, token string, header http.Header) *http.Response {
	t.Helper()
	body := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"1"}}}`
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestMCP_Unauthenticated(t *testing.T) {
	env := newTestEnv(t)
	secret, tok := env.mintPAT(t, workspace.ScopeWorkspaceRead)
	if err := env.pats.RevokeAccessToken(context.Background(), "customer-acme/alice", tok.ID); err != nil {
		t.Fatal(err)
	}
	_, sign := testJWKS(t) // a key the server does not trust
	for name, token := range map[string]string{
		"none":          "",
		"garbage":       "garbage",
		"unknown PAT":   "ftw_" + strings.Repeat("ab", 32),
		"revoked PAT":   secret,
		"foreign JWT":   sign(jwt.MapClaims{"sub": "customer-acme/alice", "iss": testIssuer, "exp": time.Now().Add(time.Hour).Unix()}),
		"malformed JWT": "eyJhbGciOiJFZERTQSIsImtpZCI6InRlc3Qta2V5In0.eyJleHAiOjF9.c2ln",
		"service acct":  env.signIn("admin/dlt-worker"),
	} {
		resp := post(t, env.srv.URL+Path, token, nil)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s: status %d, want 401", name, resp.StatusCode)
			continue
		}
		want := `Bearer resource_metadata="` + env.srv.URL + `/.well-known/oauth-protected-resource/mcp"`
		if got := resp.Header.Get("WWW-Authenticate"); !strings.HasPrefix(got, want) {
			t.Errorf("%s: WWW-Authenticate = %q, want prefix %q", name, got, want)
		}
	}
}

func TestMCP_ProtectedResourceMetadata(t *testing.T) {
	env := newTestEnv(t)
	for _, p := range []string{"/.well-known/oauth-protected-resource/mcp", "/.well-known/oauth-protected-resource"} {
		resp, err := http.Get(env.srv.URL + p)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		var meta struct {
			Resource             string   `json:"resource"`
			AuthorizationServers []string `json:"authorization_servers"`
			Scopes               []string `json:"scopes_supported"`
		}
		if err := json.Unmarshal(body, &meta); err != nil {
			t.Fatalf("%s: %v (%s)", p, err, body)
		}
		if meta.Resource != env.srv.URL+Path || !slices.Equal(meta.AuthorizationServers, []string{testIssuer}) || len(meta.Scopes) != 2 {
			t.Errorf("%s: metadata = %+v", p, meta)
		}
	}
}

func TestMCP_Origin(t *testing.T) {
	env := newTestEnv(t)
	token := env.signIn("customer-acme/alice")

	resp := post(t, env.srv.URL+Path, token, http.Header{"Origin": {"https://evil.example.test"}})
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("foreign origin: status %d, want 403", resp.StatusCode)
	}
	// Checked before authentication: a foreign page learns nothing about
	// the token either.
	resp = post(t, env.srv.URL+Path, "", http.Header{"Origin": {"https://evil.example.test"}})
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("foreign origin, no token: status %d, want 403", resp.StatusCode)
	}
	resp = post(t, env.srv.URL+Path, token, http.Header{"Origin": {testConsole}})
	if resp.StatusCode != http.StatusOK {
		t.Errorf("console origin: status %d, want 200", resp.StatusCode)
	}
	resp = post(t, env.srv.URL+Path, token, nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("no origin: status %d, want 200", resp.StatusCode)
	}
}

func TestMetadataURL(t *testing.T) {
	for in, want := range map[string]string{
		"https://api.example.test/mcp":  "https://api.example.test/.well-known/oauth-protected-resource/mcp",
		"https://api.example.test":      "https://api.example.test/.well-known/oauth-protected-resource",
		"http://127.0.0.1:8080/mcp":     "http://127.0.0.1:8080/.well-known/oauth-protected-resource/mcp",
		"not a url":                     "",
		"https://api.example.test/a/b/": "https://api.example.test/.well-known/oauth-protected-resource/a/b/",
	} {
		if got := metadataURL(in); got != want {
			t.Errorf("metadataURL(%q) = %q, want %q", in, got, want)
		}
	}
}

package server

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/duckflight"
	queryv1 "github.com/fairtier/workspace-api/proto/query/v1"
	"github.com/fairtier/workspace-api/workspace"
)

type mockCustomerReader struct {
	getBySlug   func(ctx context.Context, slug string) (*workspace.Workspace, error)
	getByUserID func(ctx context.Context, userID core.UserID) (*workspace.Workspace, error)
}

func (m *mockCustomerReader) GetWorkspace(ctx context.Context, slug string) (*workspace.Workspace, error) {
	return m.getBySlug(ctx, slug)
}

func (m *mockCustomerReader) GetWorkspaceByUser(ctx context.Context, userID core.UserID) (*workspace.Workspace, error) {
	return m.getByUserID(ctx, userID)
}

type mockExecutor struct {
	execute func(ctx context.Context, endpoint, token, sql string, maxRows int) (*duckflight.Result, error)
}

func (m *mockExecutor) Execute(ctx context.Context, endpoint, token, sql string, maxRows int) (*duckflight.Result, error) {
	return m.execute(ctx, endpoint, token, sql, maxRows)
}

func authedCtx() context.Context {
	return context.WithValue(context.Background(), userIDKey, core.UserID("user-1"))
}

func provisionedCustomer() *workspace.Workspace {
	return &workspace.Workspace{
		DuckFlightURL:       "https://duckflight.customer-acme.example.com",
		DuckFlightAuthToken: "tok-1",
	}
}

func queryServer(exec *mockExecutor) *QueryServer {
	return &QueryServer{
		Workspaces: &mockCustomerReader{
			getByUserID: func(context.Context, core.UserID) (*workspace.Workspace, error) {
				return provisionedCustomer(), nil
			},
		},
		Executor: exec,
	}
}

func TestExecuteQueryUnauthenticated(t *testing.T) {
	s := queryServer(&mockExecutor{})
	_, err := s.ExecuteQuery(context.Background(), connect.NewRequest(&queryv1.ExecuteQueryRequest{Sql: "SELECT 1"}))
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("code = %v, want unauthenticated", connect.CodeOf(err))
	}
}

func TestExecuteQueryNotEnabled(t *testing.T) {
	s := &QueryServer{
		Workspaces: &mockCustomerReader{
			getByUserID: func(context.Context, core.UserID) (*workspace.Workspace, error) {
				return &workspace.Workspace{}, nil // no DuckFlight configured
			},
		},
		Executor: &mockExecutor{},
	}
	_, err := s.ExecuteQuery(authedCtx(), connect.NewRequest(&queryv1.ExecuteQueryRequest{Sql: "SELECT 1"}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("code = %v, want failed_precondition", connect.CodeOf(err))
	}
}

func TestExecuteQueryEmptySQL(t *testing.T) {
	s := queryServer(&mockExecutor{})
	_, err := s.ExecuteQuery(authedCtx(), connect.NewRequest(&queryv1.ExecuteQueryRequest{Sql: "   "}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("code = %v, want invalid_argument", connect.CodeOf(err))
	}
}

func TestExecuteQueryRowClampAndTenantBinding(t *testing.T) {
	tests := []struct {
		name      string
		requested int32
		wantMax   int
	}{
		{name: "zero means default", requested: 0, wantMax: 500},
		{name: "negative means default", requested: -5, wantMax: 500},
		{name: "in range passes through", requested: 200, wantMax: 200},
		{name: "above cap clamps", requested: 100000, wantMax: 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotMax int
			exec := &mockExecutor{execute: func(_ context.Context, endpoint, token, sql string, maxRows int) (*duckflight.Result, error) {
				// Tenant binding: endpoint/token must come from the caller's
				// own customer record.
				if endpoint != "https://duckflight.customer-acme.example.com" || token != "tok-1" {
					t.Errorf("executor got endpoint=%q token=%q", endpoint, token)
				}
				gotMax = maxRows
				return &duckflight.Result{}, nil
			}}
			s := queryServer(exec)
			_, err := s.ExecuteQuery(authedCtx(), connect.NewRequest(&queryv1.ExecuteQueryRequest{Sql: "SELECT 1", MaxRows: tt.requested}))
			if err != nil {
				t.Fatalf("ExecuteQuery: %v", err)
			}
			if gotMax != tt.wantMax {
				t.Errorf("maxRows = %d, want %d", gotMax, tt.wantMax)
			}
		})
	}
}

func TestExecuteQueryResponseShape(t *testing.T) {
	exec := &mockExecutor{execute: func(context.Context, string, string, string, int) (*duckflight.Result, error) {
		return &duckflight.Result{
			Columns:   []duckflight.Column{{Name: "id", Type: "int64"}, {Name: "name", Type: "utf8"}},
			Rows:      [][]any{{"1", "alice"}, {"2", nil}},
			Truncated: true,
		}, nil
	}}
	s := queryServer(exec)
	resp, err := s.ExecuteQuery(authedCtx(), connect.NewRequest(&queryv1.ExecuteQueryRequest{Sql: "SELECT * FROM t"}))
	if err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	if got := resp.Msg.RowCount; got != 2 {
		t.Errorf("RowCount = %d, want 2", got)
	}
	if !resp.Msg.Truncated {
		t.Error("Truncated = false, want true")
	}
	if len(resp.Msg.Columns) != 2 || resp.Msg.Columns[0].Name != "id" || resp.Msg.Columns[0].Type != "int64" {
		t.Errorf("Columns = %v", resp.Msg.Columns)
	}
	if resp.Msg.Rows[0] != `["1","alice"]` || resp.Msg.Rows[1] != `["2",null]` {
		t.Errorf("Rows = %v", resp.Msg.Rows)
	}
}

func TestListTables(t *testing.T) {
	exec := &mockExecutor{execute: func(_ context.Context, _, _, sql string, _ int) (*duckflight.Result, error) {
		if sql != listTablesSQL {
			t.Errorf("sql = %q, want the canned listTablesSQL", sql)
		}
		return &duckflight.Result{
			Rows: [][]any{{"raw", "orders"}, {"marts", "revenue"}},
		}, nil
	}}
	s := queryServer(exec)
	resp, err := s.ListTables(authedCtx(), connect.NewRequest(&queryv1.ListTablesRequest{}))
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}
	if len(resp.Msg.Tables) != 2 || resp.Msg.Tables[0].Namespace != "raw" || resp.Msg.Tables[0].Name != "orders" {
		t.Errorf("Tables = %v", resp.Msg.Tables)
	}
}

func TestDescribeTableEscapesIdentifiers(t *testing.T) {
	var gotSQL string
	exec := &mockExecutor{execute: func(_ context.Context, _, _, sql string, _ int) (*duckflight.Result, error) {
		gotSQL = sql
		// DESCRIBE returns 6 columns: column_name, column_type, null, key,
		// default, extra. Only the first three are read.
		return &duckflight.Result{Rows: [][]any{
			{"id", "BIGINT", "YES", "PRI", nil, nil},
			{"name", "VARCHAR", "NO", nil, nil, nil},
		}}, nil
	}}
	s := queryServer(exec)
	resp, err := s.DescribeTable(authedCtx(), connect.NewRequest(&queryv1.DescribeTableRequest{
		Namespace: `ra"w`,
		Name:      `orders"; DROP TABLE x; --`,
	}))
	if err != nil {
		t.Fatalf("DescribeTable: %v", err)
	}
	// DESCRIBE forces the lazy Iceberg bind so real columns are returned; the
	// double-quote in each identifier is doubled, neutralizing the injection.
	want := `DESCRIBE "ra""w"."orders""; DROP TABLE x; --"`
	if gotSQL != want {
		t.Errorf("sql = %q\nwant  %q", gotSQL, want)
	}
	if len(resp.Msg.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(resp.Msg.Columns))
	}
	if c := resp.Msg.Columns[0]; c.Name != "id" || c.Type != "BIGINT" || !c.Nullable {
		t.Errorf("column[0] = %v", c)
	}
	if c := resp.Msg.Columns[1]; c.Name != "name" || c.Type != "VARCHAR" || c.Nullable {
		t.Errorf("column[1] = %v", c)
	}
}

func TestDescribeTableRequiresIdentifiers(t *testing.T) {
	s := queryServer(&mockExecutor{})
	_, err := s.DescribeTable(authedCtx(), connect.NewRequest(&queryv1.DescribeTableRequest{Namespace: "", Name: "t"}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("code = %v, want invalid_argument", connect.CodeOf(err))
	}
}

func TestQueryErrorMapping(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want connect.Code
	}{
		{name: "duckdb syntax error", err: status.Error(codes.InvalidArgument, `Parser Error: syntax error at or near "SELEC"`), want: connect.CodeInvalidArgument},
		{name: "catalog not found", err: status.Error(codes.NotFound, "Table with name nope does not exist"), want: connect.CodeInvalidArgument},
		{name: "statement timeout", err: status.Error(codes.DeadlineExceeded, "query exceeded time limit"), want: connect.CodeDeadlineExceeded},
		{name: "pool exhausted", err: status.Error(codes.ResourceExhausted, "pool acquire"), want: connect.CodeResourceExhausted},
		{name: "bad platform token", err: status.Error(codes.Unauthenticated, "invalid bearer token"), want: connect.CodeInternal},
		{name: "engine down", err: status.Error(codes.Unavailable, "connection refused"), want: connect.CodeUnavailable},
		{name: "context deadline", err: context.DeadlineExceeded, want: connect.CodeDeadlineExceeded},
		{name: "plain error", err: errors.New("dial duckflight: no such host"), want: connect.CodeUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queryError(tt.err)
			if connect.CodeOf(got) != tt.want {
				t.Errorf("queryError(%v) code = %v, want %v", tt.err, connect.CodeOf(got), tt.want)
			}
		})
	}
}

func TestQueryErrorKeepsEngineMessage(t *testing.T) {
	err := queryError(status.Error(codes.InvalidArgument, `Binder Error: column "nope" not found`))
	var cerr *connect.Error
	if !errors.As(err, &cerr) {
		t.Fatalf("not a connect error: %v", err)
	}
	if cerr.Message() != `Binder Error: column "nope" not found` {
		t.Errorf("message = %q, want the engine message", cerr.Message())
	}
}

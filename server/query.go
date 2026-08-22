package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/duckflight"
	queryv1 "github.com/fairtier/workspace-api/proto/query/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// Row caps for ExecuteQuery. The cap is the payload-size guard: the response
// is a unary Connect message, so "rows" is the only unbounded dimension.
const (
	defaultMaxRows = 500
	maxMaxRows     = 1000
)

// queryTimeout bounds the whole engine round-trip. DuckFlight enforces its
// own 60s statement timeout; staying above it lets the engine's own (more
// specific) error reach the user instead of a generic deadline here.
const queryTimeout = 65 * time.Second

// QueryServer proxies SQL from the Console to the tenant's DuckFlight engine.
// The endpoint and token are resolved server-side from the caller's own
// customer record — no request field can address another tenant's box.
type QueryServer struct {
	Workspaces workspace.Resolver
	Executor   duckflight.Executor
}

// engine resolves the caller's DuckFlight endpoint. It is the tenant-binding
// choke point every RPC in this service goes through.
func (s *QueryServer) engine(ctx context.Context) (endpoint, token string, err error) {
	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return "", "", connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return "", "", domainError(err)
	}
	if ws.DuckFlightURL == "" || ws.DuckFlightAuthToken == "" {
		return "", "", connect.NewError(connect.CodeFailedPrecondition, errors.New("the query engine is not enabled for this workspace"))
	}
	return ws.DuckFlightURL, ws.DuckFlightAuthToken, nil
}

func (s *QueryServer) execute(ctx context.Context, sql string, maxRows int) (*duckflight.Result, error) {
	endpoint, token, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	tctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	res, err := s.Executor.Execute(tctx, endpoint, token, sql, maxRows)
	if err != nil {
		return nil, queryError(err)
	}
	return res, nil
}

func (s *QueryServer) ExecuteQuery(ctx context.Context, req *connect.Request[queryv1.ExecuteQueryRequest]) (*connect.Response[queryv1.ExecuteQueryResponse], error) {
	if strings.TrimSpace(req.Msg.Sql) == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("sql must not be empty"))
	}
	maxRows := int(req.Msg.MaxRows)
	switch {
	case maxRows <= 0:
		maxRows = defaultMaxRows
	case maxRows > maxMaxRows:
		maxRows = maxMaxRows
	}

	start := time.Now()
	res, err := s.execute(ctx, req.Msg.Sql, maxRows)
	if err != nil {
		return nil, err
	}

	resp := &queryv1.ExecuteQueryResponse{
		RowCount:   int64(len(res.Rows)),
		Truncated:  res.Truncated,
		DurationMs: time.Since(start).Milliseconds(),
	}
	for _, c := range res.Columns {
		resp.Columns = append(resp.Columns, &queryv1.ColumnInfo{Name: c.Name, Type: c.Type})
	}
	for _, row := range res.Rows {
		enc, err := json.Marshal(row)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("encode row: %w", err))
		}
		resp.Rows = append(resp.Rows, string(enc))
	}
	return connect.NewResponse(resp), nil
}

// listTablesSQL is constant — ListTables takes no user input at all.
// system/temp are DuckDB's own internal catalogs.
const listTablesSQL = `SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog NOT IN ('system', 'temp') ORDER BY table_schema, table_name`

func (s *QueryServer) ListTables(ctx context.Context, _ *connect.Request[queryv1.ListTablesRequest]) (*connect.Response[queryv1.ListTablesResponse], error) {
	// information_schema.tables is bounded by the catalog size, but reuse the
	// hard row cap as a backstop.
	res, err := s.execute(ctx, listTablesSQL, maxMaxRows)
	if err != nil {
		return nil, err
	}
	resp := &queryv1.ListTablesResponse{}
	for _, row := range res.Rows {
		if len(row) != 2 {
			continue
		}
		ns, _ := row[0].(string)
		name, _ := row[1].(string)
		resp.Tables = append(resp.Tables, &queryv1.TableRef{Namespace: ns, Name: name})
	}
	return connect.NewResponse(resp), nil
}

func (s *QueryServer) DescribeTable(ctx context.Context, req *connect.Request[queryv1.DescribeTableRequest]) (*connect.Response[queryv1.DescribeTableResponse], error) {
	if req.Msg.Namespace == "" || req.Msg.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("namespace and name are required"))
	}
	// DESCRIBE — not information_schema.columns — because the box attaches the
	// Iceberg (Lakekeeper) catalog with DuckDB's iceberg extension, which binds
	// tables *lazily*: until a table is first referenced, its catalog entry is a
	// placeholder single column `__ UNKNOWN`, and information_schema faithfully
	// reports that placeholder. DESCRIBE forces the bind, reading the real
	// Iceberg schema. Identifiers reach an identifier position here, so the
	// injection surface is the double-quote (doubled by escapeIdentifier).
	sql := fmt.Sprintf(
		`DESCRIBE "%s"."%s"`,
		escapeIdentifier(req.Msg.Namespace), escapeIdentifier(req.Msg.Name),
	)
	res, err := s.execute(ctx, sql, maxMaxRows)
	if err != nil {
		return nil, err
	}
	// DESCRIBE columns: column_name, column_type, null, key, default, extra.
	resp := &queryv1.DescribeTableResponse{}
	for _, row := range res.Rows {
		if len(row) < 3 {
			continue
		}
		name, _ := row[0].(string)
		typ, _ := row[1].(string)
		nullable, _ := row[2].(string)
		resp.Columns = append(resp.Columns, &queryv1.ColumnSchema{
			Name:     name,
			Type:     typ,
			Nullable: strings.EqualFold(nullable, "YES"),
		})
	}
	return connect.NewResponse(resp), nil
}

// escapeIdentifier makes s safe inside a double-quoted SQL identifier.
func escapeIdentifier(s string) string {
	return strings.ReplaceAll(s, `"`, `""`)
}

// queryError translates a DuckFlight gRPC error into a Connect error the
// Console can show. DuckFlight already picks meaningful codes (DuckDB parse/
// binder errors → InvalidArgument, statement timeout → DeadlineExceeded), so
// this is mostly a code-space translation, keeping the engine's message.
func queryError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return connect.NewError(connect.CodeDeadlineExceeded, errors.New("query timed out"))
	}
	st, ok := status.FromError(err)
	if !ok {
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf("query engine: %w", err))
	}
	msg := errors.New(st.Message())
	switch st.Code() {
	case codes.InvalidArgument, codes.NotFound:
		// DuckDB syntax/binder/catalog errors — the user's SQL, shown inline.
		return connect.NewError(connect.CodeInvalidArgument, msg)
	case codes.DeadlineExceeded:
		return connect.NewError(connect.CodeDeadlineExceeded, msg)
	case codes.Canceled:
		return connect.NewError(connect.CodeCanceled, msg)
	case codes.ResourceExhausted:
		return connect.NewError(connect.CodeResourceExhausted, msg)
	case codes.Unauthenticated, codes.PermissionDenied:
		// The platform's token was rejected — a provisioning problem, not the
		// caller's fault. Don't surface "unauthenticated" to a logged-in user.
		return connect.NewError(connect.CodeInternal, errors.New("query engine rejected platform credentials"))
	case codes.Unavailable:
		return connect.NewError(connect.CodeUnavailable, errors.New("query engine unreachable"))
	default:
		return connect.NewError(connect.CodeInternal, msg)
	}
}

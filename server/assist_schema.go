package server

import (
	"context"
	"errors"
	"strings"

	"connectrpc.com/connect"

	"github.com/fairtier/workspace-api/core"
	queryv1 "github.com/fairtier/workspace-api/proto/query/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// QuerySchemaSource implements workspace.SchemaSource over the same
// QueryServer the SQL editor uses: the tenant binding, the lazy-bind DESCRIBE
// quirk, and the error mapping are all inherited rather than re-derived. The
// callerID parameters exist for the domain interface's clarity; the identity
// actually used is the one the auth interceptor bound into ctx — the same
// value, arriving by the same path as every editor query.
type QuerySchemaSource struct {
	Query *QueryServer
}

// Tables lists every table in the caller's warehouse.
func (q *QuerySchemaSource) Tables(ctx context.Context, _ core.UserID) ([]workspace.TableRef, error) {
	resp, err := q.Query.ListTables(ctx, connect.NewRequest(&queryv1.ListTablesRequest{}))
	if err != nil {
		return nil, err
	}
	out := make([]workspace.TableRef, 0, len(resp.Msg.Tables))
	for _, t := range resp.Msg.Tables {
		out = append(out, workspace.TableRef{Namespace: t.Namespace, Name: t.Name})
	}
	return out, nil
}

// Columns describes one table (DESCRIBE — forces the iceberg bind).
func (q *QuerySchemaSource) Columns(ctx context.Context, _ core.UserID, ref workspace.TableRef) ([]workspace.ColumnSchema, error) {
	resp, err := q.Query.DescribeTable(ctx, connect.NewRequest(&queryv1.DescribeTableRequest{
		Namespace: ref.Namespace,
		Name:      ref.Name,
	}))
	if err != nil {
		return nil, err
	}
	out := make([]workspace.ColumnSchema, 0, len(resp.Msg.Columns))
	for _, c := range resp.Msg.Columns {
		out = append(out, workspace.ColumnSchema{Name: c.Name, Type: c.Type, Nullable: c.Nullable})
	}
	return out, nil
}

// Explain plans the statement without executing it. The one guard of our own:
// EXPLAIN binds a single statement, so a draft carrying more than one is
// refused here rather than sent with a prefix that would only cover the
// first.
func (q *QuerySchemaSource) Explain(ctx context.Context, _ core.UserID, sql string) error {
	trimmed := strings.TrimSuffix(strings.TrimSpace(sql), ";")
	if strings.Contains(trimmed, ";") {
		return errors.New("the draft contains multiple statements; only a single query can be validated")
	}
	_, err := q.Query.execute(ctx, "EXPLAIN "+trimmed, 1)
	if err == nil {
		return nil
	}
	// Surface the engine's message (queryError already unwrapped it into a
	// Connect error) — this string lands in the draft's notes.
	var cerr *connect.Error
	if errors.As(err, &cerr) {
		return errors.New(cerr.Message())
	}
	return err
}

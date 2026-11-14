package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"

	queryv1 "github.com/fairtier/workspace-api/proto/query/v1"
)

const (
	defaultQueryRows = 200
	maxQueryRows     = 1000
)

const listTablesDesc = `List every table in the workspace's lakehouse (Apache Iceberg tables, loaded by pipelines or built by transformations), as namespace + name. A pipeline writes into the namespace named by its dataset_name; dbt transformations write into their own namespaces.

Address a table in SQL as <namespace>.<table> exactly as listed here (double-quote names that are not plain lowercase identifiers).`

const describeTableDesc = `Describe one table's columns: name, DuckDB type and nullability. Call this before writing SQL against a table you have not seen.`

const runQueryDesc = `Run one read-only SQL query against the workspace's lakehouse and return the result rows.

The engine is DuckDB (DuckDB SQL dialect: e.g. date_trunc, strftime, list/struct types, QUALIFY, SUMMARIZE, FROM-first syntax). Address tables as <namespace>.<table> as list_tables reports them.

Read-only, enforced before the query runs: exactly one statement, starting with SELECT, WITH, FROM, VALUES, TABLE, DESCRIBE, SHOW, SUMMARIZE or EXPLAIN. Statements that write or change engine state are refused, as are file and URL readers (read_csv, read_parquet, read_text, glob, 'path' in FROM, ...): query tables only. If a column is named like a SQL keyword (load, set, use, ...), double-quote it.

Results are capped at max_rows rows (default 200, at most 1000) and about 256 KB in total; truncated is true when rows were cut. Aggregate in SQL rather than fetching raw rows. Queries time out after about 60 seconds.`

type listTablesIn struct{}

type tableRef struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type listTablesOut struct {
	Tables []tableRef `json:"tables"`
}

func listTables(ctx context.Context, b Backends, _ listTablesIn) (listTablesOut, error) {
	resp, err := b.Query.ListTables(ctx, connect.NewRequest(&queryv1.ListTablesRequest{}))
	if err != nil {
		return listTablesOut{}, err
	}
	out := listTablesOut{Tables: make([]tableRef, 0, len(resp.Msg.Tables))}
	for _, t := range resp.Msg.Tables {
		out.Tables = append(out.Tables, tableRef{Namespace: t.Namespace, Name: t.Name})
	}
	return out, nil
}

type describeTableIn struct {
	Namespace string `json:"namespace" jsonschema:"the table's namespace, as list_tables reports it"`
	Name      string `json:"name" jsonschema:"the table's name, as list_tables reports it"`
}

type columnInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

type describeTableOut struct {
	Columns []columnInfo `json:"columns"`
}

func describeTable(ctx context.Context, b Backends, in describeTableIn) (describeTableOut, error) {
	resp, err := b.Query.DescribeTable(ctx, connect.NewRequest(&queryv1.DescribeTableRequest{Namespace: in.Namespace, Name: in.Name}))
	if err != nil {
		return describeTableOut{}, err
	}
	out := describeTableOut{Columns: make([]columnInfo, 0, len(resp.Msg.Columns))}
	for _, c := range resp.Msg.Columns {
		out.Columns = append(out.Columns, columnInfo{Name: c.Name, Type: c.Type, Nullable: c.Nullable})
	}
	return out, nil
}

type runQueryIn struct {
	SQL     string `json:"sql" jsonschema:"one read-only DuckDB SQL statement"`
	MaxRows int    `json:"max_rows,omitempty" jsonschema:"maximum rows to return; default 200, at most 1000"`
}

type resultColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type runQueryOut struct {
	Columns []resultColumn `json:"columns"`
	// Rows are positional, matching columns.
	Rows       [][]any `json:"rows"`
	RowCount   int     `json:"row_count"`
	Truncated  bool    `json:"truncated"`
	Note       string  `json:"note,omitempty"`
	DurationMs int64   `json:"duration_ms"`
}

func runQuery(ctx context.Context, b Backends, in runQueryIn) (runQueryOut, error) {
	if strings.TrimSpace(in.SQL) == "" {
		return runQueryOut{}, errors.New("sql is required")
	}
	if err := CheckReadOnlySQL(in.SQL); err != nil {
		return runQueryOut{}, err
	}
	maxRows := in.MaxRows
	switch {
	case maxRows <= 0:
		maxRows = defaultQueryRows
	case maxRows > maxQueryRows:
		maxRows = maxQueryRows
	}
	resp, err := b.Query.ExecuteQuery(ctx, connect.NewRequest(&queryv1.ExecuteQueryRequest{Sql: in.SQL, MaxRows: int32(maxRows)}))
	if err != nil {
		return runQueryOut{}, err
	}
	out := runQueryOut{
		Columns:    make([]resultColumn, 0, len(resp.Msg.Columns)),
		Truncated:  resp.Msg.Truncated,
		DurationMs: resp.Msg.DurationMs,
	}
	for _, c := range resp.Msg.Columns {
		out.Columns = append(out.Columns, resultColumn{Name: c.Name, Type: c.Type})
	}
	if out.Truncated {
		out.Note = fmt.Sprintf("more than %d rows; aggregate or add a LIMIT", maxRows)
	}
	return appendRows(out, resp.Msg.Rows)
}

// appendRows decodes the engine's JSON-encoded rows up to the byte budget.
func appendRows(out runQueryOut, rows []string) (runQueryOut, error) {
	out.Rows = make([][]any, 0, len(rows))
	budget := maxRowBytes
	for _, raw := range rows {
		budget -= len(raw)
		if budget < 0 {
			out.Truncated = true
			out.Note = fmt.Sprintf("result cut at %d rows to stay under %d KB; select fewer columns or aggregate", len(out.Rows), maxRowBytes>>10)
			break
		}
		var row []any
		if err := json.Unmarshal([]byte(raw), &row); err != nil {
			return runQueryOut{}, fmt.Errorf("decode result row: %w", err)
		}
		out.Rows = append(out.Rows, row)
	}
	out.RowCount = len(out.Rows)
	return out, nil
}

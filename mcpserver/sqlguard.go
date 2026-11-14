package mcpserver

import (
	"errors"
	"fmt"
	"strings"
)

// CheckReadOnlySQL is run_query's lexical read-only guard. It accepts a
// single statement that starts with a read-only keyword and contains no
// keyword, table function or file reference that could write, change engine
// state, or reach outside the attached catalog.
//
// It is defence in depth, NOT a security boundary. It works on tokens, not on
// DuckDB's grammar, so it is conservative where it cannot be sure (a column
// literally named "load" must be double-quoted), and a construct it does not
// know about may still get through. The engine it guards does not restrict
// itself: DuckFlight leaves enable_external_access on and attaches the
// Iceberg catalog read-write under a credential that can write. The real fix
// is a read-only DuckFlight session for this caller (see docs/mcp.md).
func CheckReadOnlySQL(sql string) error {
	toks, err := lexSQL(sql)
	if err != nil {
		return reject("%s", err.Error())
	}
	toks, err = singleStatement(toks)
	if err != nil {
		return err
	}
	if err := checkHead(toks[0]); err != nil {
		return err
	}
	for i := range toks {
		if err := checkToken(toks, i); err != nil {
			return err
		}
	}
	return checkTableReferences(toks)
}

// ErrQueryRejected is wrapped by every guard refusal.
var ErrQueryRejected = errors.New("run_query only runs read-only queries")

func reject(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrQueryRejected, fmt.Sprintf(format, args...))
}

// readOnlyHeads are the statement keywords run_query accepts first.
var readOnlyHeads = setOf("SELECT", "WITH", "FROM", "DESCRIBE", "SHOW", "EXPLAIN", "SUMMARIZE", "VALUES", "TABLE")

// forbiddenKeywords may not appear anywhere outside a string, quoted
// identifier or comment — as a statement head, after a CTE, under EXPLAIN
// ANALYZE (which executes its statement), or anywhere else. Checking every
// position rather than parsing is what makes WITH … INSERT and EXPLAIN
// ANALYZE DELETE fall out for free, at the price of refusing an unquoted
// column that happens to share one of these names.
var forbiddenKeywords = setOf(
	"ATTACH", "DETACH", "COPY", "INSTALL", "LOAD", "SET", "RESET", "PRAGMA",
	"CALL", "CREATE", "DROP", "ALTER", "INSERT", "UPDATE", "DELETE", "MERGE",
	"EXPORT", "IMPORT", "USE", "VACUUM", "CHECKPOINT", "TRUNCATE", "GRANT",
	"REVOKE", "BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION", "PREPARE",
	"EXECUTE", "DEALLOCATE",
)

// forbiddenFunctions are table/scalar functions that read files, URLs or
// engine secrets, or run SQL of their own. Any function whose name starts
// with read_ or ends in _scan is refused as well (read_csv, read_parquet,
// read_json*, read_text, read_blob, parquet_scan, iceberg_scan,
// postgres_scan, …). Tables are reached through the attached catalog
// (lake.<namespace>.<table>), never by path.
var forbiddenFunctions = setOf(
	"glob", "sniff_csv", "query", "query_table", "getenv", "current_setting",
	"duckdb_secrets", "which_secret", "duckdb_settings",
	"parquet_metadata", "parquet_schema", "parquet_file_metadata",
	"parquet_kv_metadata", "parquet_bloom_probe",
	"iceberg_metadata", "iceberg_snapshots",
	"st_read", "st_read_meta", "http_get", "http_post", "http_head",
	"postgres_query", "mysql_query", "sqlite_attach", "postgres_attach",
	"load_aws_credentials",
)

// tableKeywords put the next token in a table position, where DuckDB treats
// a string literal (or a quoted name) as a file path or URL to scan.
var tableKeywords = setOf("FROM", "JOIN", "TABLE", "DESCRIBE", "SUMMARIZE", "SHOW")

// clauseKeywords move the clause tracker used to tell `FROM a, 'x.csv'` (a
// second table) from `SELECT 'a', 'b'` (two literals).
var clauseKeywords = setOf(
	"SELECT", "FROM", "JOIN", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT",
	"QUALIFY", "WINDOW", "ON", "USING", "UNION", "EXCEPT", "INTERSECT",
	"VALUES", "OFFSET",
)

func setOf(items ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(items))
	for _, it := range items {
		m[it] = struct{}{}
	}
	return m
}

func in(m map[string]struct{}, k string) bool {
	_, ok := m[k]
	return ok
}

// singleStatement drops trailing semicolons and refuses anything after the
// first one.
func singleStatement(toks []token) ([]token, error) {
	end := len(toks)
	for i, t := range toks {
		if t.isPunct(";") {
			end = i
			break
		}
	}
	for _, t := range toks[end:] {
		if !t.isPunct(";") {
			return nil, reject("only a single statement is allowed")
		}
	}
	if end == 0 {
		return nil, reject("the query is empty")
	}
	return toks[:end], nil
}

func checkHead(t token) error {
	if t.kind != tokIdent || !in(readOnlyHeads, t.text) {
		return reject("a query must start with SELECT, WITH, FROM, VALUES, TABLE, DESCRIBE, SHOW, SUMMARIZE or EXPLAIN")
	}
	return nil
}

// checkToken refuses forbidden keywords and forbidden function calls.
func checkToken(toks []token, i int) error {
	t := toks[i]
	if t.kind == tokIdent && in(forbiddenKeywords, t.text) {
		return reject("%s is not allowed (if it is a column name, double-quote it)", t.text)
	}
	if !t.isName() || i+1 >= len(toks) || !toks[i+1].isPunct("(") {
		return nil
	}
	name := strings.ToLower(t.text)
	if in(forbiddenFunctions, name) || strings.HasPrefix(name, "read_") || strings.HasSuffix(name, "_scan") {
		return reject("function %s() is not allowed: query tables through the catalog, not files or URLs", name)
	}
	return nil
}

// checkTableReferences refuses a string literal — or a quoted name that looks
// like a path — in a table position: DuckDB scans `FROM 'file.csv'`,
// `FROM 's3://bucket/x.parquet'` and `SUMMARIZE 'https://…'` as files.
func checkTableReferences(toks []token) error {
	clauses := []string{""} // one entry per parenthesis depth
	for i, t := range toks {
		clauses = trackClause(clauses, t)
		if !pathLike(t) || i == 0 {
			continue
		}
		prev := toks[i-1]
		inFromList := prev.isPunct(",") && clauses[len(clauses)-1] == "FROM"
		if (prev.kind == tokIdent && in(tableKeywords, prev.text)) || inFromList {
			return reject("reading files or URLs is not allowed: query tables through the catalog")
		}
	}
	return nil
}

func trackClause(clauses []string, t token) []string {
	switch {
	case t.isPunct("("):
		return append(clauses, "")
	case t.isPunct(")"):
		if len(clauses) > 1 {
			return clauses[:len(clauses)-1]
		}
	case t.kind == tokIdent && in(clauseKeywords, t.text):
		c := t.text
		if c == "JOIN" {
			c = "FROM"
		}
		clauses[len(clauses)-1] = c
	}
	return clauses
}

// pathLike reports whether a token in a table position would be read as a
// file: any string literal, or a quoted identifier carrying path characters.
func pathLike(t token) bool {
	switch t.kind {
	case tokString:
		return true
	case tokQuotedIdent:
		return strings.ContainsAny(t.text, "./\\:*?")
	default:
		return false
	}
}

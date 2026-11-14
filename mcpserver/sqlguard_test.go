package mcpserver

import (
	"errors"
	"testing"
)

func TestCheckReadOnlySQL_Allows(t *testing.T) {
	allowed := []string{
		"SELECT 1",
		"select * from lake.sales.orders limit 10",
		"SELECT 1;",
		"SELECT 1;;  \n",
		"  -- leading comment\nSELECT 1",
		"/* block */ SELECT 1",
		"/* nested /* inner */ still comment */ SELECT 1",
		"WITH t AS (SELECT 1 AS x) SELECT x FROM t",
		"FROM lake.sales.orders SELECT count(*)",
		"DESCRIBE lake.sales.orders",
		"SHOW TABLES",
		"SHOW ALL TABLES",
		"SUMMARIZE lake.sales.orders",
		"EXPLAIN SELECT 1",
		"EXPLAIN ANALYZE SELECT * FROM lake.s.t",
		"VALUES (1, 'a'), (2, 'b')",
		"TABLE lake.sales.orders",
		// Strings and quoted identifiers may hold anything, keywords and
		// semicolons included.
		"SELECT 'DROP TABLE x; DELETE FROM y' AS s",
		"SELECT 'it''s; fine'",
		`SELECT E'a\'; DROP TABLE t; --'`,
		"SELECT $$ ; DELETE FROM t $$",
		"SELECT $tag$ ; INSERT $tag$",
		`SELECT "delete", "load", "set" FROM lake.s.t`,
		`SELECT "my;col" FROM lake.s.t`,
		"SELECT 1 -- ; DROP TABLE t",
		"SELECT a FROM lake.s.t WHERE name = 'report.csv'",
		"SELECT 'a', 'b', 'c'",
		"SELECT * FROM lake.s.t WHERE x IN ('a', 'b')",
		"SELECT replace(name, 'a', 'b'), date_trunc('month', ts) FROM lake.s.t",
		"SELECT * FROM lake.s.a JOIN lake.s.b USING (id)",
		"SELECT * FROM (SELECT 'x.csv' AS f) sub",
		"SELECT CASE WHEN x THEN 1 ELSE 0 END FROM lake.s.t",
		"SELECT $1",
		"SELECT * FROM lake.s.\"Order Lines\"",
	}
	for _, sql := range allowed {
		if err := CheckReadOnlySQL(sql); err != nil {
			t.Errorf("CheckReadOnlySQL(%q) = %v, want allowed", sql, err)
		}
	}
}

func TestCheckReadOnlySQL_Rejects(t *testing.T) {
	rejected := []struct {
		name string
		sql  string
	}{
		{"empty", ""},
		{"only semicolons", " ;; "},
		{"only a comment", "-- nothing"},
		{"two statements", "SELECT 1; SELECT 2"},
		{"stacked write", "SELECT 1; DROP TABLE lake.s.t"},
		{"write after comment-hidden semicolon", "SELECT 1 /* ; */; DELETE FROM t"},
		{"insert", "INSERT INTO t VALUES (1)"},
		{"update", "UPDATE t SET x = 1"},
		{"delete", "DELETE FROM t"},
		{"merge", "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE"},
		{"create", "CREATE TABLE t AS SELECT 1"},
		{"drop", "DROP TABLE t"},
		{"alter", "ALTER TABLE t ADD COLUMN y INT"},
		{"attach", "ATTACH 'x.db' AS x"},
		{"detach", "DETACH lake"},
		{"copy", "COPY t TO 'out.csv'"},
		{"install", "INSTALL httpfs"},
		{"load", "LOAD httpfs"},
		{"set", "SET threads = 1"},
		{"reset", "RESET threads"},
		{"pragma", "PRAGMA database_list"},
		{"call", "CALL pragma_version()"},
		{"export", "EXPORT DATABASE 'dir'"},
		{"import", "IMPORT DATABASE 'dir'"},
		{"use", "USE lake.other"},
		{"vacuum", "VACUUM"},
		{"checkpoint", "CHECKPOINT"},
		{"pivot head", "PIVOT t ON x"},
		{"cte insert", "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x"},
		{"cte delete", "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x"},
		{"cte update lowercase", "with x as (select 1) update t set a = 1"},
		{"explain analyze write", "EXPLAIN ANALYZE INSERT INTO t VALUES (1)"},
		{"explain analyze delete", "EXPLAIN ANALYZE DELETE FROM t"},
		{"explain create", "EXPLAIN CREATE TABLE t (a INT)"},
		{"write in subquery", "SELECT * FROM (DELETE FROM t RETURNING *)"},
		{"unquoted keyword column", "SELECT load FROM t"},
		{"unterminated string", "SELECT 'abc"},
		{"unterminated identifier", `SELECT "abc`},
		{"unterminated comment", "SELECT 1 /* "},
		{"unterminated nested comment", "SELECT 1 /* /* */ DROP TABLE t"},
		{"unterminated dollar", "SELECT $$ abc"},
		{"read_csv", "SELECT * FROM read_csv('/etc/passwd')"},
		{"read_csv_auto", "SELECT * FROM read_csv_auto('x.csv')"},
		{"read_parquet", "SELECT * FROM read_parquet('s3://bucket/x.parquet')"},
		{"read_text", "SELECT content FROM read_text('/proc/self/environ')"},
		{"read_blob", "SELECT * FROM read_blob('x')"},
		{"read_json", "SELECT * FROM read_json('https://example.com/x.json')"},
		{"uppercase read_text", "SELECT * FROM READ_TEXT('/etc/hosts')"},
		{"quoted read_text", `SELECT * FROM "read_text"('/etc/hosts')`},
		{"schema-qualified read_text", "SELECT * FROM main.read_text('/etc/hosts')"},
		{"parquet_scan", "SELECT * FROM parquet_scan('x.parquet')"},
		{"iceberg_scan", "SELECT * FROM iceberg_scan('s3://b/t')"},
		{"postgres_scan", "SELECT * FROM postgres_scan('dsn', 'public', 't')"},
		{"glob", "SELECT * FROM glob('/**')"},
		{"query", "SELECT * FROM query('SELECT 1')"},
		{"getenv", "SELECT getenv('HOME')"},
		{"current_setting", "SELECT current_setting('s3_secret_access_key')"},
		{"duckdb_secrets", "SELECT * FROM duckdb_secrets()"},
		{"duckdb_settings", "SELECT * FROM duckdb_settings()"},
		{"file in from", "SELECT * FROM '/etc/passwd'"},
		{"s3 in from", "SELECT * FROM 's3://bucket/data.parquet'"},
		{"url in from-first", "FROM 'https://example.com/x.csv'"},
		{"file in join", "SELECT * FROM lake.s.t JOIN 'x.csv' USING (id)"},
		{"file as second table", "SELECT * FROM lake.s.t, 'x.csv'"},
		{"file in subquery from", "SELECT * FROM (SELECT * FROM 'x.csv')"},
		{"summarize file", "SUMMARIZE 'x.parquet'"},
		{"describe file", "DESCRIBE 'x.parquet'"},
		{"table file", "TABLE 'x.parquet'"},
		{"quoted path in from", `SELECT * FROM "data.csv"`},
		{"dollar-quoted path", "SELECT * FROM $$/etc/passwd$$"},
		{"escape-string path", `SELECT * FROM E'/etc/passwd'`},
	}
	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckReadOnlySQL(tc.sql)
			if err == nil {
				t.Fatalf("CheckReadOnlySQL(%q) = nil, want rejection", tc.sql)
			}
			if !errors.Is(err, ErrQueryRejected) {
				t.Errorf("error %v does not wrap ErrQueryRejected", err)
			}
		})
	}
}

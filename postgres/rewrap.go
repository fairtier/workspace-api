package postgres

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/fairtier/workspace-api/crypto"
)

// Rotating CREDENTIAL_ENCRYPTION_KEY is a three-step operation, and this file
// is the middle one:
//
//  1. add the new key as primary and keep the old one as a previous key, so
//     both planes can still read everything;
//  2. rewrap — re-encrypt every stored value under the new key;
//  3. drop the old key, once (1) can be shown to have nothing left under it.
//
// Step 3 is the reason the envelope carries a key id at all: with one, "is any
// ciphertext still under the retired key?" is a LIKE against each column; with-
// out one it could only be answered by trial-decrypting every row, and a single
// missed column silently turns key destruction into data destruction.

// RewrapEnvVar switches the startup rewrap sweep on ("on"; anything else is
// off, including unset).
//
// Off by default, and the reason is the deploy that introduces key ids rather
// than any later rotation: a value written under the new envelope cannot be
// read by the previous release, so with the sweep on, the first rollout would
// convert every row at once and make a rollback a fleet-wide outage instead of
// a handful of unreadable rows. Turned on deliberately — during a rotation, or
// once the release carrying key ids is settled — it is an idempotent no-op
// after it converges.
const RewrapEnvVar = "CREDENTIAL_ENCRYPTION_REWRAP"

// RewrapEnabled reports whether the startup rewrap sweep should run.
func RewrapEnabled() bool {
	return os.Getenv(RewrapEnvVar) == "on"
}

// EncryptedColumn names one column holding crypto.Encryptor output, and the
// columns that identify a row in it.
type EncryptedColumn struct {
	Table      string
	KeyColumns []string
	Column     string
}

// WorkspaceEncryptedColumns is the workspace plane's complete ciphertext
// inventory — every column written through Repository.encryptCredentials.
// A new one belongs here the same day it is added, or the next rotation will
// leave it behind under a key that is about to be destroyed.
func WorkspaceEncryptedColumns() []EncryptedColumn {
	return []EncryptedColumn{
		{Table: "pipelines", KeyColumns: []string{"id"}, Column: "source_credentials"},
		{Table: "transformations", KeyColumns: []string{"id"}, Column: "git_credentials"},
		{Table: "google_oauth_grants", KeyColumns: []string{"grant_id"}, Column: "refresh_token"},
		{Table: "customer_oauth_clients", KeyColumns: []string{"customer_slug", "provider"}, Column: "client_secret"},
	}
}

// RewrapEncrypted re-encrypts every value in cols that is not already under
// enc's primary key, and reports how many it rewrote.
//
// It is a no-op unless enc names its key (crypto.KeyIdentified) — a NoOpEncryptor
// or a nil one has nothing to rotate to. Idempotent: a second run finds nothing,
// because the first left every row tagged with the primary key id.
//
// Plaintext rows are left alone; MigrateEncryptCredentials and its central
// counterpart own that transition, and conflating the two would let a
// misconfigured deployment encrypt rows it was only meant to re-key.
func RewrapEncrypted(db *sql.DB, enc crypto.Encryptor, cols []EncryptedColumn) (int, error) {
	if enc == nil {
		return 0, nil
	}
	keyed, ok := enc.(crypto.KeyIdentified)
	if !ok {
		return 0, nil
	}
	primaryID := keyed.PrimaryKeyID()

	total := 0
	for _, c := range cols {
		n, err := rewrapColumn(db, enc, primaryID, c)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// staleRow is one (key, ciphertext) pair queued for re-encryption.
type staleRow struct {
	key    []string
	stored string
}

// rewrapColumn re-encrypts one column, reading every stale row into memory
// first so the cursor is closed before any UPDATE runs.
//
// The sweep runs at startup while other replicas are already serving, so
// between reading a row and writing it back a user may have saved a new
// credential over it. Writing unconditionally would replace that save with a
// re-encryption of the value we happened to read — a silent revert of customer
// data. So each UPDATE is a compare-and-swap against the ciphertext we
// decrypted; a row someone else has since rewritten matches nothing, is left
// alone, and is not counted. It needs no retry: whoever wrote it did so under
// the primary key, which is where the rewrap was trying to get it.
func rewrapColumn(db *sql.DB, enc crypto.Encryptor, primaryID string, c EncryptedColumn) (int, error) {
	pending, err := selectStaleRows(db, primaryID, c)
	if err != nil {
		return 0, err
	}
	return applyRewrap(db, enc, c, pending)
}

// applyRewrap writes back rows that selectStaleRows found stale. Split from
// rewrapColumn so a test can do what the world does: change a row in between
// the two halves.
func applyRewrap(db *sql.DB, enc crypto.Encryptor, c EncryptedColumn, pending []staleRow) (int, error) {
	rewrote := 0
	for _, r := range pending {
		plaintext, err := enc.Decrypt(r.stored)
		if err != nil {
			return 0, fmt.Errorf("postgres: rewrap %s.%s for %s: %w",
				c.Table, c.Column, strings.Join(r.key, "/"), err)
		}

		rewrapped, err := enc.Encrypt(plaintext)
		if err != nil {
			return 0, fmt.Errorf("postgres: re-encrypt %s.%s for %s: %w",
				c.Table, c.Column, strings.Join(r.key, "/"), err)
		}

		args := make([]any, 0, len(r.key)+2)
		args = append(args, rewrapped)
		for _, k := range r.key {
			args = append(args, k)
		}
		args = append(args, r.stored)

		//nolint:gosec // identifiers come from WorkspaceEncryptedColumns, not user input
		stmt := fmt.Sprintf(`UPDATE %s SET %s = $1 WHERE %s AND %s = $%d`,
			c.Table, c.Column, whereClause(c.KeyColumns, 2),
			c.Column, len(r.key)+2)

		res, err := db.Exec(stmt, args...)
		if err != nil {
			return rewrote, fmt.Errorf("postgres: update rewrapped %s.%s for %s: %w",
				c.Table, c.Column, strings.Join(r.key, "/"), err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return rewrote, fmt.Errorf("postgres: rows affected %s.%s for %s: %w",
				c.Table, c.Column, strings.Join(r.key, "/"), err)
		}
		rewrote += int(n)
	}

	return rewrote, nil
}

// selectStaleRows reads every encrypted value in one column that is not tagged
// with the primary key id. That set is exactly: values under a previous key,
// plus legacy values written before the envelope carried an id at all.
func selectStaleRows(db *sql.DB, primaryID string, c EncryptedColumn) ([]staleRow, error) {
	//nolint:gosec // identifiers come from WorkspaceEncryptedColumns, not user input
	query := fmt.Sprintf(
		`SELECT %s, %s FROM %s WHERE %s LIKE 'enc:%%' AND %s NOT LIKE $1`,
		strings.Join(c.KeyColumns, ", "), c.Column, c.Table, c.Column, c.Column)

	rows, err := db.Query(query, primaryEnvelopePattern(primaryID))
	if err != nil {
		return nil, fmt.Errorf("postgres: query stale %s.%s: %w", c.Table, c.Column, err)
	}
	defer rows.Close()

	var pending []staleRow
	for rows.Next() {
		r := staleRow{key: make([]string, len(c.KeyColumns))}

		dest := make([]any, 0, len(c.KeyColumns)+1)
		for i := range r.key {
			dest = append(dest, &r.key[i])
		}
		dest = append(dest, &r.stored)

		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("postgres: scan %s.%s: %w", c.Table, c.Column, err)
		}
		pending = append(pending, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: iterate %s.%s: %w", c.Table, c.Column, err)
	}
	return pending, nil
}

// whereClause renders "a = $n AND b = $n+1" for a row's key columns.
func whereClause(keyColumns []string, firstPlaceholder int) string {
	terms := make([]string, len(keyColumns))
	for i, col := range keyColumns {
		terms[i] = fmt.Sprintf("%s = $%d", col, firstPlaceholder+i)
	}
	return strings.Join(terms, " AND ")
}

// primaryEnvelopePattern is the LIKE pattern matching values already under the
// primary key. Both the rewrap selection and the audit negate it, so "stale"
// means the same thing in each: carries the "enc:" envelope, but not this key's
// id — which covers a previous key's ciphertext and the legacy untagged form
// alike. The key id is hex, so it needs no LIKE escaping.
func primaryEnvelopePattern(primaryID string) string {
	return "enc:" + primaryID + ":%"
}

// StaleCiphertext is one column still holding values that are not under the
// primary key.
type StaleCiphertext struct {
	Table  string
	Column string
	Rows   int
}

// AuditStaleCiphertext answers the question that gates destroying a retired
// key: is any ciphertext anywhere in this database still not under the primary
// key?
//
// Deliberately schema-driven rather than inventory-driven. RewrapEncrypted
// works from an explicit column list, because rewriting a column is a
// destructive act that should only ever touch columns someone named — but that
// list is exactly the thing a future migration can forget to update, and a
// column missed here turns key destruction into data destruction. So this half
// asks the database instead: every text column in the schema, checked for the
// "enc:" envelope. It finds ciphertext the list has never heard of.
func AuditStaleCiphertext(db *sql.DB, primaryID string) ([]StaleCiphertext, error) {
	columns, err := textColumns(db)
	if err != nil {
		return nil, err
	}

	var stale []StaleCiphertext
	for _, c := range columns {
		//nolint:gosec // identifiers are quoted below and come from information_schema
		query := fmt.Sprintf(`SELECT count(*) FROM %s.%s WHERE %s LIKE 'enc:%%' AND %s NOT LIKE $1`,
			quoteIdentifier(c.schema), quoteIdentifier(c.Table),
			quoteIdentifier(c.Column), quoteIdentifier(c.Column))

		var n int
		if err := db.QueryRow(query, primaryEnvelopePattern(primaryID)).Scan(&n); err != nil {
			return nil, fmt.Errorf("postgres: audit %s.%s: %w", c.Table, c.Column, err)
		}
		if n > 0 {
			c.Rows = n
			stale = append(stale, c.StaleCiphertext)
		}
	}
	return stale, nil
}

// auditColumn is one candidate column, with the schema needed to address it.
type auditColumn struct {
	StaleCiphertext
	schema string
}

// textColumns lists every text-typed column of every ordinary table, which is
// the set that can hold an "enc:" envelope (the columns are all TEXT by
// migration; JSONB ones could not hold the prefix form).
func textColumns(db *sql.DB) ([]auditColumn, error) {
	rows, err := db.Query(`
		SELECT c.table_schema, c.table_name, c.column_name
		FROM information_schema.columns c
		JOIN information_schema.tables t
		  ON t.table_schema = c.table_schema AND t.table_name = c.table_name
		WHERE t.table_type = 'BASE TABLE'
		  AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
		  AND c.data_type IN ('text', 'character varying', 'character')
		ORDER BY c.table_schema, c.table_name, c.column_name`)
	if err != nil {
		return nil, fmt.Errorf("postgres: list text columns: %w", err)
	}
	defer rows.Close()

	var columns []auditColumn
	for rows.Next() {
		var c auditColumn
		if err := rows.Scan(&c.schema, &c.Table, &c.Column); err != nil {
			return nil, fmt.Errorf("postgres: scan column list: %w", err)
		}
		columns = append(columns, c)
	}
	return columns, rows.Err()
}

// quoteIdentifier renders a database-supplied identifier safe to interpolate.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

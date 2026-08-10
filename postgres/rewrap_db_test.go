package postgres

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/fairtier/workspace-api/crypto"
)

// The rewrap sweep is the one piece of this package that rewrites customer
// data, and the property that matters most about it — that it does not
// overwrite a save made while it was running — cannot be shown by inspecting
// the SQL it builds. So these tests want a real database.
//
// TEST_DATABASE_URL, skipped when unset: CI has no Postgres, and making it
// depend on one to run a two-test file is a worse trade than running these by
// hand against a throwaway container during a rotation.
func testDB(t *testing.T) *sql.DB {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// creds builds a two-key ring and the table the tests rewrap.
func creds(t *testing.T, table string) (*sql.DB, *crypto.Keyring, *crypto.Keyring, EncryptedColumn) {
	t.Helper()

	db := testDB(t)

	old := make([]byte, 32)
	for i := range old {
		old[i] = byte(i)
	}
	next := make([]byte, 32)
	for i := range next {
		next[i] = byte(200 - i)
	}

	oldRing, err := crypto.NewAESEncryptor(old)
	if err != nil {
		t.Fatalf("old ring: %v", err)
	}
	newRing, err := crypto.NewKeyring(next, old)
	if err != nil {
		t.Fatalf("new ring: %v", err)
	}

	if _, err := db.Exec(`DROP TABLE IF EXISTS ` + table); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE ` + table + ` (id text primary key, secret text)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DROP TABLE IF EXISTS ` + table) })

	return db, oldRing, newRing, EncryptedColumn{
		Table: table, KeyColumns: []string{"id"}, Column: "secret",
	}
}

func TestRewrapEncrypted_MovesRowsToThePrimaryKey(t *testing.T) {
	db, oldRing, newRing, col := creds(t, "rewrap_moves")

	stored, err := oldRing.Encrypt([]byte("hunter2"))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO `+col.Table+` VALUES ($1, $2)`, "a", stored); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Plaintext is not this sweep's business; it must survive untouched.
	if _, err := db.Exec(`INSERT INTO `+col.Table+` VALUES ($1, $2)`, "b", "not-encrypted"); err != nil {
		t.Fatalf("insert plain: %v", err)
	}

	n, err := RewrapEncrypted(db, newRing, []EncryptedColumn{col})
	if err != nil {
		t.Fatalf("rewrap: %v", err)
	}
	if n != 1 {
		t.Fatalf("rewrapped %d rows, want 1", n)
	}

	var got string
	if err := db.QueryRow(`SELECT secret FROM ` + col.Table + ` WHERE id = 'a'`).Scan(&got); err != nil {
		t.Fatalf("select: %v", err)
	}
	if want := "enc:" + newRing.PrimaryKeyID() + ":"; len(got) < len(want) || got[:len(want)] != want {
		t.Fatalf("stored %q, want the %q envelope", got, want)
	}
	plaintext, err := newRing.Decrypt(got)
	if err != nil {
		t.Fatalf("decrypt rewrapped: %v", err)
	}
	if string(plaintext) != "hunter2" {
		t.Fatalf("decrypted %q, want %q", plaintext, "hunter2")
	}

	var plain string
	if err := db.QueryRow(`SELECT secret FROM ` + col.Table + ` WHERE id = 'b'`).Scan(&plain); err != nil {
		t.Fatalf("select plain: %v", err)
	}
	if plain != "not-encrypted" {
		t.Fatalf("plaintext row became %q", plain)
	}

	// Idempotent: nothing is stale on a second pass.
	again, err := RewrapEncrypted(db, newRing, []EncryptedColumn{col})
	if err != nil {
		t.Fatalf("second rewrap: %v", err)
	}
	if again != 0 {
		t.Fatalf("second rewrap touched %d rows, want 0", again)
	}
}

// TestRewrapEncrypted_LeavesAConcurrentSaveAlone is the reason this file needs
// a database. The sweep runs at startup while another replica serves traffic,
// so a credential saved between the sweep's read and its write must survive.
//
// The two halves are driven separately on purpose: calling RewrapEncrypted
// after the save would prove nothing, because its own SELECT would no longer
// find the row stale. The lost update can only happen to a row the sweep read
// before the save and writes after it, which is exactly this interleaving.
func TestRewrapEncrypted_LeavesAConcurrentSaveAlone(t *testing.T) {
	db, oldRing, newRing, col := creds(t, "rewrap_cas")

	stale, err := oldRing.Encrypt([]byte("old-password"))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO `+col.Table+` VALUES ($1, $2)`, "a", stale); err != nil {
		t.Fatalf("insert: %v", err)
	}

	pending, err := selectStaleRows(db, newRing.PrimaryKeyID(), col)
	if err != nil {
		t.Fatalf("select stale: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("found %d stale rows, want 1", len(pending))
	}

	// ... and now, before the sweep writes back, the user saves a new
	// credential over that row.
	saved, err := newRing.Encrypt([]byte("new-password"))
	if err != nil {
		t.Fatalf("encrypt saved: %v", err)
	}
	if _, err := db.Exec(`UPDATE `+col.Table+` SET secret = $1 WHERE id = 'a'`, saved); err != nil {
		t.Fatalf("concurrent save: %v", err)
	}

	n, err := applyRewrap(db, newRing, col, pending)
	if err != nil {
		t.Fatalf("apply rewrap: %v", err)
	}
	if n != 0 {
		t.Fatalf("rewrapped %d rows, want 0 — the row had moved on", n)
	}

	var got string
	if err := db.QueryRow(`SELECT secret FROM ` + col.Table + ` WHERE id = 'a'`).Scan(&got); err != nil {
		t.Fatalf("select: %v", err)
	}
	plaintext, err := newRing.Decrypt(got)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if string(plaintext) != "new-password" {
		t.Fatalf("the save was reverted to %q", plaintext)
	}
}

func TestAuditStaleCiphertext_FindsColumnsTheInventoryDoesNot(t *testing.T) {
	db, oldRing, newRing, col := creds(t, "audit_unlisted")

	stored, err := oldRing.Encrypt([]byte("secret"))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO `+col.Table+` VALUES ($1, $2)`, "a", stored); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// The audit is schema-driven, so it must see this column without anyone
	// having named it in WorkspaceEncryptedColumns.
	stale, err := AuditStaleCiphertext(db, newRing.PrimaryKeyID())
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	found := false
	for _, s := range stale {
		if s.Table == col.Table && s.Column == col.Column {
			found = true
			if s.Rows != 1 {
				t.Fatalf("audit counted %d rows, want 1", s.Rows)
			}
		}
	}
	if !found {
		t.Fatalf("audit missed %s.%s; saw %+v", col.Table, col.Column, stale)
	}

	if _, err := RewrapEncrypted(db, newRing, []EncryptedColumn{col}); err != nil {
		t.Fatalf("rewrap: %v", err)
	}

	stale, err = AuditStaleCiphertext(db, newRing.PrimaryKeyID())
	if err != nil {
		t.Fatalf("audit after rewrap: %v", err)
	}
	for _, s := range stale {
		if s.Table == col.Table {
			t.Fatalf("still stale after rewrap: %+v", s)
		}
	}
}

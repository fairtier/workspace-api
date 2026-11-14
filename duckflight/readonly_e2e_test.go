//go:build duckflight_e2e

package duckflight

// Read-only sessions against REAL DuckFlight binaries, not the fake. Opt-in
// (build tag duckflight_e2e) because it needs running servers:
//
//	DUCKFLIGHT_E2E_NEW_RO    DuckFlight with read-only sessions, started with
//	                         AUTH_TOKENS=$RW AUTH_READONLY_TOKENS=$RO
//	DUCKFLIGHT_E2E_NEW_NORO  the same build, AUTH_TOKENS=$RW only (no
//	                         read-only instance)
//	DUCKFLIGHT_E2E_OLD       a DuckFlight from before read-only sessions,
//	                         AUTH_TOKENS=$RW
//	DUCKFLIGHT_E2E_RW_TOKEN, DUCKFLIGHT_E2E_RO_TOKEN
//
// Endpoints are http://host:port (plaintext). Each unset endpoint skips its
// part. Run: go test -tags duckflight_e2e -run E2E -v ./duckflight/

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func e2eEnv(t *testing.T, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		t.Skipf("%s not set", name)
	}
	return v
}

func e2eCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// On a DuckFlight with read-only sessions, both the read-only token and the
// read-write token plus header get a session in which a SELECT works and
// writes, file reads and settings are refused by the engine itself.
func TestE2E_NewEngineEnforcesReadOnly(t *testing.T) {
	endpoint := e2eEnv(t, "DUCKFLIGHT_E2E_NEW_RO")
	rw, ro := e2eEnv(t, "DUCKFLIGHT_E2E_RW_TOKEN"), e2eEnv(t, "DUCKFLIGHT_E2E_RO_TOKEN")
	c := NewClient()
	for name, token := range map[string]string{"read-only token": ro, "read-write token + header": rw} {
		t.Run(name, func(t *testing.T) {
			ctx := ReadOnly(e2eCtx(t))
			if err := c.ProbeReadOnly(ctx, endpoint, token); err != nil {
				t.Fatalf("ProbeReadOnly: %v", err)
			}
			res, err := c.Execute(ctx, endpoint, token, "SELECT 41 + 1 AS answer, count(*) AS n FROM range(5)", 10)
			if err != nil {
				t.Fatalf("SELECT: %v", err)
			}
			t.Logf("SELECT -> columns %v rows %v", res.Columns, res.Rows)
			for _, sql := range []string{
				"CREATE TABLE agent_wrote_this AS SELECT 1 AS x",
				"COPY (SELECT 1) TO '/tmp/agent_wrote_this.csv'",
				"SELECT * FROM read_text('/etc/hostname')",
				"SET enable_external_access = true",
				"ATTACH ':memory:' AS scratch",
			} {
				_, err := c.Execute(ctx, endpoint, token, sql, 10)
				if status.Code(err) != codes.PermissionDenied {
					t.Errorf("%q: err = %v, want PermissionDenied from the engine", sql, err)
					continue
				}
				t.Logf("%q -> %s", sql, status.Convert(err).Message())
			}
		})
	}
	// The read-write path is untouched: no header, a write succeeds.
	marker := fmt.Sprintf("rw_path_%d", time.Now().UnixNano())
	if _, err := c.Execute(e2eCtx(t), endpoint, rw, "CREATE TABLE "+marker+" AS SELECT 1 AS x", 10); err != nil {
		t.Errorf("read-write CREATE TABLE without the header: %v", err)
	}
}

// A new DuckFlight without the read-only instance refuses the read-only
// request instead of serving it read-write.
func TestE2E_NewEngineWithoutReadOnlyInstance(t *testing.T) {
	endpoint := e2eEnv(t, "DUCKFLIGHT_E2E_NEW_NORO")
	rw := e2eEnv(t, "DUCKFLIGHT_E2E_RW_TOKEN")
	assertNoReadOnlySession(t, endpoint, rw)
}

// An old DuckFlight ignores the header. The capability check stops the
// statement before it is sent — and the table it would have created is
// verifiably absent afterwards, i.e. it did not run read-write.
func TestE2E_OldEngineIsDetected(t *testing.T) {
	endpoint := e2eEnv(t, "DUCKFLIGHT_E2E_OLD")
	rw := e2eEnv(t, "DUCKFLIGHT_E2E_RW_TOKEN")
	assertNoReadOnlySession(t, endpoint, rw)

	// What the check prevents: the same header with the check skipped is
	// silently served read-write by the old engine.
	ctx := withReadOnlyHeader(e2eCtx(t))
	marker := fmt.Sprintf("header_ignored_%d", time.Now().UnixNano())
	fc, err := dial(ctx, endpoint, rw)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fc.Close() }()
	info, err := fc.Execute(ctx, "CREATE TABLE "+marker+" AS SELECT 1 AS x")
	if err != nil {
		t.Fatalf("old engine refused a header-marked write (expected it to ignore the header): %v", err)
	}
	// Flight SQL runs the statement when its ticket is fetched.
	for _, ep := range info.Endpoint {
		if err := NewClient().readEndpoint(ctx, fc, ep.Ticket, 1, &Result{}); err != nil {
			t.Fatalf("old engine, header-marked write: %v", err)
		}
	}
	res, err := NewClient().Execute(e2eCtx(t), endpoint, rw, "SELECT count(*) FROM information_schema.tables WHERE table_name = '"+marker+"'", 1)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(res.Rows) != "[[1]]" {
		t.Errorf("expected the old engine to have run the header-marked write read-write; tables named %s: %v", marker, res.Rows)
	}
	t.Logf("old engine, header only, check skipped: CREATE TABLE ran read-write (tables named %s: %v)", marker, res.Rows)

	// And a read-only token it never heard of is not a read-only session.
	if ro := os.Getenv("DUCKFLIGHT_E2E_RO_TOKEN"); ro != "" {
		if err := NewClient().ProbeReadOnly(e2eCtx(t), endpoint, ro); !errors.Is(err, ErrReadOnlyUnavailable) {
			t.Errorf("old engine, read-only token: probe err = %v, want ErrReadOnlyUnavailable", err)
		}
	}
}

func assertNoReadOnlySession(t *testing.T, endpoint, rw string) {
	t.Helper()
	c := NewClient()
	if err := c.ProbeReadOnly(e2eCtx(t), endpoint, rw); !errors.Is(err, ErrReadOnlyUnavailable) {
		t.Fatalf("ProbeReadOnly err = %v, want ErrReadOnlyUnavailable", err)
	}
	marker := fmt.Sprintf("must_not_exist_%d", time.Now().UnixNano())
	_, err := c.Execute(ReadOnly(e2eCtx(t)), endpoint, rw, "CREATE TABLE "+marker+" AS SELECT 1 AS x", 10)
	if !errors.Is(err, ErrReadOnlyUnavailable) {
		t.Fatalf("read-only Execute err = %v, want ErrReadOnlyUnavailable", err)
	}
	t.Logf("read-only request refused before running: %v", err)
	res, err := c.Execute(e2eCtx(t), endpoint, rw, "SELECT count(*) FROM information_schema.tables WHERE table_name = '"+marker+"'", 1)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(res.Rows) != "[[0]]" {
		t.Errorf("the refused statement ran anyway: %v tables named %s", res.Rows, marker)
	}
}

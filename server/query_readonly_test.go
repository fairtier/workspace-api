package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/duckflight"
	queryv1 "github.com/fairtier/workspace-api/proto/query/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// engineCall is one statement the fake engine was asked to run.
type engineCall struct {
	token    string
	readOnly bool
}

// fakeROEngine stands in for duckflight.Client: hasReadOnly decides whether a
// read-only request gets a session (otherwise ErrReadOnlyUnavailable, the
// statement not run), refuse makes the read-only session refuse statements.
type fakeROEngine struct {
	mu          sync.Mutex
	hasReadOnly bool
	refuse      bool
	calls       []engineCall
	probes      int
}

func (f *fakeROEngine) Execute(ctx context.Context, _, token, _ string, _ int) (*duckflight.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ro := duckflight.IsReadOnly(ctx)
	f.calls = append(f.calls, engineCall{token: token, readOnly: ro})
	if ro && !f.hasReadOnly {
		return nil, fmt.Errorf("%w: it does not report a read-only session", duckflight.ErrReadOnlyUnavailable)
	}
	if ro && f.refuse {
		return nil, status.Error(codes.PermissionDenied, `read-only session: statement type "INSERT" is not allowed`)
	}
	return &duckflight.Result{Columns: []duckflight.Column{{Name: "x", Type: "int64"}}, Rows: [][]any{{1}}}, nil
}

func (f *fakeROEngine) ProbeReadOnly(context.Context, string, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.probes++
	if !f.hasReadOnly {
		return fmt.Errorf("%w: old engine", duckflight.ErrReadOnlyUnavailable)
	}
	return nil
}

func (f *fakeROEngine) takeCalls() []engineCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.calls
	f.calls = nil
	return c
}

func roQueryServer(eng *fakeROEngine, ws *workspace.Workspace, ro *ReadOnlySessions) *QueryServer {
	return &QueryServer{
		Workspaces: &mockCustomerReader{
			getByUserID: func(context.Context, core.UserID) (*workspace.Workspace, error) { return ws, nil },
		},
		Executor: eng,
		ReadOnly: ro,
	}
}

func roWorkspace(readOnlyToken string) *workspace.Workspace {
	ws := provisionedCustomer()
	ws.DuckFlightReadOnlyAuthToken = readOnlyToken
	return ws
}

func runSelect(t *testing.T, s *QueryServer) error {
	t.Helper()
	_, err := s.ExecuteQuery(authedCtx(), connect.NewRequest(&queryv1.ExecuteQueryRequest{Sql: "SELECT 1"}))
	return err
}

func TestReadOnlySessionsUseTheReadOnlyCredential(t *testing.T) {
	eng := &fakeROEngine{hasReadOnly: true}
	s := roQueryServer(eng, roWorkspace("ro-tok"), &ReadOnlySessions{Prober: eng})

	if err := runSelect(t, s); err != nil {
		t.Fatal(err)
	}
	calls := eng.takeCalls()
	if len(calls) != 1 || calls[0] != (engineCall{token: "ro-tok", readOnly: true}) {
		t.Errorf("calls = %+v, want one read-only call with the read-only credential", calls)
	}
	if mode, _ := s.AgentSQLMode(authedCtx()); mode != AgentSQLEngineCredential {
		t.Errorf("mode = %q, want %q", mode, AgentSQLEngineCredential)
	}
}

func TestReadOnlySessionsSelfRestrictWithoutAReadOnlyCredential(t *testing.T) {
	eng := &fakeROEngine{hasReadOnly: true}
	s := roQueryServer(eng, roWorkspace(""), &ReadOnlySessions{Prober: eng})

	if err := runSelect(t, s); err != nil {
		t.Fatal(err)
	}
	calls := eng.takeCalls()
	if len(calls) != 1 || calls[0] != (engineCall{token: "tok-1", readOnly: true}) {
		t.Errorf("calls = %+v, want one read-only call with the read-write credential", calls)
	}
	if mode, _ := s.AgentSQLMode(authedCtx()); mode != AgentSQLEngineHeader {
		t.Errorf("mode = %q, want %q", mode, AgentSQLEngineHeader)
	}
}

func TestReadOnlySessionsFallBackOnceAndRememberIt(t *testing.T) {
	var logs bytes.Buffer
	now := time.Date(2026, 11, 14, 3, 0, 0, 0, time.UTC)
	eng := &fakeROEngine{hasReadOnly: false}
	ro := &ReadOnlySessions{
		Prober: eng,
		Logger: slog.New(slog.NewTextHandler(&logs, nil)),
		now:    func() time.Time { return now },
	}
	s := roQueryServer(eng, roWorkspace(""), ro)

	// First statement: the read-only attempt is refused before running, and
	// the statement runs once the old way.
	if err := runSelect(t, s); err != nil {
		t.Fatal(err)
	}
	want := []engineCall{{token: "tok-1", readOnly: true}, {token: "tok-1", readOnly: false}}
	if got := eng.takeCalls(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("first statement calls = %+v, want %+v", got, want)
	}

	// Within the recheck window the engine is not asked again.
	if err := runSelect(t, s); err != nil {
		t.Fatal(err)
	}
	if got := eng.takeCalls(); fmt.Sprint(got) != fmt.Sprint([]engineCall{{token: "tok-1"}}) {
		t.Errorf("second statement calls = %+v, want only the guard-only call", got)
	}
	if mode, reason := s.AgentSQLMode(authedCtx()); mode != AgentSQLGuardOnly || reason == "" {
		t.Errorf("mode = %q (%q), want %q with a reason", mode, reason, AgentSQLGuardOnly)
	}

	// After it, the next statement asks again (and falls back again).
	now = now.Add(defaultReadOnlyRecheck)
	if err := runSelect(t, s); err != nil {
		t.Fatal(err)
	}
	if got := eng.takeCalls(); len(got) != 2 || !got[0].readOnly {
		t.Errorf("after the recheck window calls = %+v, want a read-only attempt then the fallback", got)
	}

	// One warning for the whole downgrade, not one per statement.
	if n := strings.Count(logs.String(), "level=WARN"); n != 1 {
		t.Errorf("logged %d warnings, want 1:\n%s", n, logs.String())
	}
	if !strings.Contains(logs.String(), "lexical guard only") {
		t.Errorf("the warning does not say what the fallback means:\n%s", logs.String())
	}

	// The engine gaining read-only sessions is picked up and logged.
	eng.hasReadOnly = true
	now = now.Add(defaultReadOnlyRecheck)
	if err := runSelect(t, s); err != nil {
		t.Fatal(err)
	}
	if got := eng.takeCalls(); len(got) != 1 || !got[0].readOnly {
		t.Errorf("after recovery calls = %+v, want one read-only call", got)
	}
	if !strings.Contains(logs.String(), "engine-enforced read-only again") {
		t.Errorf("recovery not logged:\n%s", logs.String())
	}
}

func TestReadOnlySessionsStrictRefuses(t *testing.T) {
	eng := &fakeROEngine{hasReadOnly: false}
	s := roQueryServer(eng, roWorkspace(""), &ReadOnlySessions{Strict: true, Prober: eng, Logger: slog.New(slog.DiscardHandler)})

	err := runSelect(t, s)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "MCP_REQUIRE_READONLY_ENGINE") {
		t.Fatalf("err = %v, want FailedPrecondition naming MCP_REQUIRE_READONLY_ENGINE", err)
	}
	for _, c := range eng.takeCalls() {
		if !c.readOnly {
			t.Errorf("strict mode ran a statement read-write: %+v", c)
		}
	}
	// And keeps refusing without asking the engine each time.
	if err := runSelect(t, s); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("second err = %v, want FailedPrecondition", err)
	}
	if got := eng.takeCalls(); len(got) != 0 {
		t.Errorf("strict mode asked the engine again within the recheck window: %+v", got)
	}
	if mode, _ := s.AgentSQLMode(authedCtx()); mode != AgentSQLRefused {
		t.Errorf("mode = %q, want %q", mode, AgentSQLRefused)
	}
}

func TestReadOnlySessionsMapPolicyRefusal(t *testing.T) {
	eng := &fakeROEngine{hasReadOnly: true, refuse: true}
	s := roQueryServer(eng, roWorkspace("ro-tok"), &ReadOnlySessions{Prober: eng})

	err := runSelect(t, s)
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("err = %v, want PermissionDenied", err)
	}
	if !strings.Contains(err.Error(), "read-only session refused this statement") || !strings.Contains(err.Error(), "INSERT") {
		t.Errorf("err = %v, want the engine's reason, said as a policy refusal", err)
	}
	// A refusal is not a downgrade: nothing ran read-write.
	for _, c := range eng.takeCalls() {
		if !c.readOnly {
			t.Errorf("refused statement retried read-write: %+v", c)
		}
	}
}

// The Console's QueryServer has no ReadOnly: read-write credential, no
// read-only request, even when a read-only credential is configured.
func TestConsoleQueryPathUnchanged(t *testing.T) {
	eng := &fakeROEngine{hasReadOnly: true}
	s := roQueryServer(eng, roWorkspace("ro-tok"), nil)

	if err := runSelect(t, s); err != nil {
		t.Fatal(err)
	}
	calls := eng.takeCalls()
	if len(calls) != 1 || calls[0] != (engineCall{token: "tok-1"}) {
		t.Errorf("calls = %+v, want one read-write call with the read-write credential", calls)
	}
}

func TestAgentSQLModeProbesOnceAndUnreachableIsUnknown(t *testing.T) {
	eng := &fakeROEngine{hasReadOnly: true}
	s := roQueryServer(eng, roWorkspace(""), &ReadOnlySessions{Prober: eng})
	for range 3 {
		if mode, _ := s.AgentSQLMode(authedCtx()); mode != AgentSQLEngineHeader {
			t.Fatalf("mode = %q", mode)
		}
	}
	if eng.probes != 1 {
		t.Errorf("probed %d times, want once within the recheck window", eng.probes)
	}

	down := roQueryServer(eng, roWorkspace(""), &ReadOnlySessions{Prober: unreachableProber{}})
	if mode, _ := down.AgentSQLMode(authedCtx()); mode != AgentSQLUnknown {
		t.Errorf("mode = %q, want %q for an unreachable engine", mode, AgentSQLUnknown)
	}
}

type unreachableProber struct{}

func (unreachableProber) ProbeReadOnly(context.Context, string, string) error {
	return errors.New("connection refused")
}

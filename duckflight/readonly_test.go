package duckflight

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// fakeEngine is a Flight SQL server that behaves like the three DuckFlight
// versions the read-only path has to tell apart:
//
//   - old: no read-only sessions. It ignores the header (unknown metadata),
//     knows only its read-write tokens, and reports ServerReadOnly=false.
//   - new, read-only instance enabled: a read-only token or the header gives
//     a read-only session; ServerReadOnly is true for it; INSERT is refused
//     with PermissionDenied.
//   - new, read-only instance NOT enabled: still answers ServerReadOnly=true
//     to a read-only caller (it describes the session asked for) but refuses
//     the statement with FailedPrecondition.
type fakeEngine struct {
	flightsql.BaseServer
	roInfo flightsql.BaseServer

	readOnlySessions bool // false = old DuckFlight
	readOnlyInstance bool
	rwTokens         map[string]bool
	roTokens         map[string]bool

	mu       sync.Mutex
	executed []executedStmt
}

type executedStmt struct {
	sql      string
	readOnly bool
}

func newFakeEngine(t *testing.T, readOnlySessions, readOnlyInstance bool) (*fakeEngine, string) {
	t.Helper()
	f := &fakeEngine{
		readOnlySessions: readOnlySessions,
		readOnlyInstance: readOnlyInstance,
		rwTokens:         map[string]bool{"rw-token": true},
		roTokens:         map[string]bool{},
	}
	if readOnlySessions {
		f.roTokens["ro-token"] = true
	}
	f.Alloc = memory.DefaultAllocator
	f.roInfo.Alloc = memory.DefaultAllocator
	_ = f.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerName, "fake")
	_ = f.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerReadOnly, false)
	_ = f.roInfo.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerName, "fake")
	_ = f.roInfo.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerReadOnly, true)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	flight.RegisterFlightServiceServer(s, flightsql.NewFlightServer(f))
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return f, "http://" + lis.Addr().String()
}

// session authenticates the request and decides whether it is read-only,
// as the DuckFlight version being faked would.
func (f *fakeEngine) session(ctx context.Context) (readOnly bool, err error) {
	md, _ := metadata.FromIncomingContext(ctx)
	token := ""
	if v := md.Get("authorization"); len(v) > 0 {
		token = strings.TrimPrefix(v[0], "Bearer ")
	}
	switch {
	case f.roTokens[token]:
		readOnly = true
	case f.rwTokens[token]:
	default:
		return false, status.Error(codes.Unauthenticated, "invalid token")
	}
	if f.readOnlySessions {
		if v := md.Get(ReadOnlyHeader); len(v) > 0 && v[0] == "true" {
			readOnly = true
		}
	} else {
		readOnly = false
	}
	return readOnly, nil
}

func (f *fakeEngine) GetFlightInfoSqlInfo(ctx context.Context, cmd flightsql.GetSqlInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if _, err := f.session(ctx); err != nil {
		return nil, err
	}
	return f.BaseServer.GetFlightInfoSqlInfo(ctx, cmd, desc)
}

func (f *fakeEngine) DoGetSqlInfo(ctx context.Context, cmd flightsql.GetSqlInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	ro, err := f.session(ctx)
	if err != nil {
		return nil, nil, err
	}
	if ro {
		return f.roInfo.DoGetSqlInfo(ctx, cmd)
	}
	return f.BaseServer.DoGetSqlInfo(ctx, cmd)
}

func (f *fakeEngine) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	ro, err := f.session(ctx)
	if err != nil {
		return nil, err
	}
	if ro && !f.readOnlyInstance {
		return nil, status.Error(codes.FailedPrecondition, "read-only sessions are not enabled on this server")
	}
	if ro && strings.Contains(strings.ToUpper(cmd.GetQuery()), "INSERT") {
		return nil, status.Error(codes.PermissionDenied, "read-only session: INSERT is not allowed")
	}
	f.mu.Lock()
	f.executed = append(f.executed, executedStmt{sql: cmd.GetQuery(), readOnly: ro})
	f.mu.Unlock()
	tkt, err := flightsql.CreateStatementQueryTicket([]byte(cmd.GetQuery()))
	if err != nil {
		return nil, err
	}
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: tkt}}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (f *fakeEngine) DoGetStatement(context.Context, flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	rec := b.NewRecordBatch()
	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: rec}
	close(ch)
	return schema, ch, nil
}

func (f *fakeEngine) statements() []executedStmt {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]executedStmt(nil), f.executed...)
}

func TestReadOnlyExecute(t *testing.T) {
	tests := []struct {
		name             string
		readOnlySessions bool
		readOnlyInstance bool
		token            string
		wantUnavailable  bool
	}{
		{name: "new engine, read-only token", readOnlySessions: true, readOnlyInstance: true, token: "ro-token"},
		{name: "new engine, read-write token plus header", readOnlySessions: true, readOnlyInstance: true, token: "rw-token"},
		{name: "old engine ignores the header", token: "rw-token", wantUnavailable: true},
		{name: "old engine does not know the read-only token", token: "ro-token", wantUnavailable: true},
		{name: "new engine without the read-only instance", readOnlySessions: true, token: "rw-token", wantUnavailable: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, endpoint := newFakeEngine(t, tt.readOnlySessions, tt.readOnlyInstance)
			c := NewClient()

			res, err := c.Execute(ReadOnly(context.Background()), endpoint, tt.token, "SELECT 1", 10)
			probeErr := c.ProbeReadOnly(context.Background(), endpoint, tt.token)

			if tt.wantUnavailable {
				if !errors.Is(err, ErrReadOnlyUnavailable) {
					t.Fatalf("Execute err = %v, want ErrReadOnlyUnavailable", err)
				}
				if !errors.Is(probeErr, ErrReadOnlyUnavailable) {
					t.Errorf("ProbeReadOnly err = %v, want ErrReadOnlyUnavailable", probeErr)
				}
				// The point of the check: the statement never ran, in
				// particular not read-write on an engine that ignored the
				// header.
				if got := f.statements(); len(got) != 0 {
					t.Errorf("statements ran on an engine without a read-only session: %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if probeErr != nil {
				t.Errorf("ProbeReadOnly: %v", probeErr)
			}
			if len(res.Rows) != 1 {
				t.Errorf("rows = %v, want one", res.Rows)
			}
			for _, s := range f.statements() {
				if !s.readOnly {
					t.Errorf("statement %q ran outside a read-only session", s.sql)
				}
			}
		})
	}
}

func TestReadOnlyRefusalKeepsPermissionDenied(t *testing.T) {
	_, endpoint := newFakeEngine(t, true, true)
	_, err := NewClient().Execute(ReadOnly(context.Background()), endpoint, "rw-token", "INSERT INTO t VALUES (1)", 10)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("err = %v, want PermissionDenied from the engine", err)
	}
	if errors.Is(err, ErrReadOnlyUnavailable) {
		t.Error("a policy refusal is not 'no read-only session'")
	}
}

// Without ReadOnly the client sends no header: the Console's path is
// unchanged and stays read-write.
func TestExecuteWithoutReadOnlySendsNoHeader(t *testing.T) {
	f, endpoint := newFakeEngine(t, true, true)
	if _, err := NewClient().Execute(context.Background(), endpoint, "rw-token", "SELECT 1", 10); err != nil {
		t.Fatal(err)
	}
	got := f.statements()
	if len(got) != 1 || got[0].readOnly {
		t.Errorf("statements = %+v, want one read-write statement", got)
	}
}

package duckflight

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Read-only sessions.
//
// DuckFlight (since its read-only sessions) serves a request on a separate,
// locked-down DuckDB instance — the lake attached READ_ONLY, external access
// off, configuration locked, every statement checked by DuckDB's parser — when
// the credential is a read-only one (AUTH_READONLY_TOKENS) or the request
// carries ReadOnlyHeader. The header only ever removes privilege.
//
// The catch is an older DuckFlight: it ignores metadata it does not know, so
// it would serve a header-marked request READ-WRITE without a word. The header
// alone therefore proves nothing, and a read-only request here is preceded by
// a capability check on the same connection: GetSqlInfo must report
// SqlInfoFlightSqlServerReadOnly = true for this caller. A DuckFlight with
// read-only sessions answers true to a read-only caller and false to everyone
// else; an older one answers false to everyone. A new DuckFlight whose
// read-only instance is not enabled still answers true (the answer describes
// the session the caller asked for) but refuses the statement itself with
// FailedPrecondition — never runs it read-write — and that refusal is mapped to
// ErrReadOnlyUnavailable as well.

// ReadOnlyHeader is the request metadata with which a caller restricts itself
// to a read-only DuckFlight session.
const ReadOnlyHeader = "x-duckflight-read-only"

// ErrReadOnlyUnavailable means the engine could not give this request a
// read-only session: it is too old to have them, they are not enabled, or it
// rejected the read-only credential. The statement was not run.
var ErrReadOnlyUnavailable = errors.New("the query engine has no read-only session for this caller")

// ReadOnlyProber checks, without running any caller SQL, whether the engine
// gives the credential a read-only session. Implemented by [Client].
type ReadOnlyProber interface {
	// ProbeReadOnly returns nil when it does, an error wrapping
	// ErrReadOnlyUnavailable when it does not, and any other error when the
	// engine could not be asked (unreachable, timeout).
	ProbeReadOnly(ctx context.Context, endpoint, token string) error
}

type readOnlyCtxKey struct{}

// ReadOnly marks ctx so that [Client.Execute] runs the statement in a
// read-only session, verified as described above, or fails with
// ErrReadOnlyUnavailable without running it.
func ReadOnly(ctx context.Context) context.Context {
	return context.WithValue(ctx, readOnlyCtxKey{}, true)
}

// IsReadOnly reports whether ctx was marked by [ReadOnly].
func IsReadOnly(ctx context.Context) bool {
	v, _ := ctx.Value(readOnlyCtxKey{}).(bool)
	return v
}

func withReadOnlyHeader(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, ReadOnlyHeader, "true")
}

// ProbeReadOnly asks the engine for SqlInfoFlightSqlServerReadOnly as a
// read-only caller and then runs a constant SELECT 1 the same way: the first
// tells a DuckFlight with read-only sessions from an older one, the second a
// server whose read-only instance is enabled from one where it is not.
func (c *Client) ProbeReadOnly(ctx context.Context, endpoint, token string) error {
	fc, err := dial(ctx, endpoint, token)
	if err != nil {
		return err
	}
	defer func() { _ = fc.Close() }()
	ctx = withReadOnlyHeader(ctx)
	if err := verifyReadOnly(ctx, fc); err != nil {
		return err
	}
	info, err := fc.Execute(ctx, "SELECT 1")
	if err != nil {
		return readOnlyRefusal(true, err)
	}
	res := &Result{}
	for _, ep := range info.Endpoint {
		if err := c.readEndpoint(ctx, fc, ep.Ticket, 1, res); err != nil {
			return readOnlyRefusal(true, err)
		}
	}
	return nil
}

// verifyReadOnly fails unless the server reports this session read-only.
func verifyReadOnly(ctx context.Context, fc *flightsql.Client) error {
	ro, err := serverReadOnly(ctx, fc)
	switch {
	case err != nil:
		return readOnlyRefusal(true, err)
	case !ro:
		return fmt.Errorf("%w: it does not report a read-only session (DuckFlight without read-only sessions ignores the request header)", ErrReadOnlyUnavailable)
	}
	return nil
}

// serverReadOnly reads SqlInfoFlightSqlServerReadOnly. A server that does not
// report it at all counts as not read-only.
func serverReadOnly(ctx context.Context, fc *flightsql.Client) (bool, error) {
	info, err := fc.GetSqlInfo(ctx, []flightsql.SqlInfo{flightsql.SqlInfoFlightSqlServerReadOnly})
	if err != nil {
		return false, err
	}
	readOnly := false
	for _, ep := range info.Endpoint {
		rdr, err := fc.DoGet(ctx, ep.Ticket)
		if err != nil {
			return false, err
		}
		for rdr.Next() {
			if v, ok := sqlInfoBool(rdr.RecordBatch(), uint32(flightsql.SqlInfoFlightSqlServerReadOnly)); ok {
				readOnly = v
			}
		}
		err = rdr.Err()
		rdr.Release()
		if err != nil {
			return false, err
		}
	}
	return readOnly, nil
}

// sqlInfoBool finds id in a GetSqlInfo batch (info_name uint32, value
// dense_union) and returns its value if it is a boolean.
func sqlInfoBool(rec arrow.RecordBatch, id uint32) (value, found bool) {
	if rec.NumCols() < 2 {
		return false, false
	}
	names, ok := rec.Column(0).(*array.Uint32)
	if !ok {
		return false, false
	}
	values, ok := rec.Column(1).(*array.DenseUnion)
	if !ok {
		return false, false
	}
	for i := range names.Len() {
		if names.Value(i) != id {
			continue
		}
		b, ok := values.Field(values.ChildID(i)).(*array.Boolean)
		if !ok {
			return false, false
		}
		return b.Value(int(values.ValueOffset(i))), true
	}
	return false, false
}

// readOnlyRefusal turns the engine's answers that mean "no read-only session
// for you" into ErrReadOnlyUnavailable: FailedPrecondition is DuckFlight's
// "read-only sessions are not enabled on this server", Unauthenticated a
// read-only credential the engine does not know. Anything else — including
// PermissionDenied, the read-only session refusing a statement — is returned
// as is. Outside a read-only request err is returned unchanged.
func readOnlyRefusal(readOnly bool, err error) error {
	if !readOnly {
		return err
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	case codes.FailedPrecondition:
		return fmt.Errorf("%w: %s", ErrReadOnlyUnavailable, st.Message())
	case codes.Unauthenticated:
		return fmt.Errorf("%w: the engine rejected the credential (%s)", ErrReadOnlyUnavailable, st.Message())
	}
	return err
}

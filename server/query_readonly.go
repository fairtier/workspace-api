package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/fairtier/workspace-api/duckflight"
)

// AgentSQLMode says what actually keeps SQL written by an agent read-only.
type AgentSQLMode string

const (
	// AgentSQLEngineCredential: DuckFlight's read-only session, selected by
	// the read-only credential (WORKSPACE_DUCKFLIGHT_READONLY_AUTH_TOKEN).
	AgentSQLEngineCredential AgentSQLMode = "engine_read_only_credential"
	// AgentSQLEngineHeader: DuckFlight's read-only session, selected by the
	// read-write credential restricting itself with the request header.
	AgentSQLEngineHeader AgentSQLMode = "engine_read_only_header"
	// AgentSQLGuardOnly: the engine has no read-only session, and the
	// statement runs read-write behind the lexical guard alone.
	AgentSQLGuardOnly AgentSQLMode = "guard_only"
	// AgentSQLRefused: the engine has no read-only session and
	// MCP_REQUIRE_READONLY_ENGINE refuses to run agent SQL without one.
	AgentSQLRefused AgentSQLMode = "refused"
	// AgentSQLUnknown: the engine could not be asked (unreachable, or no
	// query engine configured).
	AgentSQLUnknown AgentSQLMode = "unknown"
)

// EngineEnforced reports whether the engine itself confines the statement.
func (m AgentSQLMode) EngineEnforced() bool {
	return m == AgentSQLEngineCredential || m == AgentSQLEngineHeader
}

// defaultReadOnlyRecheck is how long a missing read-only session is
// remembered before the next statement asks the engine again.
const defaultReadOnlyRecheck = 5 * time.Minute

// probeTimeout bounds a capability probe made only to report the mode.
const probeTimeout = 5 * time.Second

// ReadOnlySessions runs a QueryServer's statements in DuckFlight read-only
// sessions. Set on the QueryServer that backs MCP; the Console's QueryServer
// has none and keeps its read-write path.
//
// Per statement: with a read-only credential configured, the statement goes
// out with it; otherwise with the read-write credential plus the read-only
// header. Either way the client first checks, on the same connection, that
// the engine reports a read-only session (see package duckflight), so an
// older DuckFlight that ignores the header never runs it read-write.
//
// If the engine has no read-only session, the statement is either refused
// (Strict) or run once more the way it always was — read-write credential,
// no header, the MCP lexical guard as the only layer. That downgrade is
// logged as a warning once, and remembered for Recheck so the engine is not
// asked again on every statement.
type ReadOnlySessions struct {
	// Strict refuses agent SQL when the engine has no read-only session
	// (MCP_REQUIRE_READONLY_ENGINE=true).
	Strict bool
	Prober duckflight.ReadOnlyProber
	Logger *slog.Logger
	// Recheck is how long a downgrade is remembered; 0 = 5 minutes.
	Recheck time.Duration

	now func() time.Time

	mu    sync.Mutex
	state map[string]*readOnlyState // by engine endpoint
}

type readOnlyState struct {
	mode       AgentSQLMode
	reason     string
	checkedAt  time.Time
	downgraded bool // a warning has been logged for the current downgrade
}

// engineTarget is where and as whom a QueryServer's statement goes.
type engineTarget struct {
	endpoint      string
	token         string
	readOnlyToken string
}

func (r *ReadOnlySessions) clock() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

func (r *ReadOnlySessions) recheck() time.Duration {
	if r.Recheck > 0 {
		return r.Recheck
	}
	return defaultReadOnlyRecheck
}

func (r *ReadOnlySessions) logger() *slog.Logger {
	if r.Logger != nil {
		return r.Logger
	}
	return slog.Default()
}

// engineMode is the mode a working read-only session has for target.
func engineMode(t engineTarget) (AgentSQLMode, string) {
	if t.readOnlyToken != "" {
		return AgentSQLEngineCredential, t.readOnlyToken
	}
	return AgentSQLEngineHeader, t.token
}

// fallbackMode is the mode without a read-only session.
func (r *ReadOnlySessions) fallbackMode() AgentSQLMode {
	if r.Strict {
		return AgentSQLRefused
	}
	return AgentSQLGuardOnly
}

// knownDowngrade returns the remembered downgrade for endpoint while it is
// fresh.
func (r *ReadOnlySessions) knownDowngrade(endpoint string) (reason string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.state[endpoint]
	if st == nil || st.mode.EngineEnforced() || st.mode == AgentSQLUnknown {
		return "", false
	}
	if r.clock().Sub(st.checkedAt) >= r.recheck() {
		return "", false
	}
	return st.reason, true
}

// record stores what the engine just showed, logging the transitions an
// operator needs to see: the first downgrade (warning) and the recovery.
func (r *ReadOnlySessions) record(endpoint string, mode AgentSQLMode, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == nil {
		r.state = make(map[string]*readOnlyState)
	}
	st := r.state[endpoint]
	if st == nil {
		st = &readOnlyState{}
		r.state[endpoint] = st
	}
	switch {
	case mode.EngineEnforced() && st.downgraded:
		r.logger().Info("agent SQL is engine-enforced read-only again", "mode", string(mode))
		st.downgraded = false
	case (mode == AgentSQLGuardOnly || mode == AgentSQLRefused) && !st.downgraded:
		st.downgraded = true
		if r.Strict {
			r.logger().Warn("the query engine has no read-only session; agent (MCP) SQL is REFUSED until it does (MCP_REQUIRE_READONLY_ENGINE=true)",
				"reason", reason, "recheck", r.recheck().String())
		} else {
			r.logger().Warn("the query engine has no read-only session; agent (MCP) SQL falls back to read-write behind the lexical guard only. Deploy DuckFlight with read-only sessions, or set MCP_REQUIRE_READONLY_ENGINE=true to refuse instead",
				"reason", reason, "recheck", r.recheck().String())
		}
	}
	st.mode, st.reason, st.checkedAt = mode, reason, r.clock()
}

// execute runs sql for target under the read-only policy. exec must not be
// called with a timeout context shorter than the statement needs: the
// fallback reuses ctx.
func (r *ReadOnlySessions) execute(ctx context.Context, exec duckflight.Executor, t engineTarget, sql string, maxRows int) (*duckflight.Result, error) {
	reason, downgraded := r.knownDowngrade(t.endpoint)
	if !downgraded {
		mode, token := engineMode(t)
		res, err := exec.Execute(duckflight.ReadOnly(ctx), t.endpoint, token, sql, maxRows)
		if err == nil {
			r.record(t.endpoint, mode, "")
			return res, nil
		}
		if !errors.Is(err, duckflight.ErrReadOnlyUnavailable) {
			return nil, readOnlyQueryError(err)
		}
		reason = err.Error()
		r.record(t.endpoint, r.fallbackMode(), reason)
	}
	if r.Strict {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"agent SQL requires the query engine's read-only session (MCP_REQUIRE_READONLY_ENGINE=true), and it has none: %s", reason))
	}
	if t.token == "" {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"the query engine has no read-only session and no read-write credential is configured to fall back to: %s", reason))
	}
	// Today's behaviour: read-write credential, no header, the MCP lexical
	// guard as the only layer.
	res, err := exec.Execute(ctx, t.endpoint, t.token, sql, maxRows)
	if err != nil {
		return nil, queryError(err)
	}
	return res, nil
}

// mode reports the current mode for target, asking the engine when nothing
// fresh is known. It runs no caller SQL.
func (r *ReadOnlySessions) mode(ctx context.Context, t engineTarget) (AgentSQLMode, string) {
	r.mu.Lock()
	st := r.state[t.endpoint]
	if st != nil && r.clock().Sub(st.checkedAt) < r.recheck() {
		mode, reason := st.mode, st.reason
		r.mu.Unlock()
		return mode, reason
	}
	r.mu.Unlock()
	if r.Prober == nil {
		return AgentSQLUnknown, "no capability probe configured"
	}
	mode, token := engineMode(t)
	pctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	err := r.Prober.ProbeReadOnly(pctx, t.endpoint, token)
	switch {
	case err == nil:
		r.record(t.endpoint, mode, "")
		return mode, ""
	case errors.Is(err, duckflight.ErrReadOnlyUnavailable):
		mode = r.fallbackMode()
		r.record(t.endpoint, mode, err.Error())
		return mode, err.Error()
	default:
		// Unreachable is not a downgrade: nothing is recorded, and the next
		// statement finds out for itself.
		return AgentSQLUnknown, "query engine unreachable: " + err.Error()
	}
}

// readOnlyQueryError is queryError for a statement that ran in a read-only
// session, where PermissionDenied means the session refused the statement
// (a policy decision with a reason worth showing), not a bad credential.
func readOnlyQueryError(err error) error {
	if st, ok := status.FromError(err); ok && st.Code() == codes.PermissionDenied {
		return connect.NewError(connect.CodePermissionDenied, fmt.Errorf(
			"the query engine's read-only session refused this statement: %s", st.Message()))
	}
	return queryError(err)
}

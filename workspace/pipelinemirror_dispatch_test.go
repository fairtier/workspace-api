package workspace

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fairtier/workspace-api/core"
)

type funcMirror struct{ fn func() error }

func (m *funcMirror) SyncCustomer(context.Context, string, *CommitAuthor) error { return m.fn() }

// testDispatcher builds a dispatcher with a negligible backoff so retry tests
// don't spend real seconds sleeping.
func testDispatcher(m PipelineMirrorer) *pipelineMirrorDispatcher {
	d := newPipelineMirrorDispatcher(m, nil, nil)
	d.backoff = time.Millisecond
	d.maxBackoff = time.Millisecond
	return d
}

// waitIdle blocks until the customer's worker has retired (no pending work, no
// active goroutine), or fails the test.
func waitIdle(t *testing.T, d *pipelineMirrorDispatcher, slug string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		d.mu.Lock()
		active := d.active[slug]
		d.mu.Unlock()
		if !active {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("dispatcher worker did not go idle")
}

// A transient box-unreachable failure must be retried until it succeeds — a
// save is not silently dropped by a brief box outage.
func TestPipelineMirrorDispatcher_retriesTransient(t *testing.T) {
	var calls atomic.Int32
	done := make(chan struct{})
	m := &funcMirror{fn: func() error {
		if calls.Add(1) < 3 {
			return fmt.Errorf("box blip: %w", ErrBoxUnreachable)
		}
		close(done)
		return nil
	}}
	d := testDispatcher(m)
	d.enqueue(t.Context(), "u1", "acme")

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("transient failure was not retried to success (%d calls)", calls.Load())
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("want 3 attempts (2 transient failures + success), got %d", got)
	}
}

// A non-transient failure (e.g. a render error) must NOT be retried — retrying
// a deterministic failure just hammers the box and DB.
func TestPipelineMirrorDispatcher_noRetryOnPermanent(t *testing.T) {
	var calls atomic.Int32
	m := &funcMirror{fn: func() error {
		calls.Add(1)
		return errors.New("permanent render failure")
	}}
	d := testDispatcher(m)
	d.enqueue(t.Context(), "u1", "acme")
	waitIdle(t, d, "acme")

	if got := calls.Load(); got != 1 {
		t.Fatalf("a permanent error must not retry, got %d calls", got)
	}
}

// Converges for one customer must never overlap — the serialization that stops
// an older converge from deleting a newer save's file.
func TestPipelineMirrorDispatcher_serializesPerCustomer(t *testing.T) {
	var inFlight atomic.Int32
	var overlapped atomic.Bool
	m := &funcMirror{fn: func() error {
		if inFlight.Add(1) > 1 {
			overlapped.Store(true)
		}
		time.Sleep(2 * time.Millisecond)
		inFlight.Add(-1)
		return nil
	}}
	d := testDispatcher(m)
	for range 20 {
		d.enqueue(t.Context(), "u1", "acme")
	}
	waitIdle(t, d, "acme")

	if overlapped.Load() {
		t.Fatal("converges for one customer ran concurrently; per-customer serialization broken")
	}
}

// A save arriving mid-converge is coalesced into a single follow-up run rather
// than lost — the last run always re-reads and converges the latest state.
func TestPipelineMirrorDispatcher_coalescesFollowUp(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	m := &funcMirror{fn: func() error {
		if calls.Add(1) == 1 {
			close(started)
			<-release // hold the first converge so a second save queues behind it
		}
		return nil
	}}
	d := testDispatcher(m)

	d.enqueue(t.Context(), "u1", "acme")
	<-started
	d.enqueue(t.Context(), "u2", "acme") // arrives while the first converge is in flight
	close(release)
	waitIdle(t, d, "acme")

	if got := calls.Load(); got != 2 {
		t.Fatalf("want exactly 2 converges (initial + one coalesced follow-up), got %d", got)
	}
}

// ctxUserReader resolves the commit author from a request-scoped context
// value, the way the box's reader resolves it from the caller's token claims
// rather than from a table.
type ctxUserReader struct{}

type ctxUserKey struct{}

func (ctxUserReader) GetCommitUser(ctx context.Context, _ core.UserID) (*UserInfo, error) {
	email, _ := ctx.Value(ctxUserKey{}).(string)
	if email == "" {
		return nil, errors.New("no user in context")
	}
	return &UserInfo{Name: "rich", Email: email}, nil
}

// The converge runs on a fresh context by design, but the commit author may
// only be resolvable from the *saving request's* context — on a box the
// caller's identity is in their token, not in a users table. Dropping those
// values would silently attribute every mirrored commit to the platform,
// which looks like working software.
func TestPipelineMirrorDispatcher_authorResolvedFromRequestValues(t *testing.T) {
	authors := make(chan *CommitAuthor, 1)
	m := &captureMirror{authors: authors}

	d := newPipelineMirrorDispatcher(m, ctxUserReader{}, nil)
	d.backoff = time.Millisecond
	d.maxBackoff = time.Millisecond

	// A request context that is CANCELLED by the time the worker runs, as a
	// real one is once the handler has returned.
	reqCtx, cancel := context.WithCancel(context.WithValue(t.Context(), ctxUserKey{}, "rich@example.com"))
	d.enqueue(reqCtx, "u1", "acme")
	cancel()

	select {
	case got := <-authors:
		if got == nil {
			t.Fatal("commit author was nil: the request's values did not survive the hand-off")
		}
		if got.Email != "rich@example.com" {
			t.Errorf("author email = %q, want rich@example.com", got.Email)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("converge never ran")
	}
	waitIdle(t, d, "acme")
}

// A dispatcher driven without a request context (a caller that predates the
// authorCtx field, or a test) must degrade to platform attribution, never
// panic on a nil context.
func TestPipelineMirrorDispatcher_nilAuthorContext(t *testing.T) {
	authors := make(chan *CommitAuthor, 1)
	d := newPipelineMirrorDispatcher(&captureMirror{authors: authors}, ctxUserReader{}, nil)
	d.backoff = time.Millisecond
	d.maxBackoff = time.Millisecond

	d.mu.Lock()
	d.pending["acme"] = pendingSync{callerID: "u1"}
	d.active["acme"] = true
	d.mu.Unlock()
	go d.work("acme")

	select {
	case got := <-authors:
		if got != nil {
			t.Errorf("author = %+v, want nil (platform attribution)", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("converge never ran")
	}
	waitIdle(t, d, "acme")
}

type captureMirror struct{ authors chan *CommitAuthor }

func (m *captureMirror) SyncCustomer(_ context.Context, _ string, a *CommitAuthor) error {
	m.authors <- a
	return nil
}

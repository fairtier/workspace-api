package workspace

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
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
	d.enqueue("u1", "acme")

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
	d.enqueue("u1", "acme")
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
		d.enqueue("u1", "acme")
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

	d.enqueue("u1", "acme")
	<-started
	d.enqueue("u2", "acme") // arrives while the first converge is in flight
	close(release)
	waitIdle(t, d, "acme")

	if got := calls.Load(); got != 2 {
		t.Fatalf("want exactly 2 converges (initial + one coalesced follow-up), got %d", got)
	}
}

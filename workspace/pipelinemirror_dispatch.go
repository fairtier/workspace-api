package workspace

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/fairtier/workspace-api/core"
)

// pipelineMirrorDispatcher runs best-effort box-repo mirror syncs off the
// request path — serialized and coalesced per ws, with bounded retry.
//
// Serialized per customer: SyncCustomer converges the box repo to the
// customer's FULL current pipeline set, deleting any file no longer desired
// (see pipelinemirror.go deleteStale). Two converges for one customer running
// concurrently — or an older one finishing after a newer one — could delete a
// file a newer save just added. Running at most one worker per customer at a
// time removes that race; and because every run re-reads Postgres, a coalesced
// follow-up always converges the latest state, so the box never settles on a
// stale snapshot.
//
// Bounded per customer: a burst of saves spawns at most one goroutine per
// distinct customer (not one per request), so the fan-out is bounded by the
// number of active customers rather than the request rate.
//
// Bounded retry: a save must not be silently lost to a transient box outage,
// so a converge that fails because the box was unreachable (ErrBoxUnreachable)
// is retried with backoff. This is in-memory only: a process restart drops
// still-pending work, and an outage outlasting the retry budget waits for the
// next save. A durable, restart-surviving outbox is deliberately left as future
// work and is NOT claimed here.
type pipelineMirrorDispatcher struct {
	mirror PipelineMirrorer
	users  UserReader
	logger *slog.Logger

	syncTimeout time.Duration
	backoff     time.Duration
	maxBackoff  time.Duration
	maxAttempts int

	mu      sync.Mutex
	pending map[string]core.UserID // customerSlug -> latest caller; presence = dirty
	active  map[string]bool        // customerSlug -> a worker goroutine is running
}

func newPipelineMirrorDispatcher(mirror PipelineMirrorer, users UserReader, logger *slog.Logger) *pipelineMirrorDispatcher {
	return &pipelineMirrorDispatcher{
		mirror:      mirror,
		users:       users,
		logger:      logger,
		syncTimeout: 15 * time.Second,
		backoff:     1 * time.Second,
		maxBackoff:  30 * time.Second,
		maxAttempts: 5,
		pending:     make(map[string]core.UserID),
		active:      make(map[string]bool),
	}
}

// enqueue marks the customer dirty with the latest caller and ensures a worker
// is running. Non-blocking: the save returns immediately.
func (d *pipelineMirrorDispatcher) enqueue(callerID core.UserID, customerSlug string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pending[customerSlug] = callerID
	if d.active[customerSlug] {
		return // the running worker will pick up the newly-pending state
	}
	d.active[customerSlug] = true
	go d.work(customerSlug)
}

// work drains a single customer's pending saves one at a time until none
// remain, then retires the worker. There is only ever one worker per ws,
// so converges for the same customer never overlap.
func (d *pipelineMirrorDispatcher) work(customerSlug string) {
	for {
		d.mu.Lock()
		callerID, ok := d.pending[customerSlug]
		if !ok {
			d.active[customerSlug] = false
			d.mu.Unlock()
			return
		}
		delete(d.pending, customerSlug)
		d.mu.Unlock()

		d.syncWithRetry(customerSlug, callerID)
	}
}

// syncWithRetry runs one converge, retrying only a transient box-unreachable
// failure — and only while no newer save is already queued, since that queued
// save will re-converge the latest state and retrying a superseded run is
// wasted work.
func (d *pipelineMirrorDispatcher) syncWithRetry(customerSlug string, callerID core.UserID) {
	backoff := d.backoff
	for attempt := 1; ; attempt++ {
		err := d.syncOnce(customerSlug, callerID)
		if err == nil {
			return
		}
		if d.logger != nil {
			d.logger.Warn("pipeline mirror sync", "customer", customerSlug, "attempt", attempt, "err", err)
		}
		if attempt >= d.maxAttempts || !errors.Is(err, ErrBoxUnreachable) || d.superseded(customerSlug) {
			return
		}
		time.Sleep(backoff)
		if backoff < d.maxBackoff {
			backoff *= 2
		}
	}
}

// syncOnce converges once against a fresh background context (never the request
// context: this runs after the handler has returned, so it must not inherit its
// cancellation or its request-scoped values) bounded by syncTimeout.
func (d *pipelineMirrorDispatcher) syncOnce(customerSlug string, callerID core.UserID) error {
	ctx, cancel := context.WithTimeout(context.Background(), d.syncTimeout)
	defer cancel()
	return d.mirror.SyncCustomer(ctx, customerSlug, resolveCommitAuthor(ctx, d.users, callerID))
}

func (d *pipelineMirrorDispatcher) superseded(customerSlug string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.pending[customerSlug]
	return ok
}

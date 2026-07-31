package workspace

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// DefaultStuckRunTimeout is how long a pipeline run may sit in "running" before
// the sweep marks it failed. A run is "running" only between the box dlt-worker
// reporting it started and reporting a terminal result; if that worker is
// OOM-killed, evicted, or loses its node mid-load it never reports terminal and
// the row — and therefore the Console's last_run_status badge — sticks on
// "running" forever. Anything past this window is treated as a dead run. It is
// deliberately generous so a slow-but-live load on a memory-constrained box is
// not tripped; tune with PIPELINE_RUN_STUCK_TIMEOUT.
const DefaultStuckRunTimeout = time.Hour

// StuckRunStore is the sweep's view of the database.
type StuckRunStore interface {
	// FailStuckRunningRuns marks every pipeline_runs row stuck in "running"
	// longer than olderThan (measured from started_at, falling back to
	// created_at) as "failed", stamping completed_at and an error message.
	// A non-empty customerSlug restricts the sweep to that workspace's
	// pipelines; empty sweeps all workspaces (the central deployment).
	// Returns the number of rows swept.
	FailStuckRunningRuns(ctx context.Context, customerSlug string, olderThan time.Duration) (int64, error)
}

// StuckRunSweeper fails pipeline runs orphaned in "running" by a box dlt-worker
// that died mid-load (OOM, eviction, node loss). Unlike the reconcile-intent
// requeue — which can treat *any* row still 'running' at boot as an orphan
// because that work runs in this single-replica worker — a pipeline run
// executes on a *separate* machine (the customer box), so a worker restart
// says nothing about box liveness. The sweep is therefore time-based:
// only a run past the timeout is dead, and it runs periodically (not just at
// boot) so a run that goes zombie mid-uptime still heals. Runs in the worker
// (single replica).
type StuckRunSweeper struct {
	Store StuckRunStore
	// Slug scopes the sweep to one workspace's pipelines. A box sets its
	// static slug so a mispointed PG_DSN cannot fail another tenant's
	// in-flight runs; empty means all workspaces (the central deployment,
	// whose worker legitimately sweeps every tenant).
	Slug string
	// Timeout past started_at/created_at before a 'running' run is failed;
	// 0 = DefaultStuckRunTimeout.
	Timeout time.Duration
	Logger  *slog.Logger
}

// Run sweeps immediately, then on every tick until the context is cancelled.
func (s *StuckRunSweeper) Run(ctx context.Context, interval time.Duration) {
	s.Logger.InfoContext(ctx, "stuck pipeline-run sweep started",
		"timeout", s.timeout(), "interval", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := s.SweepOnce(ctx); err != nil {
			s.Logger.ErrorContext(ctx, "stuck pipeline-run sweep error", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *StuckRunSweeper) timeout() time.Duration {
	if s.Timeout > 0 {
		return s.Timeout
	}
	return DefaultStuckRunTimeout
}

// SweepOnce fails every run stuck in "running" past the timeout in one
// statement. A no-op (0 rows) is the common case and stays silent.
func (s *StuckRunSweeper) SweepOnce(ctx context.Context) error {
	n, err := s.Store.FailStuckRunningRuns(ctx, s.Slug, s.timeout())
	if err != nil {
		return fmt.Errorf("fail stuck running runs: %w", err)
	}
	if n > 0 {
		s.Logger.WarnContext(ctx, "failed pipeline runs orphaned in 'running' past timeout",
			"count", n, "timeout", s.timeout())
	}
	return nil
}

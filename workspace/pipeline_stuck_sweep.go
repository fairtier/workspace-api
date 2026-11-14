package workspace

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// The pipeline worker's own run limits, as it defaults them (dlt-worker
// PIPELINE_RUN_TIMEOUT_SECONDS, PIPELINE_MAX_RETRIES, PIPELINE_RETRY_BASE_DELAY).
// They are mirrored here because the sweep and the worker judge the same rows:
// whatever the worker may legitimately take, the sweep must wait out.
const (
	DefaultWorkerAttemptTimeout = 6 * time.Hour
	DefaultWorkerMaxRetries     = 2
	DefaultWorkerRetryBaseDelay = 30 * time.Second
)

// StuckRunGrace is added on top of the worker's longest legitimate run before
// the sweep calls a run dead. It absorbs everything the worker does around
// the attempts themselves — re-reading the definition before a retry, killing
// a timed-out attempt's process group, recording the result — and clock skew
// between the worker and the database.
const StuckRunGrace = time.Hour

// DefaultStuckRunTimeout is how long a pipeline run may sit in "running" before
// the sweep marks it failed, when nothing says otherwise: the worker's default
// worst case (every attempt running to its wall-clock limit, plus the backoff
// between them) plus StuckRunGrace — 19h01m30s.
//
// A run is "running" only between the dlt-worker recording it started and
// recording a terminal result; if that worker is OOM-killed, evicted, or loses
// its node mid-load it never records terminal and the row — and therefore the
// Console's last_run_status badge — sticks on "running" forever. That is the
// only thing the sweep is for. It is one row per run, not per attempt (the
// worker reuses the run id across retries and stamps started_at once), so the
// window has to cover all attempts: a shorter one fails runs that are still
// loading, and the worker's final write then flips them back. Derive it from
// the worker's limits with StuckRunTimeoutFor rather than setting it by hand.
const DefaultStuckRunTimeout = (DefaultWorkerMaxRetries+1)*DefaultWorkerAttemptTimeout +
	DefaultWorkerRetryBaseDelay*(1<<DefaultWorkerMaxRetries-1) +
	StuckRunGrace

// WorkerRunLimits is how long the pipeline worker lets one run take. The
// fields carry the worker's own knobs, under the worker's own names, so one
// value configures both sides.
type WorkerRunLimits struct {
	// AttemptTimeout is the wall-clock limit on one attempt
	// (PIPELINE_RUN_TIMEOUT_SECONDS). Zero or less means the worker enforces
	// none — as it does when the limit is 0 or runs are not isolated in a
	// subprocess (PIPELINE_SUBPROCESS=0).
	AttemptTimeout time.Duration
	// MaxRetries is how many times a failed attempt is retried
	// (PIPELINE_MAX_RETRIES); the run gets MaxRetries+1 attempts.
	MaxRetries int
	// RetryBaseDelay is the first backoff between attempts, doubled for each
	// later one (PIPELINE_RETRY_BASE_DELAY).
	RetryBaseDelay time.Duration
}

// DefaultWorkerRunLimits returns the worker's defaults.
func DefaultWorkerRunLimits() WorkerRunLimits {
	return WorkerRunLimits{
		AttemptTimeout: DefaultWorkerAttemptTimeout,
		MaxRetries:     DefaultWorkerMaxRetries,
		RetryBaseDelay: DefaultWorkerRetryBaseDelay,
	}
}

// maxRetriesBounded caps the retries MaxRunDuration will add up; past it the
// doubling backoff alone runs for decades, which is unbounded in practice.
const maxRetriesBounded = 30

// MaxRunDuration is the longest a run may legitimately stay "running": every
// attempt to its limit plus the backoff between them. ok is false when the
// worker sets no limit, so no duration is long enough to prove a run dead.
func (l WorkerRunLimits) MaxRunDuration() (d time.Duration, ok bool) {
	if l.AttemptTimeout <= 0 || l.MaxRetries > maxRetriesBounded {
		return 0, false
	}
	retries := max(l.MaxRetries, 0)
	d = time.Duration(retries+1) * l.AttemptTimeout
	for i := range retries {
		d += max(l.RetryBaseDelay, 0) << i
	}
	return d, true
}

// StuckRunTimeoutFor resolves the sweep's timeout from the worker's limits and
// an explicitly configured one (override, 0 = none). The result is never
// below limits.MaxRunDuration()+StuckRunGrace: a smaller override is raised
// to that floor, since a sweep that fires first fails runs that are still
// loading. A zero result means do not sweep — the worker is unbounded and
// nothing was configured. warning is non-empty when the result is not what
// was configured, or is unsafe, and says why.
func StuckRunTimeoutFor(limits WorkerRunLimits, override time.Duration) (timeout time.Duration, warning string) {
	maxRun, bounded := limits.MaxRunDuration()
	if !bounded {
		if override > 0 {
			return override, "the pipeline worker sets no run time limit, so the stuck-run sweep may fail a run that is still loading"
		}
		return 0, "the pipeline worker sets no run time limit, so the stuck-run sweep is off: no age proves a run dead"
	}
	floor := maxRun + StuckRunGrace
	if override > 0 && override < floor {
		return floor, fmt.Sprintf("stuck-run timeout %s is shorter than the pipeline worker's longest legitimate run (%s); raised to %s",
			override, maxRun, floor)
	}
	if override > 0 {
		return override, ""
	}
	return floor, ""
}

// StuckRunStore is the sweep's view of the database.
type StuckRunStore interface {
	// FailStuckRunningRuns marks every pipeline_runs row stuck in "running"
	// longer than olderThan (measured from the later of started_at and
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
	// Timeout past the later of started_at/created_at before a 'running' run
	// is failed; 0 = DefaultStuckRunTimeout. Resolve it from the worker's
	// limits with StuckRunTimeoutFor.
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
//
// The sweep gets its own root span: it runs on a ticker with no inbound
// request to hang off, and without one the statement it issues would appear in
// no trace at all.
func (s *StuckRunSweeper) SweepOnce(ctx context.Context) (err error) {
	ctx, span := tracer.Start(ctx, "StuckRunSweeper.SweepOnce", trace.WithAttributes(
		attrSlug.String(s.Slug),
	))
	defer func() { endSpan(span, err) }()

	n, err := s.Store.FailStuckRunningRuns(ctx, s.Slug, s.timeout())
	if err != nil {
		return fmt.Errorf("fail stuck running runs: %w", err)
	}
	span.SetAttributes(attribute.Int64("workspace.pipeline.runs.swept", n))
	if n > 0 {
		pipelineRunsStuck.Add(ctx, n)
		s.Logger.WarnContext(ctx, "failed pipeline runs orphaned in 'running' past timeout",
			"count", n, "timeout", s.timeout())
	}
	return nil
}

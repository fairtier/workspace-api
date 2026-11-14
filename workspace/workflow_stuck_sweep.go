package workspace

import (
	"context"
	"log/slog"
	"time"
)

// WorkflowStuckRunSweeper periodically fails workflow runs a worker left in
// "running" (killed or restarted mid-run), via WorkflowService.SweepStuckRuns.
// Like StuckRunSweeper it is time-based — the worker runs on its own schedule,
// so only a run past the timeout is presumed dead — and scoped to one
// workspace's slug.
type WorkflowStuckRunSweeper struct {
	Service *WorkflowService
	Slug    string
	// Timeout past started_at/created_at; 0 = DefaultWorkflowStuckRunTimeout.
	Timeout time.Duration
	Logger  *slog.Logger
}

// Run sweeps immediately, then on every tick until the context is cancelled.
func (s *WorkflowStuckRunSweeper) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		n, err := s.Service.SweepStuckRuns(ctx, s.Slug, s.Timeout)
		switch {
		case err != nil:
			s.Logger.ErrorContext(ctx, "stuck workflow-run sweep error", "error", err)
		case n > 0:
			s.Logger.WarnContext(ctx, "failed workflow runs stuck in 'running' past timeout", "count", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

package workspace

import (
	"context"
	"log/slog"
	"time"
)

// WorkflowStuckRunSweeper periodically fails workflow runs a worker left in
// "running" (killed or restarted mid-run), via WorkflowService.SweepStuckRuns,
// judged by Policy from the run's step transitions (see WorkflowStuckPolicy),
// and scoped to one workspace's slug.
type WorkflowStuckRunSweeper struct {
	Service *WorkflowService
	Slug    string
	Policy  WorkflowStuckPolicy
	Logger  *slog.Logger
}

// Run sweeps immediately, then on every tick until the context is cancelled.
func (s *WorkflowStuckRunSweeper) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		n, err := s.Service.SweepStuckRuns(ctx, s.Slug, s.Policy)
		switch {
		case err != nil:
			s.Logger.ErrorContext(ctx, "stuck workflow-run sweep error", "error", err)
		case n > 0:
			s.Logger.WarnContext(ctx, "failed workflow runs stuck in 'running' (no step transition reported in time)", "count", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

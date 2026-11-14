package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/fairtier/workspace-api/workspace"
)

// GetActiveRun is what keeps "Run now" from stacking a second run: it must
// see a run that is already executing, not only one still queued. Gated on
// TEST_DATABASE_URL like the other store tests.

func TestGetActiveRun(t *testing.T) {
	ctx := context.Background()

	t.Run("none when every run is finished", func(t *testing.T) {
		r, pipelineID := stuckFixture(t, "active-run-done")
		if _, err := r.DB.Exec(`INSERT INTO pipeline_runs (pipeline_id, status) VALUES ($1, 'success'), ($1, 'failed')`, pipelineID); err != nil {
			t.Fatal(err)
		}
		run, err := r.GetActiveRun(ctx, workspace.PipelineID(pipelineID))
		if err != nil {
			t.Fatal(err)
		}
		if run != nil {
			t.Fatalf("GetActiveRun = %+v, want nil", run)
		}
	})

	t.Run("a running run is active", func(t *testing.T) {
		r, pipelineID := stuckFixture(t, "active-run-running")
		id := insertRunning(t, r, pipelineID, time.Minute, time.Minute)
		run, err := r.GetActiveRun(ctx, workspace.PipelineID(pipelineID))
		if err != nil {
			t.Fatal(err)
		}
		if run == nil || run.ID != id || run.Status != "running" {
			t.Fatalf("GetActiveRun = %+v, want the running run %s", run, id)
		}
	})

	t.Run("a queued run wins over a running one", func(t *testing.T) {
		r, pipelineID := stuckFixture(t, "active-run-both")
		insertRunning(t, r, pipelineID, time.Minute, time.Minute)
		var pending string
		if err := r.DB.QueryRow(`INSERT INTO pipeline_runs (pipeline_id, status) VALUES ($1, 'pending') RETURNING id`, pipelineID).Scan(&pending); err != nil {
			t.Fatal(err)
		}
		run, err := r.GetActiveRun(ctx, workspace.PipelineID(pipelineID))
		if err != nil {
			t.Fatal(err)
		}
		if run == nil || run.ID != pending {
			t.Fatalf("GetActiveRun = %+v, want the pending run %s", run, pending)
		}
	})
}

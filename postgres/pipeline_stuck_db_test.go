package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/fairtier/workspace-api/workspace"
)

// FailStuckRunningRuns decides which rows are dead purely from timestamps, so
// what it measures from is the whole behaviour — and that can only be shown
// against real Postgres. Gated on TEST_DATABASE_URL like the rewrap tests.

// stuckFixture migrates the schema and creates one pipeline for slug, removed
// again (with its runs, by cascade) when the test ends.
func stuckFixture(t *testing.T, slug string) (*Repository, string) {
	t.Helper()
	db := testDB(t)
	if err := Migrate(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	var pipelineID string
	if err := db.QueryRow(
		`INSERT INTO pipelines (customer_slug, name, source_type, dataset_name)
		 VALUES ($1, $2, 'rest_api', 'ds') RETURNING id`,
		slug, fmt.Sprintf("stuck-%d", time.Now().UnixNano()),
	).Scan(&pipelineID); err != nil {
		t.Fatalf("insert pipeline: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM pipelines WHERE id = $1`, pipelineID) })
	return &Repository{DB: db}, pipelineID
}

// insertRunning adds a 'running' row whose started_at and created_at lie the
// given distances in the past.
func insertRunning(t *testing.T, r *Repository, pipelineID string, startedAgo, createdAgo time.Duration) string {
	t.Helper()
	var id string
	if err := r.DB.QueryRow(
		`INSERT INTO pipeline_runs (pipeline_id, status, started_at, created_at)
		 VALUES ($1, 'running', now() - make_interval(secs => $2), now() - make_interval(secs => $3))
		 RETURNING id`,
		pipelineID, startedAgo.Seconds(), createdAgo.Seconds(),
	).Scan(&id); err != nil {
		t.Fatalf("insert run: %v", err)
	}
	return id
}

func runStatus(t *testing.T, r *Repository, id string) string {
	t.Helper()
	var status string
	if err := r.DB.QueryRow(`SELECT status FROM pipeline_runs WHERE id = $1`, id).Scan(&status); err != nil {
		t.Fatalf("read run: %v", err)
	}
	return status
}

func TestFailStuckRunningRunsSweepsOldRun(t *testing.T) {
	r, pipelineID := stuckFixture(t, "stuck-old")
	dead := insertRunning(t, r, pipelineID, 3*time.Hour, 3*time.Hour)
	live := insertRunning(t, r, pipelineID, 10*time.Minute, 10*time.Minute)

	n, err := r.FailStuckRunningRuns(context.Background(), "stuck-old", time.Hour)
	if err != nil {
		t.Fatalf("FailStuckRunningRuns: %v", err)
	}
	if n != 1 {
		t.Errorf("swept = %d, want 1", n)
	}
	if got := runStatus(t, r, dead); got != "failed" {
		t.Errorf("old run status = %q, want failed", got)
	}
	if got := runStatus(t, r, live); got != "running" {
		t.Errorf("recent run status = %q, want running", got)
	}
}

// The dlt-worker stamps every pipeline of one poll tick with the tick's start
// time, and runs them one after another — so the third pipeline of a busy tick
// carries a started_at hours older than its own work. Its row is inserted when
// it really begins, though, so created_at is the later, truer liveness mark:
// the sweep must measure from whichever of the two is newer.
func TestFailStuckRunningRunsMeasuresFromNewerOfStartedAndCreated(t *testing.T) {
	r, pipelineID := stuckFixture(t, "stuck-tick")
	queued := insertRunning(t, r, pipelineID, 3*time.Hour, 5*time.Minute)

	n, err := r.FailStuckRunningRuns(context.Background(), "stuck-tick", time.Hour)
	if err != nil {
		t.Fatalf("FailStuckRunningRuns: %v", err)
	}
	if n != 0 {
		t.Errorf("swept = %d, want 0", n)
	}
	if got := runStatus(t, r, queued); got != "running" {
		t.Errorf("run inserted 5m ago (started_at stamped 3h ago) = %q, want running", got)
	}
}

// A run the sweep got wrong is corrected by the worker's own final report: the
// report is a plain update on the run id, not conditional on the row still
// being 'running', so the success overwrites status, completion time and the
// sweep's error message. (The sweep itself raises no notification — only a
// report does — so nothing "failed"-shaped is left behind either.)
func TestSweptRunIsCorrectedByLaterSuccessReport(t *testing.T) {
	r, pipelineID := stuckFixture(t, "stuck-correct")
	id := insertRunning(t, r, pipelineID, 3*time.Hour, 3*time.Hour)
	if _, err := r.FailStuckRunningRuns(context.Background(), "stuck-correct", time.Hour); err != nil {
		t.Fatalf("FailStuckRunningRuns: %v", err)
	}

	done := time.Now()
	if err := r.UpdatePipelineRun(context.Background(), &workspace.PipelineRun{
		ID: id, PipelineID: workspace.PipelineID(pipelineID), Status: "success", CompletedAt: &done, RowsLoaded: 42,
	}); err != nil {
		t.Fatalf("UpdatePipelineRun: %v", err)
	}

	var status, errMsg string
	var rows int64
	if err := r.DB.QueryRow(`SELECT status, error_message, rows_loaded FROM pipeline_runs WHERE id = $1`, id).
		Scan(&status, &errMsg, &rows); err != nil {
		t.Fatalf("read run: %v", err)
	}
	if status != "success" || errMsg != "" || rows != 42 {
		t.Errorf("after success report: status=%q error=%q rows=%d; want success, no error, 42", status, errMsg, rows)
	}
}

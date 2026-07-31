package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/fairtier/workspace-api/workspace"
)

func (r *Repository) encryptCredentials(creds json.RawMessage) (string, error) {
	if r.Encryptor == nil {
		return string(creds), nil
	}
	return r.Encryptor.Encrypt(creds)
}

func (r *Repository) decryptCredentials(stored string) (json.RawMessage, error) {
	if r.Encryptor == nil {
		return json.RawMessage(stored), nil
	}
	b, err := r.Encryptor.Decrypt(stored)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}

func (r *Repository) CreatePipeline(ctx context.Context, p *workspace.Pipeline) error {
	encCreds, err := r.encryptCredentials(p.SourceCredentials)
	if err != nil {
		return fmt.Errorf("postgres: encrypt credentials: %w", err)
	}

	err = r.DB.QueryRowContext(ctx,
		`INSERT INTO pipelines (customer_slug, name, source_type, source_config, source_credentials, dataset_name, schedule, write_disposition, merge_strategy, enabled, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		 RETURNING id`,
		p.CustomerSlug, p.Name, p.SourceType, p.SourceConfig, encCreds,
		p.DatasetName, p.Schedule, p.WriteDisposition, p.MergeStrategy, p.Enabled, p.CreatedAt, p.UpdatedAt,
	).Scan(&p.ID)
	if err != nil {
		if isUniqueViolation(err) {
			return workspace.ErrPipelineAlreadyExists
		}
		return fmt.Errorf("postgres: create pipeline: %w", err)
	}
	return nil
}

func (r *Repository) GetPipeline(ctx context.Context, id workspace.PipelineID) (*workspace.Pipeline, error) {
	var p workspace.Pipeline
	var storedCreds string
	err := r.DB.QueryRowContext(ctx,
		`SELECT id, customer_slug, name, source_type, source_config, source_credentials, dataset_name, schedule, write_disposition, merge_strategy, enabled, credentials_external, created_at, updated_at
		 FROM pipelines WHERE id = $1`,
		id,
	).Scan(&p.ID, &p.CustomerSlug, &p.Name, &p.SourceType, &p.SourceConfig, &storedCreds,
		&p.DatasetName, &p.Schedule, &p.WriteDisposition, &p.MergeStrategy, &p.Enabled, &p.CredentialsExternal, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, workspace.ErrPipelineNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: get pipeline: %w", err)
	}

	p.SourceCredentials, err = r.decryptCredentials(storedCreds)
	if err != nil {
		return nil, fmt.Errorf("postgres: decrypt credentials: %w", err)
	}
	return &p, nil
}

func (r *Repository) ListPipelinesByCustomer(ctx context.Context, customerSlug string) ([]workspace.Pipeline, error) {
	// LEFT JOIN LATERAL the most recent run (any status) per pipeline so the
	// list view can show real last-run status/time without N+1 GetPipeline calls.
	rows, err := r.DB.QueryContext(ctx,
		`SELECT p.id, p.customer_slug, p.name, p.source_type, p.source_config, p.dataset_name,
		        p.schedule, p.write_disposition, p.merge_strategy, p.enabled, p.credentials_external, p.created_at, p.updated_at,
		        lr.last_run_time, lr.last_run_status
		 FROM pipelines p
		 LEFT JOIN LATERAL (
		     SELECT COALESCE(started_at, created_at) AS last_run_time, status AS last_run_status
		     FROM pipeline_runs
		     WHERE pipeline_id = p.id
		     ORDER BY created_at DESC
		     LIMIT 1
		 ) lr ON true
		 WHERE p.customer_slug = $1 ORDER BY p.created_at`,
		customerSlug,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: list pipelines: %w", err)
	}
	defer rows.Close()

	var pipelines []workspace.Pipeline
	for rows.Next() {
		var p workspace.Pipeline
		var lastRunTime sql.NullTime
		var lastRunStatus sql.NullString
		if err := rows.Scan(&p.ID, &p.CustomerSlug, &p.Name, &p.SourceType, &p.SourceConfig,
			&p.DatasetName, &p.Schedule, &p.WriteDisposition, &p.MergeStrategy, &p.Enabled, &p.CredentialsExternal, &p.CreatedAt, &p.UpdatedAt,
			&lastRunTime, &lastRunStatus); err != nil {
			return nil, fmt.Errorf("postgres: scan pipeline: %w", err)
		}
		if lastRunTime.Valid {
			t := lastRunTime.Time
			p.LastRunTime = &t
		}
		p.LastRunStatus = lastRunStatus.String
		pipelines = append(pipelines, p)
	}
	return pipelines, rows.Err()
}

// ListPipelineCredentialsByCustomer returns decrypted source credentials
// keyed by pipeline id — the pipeline mirror's read path for rendering
// pipelines/<name>.credentials.age (pipelines-as-files Phase 3).
// Deliberately separate from ListPipelinesByCustomer, which never selects
// credentials (Console list path).
func (r *Repository) ListPipelineCredentialsByCustomer(ctx context.Context, customerSlug string) (map[workspace.PipelineID]json.RawMessage, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT id, source_credentials FROM pipelines WHERE customer_slug = $1`,
		customerSlug,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: list pipeline credentials: %w", err)
	}
	defer rows.Close()

	out := make(map[workspace.PipelineID]json.RawMessage)
	for rows.Next() {
		var id workspace.PipelineID
		var stored string
		if err := rows.Scan(&id, &stored); err != nil {
			return nil, fmt.Errorf("postgres: scan pipeline credentials: %w", err)
		}
		creds, err := r.decryptCredentials(stored)
		if err != nil {
			return nil, fmt.Errorf("postgres: decrypt credentials for %s: %w", id, err)
		}
		out[id] = creds
	}
	return out, rows.Err()
}

func (r *Repository) UpdatePipeline(ctx context.Context, p *workspace.Pipeline) error {
	encCreds, err := r.encryptCredentials(p.SourceCredentials)
	if err != nil {
		return fmt.Errorf("postgres: encrypt credentials: %w", err)
	}

	res, err := r.DB.ExecContext(ctx,
		`UPDATE pipelines SET name = $1, source_type = $2, source_config = $3, source_credentials = $4, dataset_name = $5, schedule = $6, write_disposition = $7, merge_strategy = $8, enabled = $9, updated_at = $10
		 WHERE id = $11`,
		p.Name, p.SourceType, p.SourceConfig, encCreds,
		p.DatasetName, p.Schedule, p.WriteDisposition, p.MergeStrategy, p.Enabled, p.UpdatedAt, p.ID,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return workspace.ErrPipelineAlreadyExists
		}
		return fmt.Errorf("postgres: update pipeline: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return workspace.ErrPipelineNotFound
	}
	return nil
}

func (r *Repository) DeletePipeline(ctx context.Context, id workspace.PipelineID) error {
	res, err := r.DB.ExecContext(ctx,
		`DELETE FROM pipelines WHERE id = $1`, id,
	)
	if err != nil {
		return fmt.Errorf("postgres: delete pipeline: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return workspace.ErrPipelineNotFound
	}
	return nil
}

// GetEnabledPipelines returns the pipelines the worker should act on for a
// customer: every enabled pipeline, plus any *disabled* pipeline that has a
// pending (manually-triggered) run — so "Run now" on a paused pipeline still
// executes once (TriggerNow/PendingRunID are set), while a paused pipeline
// with no pending run stays dormant.
//
// Runs stuck in "running" (box worker crashed/evicted mid-load) are reaped by
// the periodic FailStuckRunningRuns sweep (see workspace.StuckRunSweeper), not
// here.
func (r *Repository) GetEnabledPipelines(ctx context.Context, customerSlug string) ([]workspace.Pipeline, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT p.id, p.customer_slug, p.name, p.source_type, p.source_config,
		        p.source_credentials, p.dataset_name, p.schedule, p.write_disposition,
		        p.merge_strategy, p.enabled, p.created_at, p.updated_at,
		        COALESCE(pr.id::text, '') AS pending_run_id,
		        lr.last_run_at
		 FROM pipelines p
		 LEFT JOIN LATERAL (
		     SELECT r.id FROM pipeline_runs r
		     WHERE r.pipeline_id = p.id AND r.status = 'pending'
		     ORDER BY r.created_at ASC
		     LIMIT 1
		 ) pr ON true
		 LEFT JOIN LATERAL (
		     SELECT r.created_at AS last_run_at FROM pipeline_runs r
		     WHERE r.pipeline_id = p.id AND r.status = 'success'
		     ORDER BY r.created_at DESC
		     LIMIT 1
		 ) lr ON true
		 WHERE p.customer_slug = $1 AND (p.enabled = true OR pr.id IS NOT NULL)
		 ORDER BY p.created_at`,
		customerSlug,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: get enabled pipelines: %w", err)
	}
	defer rows.Close()

	var pipelines []workspace.Pipeline
	for rows.Next() {
		var p workspace.Pipeline
		var storedCreds string
		var pendingRunID string
		var lastRunAt sql.NullTime
		if err := rows.Scan(&p.ID, &p.CustomerSlug, &p.Name, &p.SourceType, &p.SourceConfig, &storedCreds,
			&p.DatasetName, &p.Schedule, &p.WriteDisposition, &p.MergeStrategy, &p.Enabled, &p.CreatedAt, &p.UpdatedAt,
			&pendingRunID, &lastRunAt); err != nil {
			return nil, fmt.Errorf("postgres: scan enabled pipeline: %w", err)
		}
		p.SourceCredentials, err = r.decryptCredentials(storedCreds)
		if err != nil {
			return nil, fmt.Errorf("postgres: decrypt credentials for pipeline %s: %w", p.ID, err)
		}
		p.PendingRunID = pendingRunID
		p.TriggerNow = pendingRunID != ""
		if lastRunAt.Valid {
			p.LastRunAt = &lastRunAt.Time
		}
		pipelines = append(pipelines, p)
	}
	return pipelines, rows.Err()
}

func (r *Repository) CreatePipelineRun(ctx context.Context, run *workspace.PipelineRun) error {
	if run.CreatedAt.IsZero() {
		run.CreatedAt = time.Now()
	}
	err := r.DB.QueryRowContext(ctx,
		`INSERT INTO pipeline_runs (pipeline_id, status, started_at, completed_at, rows_loaded, error_message, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING id`,
		run.PipelineID, run.Status, run.StartedAt, run.CompletedAt, run.RowsLoaded, run.ErrorMessage, run.CreatedAt,
	).Scan(&run.ID)
	if err != nil {
		return fmt.Errorf("postgres: create pipeline run: %w", err)
	}
	return nil
}

// GetPendingRun returns the oldest run still in "pending" for a pipeline, or
// (nil, nil) when none is queued. Used to keep manual triggers idempotent.
func (r *Repository) GetPendingRun(ctx context.Context, pipelineID workspace.PipelineID) (*workspace.PipelineRun, error) {
	var run workspace.PipelineRun
	var startedAt, completedAt sql.NullTime
	err := r.DB.QueryRowContext(ctx,
		`SELECT id, pipeline_id, status, started_at, completed_at, rows_loaded, error_message, created_at
		 FROM pipeline_runs
		 WHERE pipeline_id = $1 AND status = 'pending'
		 ORDER BY created_at ASC
		 LIMIT 1`,
		pipelineID,
	).Scan(&run.ID, &run.PipelineID, &run.Status, &startedAt, &completedAt, &run.RowsLoaded, &run.ErrorMessage, &run.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: get pending run: %w", err)
	}
	if startedAt.Valid {
		run.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		run.CompletedAt = &completedAt.Time
	}
	return &run, nil
}

func (r *Repository) UpdatePipelineRun(ctx context.Context, run *workspace.PipelineRun) error {
	res, err := r.DB.ExecContext(ctx,
		`UPDATE pipeline_runs
		 SET status = $1, started_at = COALESCE($2, started_at),
		     completed_at = COALESCE($3, completed_at),
		     rows_loaded = $4, error_message = $5
		 WHERE id = $6 AND pipeline_id = $7`,
		run.Status, run.StartedAt, run.CompletedAt, run.RowsLoaded, run.ErrorMessage, run.ID, run.PipelineID,
	)
	if err != nil {
		return fmt.Errorf("postgres: update pipeline run: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return workspace.ErrPipelineRunNotFound
	}
	return nil
}

// FailStuckRunningRuns marks pipeline runs orphaned in "running" past
// olderThan as "failed". A run is "running" only between the box dlt-worker
// reporting it started (which sets started_at) and reporting a terminal result;
// a worker killed mid-load never reports terminal, so the row — and the
// Console's last_run_status badge — sticks on "running" forever. Staleness is
// measured from started_at, falling back to created_at for the rare row whose
// started_at was never stamped. The cutoff is computed in Go so the interval
// never has to round-trip through the driver as a Postgres interval literal.
// A non-empty customerSlug scopes the sweep to that workspace's pipelines
// (pipeline_runs carries no slug of its own, so the scope goes through the
// pipelines join); empty sweeps all workspaces.
// Returns the number of rows swept. See workspace.StuckRunSweeper.
func (r *Repository) FailStuckRunningRuns(ctx context.Context, customerSlug string, olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(-olderThan)
	res, err := r.DB.ExecContext(ctx,
		`UPDATE pipeline_runs r
		    SET status = 'failed',
		        completed_at = now(),
		        error_message = 'failed by stuck-run sweep: no terminal report within timeout (box worker likely OOM-killed or evicted mid-load)'
		   FROM pipelines p
		  WHERE p.id = r.pipeline_id
		    AND r.status = 'running'
		    AND COALESCE(r.started_at, r.created_at) < $1
		    AND ($2 = '' OR p.customer_slug = $2)`,
		cutoff, customerSlug,
	)
	if err != nil {
		return 0, fmt.Errorf("postgres: fail stuck running runs: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (r *Repository) ListRecentRuns(ctx context.Context, pipelineID workspace.PipelineID, limit int) ([]workspace.PipelineRun, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT id, pipeline_id, status, started_at, completed_at, rows_loaded, error_message, created_at
		 FROM pipeline_runs WHERE pipeline_id = $1 ORDER BY created_at DESC LIMIT $2`,
		pipelineID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: list recent runs: %w", err)
	}
	defer rows.Close()

	var runs []workspace.PipelineRun
	for rows.Next() {
		var r workspace.PipelineRun
		var startedAt, completedAt sql.NullTime
		if err := rows.Scan(&r.ID, &r.PipelineID, &r.Status, &startedAt, &completedAt, &r.RowsLoaded, &r.ErrorMessage, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("postgres: scan pipeline run: %w", err)
		}
		if startedAt.Valid {
			r.StartedAt = &startedAt.Time
		}
		if completedAt.Valid {
			r.CompletedAt = &completedAt.Time
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

// isUniqueViolation checks if a PostgreSQL error is a unique constraint violation (23505).
func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "23505")
}

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/fairtier/workspace-api/workspace"
)

func (r *Repository) CreateTransformation(ctx context.Context, t *workspace.Transformation) error {
	encCreds, err := r.encryptCredentials(t.GitCredentials)
	if err != nil {
		return fmt.Errorf("postgres: encrypt credentials: %w", err)
	}

	err = r.DB.QueryRowContext(ctx,
		`INSERT INTO transformations (customer_slug, name, repo_url, repo_ref, git_credentials, schedule, trigger_after_pipeline_id, dbt_selector, enabled, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		 RETURNING id`,
		t.CustomerSlug, t.Name, t.RepoURL, t.RepoRef, encCreds,
		t.Schedule, nullUUID(string(t.TriggerAfterPipelineID)), t.DBTSelector, t.Enabled, t.CreatedAt, t.UpdatedAt,
	).Scan(&t.ID)
	if err != nil {
		if isUniqueViolation(err) {
			return workspace.ErrTransformationAlreadyExists
		}
		return fmt.Errorf("postgres: create transformation: %w", err)
	}
	return nil
}

func (r *Repository) GetTransformation(ctx context.Context, id workspace.TransformationID) (*workspace.Transformation, error) {
	var t workspace.Transformation
	var storedCreds string
	var triggerPipeline sql.NullString
	err := r.DB.QueryRowContext(ctx,
		`SELECT id, customer_slug, name, repo_url, repo_ref, git_credentials, schedule, trigger_after_pipeline_id, dbt_selector, enabled, created_at, updated_at
		 FROM transformations WHERE id = $1`,
		id,
	).Scan(&t.ID, &t.CustomerSlug, &t.Name, &t.RepoURL, &t.RepoRef, &storedCreds,
		&t.Schedule, &triggerPipeline, &t.DBTSelector, &t.Enabled, &t.CreatedAt, &t.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, workspace.ErrTransformationNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: get transformation: %w", err)
	}

	t.TriggerAfterPipelineID = workspace.PipelineID(triggerPipeline.String)
	t.GitCredentials, err = r.decryptCredentials(storedCreds)
	if err != nil {
		return nil, fmt.Errorf("postgres: decrypt credentials: %w", err)
	}
	return &t, nil
}

func (r *Repository) ListTransformationsByCustomer(ctx context.Context, customerSlug string) ([]workspace.Transformation, error) {
	// LEFT JOIN LATERAL the most recent run (any status) per transformation so
	// the list view can show real last-run status/time without N+1 lookups.
	rows, err := r.DB.QueryContext(ctx,
		`SELECT t.id, t.customer_slug, t.name, t.repo_url, t.repo_ref, t.schedule,
		        t.trigger_after_pipeline_id, t.dbt_selector, t.enabled, t.created_at, t.updated_at,
		        lr.last_run_time, lr.last_run_status
		 FROM transformations t
		 LEFT JOIN LATERAL (
		     SELECT COALESCE(started_at, created_at) AS last_run_time, status AS last_run_status
		     FROM transformation_runs
		     WHERE transformation_id = t.id
		     ORDER BY created_at DESC
		     LIMIT 1
		 ) lr ON true
		 WHERE t.customer_slug = $1 ORDER BY t.created_at`,
		customerSlug,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: list transformations: %w", err)
	}
	defer rows.Close()

	var transformations []workspace.Transformation
	for rows.Next() {
		var t workspace.Transformation
		var triggerPipeline sql.NullString
		var lastRunTime sql.NullTime
		var lastRunStatus sql.NullString
		if err := rows.Scan(&t.ID, &t.CustomerSlug, &t.Name, &t.RepoURL, &t.RepoRef, &t.Schedule,
			&triggerPipeline, &t.DBTSelector, &t.Enabled, &t.CreatedAt, &t.UpdatedAt,
			&lastRunTime, &lastRunStatus); err != nil {
			return nil, fmt.Errorf("postgres: scan transformation: %w", err)
		}
		t.TriggerAfterPipelineID = workspace.PipelineID(triggerPipeline.String)
		if lastRunTime.Valid {
			lt := lastRunTime.Time
			t.LastRunTime = &lt
		}
		t.LastRunStatus = lastRunStatus.String
		transformations = append(transformations, t)
	}
	return transformations, rows.Err()
}

func (r *Repository) UpdateTransformation(ctx context.Context, t *workspace.Transformation) error {
	encCreds, err := r.encryptCredentials(t.GitCredentials)
	if err != nil {
		return fmt.Errorf("postgres: encrypt credentials: %w", err)
	}

	res, err := r.DB.ExecContext(ctx,
		`UPDATE transformations SET name = $1, repo_url = $2, repo_ref = $3, git_credentials = $4, schedule = $5, trigger_after_pipeline_id = $6, dbt_selector = $7, enabled = $8, updated_at = $9
		 WHERE id = $10`,
		t.Name, t.RepoURL, t.RepoRef, encCreds,
		t.Schedule, nullUUID(string(t.TriggerAfterPipelineID)), t.DBTSelector, t.Enabled, t.UpdatedAt, t.ID,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return workspace.ErrTransformationAlreadyExists
		}
		return fmt.Errorf("postgres: update transformation: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return workspace.ErrTransformationNotFound
	}
	return nil
}

func (r *Repository) DeleteTransformation(ctx context.Context, id workspace.TransformationID) error {
	res, err := r.DB.ExecContext(ctx,
		`DELETE FROM transformations WHERE id = $1`, id,
	)
	if err != nil {
		return fmt.Errorf("postgres: delete transformation: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return workspace.ErrTransformationNotFound
	}
	return nil
}

func (r *Repository) GetEnabledTransformations(ctx context.Context, customerSlug string) ([]workspace.Transformation, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT t.id, t.customer_slug, t.name, t.repo_url, t.repo_ref, t.git_credentials,
		        t.schedule, t.trigger_after_pipeline_id, t.dbt_selector, t.enabled, t.created_at, t.updated_at,
		        COALESCE(pr.id::text, '') AS pending_run_id,
		        lr.last_run_at
		 FROM transformations t
		 LEFT JOIN LATERAL (
		     SELECT r.id FROM transformation_runs r
		     WHERE r.transformation_id = t.id AND r.status = 'pending'
		     ORDER BY r.created_at ASC
		     LIMIT 1
		 ) pr ON true
		 LEFT JOIN LATERAL (
		     SELECT r.created_at AS last_run_at FROM transformation_runs r
		     WHERE r.transformation_id = t.id AND r.status = 'success'
		     ORDER BY r.created_at DESC
		     LIMIT 1
		 ) lr ON true
		 WHERE t.customer_slug = $1 AND t.enabled = true
		 ORDER BY t.created_at`,
		customerSlug,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: get enabled transformations: %w", err)
	}
	defer rows.Close()

	var transformations []workspace.Transformation
	for rows.Next() {
		var t workspace.Transformation
		var storedCreds string
		var triggerPipeline sql.NullString
		var pendingRunID string
		var lastRunAt sql.NullTime
		if err := rows.Scan(&t.ID, &t.CustomerSlug, &t.Name, &t.RepoURL, &t.RepoRef, &storedCreds,
			&t.Schedule, &triggerPipeline, &t.DBTSelector, &t.Enabled, &t.CreatedAt, &t.UpdatedAt,
			&pendingRunID, &lastRunAt); err != nil {
			return nil, fmt.Errorf("postgres: scan enabled transformation: %w", err)
		}
		t.TriggerAfterPipelineID = workspace.PipelineID(triggerPipeline.String)
		t.GitCredentials, err = r.decryptCredentials(storedCreds)
		if err != nil {
			return nil, fmt.Errorf("postgres: decrypt credentials for transformation %s: %w", t.ID, err)
		}
		t.PendingRunID = pendingRunID
		t.TriggerNow = pendingRunID != ""
		if lastRunAt.Valid {
			t.LastRunAt = &lastRunAt.Time
		}
		transformations = append(transformations, t)
	}
	return transformations, rows.Err()
}

func (r *Repository) CreateTransformationRun(ctx context.Context, run *workspace.TransformationRun) error {
	if run.CreatedAt.IsZero() {
		run.CreatedAt = time.Now()
	}
	err := r.DB.QueryRowContext(ctx,
		`INSERT INTO transformation_runs (transformation_id, status, started_at, completed_at, commit_sha, models_total, models_failed, tests_total, tests_failed, model_results, error_message, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		 RETURNING id`,
		run.TransformationID, run.Status, run.StartedAt, run.CompletedAt, run.CommitSHA,
		run.ModelsTotal, run.ModelsFailed, run.TestsTotal, run.TestsFailed,
		jsonArrayOrEmpty(run.ModelResults), run.ErrorMessage, run.CreatedAt,
	).Scan(&run.ID)
	if err != nil {
		return fmt.Errorf("postgres: create transformation run: %w", err)
	}
	return nil
}

func (r *Repository) UpdateTransformationRun(ctx context.Context, run *workspace.TransformationRun) error {
	res, err := r.DB.ExecContext(ctx,
		`UPDATE transformation_runs
		 SET status = $1, started_at = COALESCE($2, started_at),
		     completed_at = COALESCE($3, completed_at),
		     commit_sha = $4, models_total = $5, models_failed = $6,
		     tests_total = $7, tests_failed = $8, model_results = $9,
		     error_message = $10
		 WHERE id = $11 AND transformation_id = $12`,
		run.Status, run.StartedAt, run.CompletedAt, run.CommitSHA,
		run.ModelsTotal, run.ModelsFailed, run.TestsTotal, run.TestsFailed,
		jsonArrayOrEmpty(run.ModelResults), run.ErrorMessage, run.ID, run.TransformationID,
	)
	if err != nil {
		return fmt.Errorf("postgres: update transformation run: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return workspace.ErrTransformationRunNotFound
	}
	return nil
}

func (r *Repository) ListRecentTransformationRuns(ctx context.Context, id workspace.TransformationID, limit int) ([]workspace.TransformationRun, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT id, transformation_id, status, started_at, completed_at, commit_sha, models_total, models_failed, tests_total, tests_failed, model_results, error_message, created_at
		 FROM transformation_runs WHERE transformation_id = $1 ORDER BY created_at DESC LIMIT $2`,
		id, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: list recent transformation runs: %w", err)
	}
	defer rows.Close()

	var runs []workspace.TransformationRun
	for rows.Next() {
		var run workspace.TransformationRun
		var startedAt, completedAt sql.NullTime
		var modelResults []byte
		if err := rows.Scan(&run.ID, &run.TransformationID, &run.Status, &startedAt, &completedAt,
			&run.CommitSHA, &run.ModelsTotal, &run.ModelsFailed, &run.TestsTotal, &run.TestsFailed,
			&modelResults, &run.ErrorMessage, &run.CreatedAt); err != nil {
			return nil, fmt.Errorf("postgres: scan transformation run: %w", err)
		}
		if startedAt.Valid {
			run.StartedAt = &startedAt.Time
		}
		if completedAt.Valid {
			run.CompletedAt = &completedAt.Time
		}
		run.ModelResults = modelResults
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

// nullUUID maps an empty string to SQL NULL so it can be inserted into a
// nullable UUID column.
func nullUUID(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// jsonArrayOrEmpty returns the raw JSON bytes, defaulting to "[]" so the
// JSONB column never receives an empty string.
func jsonArrayOrEmpty(raw []byte) []byte {
	if len(raw) == 0 {
		return []byte("[]")
	}
	return raw
}

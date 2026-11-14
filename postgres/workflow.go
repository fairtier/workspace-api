package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/fairtier/workspace-api/workspace"
)

// queryer is what both *sql.DB and *sql.Tx offer, so step loading works
// inside and outside a transaction.
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (r *Repository) CreateWorkflow(ctx context.Context, w *workspace.Workflow) error {
	return r.inTx(ctx, func(tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx,
			`INSERT INTO workflows (customer_slug, name, description, schedule, enabled, created_at, updated_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)
			 RETURNING id`,
			w.CustomerSlug, w.Name, w.Description, w.Schedule, w.Enabled, w.CreatedAt, w.UpdatedAt,
		).Scan(&w.ID)
		if err != nil {
			if isUniqueViolation(err) {
				return workspace.ErrWorkflowAlreadyExists
			}
			return fmt.Errorf("postgres: create workflow: %w", err)
		}
		return insertWorkflowSteps(ctx, tx, w.ID, w.Steps)
	})
}

func (r *Repository) UpdateWorkflow(ctx context.Context, w *workspace.Workflow) error {
	return r.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE workflows SET name = $1, description = $2, schedule = $3, enabled = $4, updated_at = $5
			 WHERE id = $6`,
			w.Name, w.Description, w.Schedule, w.Enabled, w.UpdatedAt, w.ID,
		)
		if err != nil {
			if isUniqueViolation(err) {
				return workspace.ErrWorkflowAlreadyExists
			}
			return fmt.Errorf("postgres: update workflow: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return workspace.ErrWorkflowNotFound
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM workflow_steps WHERE workflow_id = $1`, w.ID); err != nil {
			return fmt.Errorf("postgres: clear workflow steps: %w", err)
		}
		return insertWorkflowSteps(ctx, tx, w.ID, w.Steps)
	})
}

func insertWorkflowSteps(ctx context.Context, tx *sql.Tx, id workspace.WorkflowID, steps []workspace.WorkflowStep) error {
	for _, s := range steps {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO workflow_steps (workflow_id, position, kind, pipeline_id, transformation_id)
			 VALUES ($1, $2, $3, $4, $5)`,
			id, s.Position, string(s.Kind), nullUUID(string(s.PipelineID)), nullUUID(string(s.TransformationID)),
		)
		if err != nil {
			return fmt.Errorf("postgres: insert workflow step %d: %w", s.Position, err)
		}
	}
	return nil
}

func (r *Repository) DeleteWorkflow(ctx context.Context, id workspace.WorkflowID) error {
	res, err := r.DB.ExecContext(ctx, `DELETE FROM workflows WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("postgres: delete workflow: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return workspace.ErrWorkflowNotFound
	}
	return nil
}

func (r *Repository) GetWorkflow(ctx context.Context, id workspace.WorkflowID) (*workspace.Workflow, error) {
	var w workspace.Workflow
	err := r.DB.QueryRowContext(ctx,
		`SELECT id, customer_slug, name, description, schedule, enabled, created_at, updated_at
		 FROM workflows WHERE id = $1`, id,
	).Scan(&w.ID, &w.CustomerSlug, &w.Name, &w.Description, &w.Schedule, &w.Enabled, &w.CreatedAt, &w.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, workspace.ErrWorkflowNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: get workflow: %w", err)
	}
	steps, err := loadWorkflowSteps(ctx, r.DB, []string{string(w.ID)})
	if err != nil {
		return nil, err
	}
	w.Steps = steps[w.ID]
	return &w, nil
}

func (r *Repository) ListWorkflowsByCustomer(ctx context.Context, customerSlug string) ([]workspace.Workflow, error) {
	// The most recent run of any status, as ListTransformationsByCustomer.
	return r.queryWorkflows(ctx,
		`SELECT w.id, w.customer_slug, w.name, w.description, w.schedule, w.enabled, w.created_at, w.updated_at,
		        '' AS pending_run_id, lr.last_run_at, lr.last_run_status
		 FROM workflows w
		 LEFT JOIN LATERAL (
		     SELECT COALESCE(started_at, created_at) AS last_run_at, status AS last_run_status
		     FROM workflow_runs
		     WHERE workflow_id = w.id
		     ORDER BY created_at DESC, id DESC
		     LIMIT 1
		 ) lr ON true
		 WHERE w.customer_slug = $1
		 ORDER BY w.created_at`,
		customerSlug)
}

func (r *Repository) GetEnabledWorkflows(ctx context.Context, customerSlug string) ([]workspace.Workflow, error) {
	// Served: enabled workflows, plus a disabled one with a pending run (a
	// Run-now outranks enabled, as for pipelines). The last-run baseline
	// skips pending rows: a queued run has not started anything yet.
	return r.queryWorkflows(ctx,
		`SELECT w.id, w.customer_slug, w.name, w.description, w.schedule, w.enabled, w.created_at, w.updated_at,
		        COALESCE(pr.id::text, '') AS pending_run_id, lr.last_run_at, lr.last_run_status
		 FROM workflows w
		 LEFT JOIN LATERAL (
		     SELECT r.id FROM workflow_runs r
		     WHERE r.workflow_id = w.id AND r.status = 'pending'
		     ORDER BY r.created_at ASC
		     LIMIT 1
		 ) pr ON true
		 LEFT JOIN LATERAL (
		     SELECT COALESCE(r.started_at, r.created_at) AS last_run_at, r.status AS last_run_status
		     FROM workflow_runs r
		     WHERE r.workflow_id = w.id AND r.status <> 'pending'
		     ORDER BY COALESCE(r.started_at, r.created_at) DESC
		     LIMIT 1
		 ) lr ON true
		 WHERE w.customer_slug = $1 AND (w.enabled = true OR pr.id IS NOT NULL)
		 ORDER BY w.created_at`,
		customerSlug)
}

// queryWorkflows runs a workflow list query whose columns are the workflow
// row plus pending_run_id, last_run_at, last_run_status, then attaches steps.
func (r *Repository) queryWorkflows(ctx context.Context, query string, args ...any) ([]workspace.Workflow, error) {
	rows, err := r.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: list workflows: %w", err)
	}
	defer rows.Close()

	var workflows []workspace.Workflow
	var ids []string
	for rows.Next() {
		w, err := scanWorkflowListRow(rows)
		if err != nil {
			return nil, err
		}
		workflows = append(workflows, w)
		ids = append(ids, string(w.ID))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list workflows: %w", err)
	}
	if len(ids) == 0 {
		return workflows, nil
	}

	steps, err := loadWorkflowSteps(ctx, r.DB, ids)
	if err != nil {
		return nil, err
	}
	for i := range workflows {
		workflows[i].Steps = steps[workflows[i].ID]
	}
	return workflows, nil
}

func scanWorkflowListRow(rows *sql.Rows) (workspace.Workflow, error) {
	var w workspace.Workflow
	var lastRunAt sql.NullTime
	var lastRunStatus sql.NullString
	if err := rows.Scan(&w.ID, &w.CustomerSlug, &w.Name, &w.Description, &w.Schedule, &w.Enabled,
		&w.CreatedAt, &w.UpdatedAt, &w.PendingRunID, &lastRunAt, &lastRunStatus); err != nil {
		return w, fmt.Errorf("postgres: scan workflow: %w", err)
	}
	w.TriggerNow = w.PendingRunID != ""
	if lastRunAt.Valid {
		t := lastRunAt.Time
		w.LastRunAt = &t
	}
	w.LastRunStatus = lastRunStatus.String
	return w, nil
}

// loadWorkflowSteps returns the steps of the given workflows keyed by id, in
// position order, with the referenced row's current name and enabled flag.
func loadWorkflowSteps(ctx context.Context, q queryer, ids []string) (map[workspace.WorkflowID][]workspace.WorkflowStep, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT s.workflow_id, s.position, s.kind,
		        COALESCE(s.pipeline_id::text, ''), COALESCE(s.transformation_id::text, ''),
		        COALESCE(p.name, t.name, ''), COALESCE(p.enabled, t.enabled, false)
		 FROM workflow_steps s
		 LEFT JOIN pipelines p ON p.id = s.pipeline_id
		 LEFT JOIN transformations t ON t.id = s.transformation_id
		 WHERE s.workflow_id = ANY($1::uuid[])
		 ORDER BY s.workflow_id, s.position`,
		ids,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: load workflow steps: %w", err)
	}
	defer rows.Close()

	out := make(map[workspace.WorkflowID][]workspace.WorkflowStep, len(ids))
	for rows.Next() {
		var id workspace.WorkflowID
		var s workspace.WorkflowStep
		var kind, pipelineID, transformationID string
		if err := rows.Scan(&id, &s.Position, &kind, &pipelineID, &transformationID, &s.Name, &s.RefEnabled); err != nil {
			return nil, fmt.Errorf("postgres: scan workflow step: %w", err)
		}
		s.Kind = workspace.WorkflowStepKind(kind)
		s.PipelineID = workspace.PipelineID(pipelineID)
		s.TransformationID = workspace.TransformationID(transformationID)
		out[id] = append(out[id], s)
	}
	return out, rows.Err()
}

// --- Runs ---

func (r *Repository) GetPendingWorkflowRun(ctx context.Context, id workspace.WorkflowID) (*workspace.WorkflowRun, error) {
	runs, err := r.queryWorkflowRuns(ctx,
		`SELECT id, workflow_id, status, trigger, started_at, completed_at, error_message, created_at
		 FROM workflow_runs WHERE workflow_id = $1 AND status = 'pending'
		 ORDER BY created_at ASC LIMIT 1`, id)
	if err != nil || len(runs) == 0 {
		return nil, err
	}
	return &runs[0], nil
}

func (r *Repository) CreateWorkflowRun(ctx context.Context, run *workspace.WorkflowRun) error {
	if run.CreatedAt.IsZero() {
		run.CreatedAt = time.Now()
	}
	err := r.DB.QueryRowContext(ctx,
		`INSERT INTO workflow_runs (id, workflow_id, status, trigger, started_at, completed_at, error_message, created_at)
		 VALUES (COALESCE($1::uuid, gen_random_uuid()), $2, $3, COALESCE(NULLIF($4, ''), 'schedule'), $5, $6, $7, $8)
		 RETURNING id, trigger`,
		nullUUID(run.ID), run.WorkflowID, run.Status, run.Trigger, run.StartedAt, run.CompletedAt, run.ErrorMessage, run.CreatedAt,
	).Scan(&run.ID, &run.Trigger)
	if err != nil {
		return fmt.Errorf("postgres: create workflow run: %w", err)
	}
	return nil
}

func (r *Repository) UpsertWorkflowRun(ctx context.Context, run *workspace.WorkflowRun) (string, error) {
	var previous string
	err := r.inTx(ctx, func(tx *sql.Tx) error {
		var err error
		previous, err = upsertWorkflowRunRow(ctx, tx, run)
		if err != nil {
			return err
		}
		for _, sr := range run.StepRuns {
			if err := upsertWorkflowStepRun(ctx, tx, run.ID, sr); err != nil {
				return err
			}
		}
		return nil
	})
	return previous, err
}

// upsertWorkflowRunRow writes the run row and returns its previous status
// ("" when the row is new). The CTE reads the row as it was before this
// statement; the conflict WHERE refuses to move a run id to another workflow.
func upsertWorkflowRunRow(ctx context.Context, tx *sql.Tx, run *workspace.WorkflowRun) (string, error) {
	var previous sql.NullString
	err := tx.QueryRowContext(ctx,
		`WITH prev AS (SELECT status FROM workflow_runs WHERE id = $1)
		 INSERT INTO workflow_runs (id, workflow_id, status, trigger, started_at, completed_at, error_message, created_at)
		 VALUES ($1, $2, $3, COALESCE(NULLIF($4, ''), 'schedule'), $5, $6, $7, now())
		 ON CONFLICT (id) DO UPDATE SET
		     status = EXCLUDED.status,
		     trigger = CASE WHEN $4 = '' THEN workflow_runs.trigger ELSE EXCLUDED.trigger END,
		     started_at = COALESCE(EXCLUDED.started_at, workflow_runs.started_at),
		     completed_at = EXCLUDED.completed_at,
		     error_message = EXCLUDED.error_message
		 WHERE workflow_runs.workflow_id = EXCLUDED.workflow_id
		 RETURNING (SELECT status FROM prev)`,
		run.ID, run.WorkflowID, run.Status, run.Trigger, run.StartedAt, run.CompletedAt, run.ErrorMessage,
	).Scan(&previous)
	if errors.Is(err, sql.ErrNoRows) {
		return "", workspace.ErrWorkflowRunNotFound
	}
	if err != nil {
		return "", fmt.Errorf("postgres: upsert workflow run: %w", err)
	}
	return previous.String, nil
}

func upsertWorkflowStepRun(ctx context.Context, tx *sql.Tx, runID string, sr workspace.WorkflowStepRun) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO workflow_step_runs (workflow_run_id, position, kind, ref_id, name, status,
		     pipeline_run_id, transformation_run_id, started_at, completed_at, error_message)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		 ON CONFLICT (workflow_run_id, position) DO UPDATE SET
		     kind = EXCLUDED.kind,
		     ref_id = EXCLUDED.ref_id,
		     name = EXCLUDED.name,
		     status = EXCLUDED.status,
		     pipeline_run_id = COALESCE(EXCLUDED.pipeline_run_id, workflow_step_runs.pipeline_run_id),
		     transformation_run_id = COALESCE(EXCLUDED.transformation_run_id, workflow_step_runs.transformation_run_id),
		     started_at = COALESCE(EXCLUDED.started_at, workflow_step_runs.started_at),
		     completed_at = EXCLUDED.completed_at,
		     error_message = EXCLUDED.error_message`,
		runID, sr.Position, string(sr.Kind), sr.RefID, sr.Name, sr.Status,
		nullUUID(sr.PipelineRunID), nullUUID(sr.TransformationRunID), sr.StartedAt, sr.CompletedAt, sr.ErrorMessage,
	)
	if err != nil {
		return fmt.Errorf("postgres: upsert workflow step run %d: %w", sr.Position, err)
	}
	return nil
}

func (r *Repository) ListWorkflowRuns(ctx context.Context, id workspace.WorkflowID, limit int, after *workspace.WorkflowRunCursor) ([]workspace.WorkflowRun, error) {
	var afterAt *time.Time
	var afterID any
	if after != nil {
		afterAt, afterID = &after.CreatedAt, after.ID
	}
	runs, err := r.queryWorkflowRuns(ctx,
		`SELECT id, workflow_id, status, trigger, started_at, completed_at, error_message, created_at
		 FROM workflow_runs
		 WHERE workflow_id = $1
		   AND ($2::timestamptz IS NULL OR (created_at, id) < ($2::timestamptz, $3::uuid))
		 ORDER BY created_at DESC, id DESC
		 LIMIT $4`,
		id, afterAt, afterID, limit)
	if err != nil || len(runs) == 0 {
		return runs, err
	}
	return runs, r.attachStepRuns(ctx, runs)
}

func (r *Repository) queryWorkflowRuns(ctx context.Context, query string, args ...any) ([]workspace.WorkflowRun, error) {
	rows, err := r.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: list workflow runs: %w", err)
	}
	defer rows.Close()

	var runs []workspace.WorkflowRun
	for rows.Next() {
		var run workspace.WorkflowRun
		var startedAt, completedAt sql.NullTime
		if err := rows.Scan(&run.ID, &run.WorkflowID, &run.Status, &run.Trigger, &startedAt, &completedAt,
			&run.ErrorMessage, &run.CreatedAt); err != nil {
			return nil, fmt.Errorf("postgres: scan workflow run: %w", err)
		}
		run.StartedAt = timePtr(startedAt)
		run.CompletedAt = timePtr(completedAt)
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

// attachStepRuns loads the step runs of runs in one query.
func (r *Repository) attachStepRuns(ctx context.Context, runs []workspace.WorkflowRun) error {
	ids := make([]string, len(runs))
	index := make(map[string]int, len(runs))
	for i, run := range runs {
		ids[i] = run.ID
		index[run.ID] = i
	}
	rows, err := r.DB.QueryContext(ctx,
		`SELECT workflow_run_id, position, kind, ref_id, name, status,
		        COALESCE(pipeline_run_id::text, ''), COALESCE(transformation_run_id::text, ''),
		        started_at, completed_at, error_message
		 FROM workflow_step_runs
		 WHERE workflow_run_id = ANY($1::uuid[])
		 ORDER BY workflow_run_id, position`,
		ids,
	)
	if err != nil {
		return fmt.Errorf("postgres: load workflow step runs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var runID, kind string
		var sr workspace.WorkflowStepRun
		var startedAt, completedAt sql.NullTime
		if err := rows.Scan(&runID, &sr.Position, &kind, &sr.RefID, &sr.Name, &sr.Status,
			&sr.PipelineRunID, &sr.TransformationRunID, &startedAt, &completedAt, &sr.ErrorMessage); err != nil {
			return fmt.Errorf("postgres: scan workflow step run: %w", err)
		}
		sr.Kind = workspace.WorkflowStepKind(kind)
		sr.StartedAt = timePtr(startedAt)
		sr.CompletedAt = timePtr(completedAt)
		i := index[runID]
		runs[i].StepRuns = append(runs[i].StepRuns, sr)
	}
	return rows.Err()
}

func timePtr(t sql.NullTime) *time.Time {
	if !t.Valid {
		return nil
	}
	v := t.Time
	return &v
}

// inTx runs fn in a transaction, committing on nil and rolling back
// otherwise.
func (r *Repository) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres: begin: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("postgres: commit: %w", err)
	}
	return nil
}

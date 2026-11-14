package postgres

import (
	"context"
	"fmt"

	"github.com/fairtier/workspace-api/workspace"
)

// UpsertWorkflowDefinitionRender records the path + blob sha of a
// just-written (or just-adopted) workflows/<slug>.yaml file. Rows die with
// their workflow (FK cascade).
func (r *Repository) UpsertWorkflowDefinitionRender(ctx context.Context, render *workspace.WorkflowDefinitionRender) error {
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO workflow_definition_renders (workflow_id, path, blob_sha, updated_at)
		 VALUES ($1, $2, $3, now())
		 ON CONFLICT (workflow_id)
		 DO UPDATE SET path = EXCLUDED.path,
		               blob_sha = EXCLUDED.blob_sha,
		               refused_blob_sha = '', updated_at = now()`,
		render.WorkflowID, render.Path, render.BlobSHA,
	)
	if err != nil {
		return fmt.Errorf("postgres: upsert workflow definition render: %w", err)
	}
	return nil
}

// GetWorkflowDefinitionRenders returns definition-render bookkeeping for all
// of a customer's workflows, keyed by workflow id.
func (r *Repository) GetWorkflowDefinitionRenders(ctx context.Context, customerSlug string) (map[workspace.WorkflowID]workspace.WorkflowDefinitionRender, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT r.workflow_id, r.path, r.blob_sha, r.refused_blob_sha
		 FROM workflow_definition_renders r
		 JOIN workflows w ON w.id = r.workflow_id
		 WHERE w.customer_slug = $1`,
		customerSlug,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: get workflow definition renders: %w", err)
	}
	defer rows.Close()

	out := make(map[workspace.WorkflowID]workspace.WorkflowDefinitionRender)
	for rows.Next() {
		var render workspace.WorkflowDefinitionRender
		if err := rows.Scan(&render.WorkflowID, &render.Path, &render.BlobSHA, &render.RefusedBlobSHA); err != nil {
			return nil, fmt.Errorf("postgres: scan workflow definition render: %w", err)
		}
		out[render.WorkflowID] = render
	}
	return out, rows.Err()
}

// MarkWorkflowDefinitionRefused stamps the refused blob sha without touching
// the last-render bookkeeping (adopt pass, once-per-commit notification
// guard).
func (r *Repository) MarkWorkflowDefinitionRefused(ctx context.Context, id workspace.WorkflowID, refusedSHA string) error {
	_, err := r.DB.ExecContext(ctx,
		`UPDATE workflow_definition_renders
		 SET refused_blob_sha = $2, updated_at = now()
		 WHERE workflow_id = $1`,
		id, refusedSHA,
	)
	if err != nil {
		return fmt.Errorf("postgres: mark workflow definition refused: %w", err)
	}
	return nil
}

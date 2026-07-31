package postgres

import (
	"context"
	"fmt"

	"github.com/fairtier/workspace-api/workspace"
)

// UpsertPipelineDefinitionRender records the path + blob sha of a
// just-written pipelines/<slug>.yaml file. Rows die with their pipeline
// (FK cascade).
func (r *Repository) UpsertPipelineDefinitionRender(ctx context.Context, render *workspace.PipelineDefinitionRender) error {
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO pipeline_definition_renders (pipeline_id, path, blob_sha, updated_at)
		 VALUES ($1, $2, $3, now())
		 ON CONFLICT (pipeline_id)
		 DO UPDATE SET path = EXCLUDED.path,
		               blob_sha = EXCLUDED.blob_sha,
		               refused_blob_sha = '', updated_at = now()`,
		render.PipelineID, render.Path, render.BlobSHA,
	)
	if err != nil {
		return fmt.Errorf("postgres: upsert definition render: %w", err)
	}
	return nil
}

// GetPipelineDefinitionRenders returns definition-render bookkeeping for all
// of a customer's pipelines, keyed by pipeline id.
func (r *Repository) GetPipelineDefinitionRenders(ctx context.Context, customerSlug string) (map[workspace.PipelineID]workspace.PipelineDefinitionRender, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT r.pipeline_id, r.path, r.blob_sha, r.refused_blob_sha
		 FROM pipeline_definition_renders r
		 JOIN pipelines p ON p.id = r.pipeline_id
		 WHERE p.customer_slug = $1`,
		customerSlug,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: get definition renders: %w", err)
	}
	defer rows.Close()

	out := make(map[workspace.PipelineID]workspace.PipelineDefinitionRender)
	for rows.Next() {
		var render workspace.PipelineDefinitionRender
		if err := rows.Scan(&render.PipelineID, &render.Path, &render.BlobSHA, &render.RefusedBlobSHA); err != nil {
			return nil, fmt.Errorf("postgres: scan definition render: %w", err)
		}
		out[render.PipelineID] = render
	}
	return out, rows.Err()
}

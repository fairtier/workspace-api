package postgres

import (
	"context"
	"fmt"

	"github.com/fairtier/workspace-api/workspace"
)

// UpsertTransformationDefinitionRender records the path + blob sha of a
// just-written transformations/<slug>.yaml file. Rows die with their
// transformation (FK cascade).
func (r *Repository) UpsertTransformationDefinitionRender(ctx context.Context, render *workspace.TransformationDefinitionRender) error {
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO transformation_definition_renders (transformation_id, path, blob_sha, updated_at)
		 VALUES ($1, $2, $3, now())
		 ON CONFLICT (transformation_id)
		 DO UPDATE SET path = EXCLUDED.path,
		               blob_sha = EXCLUDED.blob_sha,
		               refused_blob_sha = '', updated_at = now()`,
		render.TransformationID, render.Path, render.BlobSHA,
	)
	if err != nil {
		return fmt.Errorf("postgres: upsert transformation definition render: %w", err)
	}
	return nil
}

// GetTransformationDefinitionRenders returns definition-render bookkeeping
// for all of a customer's transformations, keyed by transformation id.
func (r *Repository) GetTransformationDefinitionRenders(ctx context.Context, customerSlug string) (map[workspace.TransformationID]workspace.TransformationDefinitionRender, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT r.transformation_id, r.path, r.blob_sha, r.refused_blob_sha
		 FROM transformation_definition_renders r
		 JOIN transformations t ON t.id = r.transformation_id
		 WHERE t.customer_slug = $1`,
		customerSlug,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: get transformation definition renders: %w", err)
	}
	defer rows.Close()

	out := make(map[workspace.TransformationID]workspace.TransformationDefinitionRender)
	for rows.Next() {
		var render workspace.TransformationDefinitionRender
		if err := rows.Scan(&render.TransformationID, &render.Path, &render.BlobSHA, &render.RefusedBlobSHA); err != nil {
			return nil, fmt.Errorf("postgres: scan transformation definition render: %w", err)
		}
		out[render.TransformationID] = render
	}
	return out, rows.Err()
}

// MarkTransformationDefinitionRefused stamps the refused blob sha without
// touching the last-render bookkeeping (adopt pass, once-per-commit
// notification guard).
func (r *Repository) MarkTransformationDefinitionRefused(ctx context.Context, id workspace.TransformationID, refusedSHA string) error {
	_, err := r.DB.ExecContext(ctx,
		`UPDATE transformation_definition_renders
		 SET refused_blob_sha = $2, updated_at = now()
		 WHERE transformation_id = $1`,
		id, refusedSHA,
	)
	if err != nil {
		return fmt.Errorf("postgres: mark transformation definition refused: %w", err)
	}
	return nil
}

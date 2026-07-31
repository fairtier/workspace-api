package postgres

import (
	"context"
	"fmt"

	"github.com/fairtier/workspace-api/workspace"
)

// UpsertPipelineCredentialRender records the fingerprint + blob sha of a
// just-written pipelines/<name>.credentials.age file. Rows die with their
// pipeline (FK cascade).
func (r *Repository) UpsertPipelineCredentialRender(ctx context.Context, render *workspace.PipelineCredentialRender) error {
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO pipeline_credential_renders (pipeline_id, fingerprint, blob_sha, updated_at)
		 VALUES ($1, $2, $3, now())
		 ON CONFLICT (pipeline_id)
		 DO UPDATE SET fingerprint = EXCLUDED.fingerprint,
		               blob_sha = EXCLUDED.blob_sha, updated_at = now()`,
		render.PipelineID, render.Fingerprint, render.BlobSHA,
	)
	if err != nil {
		return fmt.Errorf("postgres: upsert credential render: %w", err)
	}
	return nil
}

// GetPipelineCredentialRenders returns render bookkeeping for all of a
// customer's pipelines, keyed by pipeline id.
func (r *Repository) GetPipelineCredentialRenders(ctx context.Context, customerSlug string) (map[workspace.PipelineID]workspace.PipelineCredentialRender, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT r.pipeline_id, r.fingerprint, r.blob_sha
		 FROM pipeline_credential_renders r
		 JOIN pipelines p ON p.id = r.pipeline_id
		 WHERE p.customer_slug = $1`,
		customerSlug,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: get credential renders: %w", err)
	}
	defer rows.Close()

	out := make(map[workspace.PipelineID]workspace.PipelineCredentialRender)
	for rows.Next() {
		var render workspace.PipelineCredentialRender
		if err := rows.Scan(&render.PipelineID, &render.Fingerprint, &render.BlobSHA); err != nil {
			return nil, fmt.Errorf("postgres: scan credential render: %w", err)
		}
		out[render.PipelineID] = render
	}
	return out, rows.Err()
}

package postgres

import (
	"context"
	"fmt"

	"github.com/fairtier/workspace-api/workspace"
)

// Phase 2B (adopt-on-drift) ownership flips — see
// workspace.PipelineCredentialOwnershipStore.
var _ workspace.PipelineCredentialOwnershipStore = (*Repository)(nil)

// SetPipelineCredentialsExternal flips the externally-managed flag on a
// pipeline's credentials.
func (r *Repository) SetPipelineCredentialsExternal(ctx context.Context, id workspace.PipelineID, external bool) error {
	res, err := r.DB.ExecContext(ctx,
		`UPDATE pipelines SET credentials_external = $2 WHERE id = $1`,
		id, external,
	)
	if err != nil {
		return fmt.Errorf("postgres: set credentials external: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return workspace.ErrPipelineNotFound
	}
	return nil
}

// DeletePipelineCredentialRender drops the render bookkeeping row — the
// reclaim half: with the row gone the next converge re-renders the file
// unconditionally, overwriting the foreign commit.
func (r *Repository) DeletePipelineCredentialRender(ctx context.Context, id workspace.PipelineID) error {
	_, err := r.DB.ExecContext(ctx,
		`DELETE FROM pipeline_credential_renders WHERE pipeline_id = $1`,
		id,
	)
	if err != nil {
		return fmt.Errorf("postgres: delete credential render: %w", err)
	}
	return nil
}

// MarkPipelineDefinitionRefused stamps the refused blob sha without touching
// the last-render bookkeeping. The row must exist (the adopt pass only acts
// on tracked renders).
func (r *Repository) MarkPipelineDefinitionRefused(ctx context.Context, id workspace.PipelineID, refusedSHA string) error {
	_, err := r.DB.ExecContext(ctx,
		`UPDATE pipeline_definition_renders SET refused_blob_sha = $2, updated_at = now() WHERE pipeline_id = $1`,
		id, refusedSHA,
	)
	if err != nil {
		return fmt.Errorf("postgres: mark definition refused: %w", err)
	}
	return nil
}

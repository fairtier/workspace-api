package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/fairtier/workspace-api/workspace"
)

// Hydration writes (control-plane/workspace-split Phase 3B): unlike the
// Create* pair, these insert the id the caller supplies instead of letting
// Postgres mint one, so a box importing its Gitea repo keeps the ids the
// rendered files carry — the same ids the dlt-worker's run rows and the
// central cache use. See workspace/pipelinemirror_import.go.

// ImportPipeline inserts a pipeline with its id preserved.
func (r *Repository) ImportPipeline(ctx context.Context, p *workspace.Pipeline) error {
	encCreds, err := r.encryptCredentials(emptyJSONObject(p.SourceCredentials))
	if err != nil {
		return fmt.Errorf("postgres: encrypt credentials: %w", err)
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO pipelines (id, customer_slug, name, source_type, source_config, source_credentials, dataset_name, schedule, write_disposition, merge_strategy, credentials_external, enabled, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
		p.ID, p.CustomerSlug, p.Name, p.SourceType, p.SourceConfig, encCreds,
		p.DatasetName, p.Schedule, p.WriteDisposition, p.MergeStrategy,
		p.CredentialsExternal, p.Enabled, p.CreatedAt, p.UpdatedAt,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return workspace.ErrPipelineAlreadyExists
		}
		return fmt.Errorf("postgres: import pipeline: %w", err)
	}
	return nil
}

// ImportTransformation inserts a transformation with its id preserved. Git
// credentials are never rendered into the repo, so an imported row has none.
func (r *Repository) ImportTransformation(ctx context.Context, t *workspace.Transformation) error {
	encCreds, err := r.encryptCredentials(emptyJSONObject(t.GitCredentials))
	if err != nil {
		return fmt.Errorf("postgres: encrypt credentials: %w", err)
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO transformations (id, customer_slug, name, repo_url, repo_ref, git_credentials, schedule, trigger_after_pipeline_id, dbt_selector, enabled, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		t.ID, t.CustomerSlug, t.Name, t.RepoURL, t.RepoRef, encCreds,
		t.Schedule, nullUUID(string(t.TriggerAfterPipelineID)), t.DBTSelector, t.Enabled,
		t.CreatedAt, t.UpdatedAt,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return workspace.ErrTransformationAlreadyExists
		}
		return fmt.Errorf("postgres: import transformation: %w", err)
	}
	return nil
}

// emptyJSONObject defaults absent credentials to "{}" — the credential
// columns are NOT NULL, and an imported row never carries any.
func emptyJSONObject(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("{}")
	}
	return raw
}

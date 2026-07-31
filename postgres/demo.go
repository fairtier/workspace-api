package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fairtier/workspace-api/workspace"
)

// SaveDemoSeed inserts or replaces the demo bookkeeping record for a customer.
func (r *Repository) SaveDemoSeed(ctx context.Context, seed *workspace.DemoSeed) error {
	repoFiles, err := json.Marshal(seed.RepoFiles)
	if err != nil {
		return fmt.Errorf("marshal repo files: %w", err)
	}
	status := seed.Status
	if status == "" {
		status = workspace.DemoStatusReady
	}
	_, err = r.DB.ExecContext(ctx, `
		INSERT INTO demo_seeds
			(customer_slug, tier, status, trips_pipeline_id, zones_pipeline_id, transformation_id, repo_files, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (customer_slug) DO UPDATE SET
			tier = EXCLUDED.tier,
			status = EXCLUDED.status,
			trips_pipeline_id = EXCLUDED.trips_pipeline_id,
			zones_pipeline_id = EXCLUDED.zones_pipeline_id,
			transformation_id = EXCLUDED.transformation_id,
			repo_files = EXCLUDED.repo_files,
			created_at = EXCLUDED.created_at`,
		seed.CustomerSlug, seed.Tier, status,
		string(seed.TripsPipelineID), string(seed.ZonesPipelineID), string(seed.TransformationID),
		repoFiles, seed.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert demo seed: %w", err)
	}
	return nil
}

// GetDemoSeed returns the demo record for a customer, or
// workspace.ErrDemoNotLoaded when none exists.
func (r *Repository) GetDemoSeed(ctx context.Context, customerSlug string) (*workspace.DemoSeed, error) {
	seed := &workspace.DemoSeed{CustomerSlug: customerSlug}
	var trips, zones, transformation string
	var repoFiles []byte
	err := r.DB.QueryRowContext(ctx, `
		SELECT tier, status, trips_pipeline_id, zones_pipeline_id, transformation_id, repo_files, created_at
		FROM demo_seeds WHERE customer_slug = $1`,
		customerSlug,
	).Scan(&seed.Tier, &seed.Status, &trips, &zones, &transformation, &repoFiles, &seed.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, workspace.ErrDemoNotLoaded
	}
	if err != nil {
		return nil, fmt.Errorf("query demo seed: %w", err)
	}
	seed.TripsPipelineID = workspace.PipelineID(trips)
	seed.ZonesPipelineID = workspace.PipelineID(zones)
	seed.TransformationID = workspace.TransformationID(transformation)
	if err := json.Unmarshal(repoFiles, &seed.RepoFiles); err != nil {
		return nil, fmt.Errorf("unmarshal repo files: %w", err)
	}
	return seed, nil
}

// DeleteDemoSeed removes the demo record for a customer (no error if absent).
func (r *Repository) DeleteDemoSeed(ctx context.Context, customerSlug string) error {
	if _, err := r.DB.ExecContext(ctx, `DELETE FROM demo_seeds WHERE customer_slug = $1`, customerSlug); err != nil {
		return fmt.Errorf("delete demo seed: %w", err)
	}
	return nil
}

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/fairtier/workspace-api/workspace"
)

// UpsertOAuthClient stores or replaces a customer's own OAuth application. The
// client secret is encrypted at rest (same Encryptor as pipeline credentials);
// the client id is not a secret and stays readable.
func (r *Repository) UpsertOAuthClient(ctx context.Context, c *workspace.OAuthClient) error {
	encSecret, err := r.encryptCredentials([]byte(c.ClientSecret))
	if err != nil {
		return fmt.Errorf("postgres: encrypt oauth client secret: %w", err)
	}
	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO customer_oauth_clients (customer_slug, provider, client_id, client_secret, updated_at, updated_by)
		 VALUES ($1, $2, $3, $4, now(), $5)
		 ON CONFLICT (customer_slug, provider)
		 DO UPDATE SET client_id = EXCLUDED.client_id, client_secret = EXCLUDED.client_secret,
		               updated_at = now(), updated_by = EXCLUDED.updated_by`,
		c.CustomerSlug, c.Provider, c.ClientID, encSecret, c.UpdatedBy,
	)
	if err != nil {
		return fmt.Errorf("postgres: upsert oauth client: %w", err)
	}
	return nil
}

// GetOAuthClient returns the customer's OAuth application, or
// workspace.ErrOAuthClientNotFound when they have not connected one.
func (r *Repository) GetOAuthClient(ctx context.Context, customerSlug, provider string) (*workspace.OAuthClient, error) {
	var c workspace.OAuthClient
	var storedSecret string
	err := r.DB.QueryRowContext(ctx,
		`SELECT customer_slug, provider, client_id, client_secret, updated_at, updated_by
		 FROM customer_oauth_clients WHERE customer_slug = $1 AND provider = $2`,
		customerSlug, provider,
	).Scan(&c.CustomerSlug, &c.Provider, &c.ClientID, &storedSecret, &c.UpdatedAt, &c.UpdatedBy)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, workspace.ErrOAuthClientNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: get oauth client: %w", err)
	}

	secret, err := r.decryptCredentials(storedSecret)
	if err != nil {
		return nil, fmt.Errorf("postgres: decrypt oauth client secret: %w", err)
	}
	c.ClientSecret = string(secret)
	return &c, nil
}

// DeleteOAuthClient disconnects the customer's OAuth application. Deleting a
// row that is not there is not an error.
func (r *Repository) DeleteOAuthClient(ctx context.Context, customerSlug, provider string) error {
	_, err := r.DB.ExecContext(ctx,
		`DELETE FROM customer_oauth_clients WHERE customer_slug = $1 AND provider = $2`,
		customerSlug, provider,
	)
	if err != nil {
		return fmt.Errorf("postgres: delete oauth client: %w", err)
	}
	return nil
}

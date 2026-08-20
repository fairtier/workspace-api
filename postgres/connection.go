package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fairtier/workspace-api/workspace"
)

// CreateConnection inserts a workspace connection. Credentials are encrypted
// at rest (same Encryptor as pipeline credentials); config is not secret.
func (r *Repository) CreateConnection(ctx context.Context, c *workspace.Connection) error {
	encCreds, err := r.encryptCredentials(c.Credentials)
	if err != nil {
		return fmt.Errorf("postgres: encrypt connection credentials: %w", err)
	}
	config := c.Config
	if len(config) == 0 {
		config = json.RawMessage(`{}`)
	}
	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO connections (id, customer_slug, type, name, status, config, credentials)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		c.ID, c.CustomerSlug, c.Type, c.Name, c.Status, []byte(config), encCreds,
	)
	if isUniqueViolation(err) {
		return workspace.ErrConnectionAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("postgres: create connection: %w", err)
	}
	return nil
}

// ReauthorizeConnection replaces a connection's stored credentials in place
// and marks it active again — the reconnect path.
//
// The id is preserved, which is the entire point: pipelines reference a
// connection by id, so re-authorizing must reach every one of them without
// touching a single pipeline row. Creating a second connection instead would
// leave every existing pipeline pointing at the dead token.
func (r *Repository) ReauthorizeConnection(ctx context.Context, customerSlug, id string, credentials json.RawMessage) error {
	encCreds, err := r.encryptCredentials(credentials)
	if err != nil {
		return fmt.Errorf("postgres: encrypt connection credentials: %w", err)
	}
	res, err := r.DB.ExecContext(ctx,
		`UPDATE connections SET credentials = $1, status = 'active', updated_at = now()
		 WHERE customer_slug = $2 AND id = $3`,
		encCreds, customerSlug, id,
	)
	if err != nil {
		return fmt.Errorf("postgres: reauthorize connection: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return workspace.ErrConnectionNotFound
	}
	return nil
}

// GetConnection returns one connection, slug-scoped.
func (r *Repository) GetConnection(ctx context.Context, customerSlug, id string) (*workspace.Connection, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT id, customer_slug, type, name, status, config, credentials, created_at, updated_at
		 FROM connections WHERE customer_slug = $1 AND id = $2`,
		customerSlug, id,
	)
	c, err := scanConnection(row, r)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, workspace.ErrConnectionNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: get connection: %w", err)
	}
	return c, nil
}

// ListConnections returns the workspace's connections, newest first.
func (r *Repository) ListConnections(ctx context.Context, customerSlug string) ([]workspace.Connection, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT id, customer_slug, type, name, status, config, credentials, created_at, updated_at
		 FROM connections WHERE customer_slug = $1 ORDER BY created_at DESC`,
		customerSlug,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: list connections: %w", err)
	}
	defer rows.Close()

	var out []workspace.Connection
	for rows.Next() {
		c, err := scanConnection(rows, r)
		if err != nil {
			return nil, fmt.Errorf("postgres: list connections: %w", err)
		}
		out = append(out, *c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list connections: %w", err)
	}
	return out, nil
}

// DeleteConnection removes a connection, slug-scoped. Deleting a row that is
// not there is not an error.
func (r *Repository) DeleteConnection(ctx context.Context, customerSlug, id string) error {
	_, err := r.DB.ExecContext(ctx,
		`DELETE FROM connections WHERE customer_slug = $1 AND id = $2`,
		customerSlug, id,
	)
	if err != nil {
		return fmt.Errorf("postgres: delete connection: %w", err)
	}
	return nil
}

// rowScanner is satisfied by both *sql.Row and *sql.Rows.
type rowScanner interface{ Scan(dest ...any) error }

func scanConnection(row rowScanner, r *Repository) (*workspace.Connection, error) {
	var c workspace.Connection
	var config []byte
	var storedCreds string
	if err := row.Scan(&c.ID, &c.CustomerSlug, &c.Type, &c.Name, &c.Status,
		&config, &storedCreds, &c.CreatedAt, &c.UpdatedAt); err != nil {
		return nil, err
	}
	c.Config = json.RawMessage(config)
	creds, err := r.decryptCredentials(storedCreds)
	if err != nil {
		return nil, fmt.Errorf("decrypt connection credentials: %w", err)
	}
	c.Credentials = creds
	return &c, nil
}

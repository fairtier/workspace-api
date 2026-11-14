package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

// Scopes cross the driver as one space-separated string and are split back
// here (array_to_string / string_to_array), so the text[] column needs no
// driver-specific array codec under database/sql. Scope values never contain
// spaces — the service accepts exactly two literals.

const accessTokenColumns = `id, customer_slug, user_sub, name, token_prefix, token_hash,
	array_to_string(scopes, ' '), created_at, expires_at, last_used_at, revoked_at`

func (r *Repository) CreateAccessToken(ctx context.Context, t *workspace.AccessToken) error {
	err := r.DB.QueryRowContext(ctx,
		`INSERT INTO access_tokens (customer_slug, user_sub, name, token_prefix, token_hash, scopes, expires_at)
		 VALUES ($1, $2, $3, $4, $5, string_to_array($6, ' '), $7)
		 RETURNING id, created_at`,
		t.CustomerSlug, string(t.UserSub), t.Name, t.Prefix, t.Hash, strings.Join(t.Scopes, " "), t.ExpiresAt,
	).Scan(&t.ID, &t.CreatedAt)
	if err != nil {
		return fmt.Errorf("postgres: create access token: %w", err)
	}
	return nil
}

func (r *Repository) ListAccessTokens(ctx context.Context, customerSlug string, userSub core.UserID) ([]workspace.AccessToken, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT `+accessTokenColumns+`
		 FROM access_tokens
		 WHERE customer_slug = $1 AND user_sub = $2
		 ORDER BY created_at DESC`,
		customerSlug, string(userSub),
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: list access tokens: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []workspace.AccessToken
	for rows.Next() {
		t, err := scanAccessToken(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: iterate access tokens: %w", err)
	}
	return out, nil
}

func (r *Repository) GetAccessTokenByHash(ctx context.Context, hash []byte) (*workspace.AccessToken, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT `+accessTokenColumns+` FROM access_tokens WHERE token_hash = $1`, hash)
	t, err := scanAccessToken(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, workspace.ErrAccessTokenNotFound
	}
	return t, err
}

func (r *Repository) RevokeAccessToken(ctx context.Context, customerSlug string, userSub core.UserID, id string, at time.Time) error {
	// COALESCE keeps the first revocation time: revoking twice succeeds but
	// does not rewrite history.
	res, err := r.DB.ExecContext(ctx,
		`UPDATE access_tokens SET revoked_at = COALESCE(revoked_at, $4)
		 WHERE id::text = $3 AND customer_slug = $1 AND user_sub = $2`,
		customerSlug, string(userSub), id, at,
	)
	if err != nil {
		return fmt.Errorf("postgres: revoke access token: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres: rows affected: %w", err)
	}
	if n == 0 {
		return workspace.ErrAccessTokenNotFound
	}
	return nil
}

func (r *Repository) TouchAccessToken(ctx context.Context, id string, at time.Time) error {
	_, err := r.DB.ExecContext(ctx,
		`UPDATE access_tokens SET last_used_at = $2 WHERE id = $1 AND (last_used_at IS NULL OR last_used_at < $2)`,
		id, at)
	if err != nil {
		return fmt.Errorf("postgres: touch access token: %w", err)
	}
	return nil
}

func scanAccessToken(row rowScanner) (*workspace.AccessToken, error) {
	var (
		t       workspace.AccessToken
		userSub string
		scopes  string
	)
	err := row.Scan(&t.ID, &t.CustomerSlug, &userSub, &t.Name, &t.Prefix, &t.Hash,
		&scopes, &t.CreatedAt, &t.ExpiresAt, &t.LastUsedAt, &t.RevokedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: scan access token: %w", err)
	}
	t.UserSub = core.UserID(userSub)
	t.Scopes = strings.Fields(scopes)
	return &t, nil
}

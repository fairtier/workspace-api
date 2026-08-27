package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/fairtier/workspace-api/workspace"
)

// CreateGoogleOAuthGrant stores a short-lived OAuth grant. The refresh token is
// encrypted at rest (same Encryptor as pipeline credentials).
func (r *Repository) CreateGoogleOAuthGrant(ctx context.Context, g *workspace.GoogleOAuthGrant) error {
	encToken, err := r.encryptCredentials([]byte(g.RefreshToken))
	if err != nil {
		return fmt.Errorf("postgres: encrypt oauth refresh token: %w", err)
	}
	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO google_oauth_grants (grant_id, customer_slug, user_sub, refresh_token, email, client_id, scopes, created_at, expires_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		g.GrantID, g.CustomerSlug, g.UserSub, encToken, g.Email, g.ClientID,
		strings.Join(g.Scopes, " "), g.CreatedAt, g.ExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("postgres: create oauth grant: %w", err)
	}
	return nil
}

// ConsumeGoogleOAuthGrant deletes and returns the grant in one round-trip, but
// only when it exists, belongs to customerSlug, and has not expired. The
// DELETE ... RETURNING makes redemption atomic and single-use.
func (r *Repository) ConsumeGoogleOAuthGrant(ctx context.Context, grantID, customerSlug string) (*workspace.GoogleOAuthGrant, error) {
	var g workspace.GoogleOAuthGrant
	var storedToken, scopes string
	err := r.DB.QueryRowContext(ctx,
		`DELETE FROM google_oauth_grants
		 WHERE grant_id = $1 AND customer_slug = $2 AND expires_at > now()
		 RETURNING grant_id, customer_slug, user_sub, refresh_token, email, client_id, scopes, created_at, expires_at`,
		grantID, customerSlug,
	).Scan(&g.GrantID, &g.CustomerSlug, &g.UserSub, &storedToken, &g.Email, &g.ClientID, &scopes, &g.CreatedAt, &g.ExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, workspace.ErrOAuthGrantNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: consume oauth grant: %w", err)
	}

	token, err := r.decryptCredentials(storedToken)
	if err != nil {
		return nil, fmt.Errorf("postgres: decrypt oauth refresh token: %w", err)
	}
	g.RefreshToken = string(token)
	g.Scopes = strings.Fields(scopes)
	return &g, nil
}

// DeleteExpiredGoogleOAuthGrants sweeps expired grants.
func (r *Repository) DeleteExpiredGoogleOAuthGrants(ctx context.Context) (int64, error) {
	res, err := r.DB.ExecContext(ctx,
		`DELETE FROM google_oauth_grants WHERE expires_at <= now()`)
	if err != nil {
		return 0, fmt.Errorf("postgres: sweep expired oauth grants: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

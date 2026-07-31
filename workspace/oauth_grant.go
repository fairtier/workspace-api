package workspace

import (
	"context"
	"time"
)

// GoogleOAuthGrant is a short-lived server-side record of a completed
// "Sign in with Google" consent: it holds the refresh token the callback
// obtained, keyed by a random grant_id the Console redeems on pipeline create.
// The refresh token never travels through the browser — the Console only ever
// holds the grant_id. RefreshToken is plaintext in the domain; the postgres
// layer encrypts it at rest.
type GoogleOAuthGrant struct {
	GrantID      string
	CustomerSlug string
	UserSub      string
	RefreshToken string
	Email        string
	CreatedAt    time.Time
	ExpiresAt    time.Time
}

// GoogleOAuthGrantStore persists the short-lived OAuth grants.
type GoogleOAuthGrantStore interface {
	// CreateGoogleOAuthGrant stores a new grant (refresh token encrypted at rest).
	CreateGoogleOAuthGrant(ctx context.Context, g *GoogleOAuthGrant) error

	// ConsumeGoogleOAuthGrant atomically fetches and deletes the grant, but only
	// when it exists, belongs to customerSlug, and has not expired. It returns
	// ErrOAuthGrantNotFound otherwise. One-time use: a redeemed grant is gone.
	ConsumeGoogleOAuthGrant(ctx context.Context, grantID, customerSlug string) (*GoogleOAuthGrant, error)

	// DeleteExpiredGoogleOAuthGrants removes expired grants and returns the count
	// deleted (periodic sweep).
	DeleteExpiredGoogleOAuthGrants(ctx context.Context) (int64, error)
}

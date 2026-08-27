package workspace

import (
	"context"
	"log/slog"
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
	// ClientID is the customer's OAuth app that minted this token. A refresh
	// token can only be refreshed by the client it was issued to, so it is
	// carried through to the stored pipeline credential and compared there.
	ClientID string
	// Scopes is what Google granted this token, as returned by the token
	// endpoint — not what was asked for (the consent screen lets the user
	// untick one). Carried through to the connection so the Console can tell a
	// Sheets-only account from a Drive-capable one.
	Scopes    []string
	CreatedAt time.Time
	ExpiresAt time.Time
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

// SweepExpiredGrants deletes expired google_oauth_grants rows on a loop, once
// immediately and then every interval, until ctx is done.
//
// A grant is a one-time, 15-minute reference the Console swaps for a stored
// refresh token, and redeeming it deletes the row — the normal path cleans up
// after itself. Abandoned consents (the popup closed, the wizard never
// finished) are what stay behind, and each holds a live Google refresh token,
// encrypted but well past the TTL that is supposed to bound it. Lives in the
// module so both binaries that mint grants — central and a box serving its
// own consent flow — inherit the same sweep.
func SweepExpiredGrants(ctx context.Context, grants GoogleOAuthGrantStore, interval time.Duration, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		// Sweep on start too: a binary restarting more often than the
		// interval would otherwise never run one.
		n, err := grants.DeleteExpiredGoogleOAuthGrants(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			logger.Warn("oauth grant sweep failed", "error", err)
		case n > 0:
			logger.Info("oauth grant sweep removed expired grants", "count", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

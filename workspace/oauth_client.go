package workspace

import (
	"context"
	"strings"
	"time"
)

// OAuthProviderGoogle is the only provider the flow supports today. The store
// is keyed by provider so a second one needs no migration, but every code path
// that mints or refreshes a token is still Google-specific.
const OAuthProviderGoogle = "google"

// OAuthClient is a customer-supplied OAuth application: the client id and
// secret they registered in their own vendor project (their Google Cloud
// project, for Sheets).
//
// It is deliberately theirs and not ours. A shared FairTier app would have to
// travel with every pipeline credential that needs to refresh offline, which
// means into the customer's own box repo — so a fleet-wide vendor secret would
// sit on every customer's machine. With a BYO client, what lands there is the
// customer's own credential, which is the correct contents for a file in their
// repo.
//
// ClientSecret is plaintext in the domain; the postgres layer encrypts it at
// rest. ClientID is not a secret and is shown back in the Console.
type OAuthClient struct {
	CustomerSlug string
	Provider     string
	ClientID     string
	ClientSecret string
	UpdatedAt    time.Time
	// UpdatedBy is the Casdoor sub of whoever last saved the pair.
	UpdatedBy string
}

// OAuthClientStore persists the per-customer OAuth applications.
type OAuthClientStore interface {
	// UpsertOAuthClient stores or replaces the pair for (customer, provider).
	// The secret is encrypted at rest.
	UpsertOAuthClient(ctx context.Context, c *OAuthClient) error

	// GetOAuthClient returns the stored pair, or ErrOAuthClientNotFound when the
	// customer has not connected one.
	GetOAuthClient(ctx context.Context, customerSlug, provider string) (*OAuthClient, error)

	// DeleteOAuthClient removes the pair. Deleting one that does not exist is
	// not an error — disconnecting twice is the same as disconnecting once.
	DeleteOAuthClient(ctx context.Context, customerSlug, provider string) error
}

// ValidateOAuthClient normalises and checks a pair on the way in. Both halves
// are required: a client id without its secret cannot exchange a code, so
// storing one would only defer the failure to the consent popup.
func ValidateOAuthClient(c *OAuthClient) error {
	c.Provider = strings.TrimSpace(c.Provider)
	if c.Provider == "" {
		c.Provider = OAuthProviderGoogle
	}
	if c.Provider != OAuthProviderGoogle {
		return ErrUnsupportedOAuthProvider
	}
	c.ClientID = strings.TrimSpace(c.ClientID)
	c.ClientSecret = strings.TrimSpace(c.ClientSecret)
	if c.ClientID == "" || c.ClientSecret == "" {
		return ErrInvalidOAuthClient
	}
	return nil
}

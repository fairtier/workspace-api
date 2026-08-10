package workspace

import (
	"context"
	"time"
)

// BoxFederationClient is the OAuth client a box mints for itself so its users
// can sign in through the central identity provider ("sign in with FairTier").
//
// The pair is generated ON THE BOX and deposited here; central registers it as
// the box's client and never mints one. That direction is the point. A client
// secret is shared with the identity provider by construction, so it cannot be
// kept from central — but it can be the box's own credential rather than one
// we mint and push down into a database its owner has root over. The customer
// can rotate it unilaterally: re-deposit and the next reconcile converges both
// ends.
//
// Its authority is confined to the OAuth endpoints at the provider (see the
// registration side), so a deposited pair authenticates the box's login flow
// and nothing else.
type BoxFederationClient struct {
	CustomerSlug string
	ClientID     string
	ClientSecret string
	Note         string
	UpdatedAt    time.Time
}

// BoxFederationClientStore persists deposited box federation clients.
// Central-only: the box-local binary does not mount BoxCredentialService.
type BoxFederationClientStore interface {
	// UpsertBoxFederationClient stores or replaces the client for a slug.
	UpsertBoxFederationClient(ctx context.Context, c *BoxFederationClient) error
	// GetBoxFederationClient returns the client for a slug, or
	// ErrBoxCredentialNotFound when the box has not deposited one yet.
	GetBoxFederationClient(ctx context.Context, customerSlug string) (*BoxFederationClient, error)
}

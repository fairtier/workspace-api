package workspace

import (
	"context"
	"time"
)

// BoxSecret is one centrally-minted credential destined for a box, addressed
// by a stable key ("betterstack_source_token", …).
//
// It is the counterpart to the deposited credentials in this package, and the
// direction is the whole point. A deposit exists so central never has to hold
// a box's admin credential; a BoxSecret exists because some credentials can
// only be minted centrally — an API token from a vendor we hold the account
// with, for instance — and until now the only way to get one onto a box was
// cloud-init, which the provider treats as create-only. A value delivered that
// way is frozen at first boot: it cannot be corrected, and it cannot be
// rotated without destroying the customer's machine.
//
// Rows are written by the provisioning worker from the tenant root's outputs
// and read back by the box's own sync Job, so the flow stays
// Terraform → central → box with Terraform still the source of truth.
type BoxSecret struct {
	CustomerSlug string
	// Key is the box-facing name, stable across rotations. It is what the
	// sync Job maps onto a Kubernetes Secret field, so renaming one is a
	// breaking change for that box's manifests.
	Key string
	// Value is stored encrypted at rest by the implementation, with the same
	// CREDENTIAL_ENCRYPTION_KEY machinery as the other box credentials.
	Value string
	// Note is free-form audit context (which root/module minted it).
	Note      string
	UpdatedAt time.Time
}

// BoxSecretStore persists centrally-minted box secrets.
type BoxSecretStore interface {
	// UpsertBoxSecret stores or replaces one key for a slug. Called by the
	// worker after every successful apply, so it must be idempotent for an
	// unchanged value.
	UpsertBoxSecret(ctx context.Context, secret *BoxSecret) error
	// GetBoxSecrets returns the requested keys for a slug. An empty keys
	// slice means every key held for that slug. Keys with no row are omitted
	// from the result rather than reported — see FetchBoxSecretsResponse.
	GetBoxSecrets(ctx context.Context, customerSlug string, keys []string) (map[string]string, error)
}

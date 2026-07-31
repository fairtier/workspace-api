package workspace

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/fairtier/workspace-api/core"
)

// Box-local (static) implementations of the tenancy and credential ports.
// A dedicated-VM box serves exactly
// one workspace, whose identity, endpoints, and repo credentials are all
// known from local config — no customers table, no deposit tables. The
// box-local cmd/workspace_api binary wires these; the central deployment
// keeps using postgres.Repository.

// StaticResolver is a Resolver over a single fixed workspace. Any
// authenticated *person* belongs to it: the box's own Casdoor is the only
// token issuer this deployment trusts, and it holds only this customer's
// users — the issuer, not a lookup, is the tenant boundary.
//
// That reasoning covers the tenant boundary but not the principal one, and
// the box Casdoor signs machine identities too, so GetWorkspaceByUser keeps
// service accounts out (see below).
type StaticResolver struct {
	Workspace Workspace
}

var (
	_ Resolver        = (*StaticResolver)(nil)
	_ WorkspaceLister = (*StaticResolver)(nil)
)

// GetWorkspace returns the workspace when slug addresses it, and
// ErrWorkspaceNotFound for every other slug — a static deployment must never
// answer for tenants it does not serve.
func (r *StaticResolver) GetWorkspace(_ context.Context, slug string) (*Workspace, error) {
	if slug != r.Workspace.Slug {
		return nil, ErrWorkspaceNotFound
	}
	ws := r.Workspace
	return &ws, nil
}

// GetWorkspaceByUser returns the single workspace for every human caller, and
// ErrWorkspaceNotFound for service accounts.
//
// Centrally the customers-table lookup doubles as the principal check: a
// subject with no provisioned user row is rejected. A static deployment has
// no such table, so it would otherwise admit every subject the box's Casdoor
// signs — including the Lakekeeper data-platform service accounts handed to
// end users with role "reader" for BYO-dbt/BYO-Rill, and the dlt-worker's own
// pair. Those must not reach the product surface: it would turn a read-only
// warehouse credential into a workspace admin (CreatePipeline,
// BoxRepoService.PutFile, LakekeeperUserService.AddUser with role "admin").
func (r *StaticResolver) GetWorkspaceByUser(_ context.Context, userID core.UserID) (*Workspace, error) {
	if userID == "" || userID.IsServiceAccount() {
		return nil, ErrWorkspaceNotFound
	}
	// A human's subject is their bare Casdoor user ID; an owner-qualified
	// subject ("<org>/<name>") must belong to this workspace's own org —
	// anything else is an identity from an organization this deployment
	// does not serve.
	if owner, _, ok := strings.Cut(string(userID), "/"); ok && owner != r.Workspace.CasdoorOrg {
		return nil, ErrWorkspaceNotFound
	}
	ws := r.Workspace
	return &ws, nil
}

// ListVMWorkspaceSlugs implements WorkspaceLister (the adopt-sweep scope):
// just the one workspace.
func (r *StaticResolver) ListVMWorkspaceSlugs(_ context.Context) ([]string, error) {
	return []string{r.Workspace.Slug}, nil
}

// errStaticCredential rejects writes to config-backed credentials: deposits
// only exist in the central deployment (BoxCredentialService is not mounted
// on a box), so any Upsert reaching a static store is a wiring bug.
var errStaticCredential = errors.New("box credentials are static config on this deployment")

// StaticBoxCredentials satisfies the box credential stores
// (BoxGitCredentialStore, BoxSnapshotCredentialStore, BoxAgeKeyStore) from
// local config. Empty values report ErrBoxCredentialNotFound, which the
// consumers already treat as "feature not available yet".
type StaticBoxCredentials struct {
	Slug string
	// GitUsername/GitToken authenticate the box Gitea contents API
	// (the same write-scoped token the box seed deposits centrally today).
	GitUsername string
	GitToken    string
	// SnapshotToken is the rill-snapshot sidecar bearer token.
	SnapshotToken string
	// AgePublicKey is the box age recipient ("age1...") credential files are
	// encrypted to; the private key stays in the box Secret.
	AgePublicKey string
}

var (
	_ BoxGitCredentialStore      = (*StaticBoxCredentials)(nil)
	_ BoxSnapshotCredentialStore = (*StaticBoxCredentials)(nil)
	_ BoxAgeKeyStore             = (*StaticBoxCredentials)(nil)
)

// check gates every read on the deployment's own slug.
func (s *StaticBoxCredentials) check(customerSlug, value string) error {
	if customerSlug != s.Slug {
		return ErrWorkspaceNotFound
	}
	if value == "" {
		return ErrBoxCredentialNotFound
	}
	return nil
}

func (s *StaticBoxCredentials) GetBoxGitCredential(_ context.Context, customerSlug string) (*BoxGitCredential, error) {
	if err := s.check(customerSlug, s.GitToken); err != nil {
		return nil, err
	}
	return &BoxGitCredential{CustomerSlug: s.Slug, Username: s.GitUsername, Token: s.GitToken}, nil
}

func (s *StaticBoxCredentials) UpsertBoxGitCredential(context.Context, *BoxGitCredential) error {
	return errStaticCredential
}

func (s *StaticBoxCredentials) GetBoxSnapshotCredential(_ context.Context, customerSlug string) (*BoxSnapshotCredential, error) {
	if err := s.check(customerSlug, s.SnapshotToken); err != nil {
		return nil, err
	}
	return &BoxSnapshotCredential{CustomerSlug: s.Slug, Token: s.SnapshotToken}, nil
}

func (s *StaticBoxCredentials) UpsertBoxSnapshotCredential(context.Context, *BoxSnapshotCredential) error {
	return errStaticCredential
}

func (s *StaticBoxCredentials) GetBoxAgeKey(_ context.Context, customerSlug string) (*BoxAgeKey, error) {
	if err := s.check(customerSlug, s.AgePublicKey); err != nil {
		return nil, err
	}
	return &BoxAgeKey{CustomerSlug: s.Slug, PublicKey: s.AgePublicKey, UpdatedAt: time.Time{}}, nil
}

func (s *StaticBoxCredentials) UpsertBoxAgeKey(context.Context, *BoxAgeKey) error {
	return errStaticCredential
}

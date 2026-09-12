// Package workspace is the workspace-plane domain: the product services
// (pipelines, transformations, queries, warehouses, notifications, box
// repos, …) destined to run on the customer box. It must stay deployable
// without the FairTier control plane, so it may import neither the
// control-plane domain package nor any infra package (enforced by depguard) —
// everything arrives through ports declared here.
package workspace

import (
	"cmp"
	"context"
	"strings"

	"github.com/fairtier/workspace-api/core"
)

// Workspace is the per-tenant projection the workspace plane needs. It is
// the seam that makes the plane portable: centrally it is resolved from the
// customers table (postgres.Repository); on the box (Phase 3) there is no
// customers table and it is satisfiable from static config/env.
type Workspace struct {
	Slug string
	// OnVM reports the dedicated-VM substrate (any provider): box Casdoor,
	// box Gitea, published endpoints instead of cluster-internal Services.
	OnVM bool

	// CustomerDomain is the public workspace domain, possibly carrying a
	// wildcard prefix ("*.customer-<slug>.fairtier.com").
	CustomerDomain string
	// Namespace is the tenant namespace on the shared substrate; empty on VM.
	Namespace string

	// LakekeeperURL is the ADVERTISED catalog URL: what
	// BootstrapFromWorkspace publishes in /.well-known/fairtier-workspace for
	// the Console's connection card, and what a customer pastes into their own
	// DuckDB or Spark. It has to resolve from a browser on the public
	// internet. It is NOT necessarily what this process dials — see
	// LakekeeperDialURL and LakekeeperServiceURL.
	LakekeeperURL       string
	LakekeeperWarehouse string

	// LakekeeperDialURL replaces the address THIS process dials for the
	// catalog. Empty = the cluster-internal Service on the shared substrate,
	// the advertised URL on a box. Nothing advertises it, so an in-cluster
	// value is safe here and would break the Console anywhere else.
	LakekeeperDialURL string

	// CasdoorIssuer is the Casdoor base URL owning the workspace OIDC
	// client; empty means the central Casdoor (the TokenProvider default).
	CasdoorIssuer    string
	OIDCClientID     string
	OIDCClientSecret string

	// CasdoorOrg is the Casdoor organization that owns this workspace's
	// users. It is supplied by the resolver (centrally from the control
	// plane, on the box from WORKSPACE_CASDOOR_ORG) and never derived here:
	// the "customer-<slug>" naming is the control plane's convention, created
	// by infra/modules/customer/casdoor.tf, so the workspace plane treats it
	// as opaque.
	CasdoorOrg string

	// DuckFlightURL is the ADVERTISED Flight SQL endpoint, published in the
	// same bootstrap document and shown in the Console's connection card so a
	// customer's own client can reach it. Whether it is set is also what
	// "this workspace has a query engine" means.
	DuckFlightURL       string
	DuckFlightAuthToken string

	// DuckFlightDialURL replaces the address THIS process dials for
	// ExecuteQuery. Empty = the advertised URL. An http:// value dials
	// plaintext h2c, which is exactly what the box's in-cluster
	// duckflight:31337 speaks behind its TLS-terminating IngressRoute.
	DuckFlightDialURL string

	// EffectiveS3 is the resolved customer bucket (R2-derived or BYOS).
	EffectiveS3 core.S3Config

	RillEnabled bool
	CubeEnabled bool

	// RillURL / CubeURL are the external (browser-facing) UIs of the
	// respective apps, for the Console to link out to. Meaningful only while
	// the app is enabled.
	RillURL string
	CubeURL string

	// GiteaURL / SnapshotURL replace the public box hostnames this process
	// would otherwise dial for its OWN services. Empty = derive the public
	// form from CustomerDomain, which is what central always does.
	//
	// Gitea and the Rill snapshot sidecar are advertised to nobody, so one
	// field can be both the dial address and the only address. The catalog and
	// the query engine cannot: publishing an in-cluster value would fix the
	// dial and hand the customer's browser an unreachable address, which is
	// why each of those carries a separate *DialURL above.
	GiteaURL    string
	SnapshotURL string
}

// BoxGiteaURL is the base URL for the box's own Gitea. Prefer an in-cluster
// override: the public hostname hairpins pod → own node, which is this fleet's
// recorded boot-flake shape (see the AUTH_JWKS_URL split in the box chart, and
// the same move already made for duckflight and rill), and it additionally
// waits on git.<domain>'s Let's Encrypt certificate — which the adopt and
// hydration sweeps do not, since they run at process start.
func (w *Workspace) BoxGiteaURL() string {
	return cmp.Or(w.GiteaURL, "https://git."+w.BareDomain())
}

// BoxSnapshotURL is the base URL for the box's own Rill snapshot sidecar,
// with the same override rationale as BoxGiteaURL.
func (w *Workspace) BoxSnapshotURL() string {
	return cmp.Or(w.SnapshotURL, "https://rill-snapshot."+w.BareDomain())
}

// BareDomain is CustomerDomain without the wildcard-certificate prefix. Every
// derived endpoint needs this form; central rows may carry the "*." variant.
func (w *Workspace) BareDomain() string {
	return strings.TrimPrefix(w.CustomerDomain, "*.")
}

// LakekeeperServiceURL is the address this process dials for the catalog,
// which is not necessarily the one it advertises.
//
// An explicit LakekeeperDialURL wins everywhere. Failing that, the shared
// substrate prefers its cluster-internal Service because the external URL goes
// through Envoy Gateway, which decodes %2F in paths and so breaks DELETE
// /management/v1/user/{user_id} for IDs containing "/". On a VM box Namespace
// is empty and there is no Envoy, so the advertised URL is the fallback and
// the dial URL is what avoids sending a pod out to its own node's public
// hostname — this fleet's recorded boot-flake shape, the same one AUTH_JWKS_URL
// and BoxGiteaURL exist for.
func (w *Workspace) LakekeeperServiceURL() string {
	if w.LakekeeperDialURL != "" {
		return w.LakekeeperDialURL
	}
	if w.Namespace == "" {
		return w.LakekeeperURL
	}
	return "http://lakekeeper." + w.Namespace + ".svc:8181"
}

// DuckFlightServiceURL is the address this process dials for Flight SQL, which
// is not necessarily the one it advertises. Empty means there is nothing to
// dial at all: no override and no advertised endpoint.
func (w *Workspace) DuckFlightServiceURL() string {
	return cmp.Or(w.DuckFlightDialURL, w.DuckFlightURL)
}

// RillSnapshotURL returns the cluster-internal URL for the Rill snapshot
// sidecar (shared substrate only).
func (w *Workspace) RillSnapshotURL() string {
	return "http://rill-snapshot." + w.Namespace + ".svc:8484"
}

// CubeSnapshotURL returns the cluster-internal URL for the Cube snapshot
// sidecar (shared substrate only).
func (w *Workspace) CubeSnapshotURL() string {
	return "http://cube-snapshot." + w.Namespace + ".svc:8484"
}

// Resolver is the tenant-lookup port every workspace service binds through.
// Implementations must scope strictly by the given key — no request field
// can ever address another tenant's workspace.
type Resolver interface {
	// GetWorkspace returns the workspace for a customer slug.
	GetWorkspace(ctx context.Context, slug string) (*Workspace, error)

	// GetWorkspaceByUser returns the workspace the user belongs to.
	GetWorkspaceByUser(ctx context.Context, userID core.UserID) (*Workspace, error)
}

// UserInfo is the commit-attribution projection of the acting user.
type UserInfo struct {
	Name        string
	DisplayName string
	Email       string
}

// UserReader resolves the acting user for commit attribution
// (BoxRepoService, PipelineService) — nothing more.
type UserReader interface {
	// GetCommitUser returns the user behind a caller ID (the Casdoor subject
	// carried in Console JWTs).
	GetCommitUser(ctx context.Context, callerID core.UserID) (*UserInfo, error)
}

// Package workspace is the workspace-plane domain: the product services
// (pipelines, transformations, queries, warehouses, notifications, box
// repos, …) destined to run on the customer box. It must stay deployable
// without the FairTier control plane, so it may import neither the
// control-plane domain package nor any infra package (enforced by depguard) —
// everything arrives through ports declared here.
package workspace

import (
	"context"

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

	// LakekeeperURL is the external (Console-facing) catalog URL.
	LakekeeperURL       string
	LakekeeperWarehouse string

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

	DuckFlightURL       string
	DuckFlightAuthToken string

	// EffectiveS3 is the resolved customer bucket (R2-derived or BYOS).
	EffectiveS3 core.S3Config

	RillEnabled bool
	CubeEnabled bool

	// RillURL / CubeURL are the external (browser-facing) UIs of the
	// respective apps, for the Console to link out to. Meaningful only while
	// the app is enabled.
	RillURL string
	CubeURL string
}

// LakekeeperServiceURL returns the cluster-internal Lakekeeper URL on the
// shared substrate. The external URL goes through Envoy Gateway which
// decodes %2F in paths, breaking DELETE /management/v1/user/{user_id} for
// IDs containing "/". On VM boxes Namespace is empty and the external URL
// is the only route.
func (w *Workspace) LakekeeperServiceURL() string {
	if w.Namespace == "" {
		return w.LakekeeperURL
	}
	return "http://lakekeeper." + w.Namespace + ".svc:8181"
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

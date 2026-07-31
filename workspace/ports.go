package workspace

import (
	"context"

	"github.com/fairtier/workspace-api/core"
)

// The interfaces here are the workspace plane's consumer-defined view of
// shared clients (Lakekeeper, Casdoor). The control plane declares its own
// (wider) interfaces over the same concrete clients; both are satisfied
// structurally, so neither plane imports the other.

// LakekeeperClient is the warehouse/user-management surface of the
// Lakekeeper REST API the workspace plane drives. Provisioning-only
// operations (Bootstrap, storage migration) belong to the control plane.
type LakekeeperClient interface {
	CreateWarehouse(ctx context.Context, lakekeeperURL string, token string, name string, s3 core.S3Config) (warehouseName string, err error)
	ListWarehouses(ctx context.Context, lakekeeperURL string, token string) ([]core.Warehouse, error)
	GetWarehouseID(ctx context.Context, lakekeeperURL, token, warehouseName string) (warehouseID string, err error)
	CreateUser(ctx context.Context, lakekeeperURL, token, userID, name string) error
	DeleteUser(ctx context.Context, lakekeeperURL, token, userID string) error
	AssignWarehouseRole(ctx context.Context, lakekeeperURL, token, warehouseID, userID, role string) error
	RemoveWarehouseRole(ctx context.Context, lakekeeperURL, token, warehouseID, userID string) error
	// GetWarehouseAssignments returns all user-to-relation assignments for a
	// warehouse. Used to resolve which role each user has.
	GetWarehouseAssignments(ctx context.Context, lakekeeperURL, token, warehouseID string) ([]core.WarehouseAssignment, error)
}

// TokenProvider obtains access tokens from Casdoor.
type TokenProvider interface {
	// GetClientToken performs an OAuth2 client_credentials grant. issuer is
	// the Casdoor base URL to authenticate against; empty means the
	// provider's configured default endpoint (the platform's central
	// Casdoor). VM boxes pass their on-box Casdoor URL (Workspace.CasdoorIssuer).
	GetClientToken(ctx context.Context, issuer, clientID, clientSecret string) (string, error)
}

// CasdoorAppManager manages per-user Casdoor applications (service
// accounts) for OAuth2 client_credentials authentication.
type CasdoorAppManager interface {
	AddApp(ctx context.Context, org, name string) (*core.CasdoorApp, error)
	DeleteApp(ctx context.Context, org, name string) error
	ListApps(ctx context.Context, org string) ([]core.CasdoorApp, error)
}

// PipelineAlerter, when set on PipelineService, sends an out-of-band alert
// (email) for a failed run. Optional (nil = no alerts). Best-effort: errors
// are logged by the caller, never propagated to the worker's run report.
// The email implementation stays on the control plane (domain.
// EmailAlertService) — it resolves the owner via central identity.
type PipelineAlerter interface {
	AlertPipelineFailure(ctx context.Context, customerSlug, pipelineName, errorMessage string) error
}

// AudienceUpdater manages the Lakekeeper OIDC audience list in Kubernetes
// (shared substrate only — VM boxes converge audiences with an on-box
// CronJob).
type AudienceUpdater interface {
	UpdateAudiences(ctx context.Context, namespace string, audiences []string) error
}

package server

import (
	"database/sql"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/oauthgoogle"
	"github.com/fairtier/workspace-api/proto/assist/v1/assistv1connect"
	"github.com/fairtier/workspace-api/proto/boxcredential/v1/boxcredentialv1connect"
	"github.com/fairtier/workspace-api/proto/boxrepo/v1/boxrepov1connect"
	"github.com/fairtier/workspace-api/proto/connection/v1/connectionv1connect"
	"github.com/fairtier/workspace-api/proto/demo/v1/demov1connect"
	"github.com/fairtier/workspace-api/proto/lakekeeper_user/v1/lakekeeperuserv1connect"
	"github.com/fairtier/workspace-api/proto/notification/v1/notificationv1connect"
	"github.com/fairtier/workspace-api/proto/oauthclient/v1/oauthclientv1connect"
	"github.com/fairtier/workspace-api/proto/pipeline/v1/pipelinev1connect"
	"github.com/fairtier/workspace-api/proto/pipeline_assist/v1/pipelineassistv1connect"
	"github.com/fairtier/workspace-api/proto/query/v1/queryv1connect"
	"github.com/fairtier/workspace-api/proto/snapshot/v1/snapshotv1connect"
	"github.com/fairtier/workspace-api/proto/transformation/v1/transformationv1connect"
	"github.com/fairtier/workspace-api/proto/warehouse/v1/warehousev1connect"
	"github.com/fairtier/workspace-api/proto/workspace_health/v1/workspacehealthv1connect"
	"github.com/fairtier/workspace-api/workspace"
)

// The workspace plane: the product services destined to run on the customer
// box. Registration lives in this package — not in any one binary's main — so
// the box-local cmd/workspace_api binary and the FairTier control plane mount
// the exact same service group; keep control-plane services (billing,
// identity, provisioning) out of here.

// WorkspacePlaneServers bundles the workspace plane's public-mux handlers.
type WorkspacePlaneServers struct {
	// Health answers workspace_health.v1.HealthService. Every deployment of
	// this plane has one — it is mounted without the auth interceptor, so a
	// probe never needs a token.
	Health          *HealthServer
	LakekeeperUsers *LakekeeperUserServer
	Warehouses      *WarehouseServer
	Snapshots       *SnapshotServer
	Pipelines       *PipelineServer // Console variant, with FileDrop
	Transformations *TransformationServer
	PipelineAssist  *PipelineAssistServer
	Assist          *AssistServer
	BoxRepos        *BoxRepoServer
	Demo            *DemoServer
	Notifications   *NotificationServer
	Query           *QueryServer
	// OAuthClients lets a workspace connect its OWN vendor OAuth application
	// (Google, for Sheets). Optional: nil simply does not mount the service,
	// and the Console then hides the Integrations card.
	OAuthClients *OAuthClientServer
	// Connections manages workspace-level Connections (connect Google once;
	// pipelines and the query engine both consume it). Optional: nil does not
	// mount the service, and the Console hides the surface.
	Connections *ConnectionServer
}

// RegisterWorkspacePlane mounts the workspace plane's Connect services on the
// public mux.
//
// PipelineService and TransformationService carry worker-facing RPCs in the
// same proto service as the user-facing ones, so they are mounted here too;
// those RPCs deny every caller unless the handler was built by
// NewInternal*Server (workerAuth), which only RegisterWorkspaceInternal does.
// Handing one of those instances to the public mux would re-open the leak,
// so — as with the internal mux — that is a startup panic, not a subtle bug.
func RegisterWorkspacePlane(mux *http.ServeMux, s WorkspacePlaneServers, opts connect.HandlerOption) {
	if s.Pipelines == nil || s.Pipelines.worker.internal {
		panic("RegisterWorkspacePlane: Pipelines must be a public-mux PipelineServer")
	}
	if s.Transformations == nil || s.Transformations.worker.internal {
		panic("RegisterWorkspacePlane: Transformations must be a public-mux TransformationServer")
	}
	if s.Health == nil {
		panic("RegisterWorkspacePlane: Health must be set")
	}
	// Deliberately without opts: a health check that needs a JWT is useless to
	// the operator asking whether auth itself is up.
	mux.Handle(workspacehealthv1connect.NewHealthServiceHandler(s.Health))
	mux.Handle(lakekeeperuserv1connect.NewLakekeeperUserServiceHandler(s.LakekeeperUsers, opts))
	mux.Handle(warehousev1connect.NewWarehouseServiceHandler(s.Warehouses, opts))
	mux.Handle(snapshotv1connect.NewSnapshotServiceHandler(s.Snapshots, opts))
	mux.Handle(pipelinev1connect.NewPipelineServiceHandler(s.Pipelines, opts))
	mux.Handle(transformationv1connect.NewTransformationServiceHandler(s.Transformations, opts))
	mux.Handle(pipelineassistv1connect.NewPipelineAssistServiceHandler(s.PipelineAssist, opts))
	mux.Handle(assistv1connect.NewAssistServiceHandler(s.Assist, opts))
	mux.Handle(boxrepov1connect.NewBoxRepoServiceHandler(s.BoxRepos, opts))
	mux.Handle(demov1connect.NewDemoServiceHandler(s.Demo, opts))
	mux.Handle(notificationv1connect.NewNotificationServiceHandler(s.Notifications, opts))
	mux.Handle(queryv1connect.NewQueryServiceHandler(s.Query, opts))
	if s.OAuthClients != nil {
		mux.Handle(oauthclientv1connect.NewOAuthClientServiceHandler(s.OAuthClients, opts))
	}
	if s.Connections != nil {
		mux.Handle(connectionv1connect.NewConnectionServiceHandler(s.Connections, opts))
	}
}

// WorkspaceInternalServers bundles the worker-facing handlers (:8081).
// Pipelines and Transformations must come from NewInternalPipelineServer /
// NewInternalTransformationServer — the public-mux instances cannot be reused
// here, and RegisterWorkspaceInternal rejects them.
type WorkspaceInternalServers struct {
	Pipelines       *PipelineServer // worker variant: polls configs + reports runs, no FileDrop
	Transformations *TransformationServer
	// BoxCredentials receives box-deposited git/snapshot/age credentials.
	// The deposit tables live in central Postgres, but the service exists
	// purely for the box — it retires with the Phase 3 physical split, when
	// the editor runs on the box and needs no deposited credentials. Nil
	// skips registration (the box-local workspace_api reads its own
	// credentials from static config and never receives deposits).
	BoxCredentials *BoxCredentialServer
}

// RegisterWorkspaceInternal mounts the worker-facing RPCs on the internal mux.
// Handlers that were not built for this mux are a wiring bug that would
// silently answer every worker poll with PermissionDenied, so it panics at
// startup rather than serving a dead internal API.
func RegisterWorkspaceInternal(mux *http.ServeMux, s WorkspaceInternalServers, opts connect.HandlerOption) {
	if s.Pipelines == nil || !s.Pipelines.worker.internal {
		panic("RegisterWorkspaceInternal: Pipelines must come from NewInternalPipelineServer")
	}
	if s.Transformations == nil || !s.Transformations.worker.internal {
		panic("RegisterWorkspaceInternal: Transformations must come from NewInternalTransformationServer")
	}
	mux.Handle(pipelinev1connect.NewPipelineServiceHandler(s.Pipelines, opts))
	mux.Handle(transformationv1connect.NewTransformationServiceHandler(s.Transformations, opts))
	if s.BoxCredentials != nil {
		mux.Handle(boxcredentialv1connect.NewBoxCredentialServiceHandler(s.BoxCredentials, opts))
	}
}

// RegisterWorkspacePlainHTTP mounts the workspace plane's non-Connect
// endpoints: file-drop upload streaming (browsers cannot client-stream over
// connect-web), the Google OAuth broker redirect pair, and health probes.
//
// workspaces/grants/oauthClients back the Google OAuth flow; all three may be
// nil when googleOAuth is nil (the handlers then answer 501 before touching any
// of them, which is the clean Console fallback to the service-account path).
// The OAuth app is the CUSTOMER's on every tier — they register their own Google
// client and it is looked up per tenant — so this deployment supplies only the
// redirect URL and the state-signing key.
func RegisterWorkspacePlainHTTP(mux *http.ServeMux, logger *slog.Logger, auth core.UserAuth, db *sql.DB,
	workspaces workspace.Resolver, grants workspace.GoogleOAuthGrantStore,
	oauthClients workspace.OAuthClientStore,
	fileDropSvc *workspace.FileDropService, googleOAuth *oauthgoogle.Client, consoleOrigin string,
) {
	mux.HandleFunc("POST /filedrop/{pipelineID}/{filename}", FileDropUploadHandler(logger, auth, fileDropSvc))

	// "Sign in with Google" for Google Sheets sources (outside ConnectRPC:
	// /start returns the consent URL as JSON, /callback is Google's redirect
	// target and postMessages the grant back to the Console popup).
	mux.HandleFunc("GET /oauth/google/start", GoogleOAuthStartHandler(logger, auth, googleOAuth, workspaces, oauthClients))
	mux.HandleFunc("GET /oauth/google/callback", GoogleOAuthCallbackHandler(logger, googleOAuth, grants, oauthClients, consoleOrigin))
	mux.HandleFunc("/healthz", LivenessHandler())
	mux.HandleFunc("/readyz", ReadinessHandler(logger, db))
}

// RegisterWorkspaceBootstrap mounts the pre-authentication discovery document.
//
// Separate from RegisterWorkspacePlainHTTP, and additive on purpose: only a
// deployment that serves exactly ONE workspace can answer this. Central serves
// many, so answering there would mean choosing a tenant before the caller has
// authenticated — the very question this document exists to let the caller
// resolve. Central therefore does not call this, and keeping it a separate
// function says so without a nil argument at a call site that would otherwise
// have to be edited in lockstep with every module bump.
//
// Unauthenticated by design (see WorkspaceBootstrap), and mounted beside the
// probes for the same reason they are: a caller must be able to ask before it
// holds a token.
func RegisterWorkspaceBootstrap(mux *http.ServeMux, logger *slog.Logger, doc *WorkspaceBootstrap) {
	if doc == nil {
		return
	}
	mux.HandleFunc("GET /.well-known/fairtier-workspace", WorkspaceBootstrapHandler(logger, doc))
}

// WorkspaceServiceNames lists the workspace plane's Connect service names for
// gRPC health/reflection. BoxCredentialServiceName is part of the central
// deployment only — callers that skip its registration (the box binary)
// filter it out.
var WorkspaceServiceNames = []string{
	workspacehealthv1connect.HealthServiceName,
	lakekeeperuserv1connect.LakekeeperUserServiceName,
	warehousev1connect.WarehouseServiceName,
	snapshotv1connect.SnapshotServiceName,
	pipelinev1connect.PipelineServiceName,
	pipelineassistv1connect.PipelineAssistServiceName,
	assistv1connect.AssistServiceName,
	boxrepov1connect.BoxRepoServiceName,
	boxcredentialv1connect.BoxCredentialServiceName,
	demov1connect.DemoServiceName,
	notificationv1connect.NotificationServiceName,
	queryv1connect.QueryServiceName,
	transformationv1connect.TransformationServiceName,
}

package server

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/fairtier/workspace-api/workspace"
)

// The box's unauthenticated bootstrap document.
//
// A browser reaching a box has a chicken-and-egg problem: it cannot present a
// token until it knows which Casdoor to get one from, and only the box knows
// that. So this endpoint answers before authentication — which is exactly why
// it must carry nothing but public facts.
//
// Everything here is already public by construction. The issuer and the
// customer domain are public DNS names; the slug is in the hostname the
// request was just sent to; the Casdoor org is a label, not a credential; and
// an OAuth *public-client* id is public by RFC 7636 design (PKCE exists
// precisely so a browser client needs no secret). The same disclosure class as
// the OIDC discovery document Casdoor already serves unauthenticated.
//
// The Workspace this is built from is NOT that class — it carries the OIDC
// client secret, the DuckFlight token and the customer's S3 keys. That is the
// whole risk here, and why BootstrapFromWorkspace copies fields one by one
// instead of embedding or marshalling the Workspace: adding a field to
// Workspace must never widen this document by default. TestBootstrapLeaksNoSecret
// enforces it.
type WorkspaceBootstrap struct {
	Slug           string `json:"slug"`
	CustomerDomain string `json:"customer_domain"`

	// CasdoorIssuer is the OIDC issuer the Console must authenticate
	// against, and the only one this deployment trusts.
	CasdoorIssuer string `json:"casdoor_issuer"`
	CasdoorOrg    string `json:"casdoor_org"`
	// ConsoleClientID is the box Casdoor app the Console starts its PKCE
	// flow with (WORKSPACE_CONSOLE_CLIENT_ID). Empty means the box has no
	// Console app seeded yet; the Console then reports that the workspace
	// advertises no sign-in rather than starting a flow that cannot finish.
	ConsoleClientID string `json:"console_client_id"`

	Capabilities WorkspaceCapabilities `json:"capabilities"`
}

// WorkspaceCapabilities tells the Console which product surfaces this
// deployment can actually serve, so it can hide the rest.
//
// On a self-hosted box this document is the ONLY discovery path — there is no
// control plane to ask — which is why the capabilities travel with it rather
// than coming from GetCustomerStatus.
type WorkspaceCapabilities struct {
	// ControlPlane is always false here: a deployment serving this endpoint
	// is a box, and a box has no billing, provisioning or central identity.
	// It is stated rather than implied so the Console has one field to read
	// on both paths.
	ControlPlane bool `json:"control_plane"`

	Rill       bool `json:"rill"`
	Cube       bool `json:"cube"`
	DuckFlight bool `json:"duckflight"`
	FileDrop   bool `json:"filedrop"`

	// GoogleOAuth reports only that this deployment CAN run the flow — it has a
	// redirect URL and a state key. Whether the customer has connected their own
	// Google app is a separate, mutable fact and deliberately not here: this
	// document is built once at startup, so a value that changes the moment
	// someone saves a client pair would be stale for the life of the process.
	// The Console reads that half from OAuthClientService.GetOAuthClient.
	GoogleOAuth bool `json:"google_oauth"`
}

// BootstrapFromWorkspace selects the public subset of a workspace.
//
// fileDrop and googleOAuth are passed in rather than derived because they are
// wiring facts, not workspace facts: both services are nil-able at startup and
// answer 501 when absent, so the capability has to follow what was actually
// mounted.
func BootstrapFromWorkspace(ws *workspace.Workspace, consoleClientID string, fileDrop, googleOAuth bool) *WorkspaceBootstrap {
	return &WorkspaceBootstrap{
		Slug:            ws.Slug,
		CustomerDomain:  ws.CustomerDomain,
		CasdoorIssuer:   ws.CasdoorIssuer,
		CasdoorOrg:      ws.CasdoorOrg,
		ConsoleClientID: consoleClientID,
		Capabilities: WorkspaceCapabilities{
			ControlPlane: false,
			Rill:         ws.RillEnabled,
			Cube:         ws.CubeEnabled,
			DuckFlight:   ws.DuckFlightURL != "",
			FileDrop:     fileDrop,
			GoogleOAuth:  googleOAuth,
		},
	}
}

// WorkspaceBootstrapHandler serves the document at
// /.well-known/fairtier-workspace.
//
// Deliberately plain HTTP rather than a Connect RPC: it is fetched before any
// client exists to make an RPC with, and keeping it out of the proto surface
// means changing it costs no tag → image → go get → codegen round trip.
func WorkspaceBootstrapHandler(logger *slog.Logger, doc *WorkspaceBootstrap) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// The Console caches this for its session; a short public cache
		// keeps a reload from re-fetching it while still letting a redeploy
		// that changes the client id take effect within the minute.
		w.Header().Set("Cache-Control", "public, max-age=60")
		if err := json.NewEncoder(w).Encode(doc); err != nil && logger != nil {
			logger.WarnContext(r.Context(), "write workspace bootstrap document", "err", err)
		}
	}
}

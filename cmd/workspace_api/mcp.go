package main

import (
	"cmp"
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/duckflight"
	"github.com/fairtier/workspace-api/mcpserver"
	"github.com/fairtier/workspace-api/server"
	"github.com/fairtier/workspace-api/version"
	"github.com/fairtier/workspace-api/workspace"
)

// mcpConfig is the MCP endpoint's env:
//
//	MCP_ENABLED=true   serve /mcp and AccessTokenService (off by default)
//	WORKSPACE_API_URL  this API's public base URL, from which the MCP
//	                   resource identifier (<url>/mcp) is derived. Defaults
//	                   to https://api.<customer domain>.
//	MCP_REQUIRE_READONLY_ENGINE=true
//	                   refuse agent SQL when DuckFlight has no read-only
//	                   session, instead of falling back to the lexical
//	                   guard alone (off by default during the PoC; the
//	                   recommended production setting once DuckFlight with
//	                   read-only sessions is deployed)
//
// The read-only DuckFlight credential itself
// (WORKSPACE_DUCKFLIGHT_READONLY_AUTH_TOKEN) is a workspace fact, read with
// the other WORKSPACE_DUCKFLIGHT_* env in loadStaticWorkspace.
type mcpConfig struct {
	Enabled               bool
	APIURL                string
	RequireReadOnlyEngine bool
}

func loadMCPConfig(getenv func(string) string, ws *workspace.Workspace) mcpConfig {
	return mcpConfig{
		Enabled:               getenv("MCP_ENABLED") == "true",
		APIURL:                strings.TrimSuffix(cmp.Or(getenv("WORKSPACE_API_URL"), "https://api."+ws.CustomerDomain), "/"),
		RequireReadOnlyEngine: getenv("MCP_REQUIRE_READONLY_ENGINE") == "true",
	}
}

// endpointURL is the MCP endpoint's public URL.
func (c mcpConfig) endpointURL() string { return c.APIURL + mcpserver.Path }

// advertise puts the endpoint in the bootstrap document while it is served.
func (c mcpConfig) advertise(doc *server.WorkspaceBootstrap) {
	if c.Enabled {
		doc.AdvertiseMCP(c.endpointURL())
	}
}

// buildMCPQuery is the QueryServer behind the MCP SQL tools: the Console's
// handlers, but every statement in a DuckFlight read-only session (see
// server.ReadOnlySessions). The Console keeps its own read-write instance.
func buildMCPQuery(cfg mcpConfig, logger *slog.Logger, resolver workspace.Resolver, client *duckflight.Client) *server.QueryServer {
	return &server.QueryServer{
		Workspaces: resolver,
		Executor:   client,
		ReadOnly:   &server.ReadOnlySessions{Strict: cfg.RequireReadOnlyEngine, Prober: client, Logger: logger},
	}
}

// sqlSafety reports q's mode in the shape workspace_status serves.
func sqlSafety(q *server.QueryServer) func(context.Context) mcpserver.SQLSafety {
	return func(ctx context.Context) mcpserver.SQLSafety {
		return toSQLSafety(q.AgentSQLMode(ctx))
	}
}

func toSQLSafety(mode server.AgentSQLMode, detail string) mcpserver.SQLSafety {
	by := string(mode)
	if mode.EngineEnforced() {
		by = "engine"
	}
	return mcpserver.SQLSafety{EnforcedBy: by, Mode: string(mode), Detail: detail}
}

// logAgentSQLMode says at startup what keeps agent SQL read-only. It asks the
// engine, so it runs in the background: an engine that is still starting
// must not hold up this one. The engine is asked again later anyway — this
// line is for the operator, not a decision.
// Nothing is logged while MCP is disabled.
func logAgentSQLMode(ctx context.Context, cfg mcpConfig, logger *slog.Logger, q *server.QueryServer, ws *workspace.Workspace) {
	if !cfg.Enabled {
		return
	}
	strict := cfg.RequireReadOnlyEngine
	mode, detail := q.ProbeAgentSQLMode(ctx, ws)
	switch {
	case mode.EngineEnforced():
		logger.Info("MCP agent SQL is engine-enforced read-only (DuckFlight read-only session, behind the lexical guard)", "mode", string(mode))
	case mode == server.AgentSQLUnknown:
		logger.Warn("MCP agent SQL mode unknown at startup; it is decided per statement once the query engine answers", "detail", detail, "require_readonly_engine", strict)
	default:
		// The downgrade itself was logged by ReadOnlySessions; this line
		// states the startup result in the same words for every mode.
		logger.Warn("MCP agent SQL is NOT engine-enforced read-only", "mode", string(mode), "detail", detail, "require_readonly_engine", strict)
	}
}

// buildMCP wires the MCP endpoint and the token service it authenticates
// with. Both exist only when MCP_ENABLED=true: the returned AccessTokenServer
// is then mounted with the rest of the workspace plane, and mount adds /mcp
// plus its protected-resource metadata. Disabled, the server is nil (not
// mounted) and mount does nothing.
//
// Access tokens are accepted by /mcp alone. The ConnectRPC services keep
// authenticating sign-in tokens only — that is deliberate, and it is what
// keeps a leaked access token from managing tokens.
func buildMCP(cfg mcpConfig, logger *slog.Logger, ws *workspace.Workspace, resolver workspace.Resolver,
	tokens workspace.AccessTokenStore, userAuth core.UserAuth, backends mcpserver.Backends, origins []string,
) (*server.AccessTokenServer, func(*http.ServeMux)) {
	if !cfg.Enabled {
		logger.Info("MCP endpoint disabled (MCP_ENABLED is not true)")
		return nil, func(*http.ServeMux) {}
	}
	pats := &workspace.AccessTokenService{Workspaces: resolver, Tokens: tokens, Logger: logger}
	opts := mcpserver.Options{
		ResourceURL:          cfg.endpointURL(),
		AuthorizationServers: []string{ws.CasdoorIssuer},
		AllowedOrigins:       origins,
		Verifier:             mcpserver.NewTokenVerifier(pats, userAuth),
		Version:              version.Binary(),
		Logger:               logger,
	}
	logger.Info("MCP endpoint enabled", "resource", opts.ResourceURL, "require_readonly_engine", cfg.RequireReadOnlyEngine)
	return &server.AccessTokenServer{Service: pats}, func(mux *http.ServeMux) {
		mcpserver.Register(mux, backends, opts)
	}
}

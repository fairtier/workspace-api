package main

import (
	"cmp"
	"log/slog"
	"net/http"
	"strings"

	"github.com/fairtier/workspace-api/core"
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
type mcpConfig struct {
	Enabled bool
	APIURL  string
}

func loadMCPConfig(getenv func(string) string, ws *workspace.Workspace) mcpConfig {
	return mcpConfig{
		Enabled: getenv("MCP_ENABLED") == "true",
		APIURL:  strings.TrimSuffix(cmp.Or(getenv("WORKSPACE_API_URL"), "https://api."+ws.CustomerDomain), "/"),
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
		ResourceURL:          cfg.APIURL + mcpserver.Path,
		AuthorizationServers: []string{ws.CasdoorIssuer},
		AllowedOrigins:       origins,
		Verifier:             mcpserver.NewTokenVerifier(pats, userAuth),
		Version:              version.Binary(),
		Logger:               logger,
	}
	logger.Info("MCP endpoint enabled", "resource", opts.ResourceURL)
	return &server.AccessTokenServer{Service: pats}, func(mux *http.ServeMux) {
		mcpserver.Register(mux, backends, opts)
	}
}

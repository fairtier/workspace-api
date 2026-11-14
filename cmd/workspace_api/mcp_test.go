package main

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/duckflight"
	"github.com/fairtier/workspace-api/mcpserver"
	"github.com/fairtier/workspace-api/server"
	"github.com/fairtier/workspace-api/workspace"
)

func TestLoadMCPConfig(t *testing.T) {
	ws := &workspace.Workspace{CustomerDomain: "customer-acme.example.com"}
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

	cfg := loadMCPConfig(env(nil), ws)
	if cfg.Enabled {
		t.Error("MCP is on by default")
	}
	if cfg.APIURL != "https://api.customer-acme.example.com" {
		t.Errorf("default API URL = %q", cfg.APIURL)
	}
	cfg = loadMCPConfig(env(map[string]string{"MCP_ENABLED": "true", "WORKSPACE_API_URL": "https://data.example.com/"}), ws)
	if !cfg.Enabled || cfg.APIURL != "https://data.example.com" {
		t.Errorf("cfg = %+v", cfg)
	}
	if loadMCPConfig(env(map[string]string{"MCP_ENABLED": "1"}), ws).Enabled {
		t.Error(`MCP_ENABLED=1 enabled it; only "true" does`)
	}
	if cfg.RequireReadOnlyEngine {
		t.Error("MCP_REQUIRE_READONLY_ENGINE is on by default")
	}
	if !loadMCPConfig(env(map[string]string{"MCP_REQUIRE_READONLY_ENGINE": "true"}), ws).RequireReadOnlyEngine {
		t.Error("MCP_REQUIRE_READONLY_ENGINE=true did not turn strict mode on")
	}
}

func TestBuildMCPQueryIsReadOnly(t *testing.T) {
	q := buildMCPQuery(mcpConfig{RequireReadOnlyEngine: true}, slog.New(slog.DiscardHandler), &workspace.StaticResolver{}, duckflight.NewClient())
	if q.ReadOnly == nil || !q.ReadOnly.Strict {
		t.Fatalf("MCP query server = %+v, want read-only sessions in strict mode", q)
	}
}

func TestToSQLSafety(t *testing.T) {
	for mode, want := range map[server.AgentSQLMode]string{
		server.AgentSQLEngineCredential: "engine",
		server.AgentSQLEngineHeader:     "engine",
		server.AgentSQLGuardOnly:        "guard_only",
		server.AgentSQLRefused:          "refused",
		server.AgentSQLUnknown:          "unknown",
	} {
		if got := toSQLSafety(mode, "").EnforcedBy; got != want {
			t.Errorf("%s: enforced_by = %q, want %q", mode, got, want)
		}
	}
}

func TestBuildMCP(t *testing.T) {
	ws := &workspace.Workspace{Slug: "acme", CasdoorIssuer: "https://auth.example.com"}
	resolver := &workspace.StaticResolver{Workspace: *ws}
	logger := slog.New(slog.DiscardHandler)
	serve := func(mount func(*http.ServeMux), path string) int {
		mux := http.NewServeMux()
		mount(mux)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}")))
		return rec.Code
	}

	tokens, mount := buildMCP(mcpConfig{}, logger, ws, resolver, &workspace.MemoryAccessTokenStore{}, core.UserAuth{}, mcpserver.Backends{}, nil)
	if tokens != nil {
		t.Error("AccessTokenService built while MCP is disabled")
	}
	if code := serve(mount, "/mcp"); code != http.StatusNotFound {
		t.Errorf("disabled: /mcp answered %d, want 404", code)
	}

	tokens, mount = buildMCP(mcpConfig{Enabled: true, APIURL: "https://api.example.com"}, logger, ws, resolver, &workspace.MemoryAccessTokenStore{}, core.UserAuth{}, mcpserver.Backends{}, nil)
	if tokens == nil {
		t.Fatal("no AccessTokenService while MCP is enabled")
	}
	if code := serve(mount, "/mcp"); code != http.StatusUnauthorized {
		t.Errorf("enabled: unauthenticated /mcp answered %d, want 401", code)
	}
}

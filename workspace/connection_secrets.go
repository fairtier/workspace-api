package workspace

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

// BoxSecretMinter produces dynamically-minted box secrets, merged into the
// FetchBoxSecrets response on top of the static box_secrets rows. Where a
// static row holds a value someone stored, a minted key is derived fresh at
// fetch time — which is what makes short-lived credentials (a ~1h Google
// access token) deliverable over a */15 sync loop: every fetch carries a
// token with most of its lifetime ahead of it.
type BoxSecretMinter interface {
	// MintBoxSecrets returns the minted keys for one tenant. A key that
	// cannot be minted right now is omitted, never errored: the box-side sync
	// script skips a Secret whose keys are incomplete and leaves the previous
	// (still valid) one in place, so omission degrades gracefully while an
	// error would starve unrelated secrets in the same fetch.
	MintBoxSecrets(ctx context.Context, customerSlug string) (map[string]string, error)
}

// BoxSecretKeyDuckFlightReconcileSQL is the box-secret key holding the SQL
// that DuckFlight's reconcile watcher executes (LOAD + CREATE OR REPLACE
// SECRET statements for the workspace's live connections). The box-secrets
// mapping turns it into the duckflight-reconcile-sql Secret mounted into the
// engine.
const BoxSecretKeyDuckFlightReconcileSQL = "duckflight_reconcile_sql"

// ConnectionBoxSecrets renders the workspace's connections into engine-facing
// box secrets. Today that is one key: the DuckFlight reconcile SQL carrying a
// freshly minted Google access token per google connection (query-time
// federation via the gsheets extension).
//
// The SQL is rendered HERE, centrally, so the engine and the box manifests
// stay source-agnostic: DuckFlight just executes whatever the file says.
type ConnectionBoxSecrets struct {
	Connections ConnectionStore
	// OAuthClients resolves the customer's own OAuth app — the pair the
	// refresh token was issued to. Same store the pipeline paths use.
	OAuthClients OAuthClientStore
	// Google mints access tokens from refresh tokens (oauthgoogle adapter).
	Google GoogleTokenMinter
	Logger *slog.Logger
}

// MintBoxSecrets renders the reconcile SQL for the tenant's active google
// connections. No google connection (or nothing mintable) → no key, by the
// omission contract above.
//
// PoC limitation, documented in docs/plans/query-time-federation.md: DuckDB
// gsheet secrets are engine-wide, not per-sheet, so ONE google connection
// drives the engine token. The first active one wins; scoping to multiple
// Google accounts is a follow-up.
func (m *ConnectionBoxSecrets) MintBoxSecrets(ctx context.Context, customerSlug string) (map[string]string, error) {
	if m.Connections == nil || m.Google == nil || m.OAuthClients == nil {
		return nil, nil
	}
	conns, err := m.Connections.ListConnections(ctx, customerSlug)
	if err != nil {
		return nil, fmt.Errorf("list connections: %w", err)
	}

	for _, c := range conns {
		if c.Type != ConnectionTypeGoogle || c.Status != "active" {
			continue
		}
		sql, err := m.renderGoogleReconcileSQL(ctx, &c)
		if err != nil {
			// Log-and-omit: the previous Secret (with its still-valid token)
			// stays in place on the box. The connection id is not a secret.
			if m.Logger != nil {
				m.Logger.WarnContext(ctx, "mint duckflight reconcile sql",
					"slug", customerSlug, "connection", c.ID, "err", err)
			}
			return nil, nil
		}
		return map[string]string{BoxSecretKeyDuckFlightReconcileSQL: sql}, nil
	}
	return nil, nil
}

// renderGoogleReconcileSQL mints a fresh access token for the connection and
// renders the engine SQL: load the gsheets extension, then create/replace the
// engine-wide gsheet secret. Idempotent and safe to re-execute; DuckFlight
// re-runs it whenever the content changes (each mint changes the token, so
// every sync cycle refreshes the engine).
func (m *ConnectionBoxSecrets) renderGoogleReconcileSQL(ctx context.Context, c *Connection) (string, error) {
	gc, err := c.googleCredentials()
	if err != nil {
		return "", err
	}
	client, err := m.OAuthClients.GetOAuthClient(ctx, c.CustomerSlug, OAuthProviderGoogle)
	if err != nil {
		return "", fmt.Errorf("resolve oauth client: %w", err)
	}
	if gc.ClientID != "" && gc.ClientID != client.ClientID {
		// The customer swapped their Google app; tokens minted by the old one
		// are dead. Same staleness rule as pipeline credentials.
		return "", fmt.Errorf("connection was authorized with a different OAuth client; reconnect Google")
	}
	token, _, err := m.Google.AccessToken(ctx, gc.RefreshToken, client.ClientID, client.ClientSecret)
	if err != nil {
		return "", fmt.Errorf("mint access token: %w", err)
	}

	var b strings.Builder
	b.WriteString("-- Rendered by FairTier from this workspace's Connections. Do not edit;\n")
	b.WriteString("-- refreshed every sync cycle with a newly minted access token.\n")
	b.WriteString("LOAD gsheets;\n")
	b.WriteString("CREATE OR REPLACE SECRET gsheets_token (TYPE gsheet, PROVIDER access_token, TOKEN ")
	b.WriteString(quoteSQLLiteral(token))
	b.WriteString(");\n")
	return b.String(), nil
}

// quoteSQLLiteral renders s as a single-quoted DuckDB string literal, doubling
// embedded quotes — the server-side twin of the engine's own quoteLiteral.
// The token is machine-minted and should never contain a quote, but the SQL
// crosses a trust boundary into the shared engine, so it is escaped anyway.
func quoteSQLLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

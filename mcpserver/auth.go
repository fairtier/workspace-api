package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/auth"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

// The MCP endpoint is an OAuth 2.1 resource server in the MCP sense: it
// accepts a bearer token, answers 401 with a WWW-Authenticate challenge that
// points at its RFC 9728 metadata, and never issues tokens itself. Two kinds
// of bearer are accepted:
//
//   - workspace access tokens (ftw_…), minted through AccessTokenService and
//     carrying explicit scopes;
//   - the box Casdoor's own sign-in JWTs, verified exactly as the ConnectRPC
//     interceptor verifies them. A signed-in person is a full workspace member
//     everywhere else, so such a token carries both scopes here too.

// PATAuthenticator verifies a workspace access token.
type PATAuthenticator interface {
	AuthenticateAccessToken(ctx context.Context, secret string) (*workspace.AccessToken, error)
}

// JWTAuthenticator verifies a box sign-in token. core.UserAuth implements it.
type JWTAuthenticator interface {
	Authenticate(ctx context.Context, token string) (core.Identity, error)
}

// Caller is who an MCP request acts as, stored in auth.TokenInfo.Extra.
type Caller struct {
	UserID  core.UserID
	Profile core.TokenProfile
	// Via names the credential for audit trails in commit messages, never
	// the secret: "access token ftw_1a2b3c4d (laptop)" or "sign-in token".
	Via string
}

const callerKey = "fairtier.caller"

func callerFromTokenInfo(ti *auth.TokenInfo) (Caller, bool) {
	if ti == nil || ti.Extra == nil {
		return Caller{}, false
	}
	c, ok := ti.Extra[callerKey].(Caller)
	return c, ok && c.UserID != ""
}

func hasScope(ti *auth.TokenInfo, scope string) bool {
	return ti != nil && workspace.HasAccessScope(ti.Scopes, scope)
}

// NewTokenVerifier returns the auth.TokenVerifier /mcp authenticates with.
// Either authenticator may be nil, which disables that kind of bearer.
func NewTokenVerifier(pats PATAuthenticator, jwts JWTAuthenticator) auth.TokenVerifier {
	return func(ctx context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
		if workspace.LooksLikeAccessToken(token) {
			return verifyPAT(ctx, pats, token)
		}
		return verifyJWT(ctx, jwts, token)
	}
}

func verifyPAT(ctx context.Context, pats PATAuthenticator, token string) (*auth.TokenInfo, error) {
	if pats == nil {
		return nil, fmt.Errorf("%w: access tokens are not enabled", auth.ErrInvalidToken)
	}
	t, err := pats.AuthenticateAccessToken(ctx, token)
	if errors.Is(err, workspace.ErrInvalidAccessToken) {
		return nil, fmt.Errorf("%w: unknown, revoked or expired access token", auth.ErrInvalidToken)
	}
	if err != nil {
		// A store outage: a 500, so the client retries rather than
		// discarding a token that may be perfectly good.
		return nil, errors.New("access token verification is unavailable")
	}
	caller := Caller{
		UserID: t.UserSub,
		// No profile claims: the token row is not an identity document.
		// Commit attribution falls back to the platform identity, and
		// the commit message names the token instead (see Caller.Via).
		Profile: core.TokenProfile{Subject: string(t.UserSub)},
		Via:     fmt.Sprintf("access token %s… (%s)", t.Prefix, t.Name),
	}
	return &auth.TokenInfo{
		Scopes:     slices.Clone(t.Scopes),
		Expiration: t.ExpiresAt,
		UserID:     string(t.UserSub),
		Extra:      map[string]any{callerKey: caller},
	}, nil
}

func verifyJWT(ctx context.Context, jwts JWTAuthenticator, token string) (*auth.TokenInfo, error) {
	if jwts == nil {
		return nil, fmt.Errorf("%w: sign-in tokens are not accepted", auth.ErrInvalidToken)
	}
	id, err := jwts.Authenticate(ctx, token)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", auth.ErrInvalidToken, err)
	}
	return &auth.TokenInfo{
		Scopes:     []string{workspace.ScopeWorkspaceRead, workspace.ScopeWorkspaceWrite},
		Expiration: id.ExpiresAt,
		UserID:     string(id.UserID),
		Extra:      map[string]any{callerKey: Caller{UserID: id.UserID, Profile: id.Profile, Via: "sign-in token"}},
	}, nil
}

// requireAllowedOrigin implements the MCP transport's Origin rule: a request
// that carries an Origin must come from an allowed one, or a web page the
// person happens to visit could drive their local agent's token (DNS
// rebinding and plain cross-site POSTs alike). Requests without an Origin —
// every native MCP client — pass. The allowlist is the API's own CORS list,
// so the only browser origin admitted is the workspace Console's.
func requireAllowedOrigin(allowed []string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && !slices.ContainsFunc(allowed, func(a string) bool { return strings.EqualFold(a, origin) }) {
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

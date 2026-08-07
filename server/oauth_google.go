package server

import (
	"encoding/json"
	"errors"
	"html/template"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/fairtier/workspace-api/oauthgoogle"
	"github.com/fairtier/workspace-api/workspace"
)

// grantTTL bounds how long a minted OAuth grant may sit unredeemed before the
// wizard must reconnect. Matches the state TTL in the oauthgoogle package.
const grantTTL = 15 * time.Minute

// oauthClientNotConfiguredCode is returned to the Console alongside HTTP 412 so
// it can tell "this workspace has not connected a Google app yet" (a setup step
// the user can complete) apart from a 501 "this server cannot do OAuth at all"
// (nothing the user can do). Collapsing the two would hide the Connect button
// exactly when it should be inviting the customer to Integrations.
const oauthClientNotConfiguredCode = "oauth_client_not_configured"

// GoogleOAuthStartHandler begins the "Sign in with Google" flow for a Google
// Sheets pipeline source: GET /oauth/google/start. It is JWT-authed like the
// RPC mux, resolves the caller's tenant and that tenant's own OAuth app, mints a
// signed state, and returns the Google consent URL as JSON {"auth_url": "..."}.
// The Console opens that URL in a popup.
//
// Two distinct refusals: 501 when the deployment has no OAuth configuration at
// all (Console falls back to the service-account path), 412 when this customer
// has not connected their own Google app yet.
func GoogleOAuthStartHandler(logger *slog.Logger, auth UserAuth, client *oauthgoogle.Client, workspaces workspace.Resolver, clients workspace.OAuthClientStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if client == nil || clients == nil {
			writeOAuthError(w, http.StatusNotImplemented, "Sign in with Google is not configured on this server")
			return
		}

		userID, err := auth.UserIDFromBearer(ctx, r.Header.Get("Authorization"))
		if err != nil {
			writeOAuthError(w, http.StatusUnauthorized, "authentication required")
			return
		}

		ws, err := workspaces.GetWorkspaceByUser(ctx, userID)
		if err != nil {
			writeOAuthError(w, http.StatusForbidden, "no tenant for this user")
			return
		}

		cc, err := clients.GetOAuthClient(ctx, ws.Slug, workspace.OAuthProviderGoogle)
		if errors.Is(err, workspace.ErrOAuthClientNotFound) {
			writeOAuthErrorCode(w, http.StatusPreconditionFailed,
				"connect your own Google OAuth app first", oauthClientNotConfiguredCode)
			return
		}
		if err != nil {
			logger.ErrorContext(ctx, "oauth: load customer client", "err", err, "slug", ws.Slug)
			writeOAuthError(w, http.StatusInternalServerError, "could not start sign-in")
			return
		}

		state, err := client.SignState(string(userID), ws.Slug)
		if err != nil {
			logger.ErrorContext(ctx, "oauth: sign state", "err", err)
			writeOAuthError(w, http.StatusInternalServerError, "could not start sign-in")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"auth_url": client.AuthURL(state, cc.ClientID)})
	}
}

// GoogleOAuthCallbackHandler completes the flow: Google redirects the popup to
// GET /oauth/google/callback?code=...&state=... . There is no Console JWT here
// — trust comes from the signed state. It exchanges the code for a refresh
// token, stores a short-lived grant, and returns a tiny HTML page that
// postMessages {grant_id, email} back to the Console opener (restricted to
// consoleOrigin) and closes. The refresh token never reaches the browser.
func GoogleOAuthCallbackHandler(logger *slog.Logger, client *oauthgoogle.Client, grants workspace.GoogleOAuthGrantStore, clients workspace.OAuthClientStore, consoleOrigin string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if client == nil || clients == nil {
			renderOAuthResult(w, consoleOrigin, oauthResult{Error: "Sign in with Google is not configured"})
			return
		}

		// The user may have denied consent, or Google reported an error.
		if e := r.URL.Query().Get("error"); e != "" {
			renderOAuthResult(w, consoleOrigin, oauthResult{Error: "access to Google Sheets was not granted"})
			return
		}

		state := r.URL.Query().Get("state")
		userSub, slug, err := client.VerifyState(state)
		if err != nil {
			logger.WarnContext(ctx, "oauth: bad state", "err", err)
			renderOAuthResult(w, consoleOrigin, oauthResult{Error: "the sign-in link expired; please try again"})
			return
		}

		code := r.URL.Query().Get("code")
		if code == "" {
			renderOAuthResult(w, consoleOrigin, oauthResult{Error: "missing authorization code"})
			return
		}

		// The exchange must use the same client the consent URL was built with.
		// The state binds the tenant, so this is that customer's own app — not a
		// second lookup that could drift from the first.
		cc, err := clients.GetOAuthClient(ctx, slug, workspace.OAuthProviderGoogle)
		if err != nil {
			logger.ErrorContext(ctx, "oauth: load customer client", "err", err, "slug", slug)
			renderOAuthResult(w, consoleOrigin, oauthResult{Error: "could not complete the Google sign-in"})
			return
		}

		tok, err := client.Exchange(ctx, code, cc.ClientID, cc.ClientSecret)
		if err != nil {
			logger.ErrorContext(ctx, "oauth: code exchange", "err", err)
			renderOAuthResult(w, consoleOrigin, oauthResult{Error: "could not complete the Google sign-in"})
			return
		}

		now := time.Now()
		grant := &workspace.GoogleOAuthGrant{
			GrantID:      uuid.NewString(),
			CustomerSlug: slug,
			UserSub:      userSub,
			RefreshToken: tok.RefreshToken,
			Email:        tok.Email,
			ClientID:     cc.ClientID,
			CreatedAt:    now,
			ExpiresAt:    now.Add(grantTTL),
		}
		if err := grants.CreateGoogleOAuthGrant(ctx, grant); err != nil {
			logger.ErrorContext(ctx, "oauth: store grant", "err", err)
			renderOAuthResult(w, consoleOrigin, oauthResult{Error: "could not save the Google connection"})
			return
		}

		renderOAuthResult(w, consoleOrigin, oauthResult{GrantID: grant.GrantID, Email: grant.Email})
	}
}

func writeOAuthError(w http.ResponseWriter, status int, msg string) {
	writeOAuthErrorCode(w, status, msg, "")
}

// writeOAuthErrorCode adds a stable machine-readable code the Console can branch
// on, so the copy can be reworded without breaking the client.
func writeOAuthErrorCode(w http.ResponseWriter, status int, msg, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body := map[string]string{"error": msg}
	if code != "" {
		body["code"] = code
	}
	_ = json.NewEncoder(w).Encode(body)
}

// oauthResult is the payload postMessaged back to the Console opener. On success
// GrantID + Email are set; on failure Error is set.
type oauthResult struct {
	GrantID string `json:"grant_id,omitempty"`
	Email   string `json:"email,omitempty"`
	Error   string `json:"error,omitempty"`
}

// oauthCallbackTmpl posts the result to the opener (restricted to the Console
// origin) and closes the popup. json.Marshal HTML-escapes <, >, & so the
// embedded payload cannot break out of the <script> block. With no known
// Console origin (CORS_ALLOWED_ORIGINS unset) the message is not sent at
// all — a "*" target would hand the grant ID to whatever page opened the
// popup.
var oauthCallbackTmpl = template.Must(template.New("cb").Parse(`<!doctype html>
<html><head><meta charset="utf-8"><title>Connecting…</title></head>
<body style="font-family:system-ui;padding:2rem;color:#334155">
<p>{{if .Error}}Sign-in failed. You can close this window.{{else}}Connected. You can close this window.{{end}}</p>
<script>
(function () {
  var payload = {{.Payload}};
  var target = {{.Origin}};
  try {
    if (window.opener && target) {
      window.opener.postMessage({ type: "fairtier-google-oauth", result: payload }, target);
    }
  } catch (e) {}
  setTimeout(function () { window.close(); }, 300);
})();
</script>
</body></html>`))

// renderOAuthResult writes the popup HTML that hands the result to the Console.
func renderOAuthResult(w http.ResponseWriter, consoleOrigin string, res oauthResult) {
	payload, err := json.Marshal(res)
	if err != nil {
		payload = []byte(`{"error":"internal error"}`)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// The page must be allowed to run its inline script; it is fully
	// self-contained and posts only to the Console origin.
	_ = oauthCallbackTmpl.Execute(w, map[string]any{
		"Payload": template.JS(payload),
		"Origin":  consoleOrigin,
	})
}

// Package oauthgoogle implements the delegated-user "Sign in with Google" flow
// for the Google-backed pipeline sources — Sheets, and Drive files read through
// the duckdb/gdrive extension: it builds the consent URL, signs/verifies the
// OAuth state parameter, and exchanges an authorization code for a refresh
// token. Which of the two a consent asks for is the caller's choice
// (Capability), so a Sheets pipeline never requests Drive access. It is a thin, no-SDK wrapper over Google's OAuth 2.0 endpoints (same
// plain net/http style as the llm package).
//
// The OAuth application is the CUSTOMER's, not ours: each workspace registers
// its own client in its own Google Cloud project and the pair is looked up per
// tenant (workspace.OAuthClientStore). So this package holds only the
// deployment-wide half of the configuration — the redirect URL Google calls back
// on, and the HMAC key the state parameter is signed with — and takes the client
// pair per call.
//
// That split is deliberate rather than incidental. The refresh token obtained
// here is stored per pipeline and has to be refreshed offline by a worker on the
// customer's own machine, which means the client pair travels with it into the
// customer's own repo. A shared FairTier app would therefore put our identity on
// every customer's box; a BYO client puts theirs on their own.
//
// Note the consequence for the state key: it can no longer be derived from a
// client secret, because there is no single client secret any more.
package oauthgoogle

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/fairtier/workspace-api/core"
)

const (
	defaultAuthEndpoint  = "https://accounts.google.com/o/oauth2/v2/auth"
	defaultTokenEndpoint = "https://oauth2.googleapis.com/token"

	// SheetsReadonlyScope reads spreadsheet values by ID. It is a *sensitive*
	// scope (needs consent-screen verification) but deliberately NOT a drive.*
	// *restricted* scope — reading a sheet by ID needs no Drive access, which
	// keeps the app clear of Google's third-party security assessment.
	SheetsReadonlyScope = core.GoogleSheetsReadonlyScope

	// DriveFileScope reaches individual Drive files — the ones the user picks
	// through Google's own Picker, or that the app itself created — and never
	// the whole Drive. Like Sheets it is a *sensitive* scope and NOT a
	// *restricted* one, so it stays clear of the third-party security
	// assessment that drive.readonly (the gdrive extension's own default)
	// would drag in.
	//
	// Verified 2026-08-27 against gdrive v2026.08.07 on duckdb 1.5.5, under a
	// grant carrying this scope and no other Drive scope: read_csv and
	// read_pdf over gdrive://id:<id> both succeed (files.get 200 +
	// files.media 206). Addressing a file by PATH does not work under it and
	// is not a supported shape here — gdrive://Reports/monthly.pdf resolves
	// each segment with files.list, which sees only the handful of files this
	// grant covers, never the folder they sit in. Drive files are addressed
	// by id. See docs/plans/duckdb-source-ui.md in the platform monorepo.
	DriveFileScope = core.GoogleDriveFileScope

	// baseScopes is what every consent asks for: the Sheets read, plus
	// openid+email so the token response carries an id_token we can read the
	// granting account's email from (for display).
	baseScopes = "openid email " + SheetsReadonlyScope

	// stateTTL bounds how long a consent round-trip may take. Matches the grant
	// TTL on the server side.
	stateTTL = 15 * time.Minute
)

// Capability names an extra authorization a consent may ask for on top of the
// base set. It exists so a Sheets pipeline never triggers a Drive consent
// screen: the caller says what the source actually needs, and only that is
// requested. Widening later is cheap — AuthURL sets include_granted_scopes, so
// a second consent adds Drive to the same account rather than replacing what
// it already granted.
type Capability string

const (
	// CapabilityNone asks for the base set only (sign-in + Sheets).
	CapabilityNone Capability = ""
	// CapabilityDrive additionally asks for DriveFileScope — what a
	// duckdb/gdrive pipeline needs.
	CapabilityDrive Capability = "drive"
)

// ParseCapability maps the wire value (a query parameter) onto a Capability.
// An unknown value is an error rather than a silent downgrade to the base set:
// silently dropping the Drive request would mint a token that fails on the box
// hours later, which is the failure mode the capability exists to remove.
func ParseCapability(s string) (Capability, error) {
	switch Capability(s) {
	case CapabilityNone:
		return CapabilityNone, nil
	case CapabilityDrive:
		return CapabilityDrive, nil
	default:
		return CapabilityNone, fmt.Errorf("oauthgoogle: unknown capability %q", s)
	}
}

// scopes returns the space-separated scope string to request.
func (capability Capability) scopes() string {
	if capability == CapabilityDrive {
		return baseScopes + " " + DriveFileScope
	}
	return baseScopes
}

// AllScopes is every scope this deployment can ever request, which is what the
// customer has to list on their own OAuth consent screen. Requesting a scope
// their app does not declare is refused by Google at the consent, not at the
// call — so the Console shows this list, not the per-capability subset.
func AllScopes() []string {
	return []string{"openid", "email", SheetsReadonlyScope, DriveFileScope}
}

// Client drives the Google OAuth flow. It carries the deployment-wide
// configuration only; the customer's client id and secret are passed to the
// calls that need them.
type Client struct {
	redirectURL string
	stateSecret []byte

	// Overridable for tests; default to Google's real endpoints.
	AuthEndpoint  string
	TokenEndpoint string
	HTTPClient    *http.Client
}

// New builds a Client. Both inputs are required; when either is empty the caller
// should treat OAuth as disabled for the whole deployment and pass a nil *Client
// around (the handlers report 501 and the Console falls back to the
// service-account path). A customer having no OAuth app of their own is a
// different, per-tenant condition — see workspace.ErrOAuthClientNotFound.
func New(redirectURL, stateSecret string) (*Client, error) {
	if redirectURL == "" || stateSecret == "" {
		return nil, errors.New("oauthgoogle: redirect_url and state_secret are both required")
	}
	return &Client{
		redirectURL:   redirectURL,
		stateSecret:   []byte(stateSecret),
		AuthEndpoint:  defaultAuthEndpoint,
		TokenEndpoint: defaultTokenEndpoint,
	}, nil
}

// RedirectURL is the callback URI Google must be configured with. The Console
// shows it so the customer can paste the exact string into their Google Cloud
// OAuth client — it is never guessed browser-side.
func (c *Client) RedirectURL() string { return c.redirectURL }

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: 30 * time.Second}
}

// AuthURL builds the Google consent URL for the given signed state, against the
// customer's own OAuth client, asking for the base scopes plus whatever the
// capability adds. access_type offline + prompt=consent guarantees a
// refresh_token on every grant.
func (c *Client) AuthURL(state, clientID string, capability Capability) string {
	v := url.Values{}
	v.Set("client_id", clientID)
	v.Set("redirect_uri", c.redirectURL)
	v.Set("response_type", "code")
	v.Set("scope", capability.scopes())
	v.Set("access_type", "offline")
	v.Set("prompt", "consent")
	v.Set("include_granted_scopes", "true")
	v.Set("state", state)
	return c.AuthEndpoint + "?" + v.Encode()
}

// stateClaims is the signed OAuth state: it binds the consent round-trip to the
// Console user and their tenant so the callback (which carries no Console JWT)
// can attribute the grant, and the random ID makes it single-use/CSRF-resistant.
type stateClaims struct {
	Slug string `json:"slug"`
	jwt.RegisteredClaims
}

// SignState mints an HMAC-signed state token for a user/tenant.
func (c *Client) SignState(userSub, customerSlug string) (string, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("oauthgoogle: nonce: %w", err)
	}
	now := time.Now()
	claims := stateClaims{
		Slug:      customerSlug,
		Subject:   userSub,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(stateTTL)),
		ID:        base64.RawURLEncoding.EncodeToString(nonce),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString(c.stateSecret)
	if err != nil {
		return "", fmt.Errorf("oauthgoogle: sign state: %w", err)
	}
	return signed, nil
}

// VerifyState validates a state token and returns the bound user and tenant.
func (c *Client) VerifyState(state string) (userSub, customerSlug string, err error) {
	var claims stateClaims
	_, err = jwt.ParseWithClaims(state, &claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return c.stateSecret, nil
	}, jwt.WithExpirationRequired())
	if err != nil {
		return "", "", fmt.Errorf("oauthgoogle: verify state: %w", err)
	}
	if claims.Subject == "" {
		return "", "", errors.New("oauthgoogle: state missing subject")
	}
	return claims.Subject, claims.Slug, nil
}

// TokenResult is the outcome of a successful code exchange.
type TokenResult struct {
	// RefreshToken is the long-lived credential stored per pipeline.
	RefreshToken string
	// Email is the granting Google account, for Console display only.
	Email string
	// Scopes is what Google actually granted, which is not necessarily what
	// was asked for: the consent screen lets the user untick a scope. Recorded
	// with the grant so the Console can tell a Sheets-only account from a
	// Drive-capable one instead of letting a run on the box discover it.
	Scopes []string
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	IDToken      string `json:"id_token"`
	Scope        string `json:"scope"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	Error        string `json:"error"`
	ErrorDesc    string `json:"error_description"`
}

// Exchange trades an authorization code for a refresh token (and reads the
// granting email from the id_token), using the customer's own OAuth client — it
// must be the same pair the consent URL was built with, or Google rejects the
// code. A missing refresh_token is an error: prompt=consent should always yield
// one, and without it the pipeline could not run unattended.
func (c *Client) Exchange(ctx context.Context, code, clientID, clientSecret string) (*TokenResult, error) {
	form := url.Values{}
	form.Set("code", code)
	form.Set("client_id", clientID)
	form.Set("client_secret", clientSecret)
	form.Set("redirect_uri", c.redirectURL)
	form.Set("grant_type", "authorization_code")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.TokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("oauthgoogle: build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("oauthgoogle: token exchange: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return nil, fmt.Errorf("oauthgoogle: decode token response (status %d): %w", resp.StatusCode, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || tr.Error != "" {
		return nil, fmt.Errorf("oauthgoogle: token exchange failed (status %d): %s %s", resp.StatusCode, tr.Error, tr.ErrorDesc)
	}
	if tr.RefreshToken == "" {
		return nil, errors.New("oauthgoogle: no refresh_token returned (the account may need to revoke prior access and re-consent)")
	}
	return &TokenResult{
		RefreshToken: tr.RefreshToken,
		Email:        emailFromIDToken(tr.IDToken),
		Scopes:       strings.Fields(tr.Scope),
	}, nil
}

// emailFromIDToken reads the "email" claim from a Google id_token without
// verifying the signature — the token came straight from Google's token
// endpoint over TLS, and it is used for display only. Returns "" on any problem.
func emailFromIDToken(idToken string) string {
	parts := strings.Split(idToken, ".")
	if len(parts) != 3 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var claims struct {
		Email string `json:"email"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return ""
	}
	return claims.Email
}

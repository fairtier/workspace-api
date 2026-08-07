// Package oauthgoogle implements the delegated-user "Sign in with Google" flow
// for Google Sheets pipeline sources: it builds the consent URL, signs/verifies
// the OAuth state parameter, and exchanges an authorization code for a refresh
// token. It is a thin, no-SDK wrapper over Google's OAuth 2.0 endpoints (same
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
)

const (
	defaultAuthEndpoint  = "https://accounts.google.com/o/oauth2/v2/auth"
	defaultTokenEndpoint = "https://oauth2.googleapis.com/token"

	// SheetsReadonlyScope reads spreadsheet values by ID. It is a *sensitive*
	// scope (needs consent-screen verification) but deliberately NOT a drive.*
	// *restricted* scope — reading a sheet by ID needs no Drive access, which
	// keeps the app clear of Google's third-party security assessment.
	SheetsReadonlyScope = "https://www.googleapis.com/auth/spreadsheets.readonly"

	// scopes also requests openid+email so the token response carries an
	// id_token we can read the granting account's email from (for display).
	scopes = "openid email " + SheetsReadonlyScope

	// stateTTL bounds how long a consent round-trip may take. Matches the grant
	// TTL on the server side.
	stateTTL = 15 * time.Minute
)

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
// customer's own OAuth client. access_type offline + prompt=consent guarantees a
// refresh_token on every grant.
func (c *Client) AuthURL(state, clientID string) string {
	v := url.Values{}
	v.Set("client_id", clientID)
	v.Set("redirect_uri", c.redirectURL)
	v.Set("response_type", "code")
	v.Set("scope", scopes)
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
		Slug: customerSlug,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userSub,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(stateTTL)),
			ID:        base64.RawURLEncoding.EncodeToString(nonce),
		},
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

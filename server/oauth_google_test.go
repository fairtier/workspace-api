package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/oauthgoogle"
	"github.com/fairtier/workspace-api/workspace"
)

type stubGrantStore struct {
	created *workspace.GoogleOAuthGrant
	err     error
}

func (s *stubGrantStore) CreateGoogleOAuthGrant(_ context.Context, g *workspace.GoogleOAuthGrant) error {
	if s.err != nil {
		return s.err
	}
	s.created = g
	return nil
}
func (s *stubGrantStore) ConsumeGoogleOAuthGrant(context.Context, string, string) (*workspace.GoogleOAuthGrant, error) {
	return nil, workspace.ErrOAuthGrantNotFound
}
func (s *stubGrantStore) DeleteExpiredGoogleOAuthGrants(context.Context) (int64, error) {
	return 0, nil
}

// stubOAuthClientStore serves one customer's own OAuth app.
type stubOAuthClientStore struct {
	client *workspace.OAuthClient
	err    error
}

func (s *stubOAuthClientStore) UpsertOAuthClient(context.Context, *workspace.OAuthClient) error {
	return s.err
}
func (s *stubOAuthClientStore) GetOAuthClient(_ context.Context, _, _ string) (*workspace.OAuthClient, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.client == nil {
		return nil, workspace.ErrOAuthClientNotFound
	}
	return s.client, nil
}
func (s *stubOAuthClientStore) DeleteOAuthClient(context.Context, string, string) error { return s.err }

func testCustomerClients() *stubOAuthClientStore {
	return &stubOAuthClientStore{client: &workspace.OAuthClient{
		CustomerSlug: "acme", Provider: workspace.OAuthProviderGoogle,
		ClientID: "cid", ClientSecret: "csecret",
	}}
}

func testOAuthClient(t *testing.T, tokenEndpoint string) *oauthgoogle.Client {
	t.Helper()
	c, err := oauthgoogle.New("https://api.example.com/oauth/google/callback", "secret")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if tokenEndpoint != "" {
		c.TokenEndpoint = tokenEndpoint
	}
	return c
}

func TestGoogleOAuthStart_DisabledReturns501(t *testing.T) {
	h := GoogleOAuthStartHandler(slog.Default(), core.UserAuth{}, nil, nil, nil)
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/oauth/google/start", nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", rec.Code)
	}
}

// startHandlerFor wires a real JWT so the handler reaches the customer-client
// lookup rather than bailing at auth.
func startHandlerFor(t *testing.T, clients workspace.OAuthClientStore) (http.HandlerFunc, string) {
	t.Helper()
	jwks, sign := testJWKS(t)
	const iss = "https://auth.customer-acme.example.com"
	auth := core.UserAuth{JWKS: jwks, Issuer: iss}
	ws := &workspace.Workspace{Slug: "acme"}
	token := sign(jwt.MapClaims{
		"sub": "customer-acme/alice",
		"iss": iss,
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	return GoogleOAuthStartHandler(slog.Default(), auth, testOAuthClient(t, ""), &stubCustomers{ws: ws}, clients), token
}

// A customer who has not connected their own Google app must get a distinct,
// actionable refusal — not the 501 that means "this server cannot do OAuth",
// which the Console renders by hiding the button entirely.
func TestGoogleOAuthStart_NoCustomerClientReturns412(t *testing.T) {
	h, token := startHandlerFor(t, &stubOAuthClientStore{})
	req := httptest.NewRequest(http.MethodGet, "/oauth/google/start", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h(rec, req)

	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("status = %d, want 412: %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["code"] != oauthClientNotConfiguredCode {
		t.Fatalf("code = %q, want %q", body["code"], oauthClientNotConfiguredCode)
	}
}

// The consent URL must carry the CUSTOMER's client id.
func TestGoogleOAuthStart_UsesCustomerClientID(t *testing.T) {
	h, token := startHandlerFor(t, testCustomerClients())
	req := httptest.NewRequest(http.MethodGet, "/oauth/google/start", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	u, err := url.Parse(body["auth_url"])
	if err != nil {
		t.Fatalf("parse auth_url: %v", err)
	}
	if got := u.Query().Get("client_id"); got != "cid" {
		t.Fatalf("consent URL client_id = %q, want the customer's %q", got, "cid")
	}
}

func TestGoogleOAuthCallback_Success(t *testing.T) {
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"email":"user@gmail.com"}`))
	idToken := "h." + payload + ".s"
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"refresh_token": "1//r", "id_token": idToken})
	}))
	defer tokenSrv.Close()

	client := testOAuthClient(t, tokenSrv.URL)
	state, err := client.SignState("customer-acme/alice", "acme")
	if err != nil {
		t.Fatalf("SignState: %v", err)
	}
	store := &stubGrantStore{}
	h := GoogleOAuthCallbackHandler(slog.Default(), client, store, testCustomerClients(), "https://console.fairtier.com")

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/oauth/google/callback?code=abc&state="+state, nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if store.created == nil {
		t.Fatal("no grant stored")
	}
	if store.created.CustomerSlug != "acme" || store.created.RefreshToken != "1//r" || store.created.Email != "user@gmail.com" {
		t.Fatalf("bad grant: %+v", store.created)
	}
	// The grant records which of the customer's apps minted the token; without
	// it a later app swap cannot be told from a working connection.
	if store.created.ClientID != "cid" {
		t.Fatalf("grant did not record the minting client: %+v", store.created)
	}
	body := rec.Body.String()
	if !strings.Contains(body, store.created.GrantID) {
		t.Fatalf("callback HTML missing grant_id: %s", body)
	}
	if strings.Contains(body, "1//r") {
		t.Fatalf("refresh token leaked into the browser page: %s", body)
	}
	if !strings.Contains(body, "https://console.fairtier.com") {
		t.Fatalf("postMessage target origin missing: %s", body)
	}
}

func TestGoogleOAuthCallback_Denied(t *testing.T) {
	client := testOAuthClient(t, "")
	store := &stubGrantStore{}
	h := GoogleOAuthCallbackHandler(slog.Default(), client, store, testCustomerClients(), "https://console.fairtier.com")

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/oauth/google/callback?error=access_denied&state=x", nil))

	if store.created != nil {
		t.Fatal("grant stored despite denial")
	}
	if !strings.Contains(rec.Body.String(), "error") {
		t.Fatalf("expected an error result in the page: %s", rec.Body.String())
	}
}

func TestGoogleOAuthCallback_BadState(t *testing.T) {
	client := testOAuthClient(t, "")
	store := &stubGrantStore{}
	h := GoogleOAuthCallbackHandler(slog.Default(), client, store, testCustomerClients(), "https://console.fairtier.com")

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/oauth/google/callback?code=abc&state=tampered", nil))

	if store.created != nil {
		t.Fatal("grant stored despite bad state")
	}
}

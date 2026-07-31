package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

func testOAuthClient(t *testing.T, tokenEndpoint string) *oauthgoogle.Client {
	t.Helper()
	c, err := oauthgoogle.New("cid", "csecret", "https://api.example.com/oauth/google/callback", "secret")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if tokenEndpoint != "" {
		c.TokenEndpoint = tokenEndpoint
	}
	return c
}

func TestGoogleOAuthStart_DisabledReturns501(t *testing.T) {
	h := GoogleOAuthStartHandler(slog.Default(), UserAuth{}, nil, nil)
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/oauth/google/start", nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", rec.Code)
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
	h := GoogleOAuthCallbackHandler(slog.Default(), client, store, "https://console.fairtier.com")

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
	h := GoogleOAuthCallbackHandler(slog.Default(), client, store, "https://console.fairtier.com")

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
	h := GoogleOAuthCallbackHandler(slog.Default(), client, store, "https://console.fairtier.com")

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/oauth/google/callback?code=abc&state=tampered", nil))

	if store.created != nil {
		t.Fatal("grant stored despite bad state")
	}
}

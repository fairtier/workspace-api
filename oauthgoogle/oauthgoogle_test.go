package oauthgoogle

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func newTestClient(t *testing.T) *Client {
	t.Helper()
	c, err := New("cid", "csecret", "https://api.example.com/oauth/google/callback", "state-secret")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestNewRequiresAllFields(t *testing.T) {
	if _, err := New("", "s", "r", "k"); err == nil {
		t.Fatal("expected error for empty client_id")
	}
	if _, err := New("c", "s", "r", ""); err == nil {
		t.Fatal("expected error for empty state_secret")
	}
}

func TestStateRoundTrip(t *testing.T) {
	c := newTestClient(t)
	state, err := c.SignState("customer-acme/alice", "acme")
	if err != nil {
		t.Fatalf("SignState: %v", err)
	}
	sub, slug, err := c.VerifyState(state)
	if err != nil {
		t.Fatalf("VerifyState: %v", err)
	}
	if sub != "customer-acme/alice" || slug != "acme" {
		t.Fatalf("got sub=%q slug=%q", sub, slug)
	}
}

func TestVerifyStateRejectsTampered(t *testing.T) {
	c := newTestClient(t)
	other, _ := New("cid", "csecret", "https://api.example.com/oauth/google/callback", "different-secret")
	state, _ := c.SignState("sub", "acme")
	if _, _, err := other.VerifyState(state); err == nil {
		t.Fatal("expected verification failure under a different secret")
	}
}

func TestAuthURL(t *testing.T) {
	c := newTestClient(t)
	raw := c.AuthURL("the-state")
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	q := u.Query()
	if q.Get("access_type") != "offline" || q.Get("prompt") != "consent" {
		t.Fatalf("missing offline/consent: %v", q)
	}
	if q.Get("state") != "the-state" || q.Get("client_id") != "cid" {
		t.Fatalf("bad state/client_id: %v", q)
	}
	if !strings.Contains(q.Get("scope"), SheetsReadonlyScope) {
		t.Fatalf("scope missing sheets readonly: %q", q.Get("scope"))
	}
}

func TestExchange(t *testing.T) {
	// id_token with an email claim (payload only needs to decode; signature unused).
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"email":"user@gmail.com"}`))
	idToken := "h." + payload + ".s"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
		}
		if r.Form.Get("code") != "auth-code" || r.Form.Get("grant_type") != "authorization_code" {
			t.Errorf("bad form: %v", r.Form)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "at",
			"refresh_token": "1//refresh",
			"id_token":      idToken,
		})
	}))
	defer srv.Close()

	c := newTestClient(t)
	c.TokenEndpoint = srv.URL

	res, err := c.Exchange(context.Background(), "auth-code")
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if res.RefreshToken != "1//refresh" {
		t.Fatalf("refresh token: %q", res.RefreshToken)
	}
	if res.Email != "user@gmail.com" {
		t.Fatalf("email: %q", res.Email)
	}
}

func TestExchangeMissingRefreshToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "at"})
	}))
	defer srv.Close()

	c := newTestClient(t)
	c.TokenEndpoint = srv.URL
	if _, err := c.Exchange(context.Background(), "code"); err == nil {
		t.Fatal("expected error when no refresh_token returned")
	}
}

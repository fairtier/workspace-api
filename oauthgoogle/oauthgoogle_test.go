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
	c, err := New("https://api.example.com/oauth/google/callback", "state-secret")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestNewRequiresAllFields(t *testing.T) {
	if _, err := New("", "k"); err == nil {
		t.Fatal("expected error for empty redirect_url")
	}
	if _, err := New("r", ""); err == nil {
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
	other, _ := New("https://api.example.com/oauth/google/callback", "different-secret")
	state, _ := c.SignState("sub", "acme")
	if _, _, err := other.VerifyState(state); err == nil {
		t.Fatal("expected verification failure under a different secret")
	}
}

func TestAuthURL(t *testing.T) {
	c := newTestClient(t)
	raw := c.AuthURL("the-state", "cid", CapabilityNone)
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
	// The base consent must NOT ask for Drive: a Sheets pipeline putting a
	// Drive permission in front of the user is the thing the capability
	// parameter exists to prevent.
	if strings.Contains(q.Get("scope"), DriveFileScope) {
		t.Fatalf("base consent asked for Drive: %q", q.Get("scope"))
	}
}

func TestAuthURLDriveCapability(t *testing.T) {
	c := newTestClient(t)
	u, err := url.Parse(c.AuthURL("the-state", "cid", CapabilityDrive))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	scope := u.Query().Get("scope")
	if !strings.Contains(scope, DriveFileScope) {
		t.Fatalf("drive capability did not ask for %s: %q", DriveFileScope, scope)
	}
	// Additive, never a swap: the Drive consent still carries the base scopes,
	// so one connection can serve a Sheets pipeline and a Drive pipeline both.
	if !strings.Contains(scope, SheetsReadonlyScope) {
		t.Fatalf("drive capability dropped the base scopes: %q", scope)
	}
	// drive.file is the whole point — drive.readonly is restricted and would
	// put the customer in front of Google's security assessment.
	if strings.Contains(scope, "auth/drive.readonly") {
		t.Fatalf("asked for a restricted Drive scope: %q", scope)
	}
}

func TestParseCapability(t *testing.T) {
	for _, in := range []string{"", "drive"} {
		if _, err := ParseCapability(in); err != nil {
			t.Fatalf("ParseCapability(%q): %v", in, err)
		}
	}
	// An unknown capability is refused rather than downgraded: silently
	// dropping it would mint a token that fails on the box hours later.
	if _, err := ParseCapability("dropbox"); err == nil {
		t.Fatal("expected an unknown capability to be refused")
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
		// The pair comes from the caller (the customer's own app), not from the
		// Client — that is the whole point of the BYO split.
		if r.Form.Get("client_id") != "cid" || r.Form.Get("client_secret") != "csecret" {
			t.Errorf("exchange did not use the passed client pair: %v", r.Form)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "at",
			"refresh_token": "1//refresh",
			"id_token":      idToken,
			"scope":         "openid email " + SheetsReadonlyScope,
		})
	}))
	defer srv.Close()

	c := newTestClient(t)
	c.TokenEndpoint = srv.URL

	res, err := c.Exchange(context.Background(), "auth-code", "cid", "csecret")
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if res.RefreshToken != "1//refresh" {
		t.Fatalf("refresh token: %q", res.RefreshToken)
	}
	if res.Email != "user@gmail.com" {
		t.Fatalf("email: %q", res.Email)
	}
	// What Google GRANTED, which is not necessarily what was asked for.
	if len(res.Scopes) != 3 || res.Scopes[2] != SheetsReadonlyScope {
		t.Fatalf("granted scopes: %v", res.Scopes)
	}
}

func TestExchangeMissingRefreshToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "at"})
	}))
	defer srv.Close()

	c := newTestClient(t)
	c.TokenEndpoint = srv.URL
	if _, err := c.Exchange(context.Background(), "code", "cid", "csecret"); err == nil {
		t.Fatal("expected error when no refresh_token returned")
	}
}

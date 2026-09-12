package relay

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func newTokenServer(t *testing.T, mints *atomic.Int64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mints.Add(1)
		if err := r.ParseForm(); err != nil || r.PostForm.Get("grant_type") != "client_credentials" {
			t.Errorf("bad token request: form=%v err=%v", r.PostForm, err)
		}
		if r.PostForm.Get("client_id") != "box-client" || r.PostForm.Get("client_secret") != "box-secret" {
			t.Errorf("bad client pair: %v", r.PostForm)
		}
		_, _ = w.Write([]byte(`{"access_token":"tok-` + string(rune('0'+mints.Load())) + `","expires_in":3600}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestTokenSource_Bearer(t *testing.T) {
	t.Run("mints once and caches", func(t *testing.T) {
		var mints atomic.Int64
		srv := newTokenServer(t, &mints)
		s := &TokenSource{TokenURL: srv.URL, ClientID: "box-client", ClientSecret: "box-secret", HTTPClient: srv.Client()}
		for i := 0; i < 3; i++ {
			tok, err := s.Bearer(context.Background(), false)
			if err != nil || tok != "tok-1" {
				t.Fatalf("call %d: tok=%q err=%v", i, tok, err)
			}
		}
		if n := mints.Load(); n != 1 {
			t.Errorf("want 1 mint for 3 calls, got %d", n)
		}
	})

	t.Run("force mints a fresh token", func(t *testing.T) {
		var mints atomic.Int64
		srv := newTokenServer(t, &mints)
		s := &TokenSource{TokenURL: srv.URL, ClientID: "box-client", ClientSecret: "box-secret", HTTPClient: srv.Client()}
		if _, err := s.Bearer(context.Background(), false); err != nil {
			t.Fatal(err)
		}
		tok, err := s.Bearer(context.Background(), true)
		if err != nil || tok != "tok-2" {
			t.Fatalf("forced: tok=%q err=%v, want tok-2", tok, err)
		}
	})

	t.Run("non-2xx from the token endpoint is an error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "nope", http.StatusForbidden)
		}))
		t.Cleanup(srv.Close)
		s := &TokenSource{TokenURL: srv.URL, ClientID: "a", ClientSecret: "b", HTTPClient: srv.Client()}
		if _, err := s.Bearer(context.Background(), false); err == nil {
			t.Fatal("want error, got nil")
		}
	})
}

func TestTokenExpiry(t *testing.T) {
	// exp claim 2000000000 (2033-05-18); no expires_in → the JWT's own exp wins.
	jwt := "h." + "eyJleHAiOjIwMDAwMDAwMDB9" + ".s"
	if got := tokenExpiry(jwt, 0).Unix(); got != 2000000000 {
		t.Errorf("exp from claim = %d, want 2000000000", got)
	}
	if got := tokenExpiry("opaque", 0); got.IsZero() {
		t.Error("opaque token without expires_in must still get a conservative expiry")
	}
}

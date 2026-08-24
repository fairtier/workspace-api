package llm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/fairtier/workspace-api/workspace"
)

// relayFixture is one fake box Casdoor + FairTier API relay in a single
// server, mirroring the two endpoints RemoteCaller talks to.
type relayFixture struct {
	srv        *httptest.Server
	tokenMints atomic.Int64
	relayCalls atomic.Int64
	// relayHandler answers the relay POST; swapped per test.
	relayHandler func(w http.ResponseWriter, r *http.Request)
}

func newRelayFixture(t *testing.T) *relayFixture {
	t.Helper()
	f := &relayFixture{}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/login/oauth/access_token", func(w http.ResponseWriter, r *http.Request) {
		f.tokenMints.Add(1)
		if err := r.ParseForm(); err != nil || r.PostForm.Get("grant_type") != "client_credentials" {
			t.Errorf("bad token request: form=%v err=%v", r.PostForm, err)
		}
		if r.PostForm.Get("client_id") != "box-client" || r.PostForm.Get("client_secret") != "box-secret" {
			t.Errorf("bad client pair: %v", r.PostForm)
		}
		_, _ = w.Write([]byte(`{"access_token":"tok-` +
			// distinct token per mint so the retry test can see the refresh
			string(rune('0'+f.tokenMints.Load())) + `","expires_in":3600}`))
	})
	mux.HandleFunc("/assistrelay.v1.AssistRelayService/Complete", func(w http.ResponseWriter, r *http.Request) {
		f.relayCalls.Add(1)
		f.relayHandler(w, r)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *relayFixture) caller() *RemoteCaller {
	c := NewRemoteCaller(f.srv.URL, f.srv.URL+"/api/login/oauth/access_token", "box-client", "box-secret", nil)
	c.HTTPClient = f.srv.Client()
	return c
}

func TestRemoteCaller_Complete(t *testing.T) {
	req := StructuredRequest{
		System:    "sys",
		Prompt:    "draft something",
		Schema:    map[string]any{"type": "object", "required": []string{"name"}},
		MaxTokens: 2048,
		Kind:      "pipeline",
	}

	t.Run("happy path relays the request and decodes proto3-JSON int64 usage", func(t *testing.T) {
		f := newRelayFixture(t)
		var got relayRequest
		var gotAuth string
		f.relayHandler = func(w http.ResponseWriter, r *http.Request) {
			gotAuth = r.Header.Get("Authorization")
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Errorf("decode relay request: %v", err)
			}
			// proto3 JSON writes int64 as strings — the decoder must cope.
			_, _ = w.Write([]byte(`{"outputJson":"{\"name\":\"drafted\"}","promptTokens":"1200","completionTokens":"340"}`))
		}

		res, err := f.caller().Complete(context.Background(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		var out struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(res.JSON, &out); err != nil || out.Name != "drafted" {
			t.Fatalf("unexpected output %s (err %v)", res.JSON, err)
		}
		if res.Usage.InputTokens != 1200 || res.Usage.OutputTokens != 340 {
			t.Errorf("usage = %+v, want 1200 in / 340 out", res.Usage)
		}
		if gotAuth != "Bearer tok-1" {
			t.Errorf("want minted bearer, got %q", gotAuth)
		}
		if got.System != "sys" || got.Prompt != "draft something" || got.MaxTokens != 2048 || got.Kind != "pipeline" {
			t.Errorf("bad relay request: %+v", got)
		}
		if !strings.Contains(got.SchemaJSON, `"required":["name"]`) {
			t.Errorf("schema JSON missing from relay request: %s", got.SchemaJSON)
		}
	})

	t.Run("token is cached across calls", func(t *testing.T) {
		f := newRelayFixture(t)
		f.relayHandler = func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"outputJson":"{}"}`))
		}

		c := f.caller()
		for range 3 {
			if _, err := c.Complete(context.Background(), req); err != nil {
				t.Fatal(err)
			}
		}
		if n := f.tokenMints.Load(); n != 1 {
			t.Errorf("want 1 token mint for 3 calls, got %d", n)
		}
	})

	t.Run("401 refreshes the token and retries once", func(t *testing.T) {
		f := newRelayFixture(t)
		f.relayHandler = func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") == "Bearer tok-1" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"code":"unauthenticated","message":"expired"}`))
				return
			}
			_, _ = w.Write([]byte(`{"outputJson":"{}"}`))
		}

		c := f.caller()
		if _, err := c.Complete(context.Background(), req); err != nil {
			t.Fatalf("want retry to succeed, got %v", err)
		}
		if n := f.tokenMints.Load(); n != 2 {
			t.Errorf("want a re-mint after 401, got %d mints", n)
		}
		if n := f.relayCalls.Load(); n != 2 {
			t.Errorf("want exactly one retry, got %d relay calls", n)
		}
	})

	t.Run("unimplemented maps to ErrDraftNotConfigured", func(t *testing.T) {
		f := newRelayFixture(t)
		f.relayHandler = func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotImplemented)
			_, _ = w.Write([]byte(`{"code":"unimplemented","message":"no provider key"}`))
		}
		_, err := f.caller().Complete(context.Background(), req)
		if !errors.Is(err, workspace.ErrDraftNotConfigured) {
			t.Fatalf("want ErrDraftNotConfigured, got %v", err)
		}
	})

	t.Run("resource_exhausted maps to ErrDraftRateLimited", func(t *testing.T) {
		f := newRelayFixture(t)
		f.relayHandler = func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"code":"resource_exhausted","message":"slow down"}`))
		}
		_, err := f.caller().Complete(context.Background(), req)
		if !errors.Is(err, workspace.ErrDraftRateLimited) {
			t.Fatalf("want ErrDraftRateLimited, got %v", err)
		}
	})

	t.Run("other errors surface code and message", func(t *testing.T) {
		f := newRelayFixture(t)
		f.relayHandler = func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":"invalid_argument","message":"schema_json is not valid JSON"}`))
		}
		_, err := f.caller().Complete(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), "invalid_argument") || !strings.Contains(err.Error(), "schema_json") {
			t.Fatalf("want surfaced connect error, got %v", err)
		}
	})

	t.Run("empty relay output is an error", func(t *testing.T) {
		f := newRelayFixture(t)
		f.relayHandler = func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"outputJson":""}`))
		}
		_, err := f.caller().Complete(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), "empty relay response") {
			t.Fatalf("want empty-response error, got %v", err)
		}
	})
}

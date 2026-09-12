package relay

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/fairtier/workspace-api/workspace"
)

var _ workspace.PipelineAlerter = (*AlertClient)(nil)

type alertFixture struct {
	srv        *httptest.Server
	tokenMints atomic.Int64
	alertCalls atomic.Int64
	handler    func(w http.ResponseWriter, r *http.Request)
}

func newAlertFixture(t *testing.T) *alertFixture {
	t.Helper()
	f := &alertFixture{}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/login/oauth/access_token", func(w http.ResponseWriter, _ *http.Request) {
		f.tokenMints.Add(1)
		_, _ = w.Write([]byte(`{"access_token":"tok-` + string(rune('0'+f.tokenMints.Load())) + `","expires_in":3600}`))
	})
	mux.HandleFunc("/alertrelay.v1.AlertRelayService/ReportPipelineFailure", func(w http.ResponseWriter, r *http.Request) {
		f.alertCalls.Add(1)
		f.handler(w, r)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *alertFixture) client() *AlertClient {
	tokens := &TokenSource{TokenURL: f.srv.URL + "/api/login/oauth/access_token", ClientID: "c", ClientSecret: "s", HTTPClient: f.srv.Client()}
	c := NewAlertClient(f.srv.URL, tokens, nil)
	c.HTTPClient = f.srv.Client()
	return c
}

func TestAlertClient_AlertPipelineFailure(t *testing.T) {
	t.Run("posts the proto3-JSON body with the minted bearer", func(t *testing.T) {
		f := newAlertFixture(t)
		var gotAuth string
		var gotBody map[string]any
		f.handler = func(w http.ResponseWriter, r *http.Request) {
			gotAuth = r.Header.Get("Authorization")
			_ = json.NewDecoder(r.Body).Decode(&gotBody)
			_, _ = w.Write([]byte(`{"suppressed":false}`))
		}
		if err := f.client().AlertPipelineFailure(context.Background(), "fokume", "nightly", "boom"); err != nil {
			t.Fatal(err)
		}
		if gotAuth != "Bearer tok-1" {
			t.Errorf("Authorization = %q", gotAuth)
		}
		if gotBody["pipelineName"] != "nightly" || gotBody["errorMessage"] != "boom" {
			t.Errorf("body = %v", gotBody)
		}
	})

	t.Run("suppressed is a success", func(t *testing.T) {
		f := newAlertFixture(t)
		f.handler = func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"suppressed":true}`)) }
		if err := f.client().AlertPipelineFailure(context.Background(), "fokume", "nightly", "boom"); err != nil {
			t.Fatalf("suppressed must not be an error: %v", err)
		}
	})

	t.Run("unimplemented is a silent no-op", func(t *testing.T) {
		f := newAlertFixture(t)
		f.handler = func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotImplemented)
			_, _ = w.Write([]byte(`{"code":"unimplemented","message":"no email sender is configured"}`))
		}
		if err := f.client().AlertPipelineFailure(context.Background(), "fokume", "nightly", "boom"); err != nil {
			t.Fatalf("unimplemented must not be an error: %v", err)
		}
	})

	t.Run("401 refreshes the token and retries once", func(t *testing.T) {
		f := newAlertFixture(t)
		f.handler = func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") == "Bearer tok-1" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"code":"unauthenticated","message":"expired"}`))
				return
			}
			_, _ = w.Write([]byte(`{"suppressed":false}`))
		}
		if err := f.client().AlertPipelineFailure(context.Background(), "fokume", "nightly", "boom"); err != nil {
			t.Fatal(err)
		}
		if f.alertCalls.Load() != 2 || f.tokenMints.Load() != 2 {
			t.Errorf("calls=%d mints=%d; want 2 and 2", f.alertCalls.Load(), f.tokenMints.Load())
		}
	})

	t.Run("a second 401 is final", func(t *testing.T) {
		f := newAlertFixture(t)
		f.handler = func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":"unauthenticated"}`))
		}
		err := f.client().AlertPipelineFailure(context.Background(), "fokume", "nightly", "boom")
		if err == nil || f.alertCalls.Load() != 2 {
			t.Fatalf("err=%v calls=%d; want error after exactly 2 calls", err, f.alertCalls.Load())
		}
	})

	t.Run("other failures are errors naming the status and code", func(t *testing.T) {
		f := newAlertFixture(t)
		f.handler = func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"code":"unavailable","message":"could not send the failure alert"}`))
		}
		err := f.client().AlertPipelineFailure(context.Background(), "fokume", "nightly", "boom")
		if err == nil || !strings.Contains(err.Error(), "503") || !strings.Contains(err.Error(), "unavailable") {
			t.Fatalf("err = %v", err)
		}
	})
}

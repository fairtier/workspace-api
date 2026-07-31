package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

type stubObjectStore struct {
	putKey  string
	putBody string
}

func (s *stubObjectStore) Put(_ context.Context, _ core.S3Config, key string, _ int64, body io.Reader) error {
	s.putKey = key
	b, _ := io.ReadAll(body)
	s.putBody = string(b)
	return nil
}

func (s *stubObjectStore) Delete(context.Context, core.S3Config, string) error { return nil }

func (s *stubObjectStore) Head(context.Context, core.S3Config, string) (bool, error) {
	return true, nil
}

type stubCustomers struct{ ws *workspace.Workspace }

func (s *stubCustomers) GetWorkspace(context.Context, string) (*workspace.Workspace, error) {
	return s.ws, nil
}
func (s *stubCustomers) GetWorkspaceByUser(context.Context, core.UserID) (*workspace.Workspace, error) {
	return s.ws, nil
}

type stubPipelines struct {
	workspace.PipelineRepository // panic on unused methods
	pipeline                     *workspace.Pipeline
}

func (s *stubPipelines) GetPipeline(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
	return s.pipeline, nil
}
func (s *stubPipelines) UpdatePipeline(context.Context, *workspace.Pipeline) error { return nil }

func fileDropTestServer(t *testing.T) (*httptest.Server, *stubObjectStore, func(claims jwt.MapClaims) string) {
	t.Helper()
	jwks, sign := testJWKS(t)
	const iss = "https://auth.customer-acme.example.com"
	auth := UserAuth{JWKS: jwks, Issuer: iss}
	signWithIss := func(claims jwt.MapClaims) string {
		if _, ok := claims["iss"]; !ok {
			claims["iss"] = iss
		}
		return sign(claims)
	}

	ws := &workspace.Workspace{Slug: "acme"}
	ws.EffectiveS3 = core.S3Config{
		Bucket: "ft-acme", Endpoint: "https://x", Region: "auto",
		AccessKeyID: "k", SecretAccessKey: "s",
	}
	store := &stubObjectStore{}
	svc := &workspace.FileDropService{
		Workspaces: &stubCustomers{ws: ws},
		Pipelines: &stubPipelines{pipeline: &workspace.Pipeline{
			ID:           "pid-1",
			CustomerSlug: "acme",
			SourceType:   workspace.SourceTypeFileUpload,
		}},
		Store: store,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /filedrop/{pipelineID}/{filename}", FileDropUploadHandler(slog.Default(), auth, svc))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, store, signWithIss
}

func TestFileDropUploadHandler(t *testing.T) {
	srv, store, sign := fileDropTestServer(t)
	token := sign(jwt.MapClaims{"sub": "u1", "exp": time.Now().Add(time.Hour).Unix()})

	post := func(path, auth, body string) *http.Response {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(body))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		t.Cleanup(func() { _ = resp.Body.Close() })
		return resp
	}

	t.Run("rejects missing token", func(t *testing.T) {
		resp := post("/filedrop/pid-1/orders.csv", "", "a,b")
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", resp.StatusCode)
		}
	})

	t.Run("rejects unsupported extension", func(t *testing.T) {
		resp := post("/filedrop/pid-1/orders.exe", "Bearer "+token, "MZ")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.StatusCode)
		}
	})

	t.Run("uploads and returns the recorded file", func(t *testing.T) {
		resp := post("/filedrop/pid-1/orders.csv", "Bearer "+token, "a,b\n1,2\n")
		if resp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
		}
		if store.putKey != "uploads/pid-1/orders.csv" {
			t.Errorf("put key = %q", store.putKey)
		}
		if store.putBody != "a,b\n1,2\n" {
			t.Errorf("put body = %q", store.putBody)
		}
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), `"name":"orders"`) {
			t.Errorf("response = %s", body)
		}
	})
}

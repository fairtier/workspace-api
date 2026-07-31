package objstore

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fairtier/workspace-api/core"
)

func testClient(srv *httptest.Server) *Client {
	c := New()
	c.client = srv.Client()
	c.now = func() time.Time { return time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC) }
	return c
}

func testConfig(endpoint string) core.S3Config {
	return core.S3Config{
		Bucket:          "ft-acme",
		Endpoint:        endpoint,
		Region:          "auto",
		AccessKeyID:     "AKIATEST",
		SecretAccessKey: "secret",
	}
}

func TestPutStreamsBodyAndSigns(t *testing.T) {
	var got *http.Request
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Clone(context.Background())
		body, _ = io.ReadAll(r.Body)
	}))
	defer srv.Close()

	err := testClient(srv).Put(context.Background(), testConfig(srv.URL),
		"uploads/pid-1/orders.csv", 11, strings.NewReader("a,b\n1,2\n3,4"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	if got.Method != http.MethodPut {
		t.Errorf("method = %s, want PUT", got.Method)
	}
	if got.URL.Path != "/ft-acme/uploads/pid-1/orders.csv" {
		t.Errorf("path = %s", got.URL.Path)
	}
	if string(body) != "a,b\n1,2\n3,4" {
		t.Errorf("body = %q", body)
	}
	if got.ContentLength != 11 {
		t.Errorf("content-length = %d, want 11", got.ContentLength)
	}
	if h := got.Header.Get("x-amz-content-sha256"); h != "UNSIGNED-PAYLOAD" {
		t.Errorf("x-amz-content-sha256 = %q", h)
	}
	auth := got.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 Credential=AKIATEST/20260712/auto/s3/aws4_request") {
		t.Errorf("authorization = %q", auth)
	}
	if !strings.Contains(auth, "SignedHeaders=host;x-amz-content-sha256;x-amz-date") {
		t.Errorf("authorization missing signed headers: %q", auth)
	}
}

func TestPutErrorSurfacesStatusAndBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("<Error><Code>AccessDenied</Code></Error>"))
	}))
	defer srv.Close()

	err := testClient(srv).Put(context.Background(), testConfig(srv.URL), "k", 1, strings.NewReader("x"))
	if err == nil || !strings.Contains(err.Error(), "HTTP 403") || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("err = %v, want HTTP 403 with body", err)
	}
}

func TestDeleteTreats404AsSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	if err := testClient(srv).Delete(context.Background(), testConfig(srv.URL), "uploads/p/x.csv"); err != nil {
		t.Fatalf("Delete on 404: %v", err)
	}
}

func TestDeleteSignsEmptyPayload(t *testing.T) {
	var got *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Clone(context.Background())
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	if err := testClient(srv).Delete(context.Background(), testConfig(srv.URL), "uploads/p/x.csv"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if h := got.Header.Get("x-amz-content-sha256"); h != emptyPayloadSHA256 {
		t.Errorf("x-amz-content-sha256 = %q, want empty-payload hash", h)
	}
}

func TestHeadReportsExistence(t *testing.T) {
	cases := []struct {
		status   int
		want     bool
		wantErr  bool
		wantHead bool // request method should be HEAD
	}{
		{status: http.StatusOK, want: true},
		{status: http.StatusNotFound, want: false},
		{status: http.StatusForbidden, wantErr: true},
	}
	for _, tc := range cases {
		var method string
		var sha string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			method = r.Method
			sha = r.Header.Get("x-amz-content-sha256")
			w.WriteHeader(tc.status)
		}))
		got, err := testClient(srv).Head(context.Background(), testConfig(srv.URL), "uploads/p/orders.csv")
		srv.Close()
		if tc.wantErr {
			if err == nil {
				t.Errorf("status %d: want error, got exists=%v", tc.status, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("status %d: unexpected error: %v", tc.status, err)
		}
		if got != tc.want {
			t.Errorf("status %d: exists = %v, want %v", tc.status, got, tc.want)
		}
		if method != http.MethodHead {
			t.Errorf("status %d: method = %s, want HEAD", tc.status, method)
		}
		if sha != emptyPayloadSHA256 {
			t.Errorf("status %d: x-amz-content-sha256 = %q, want empty-payload hash", tc.status, sha)
		}
	}
}

func TestKeyEncoding(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.EscapedPath()
	}))
	defer srv.Close()

	err := testClient(srv).Put(context.Background(), testConfig(srv.URL),
		"uploads/p/my report 2026.csv", 1, strings.NewReader("x"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if path != "/ft-acme/uploads/p/my%20report%202026.csv" {
		t.Errorf("escaped path = %s", path)
	}
}

func TestMissingConfigRejected(t *testing.T) {
	c := New()
	err := c.Put(context.Background(), core.S3Config{Bucket: "b"}, "k", 1, strings.NewReader("x"))
	if err == nil || !strings.Contains(err.Error(), "endpoint") {
		t.Fatalf("err = %v, want missing endpoint", err)
	}
	err = c.Put(context.Background(), core.S3Config{Bucket: "b", Endpoint: "https://x"}, "k", 1, strings.NewReader("x"))
	if err == nil || !strings.Contains(err.Error(), "credentials") {
		t.Fatalf("err = %v, want missing credentials", err)
	}
}

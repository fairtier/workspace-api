package gitea

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fairtier/workspace-api/workspace"
)

func testClient(srv *httptest.Server) *Client {
	return &Client{
		BaseURL:    srv.URL,
		Username:   "fairtier-admin",
		Token:      "tok",
		HTTPClient: srv.Client(),
	}
}

func TestClient_ListTree(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/repos/fairtier-admin/rill/git/trees/HEAD" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if r.URL.Query().Get("recursive") != "true" {
			t.Errorf("want recursive=true")
		}
		user, pass, _ := r.BasicAuth()
		if user != "fairtier-admin" || pass != "tok" {
			t.Errorf("bad basic auth %s:%s", user, pass)
		}
		_, _ = w.Write([]byte(`{"tree":[
			{"path":"models/orders.sql","type":"blob","sha":"abc"},
			{"path":"models","type":"tree","sha":"dir"},
			{"path":"dashboards/rev.yaml","type":"blob","sha":"def"}
		]}`))
	}))
	defer srv.Close()

	entries, err := testClient(srv).ListTree(context.Background(), "rill")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("want 2 blobs (tree filtered), got %d", len(entries))
	}
	if entries[0].Path != "models/orders.sql" || entries[0].SHA != "abc" {
		t.Fatalf("bad entry: %+v", entries[0])
	}
}

func TestClient_GetContents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/repos/fairtier-admin/rill/contents/models/orders.sql" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		content := base64.StdEncoding.EncodeToString([]byte("SELECT 1"))
		_ = json.NewEncoder(w).Encode(map[string]string{
			"content": content, "encoding": "base64", "sha": "abc",
		})
	}))
	defer srv.Close()

	content, sha, err := testClient(srv).GetContents(context.Background(), "rill", "models/orders.sql")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if content != "SELECT 1" || sha != "abc" {
		t.Fatalf("bad decode: %q %q", content, sha)
	}
}

func TestClient_PutContents(t *testing.T) {
	t.Run("create uses POST without sha", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Errorf("want POST for create, got %s", r.Method)
			}
			var body putContentsRequest
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.SHA != "" {
				t.Errorf("create must not send sha, got %q", body.SHA)
			}
			decoded, _ := base64.StdEncoding.DecodeString(body.Content)
			if string(decoded) != "hello" {
				t.Errorf("content not base64 round-tripped: %q", decoded)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"content":{"sha":"new"}}`))
		}))
		defer srv.Close()

		sha, err := testClient(srv).PutContents(context.Background(), "rill", "models/new.sql", "hello", "", "msg", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sha != "new" {
			t.Fatalf("want new sha, got %q", sha)
		}
	})

	t.Run("update uses PUT with sha", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPut {
				t.Errorf("want PUT for update, got %s", r.Method)
			}
			var body putContentsRequest
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.SHA != "abc" {
				t.Errorf("want sha abc, got %q", body.SHA)
			}
			_, _ = w.Write([]byte(`{"content":{"sha":"def"}}`))
		}))
		defer srv.Close()

		sha, err := testClient(srv).PutContents(context.Background(), "rill", "models/orders.sql", "x", "abc", "msg", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sha != "def" {
			t.Fatalf("want updated sha, got %q", sha)
		}
	})

	t.Run("stale sha maps to ErrRepoFileChanged", func(t *testing.T) {
		for _, status := range []int{http.StatusConflict, http.StatusUnprocessableEntity} {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}))
			_, err := testClient(srv).PutContents(context.Background(), "rill", "a/b.sql", "x", "stale", "msg", nil)
			srv.Close()
			if !errors.Is(err, workspace.ErrRepoFileChanged) {
				t.Fatalf("status %d: want ErrRepoFileChanged, got %v", status, err)
			}
		}
	})

	t.Run("other errors carry status and body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("token lacks scope"))
		}))
		defer srv.Close()

		_, err := testClient(srv).PutContents(context.Background(), "rill", "a/b.sql", "x", "", "msg", nil)
		if err == nil || !strings.Contains(err.Error(), "403") || !strings.Contains(err.Error(), "token lacks scope") {
			t.Fatalf("want 403 error with body, got %v", err)
		}
	})

	t.Run("author is sent when set, omitted when nil", func(t *testing.T) {
		var raw map[string]json.RawMessage
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw = nil
			_ = json.NewDecoder(r.Body).Decode(&raw)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"content":{"sha":"new"}}`))
		}))
		defer srv.Close()

		author := &workspace.CommitAuthor{Name: "Alice", Email: "alice@example.com"}
		if _, err := testClient(srv).PutContents(context.Background(), "rill", "a.sql", "x", "", "msg", author); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		var got commitIdentity
		if err := json.Unmarshal(raw["author"], &got); err != nil || got.Name != "Alice" || got.Email != "alice@example.com" {
			t.Fatalf("author not sent, got %+v (err %v)", got, err)
		}
		// An explicit platform committer must ride along: Gitea mirrors a
		// lone author into the committer instead of using the token owner.
		var committer commitIdentity
		if err := json.Unmarshal(raw["committer"], &committer); err != nil || committer != *platformCommitter {
			t.Fatalf("want platform committer, got %+v (err %v)", committer, err)
		}

		if _, err := testClient(srv).PutContents(context.Background(), "rill", "a.sql", "x", "", "msg", nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := raw["author"]; ok {
			t.Fatal("nil author must omit the author field")
		}
		if _, ok := raw["committer"]; ok {
			t.Fatal("nil author must omit the committer field")
		}
	})
}

func TestClient_transportErrorIsBoxUnreachable(t *testing.T) {
	// A closed server's address refuses connections — the box-down case that
	// surfaced ListPipelineVersions as an opaque `internal` error. It must now
	// carry ErrBoxUnreachable so the API answers a retryable Unavailable.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	c := testClient(srv)
	srv.Close() // nothing listens on c.BaseURL now → connection refused

	_, err := c.ListCommits(context.Background(), "pipelines", "pipelines/x.yaml", 20)
	if err == nil {
		t.Fatal("want error from an unreachable box, got nil")
	}
	if !errors.Is(err, workspace.ErrBoxUnreachable) {
		t.Fatalf("want ErrBoxUnreachable, got %v", err)
	}
}

func TestClient_writeTransportErrorIsNotBoxUnreachable(t *testing.T) {
	// A write whose response is lost to a transport error may already have
	// taken effect on the box, so it must NOT be surfaced as a retryable
	// Unavailable (which would invite a double-applying client retry). Only
	// idempotent GETs earn ErrBoxUnreachable.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	c := testClient(srv)
	srv.Close() // nothing listens on c.BaseURL now → connection refused

	author := &workspace.CommitAuthor{Name: "Alice", Email: "alice@example.com"}
	_, err := c.PutContents(context.Background(), "pipelines", "pipelines/x.yaml", "content", "", "msg", author)
	if err == nil {
		t.Fatal("want error from an unreachable box, got nil")
	}
	if errors.Is(err, workspace.ErrBoxUnreachable) {
		t.Fatalf("a write (PUT) transport failure must not be classified unreachable: %v", err)
	}
}

func TestClient_statusErrorIsNotBoxUnreachable(t *testing.T) {
	// A reachable box answering 500 is a real answer, not "unreachable": it
	// must stay an internal error, not be masked as a retryable Unavailable.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := testClient(srv).ListCommits(context.Background(), "pipelines", "pipelines/x.yaml", 20)
	if err == nil {
		t.Fatal("want error from a 500, got nil")
	}
	if errors.Is(err, workspace.ErrBoxUnreachable) {
		t.Fatalf("a 500 status must not be classified unreachable: %v", err)
	}
}

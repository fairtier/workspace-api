package workspace_test

import (
	"context"
	"errors"
	"testing"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

type fakeRepoClient struct {
	baseURL, username, token string
	entries                  []workspace.RepoFileEntry
	putSHA                   string
	putAuthor                *workspace.CommitAuthor // author passed to the last PutContents
}

func (f *fakeRepoClient) ListTree(context.Context, string) ([]workspace.RepoFileEntry, error) {
	return f.entries, nil
}

func (f *fakeRepoClient) GetContents(context.Context, string, string) (string, string, error) {
	return "content", "sha1", nil
}

func (f *fakeRepoClient) PutContents(_ context.Context, _, _, _, _, _ string, author *workspace.CommitAuthor) (string, error) {
	f.putAuthor = author
	return f.putSHA, nil
}

func (f *fakeRepoClient) DeleteContents(context.Context, string, string, string, string, *workspace.CommitAuthor) error {
	return nil
}

func (f *fakeRepoClient) ListCommits(context.Context, string, string, int) ([]workspace.RepoCommit, error) {
	return []workspace.RepoCommit{{SHA: "c0ffee1234567", Message: "Update via FairTier Console", AuthorName: "Alice"}}, nil
}

func (f *fakeRepoClient) GetContentsAt(context.Context, string, string, string) (string, string, error) {
	return "old content", "oldsha", nil
}

// fakeMirrorClient is an in-memory push-mirror surface: at most one mirror,
// like the Console model.
type fakeMirrorClient struct {
	mirrors []workspace.PushMirror
	added   []string // remote URLs passed to AddPushMirror
	deleted []string // remote names passed to DeletePushMirror
	syncs   int
}

func (f *fakeMirrorClient) ListPushMirrors(context.Context, string) ([]workspace.PushMirror, error) {
	return f.mirrors, nil
}

func (f *fakeMirrorClient) AddPushMirror(_ context.Context, _, remoteURL, _, _, _ string) error {
	f.added = append(f.added, remoteURL)
	f.mirrors = append(f.mirrors, workspace.PushMirror{RemoteName: "remote_mirror_1", RemoteURL: remoteURL})
	return nil
}

func (f *fakeMirrorClient) DeletePushMirror(_ context.Context, _, remoteName string) error {
	f.deleted = append(f.deleted, remoteName)
	f.mirrors = nil
	return nil
}

func (f *fakeMirrorClient) SyncPushMirrors(context.Context, string) error {
	f.syncs++
	return nil
}

type fakeCredStore struct {
	cred *workspace.BoxGitCredential
	err  error
}

func (f *fakeCredStore) UpsertBoxGitCredential(context.Context, *workspace.BoxGitCredential) error {
	return nil
}

func (f *fakeCredStore) GetBoxGitCredential(context.Context, string) (*workspace.BoxGitCredential, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.cred, nil
}

func boxCustomer() *workspace.Workspace {
	c := &workspace.Workspace{Slug: "acme"}
	c.OnVM = true
	c.RillEnabled = true
	c.CustomerDomain = "*.customer-acme.fairtier.com"
	return c
}

func boxRepoService(ws *workspace.Workspace, store workspace.BoxGitCredentialStore, client *fakeRepoClient) *workspace.BoxRepoService {
	return &workspace.BoxRepoService{
		Workspaces: &mockCustomerReader{
			getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
				return ws, nil
			},
		},
		Credentials: store,
		NewClient: func(baseURL, username, token string) workspace.RepoFileClient {
			client.baseURL = baseURL
			client.username = username
			client.token = token
			return client
		},
	}
}

func TestBoxRepoService_Gates(t *testing.T) {
	store := &fakeCredStore{cred: &workspace.BoxGitCredential{Username: "fairtier-admin", Token: "tok"}}

	t.Run("rejects unknown repo", func(t *testing.T) {
		svc := boxRepoService(boxCustomer(), store, &fakeRepoClient{})
		_, err := svc.ListFiles(context.Background(), "u1", "secrets")
		var invalid *workspace.ErrInvalidSourceConfig
		if !errors.As(err, &invalid) || invalid.Field != "repo" {
			t.Fatalf("want repo allowlist error, got %v", err)
		}
	})

	t.Run("rejects shared substrate", func(t *testing.T) {
		c := boxCustomer()
		c.OnVM = false
		svc := boxRepoService(c, store, &fakeRepoClient{})
		_, err := svc.ListFiles(context.Background(), "u1", "rill")
		if !errors.Is(err, workspace.ErrBoxRepoUnavailable) {
			t.Fatalf("want ErrBoxRepoUnavailable, got %v", err)
		}
	})

	t.Run("rejects rill repo when rill disabled", func(t *testing.T) {
		c := boxCustomer()
		c.RillEnabled = false
		svc := boxRepoService(c, store, &fakeRepoClient{})
		_, err := svc.ListFiles(context.Background(), "u1", "rill")
		if !errors.Is(err, workspace.ErrBoxRepoUnavailable) {
			t.Fatalf("want ErrBoxRepoUnavailable, got %v", err)
		}
		// transformations repo does not depend on Rill.
		if _, err := svc.ListFiles(context.Background(), "u1", "transformations"); err != nil {
			t.Fatalf("transformations should not gate on rill: %v", err)
		}
	})

	t.Run("no credential store at all is unavailable, not a panic", func(t *testing.T) {
		// Central after split Phase 3E: the deposits are retired, so nothing
		// wires a credential source and every box's repo editor is served by
		// the box itself. The gate must answer before the store is touched.
		svc := boxRepoService(boxCustomer(), nil, &fakeRepoClient{})
		_, err := svc.ListFiles(context.Background(), "u1", "rill")
		if !errors.Is(err, workspace.ErrBoxRepoUnavailable) {
			t.Fatalf("want ErrBoxRepoUnavailable, got %v", err)
		}
	})

	t.Run("requires deposited credential", func(t *testing.T) {
		svc := boxRepoService(boxCustomer(), &fakeCredStore{err: workspace.ErrBoxCredentialNotFound}, &fakeRepoClient{})
		_, err := svc.ListFiles(context.Background(), "u1", "rill")
		if !errors.Is(err, workspace.ErrBoxCredentialNotFound) {
			t.Fatalf("want ErrBoxCredentialNotFound, got %v", err)
		}
	})

	t.Run("builds gitea URL from customer domain sans wildcard", func(t *testing.T) {
		client := &fakeRepoClient{}
		svc := boxRepoService(boxCustomer(), store, client)
		if _, err := svc.ListFiles(context.Background(), "u1", "rill"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if client.baseURL != "https://git.customer-acme.fairtier.com" {
			t.Fatalf("bad gitea base URL: %q", client.baseURL)
		}
		if client.username != "fairtier-admin" || client.token != "tok" {
			t.Fatalf("credential not passed: %s %s", client.username, client.token)
		}
	})

	t.Run("unprovisioned domain unavailable", func(t *testing.T) {
		c := boxCustomer()
		c.CustomerDomain = ""
		svc := boxRepoService(c, store, &fakeRepoClient{})
		_, err := svc.ListFiles(context.Background(), "u1", "rill")
		if !errors.Is(err, workspace.ErrBoxRepoUnavailable) {
			t.Fatalf("want ErrBoxRepoUnavailable, got %v", err)
		}
	})
}

func TestBoxRepoService_Files(t *testing.T) {
	store := &fakeCredStore{cred: &workspace.BoxGitCredential{Username: "fairtier-admin", Token: "tok"}}

	t.Run("list hides dotfiles and platform files", func(t *testing.T) {
		client := &fakeRepoClient{entries: []workspace.RepoFileEntry{
			{Path: "models/orders.sql", SHA: "a"},
			{Path: ".gitignore", SHA: "b"},
			// rill.yaml is customer-owned (BYO-Rill) — visible and editable.
			{Path: "rill.yaml", SHA: "c"},
			{Path: "duckdb.yaml", SHA: "d"},
			{Path: ".rill/state", SHA: "e"},
			{Path: "dashboards/rev.yaml", SHA: "f"},
		}}
		svc := boxRepoService(boxCustomer(), store, client)
		entries, err := svc.ListFiles(context.Background(), "u1", "rill")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 3 || entries[0].Path != "models/orders.sql" || entries[1].Path != "rill.yaml" || entries[2].Path != "dashboards/rev.yaml" {
			t.Fatalf("bad filtering: %+v", entries)
		}
	})

	for _, tc := range []struct {
		name, path string
	}{
		{"platform file", "duckdb.yaml"},
		{"env file", ".env"},
		{"traversal", "models/../../etc/passwd"},
		{"absolute", "/etc/passwd"},
		{"dot dir", ".git/config"},
	} {
		t.Run("put rejects "+tc.name, func(t *testing.T) {
			svc := boxRepoService(boxCustomer(), store, &fakeRepoClient{putSHA: "new"})
			_, err := svc.PutFile(context.Background(), "u1", "rill", tc.path, "x", "", "")
			if _, ok := errors.AsType[*workspace.ErrInvalidSourceConfig](err); !ok {
				t.Fatalf("want path validation error for %q, got %v", tc.path, err)
			}
		})
	}

	t.Run("put happy path", func(t *testing.T) {
		svc := boxRepoService(boxCustomer(), store, &fakeRepoClient{putSHA: "new"})
		sha, err := svc.PutFile(context.Background(), "u1", "rill", "dashboards/rev.yaml", "type: explore", "old", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sha != "new" {
			t.Fatalf("want new sha, got %q", sha)
		}
	})

	t.Run("get happy path", func(t *testing.T) {
		svc := boxRepoService(boxCustomer(), store, &fakeRepoClient{})
		content, sha, err := svc.GetFile(context.Background(), "u1", "rill", "models/orders.sql")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if content != "content" || sha != "sha1" {
			t.Fatalf("bad result: %q %q", content, sha)
		}
	})
}

// fakeUserReader resolves the acting user for commit attribution.
type fakeUserReader struct {
	user *workspace.UserInfo
	err  error
}

func (f *fakeUserReader) GetCommitUser(context.Context, core.UserID) (*workspace.UserInfo, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.user, nil
}

func TestBoxRepoService_CommitAuthor(t *testing.T) {
	store := &fakeCredStore{cred: &workspace.BoxGitCredential{Username: "fairtier-admin", Token: "tok"}}
	put := func(svc *workspace.BoxRepoService) {
		t.Helper()
		if _, err := svc.PutFile(context.Background(), "u1", "rill", "dashboards/rev.yaml", "x", "old", ""); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	t.Run("acting user becomes the git author", func(t *testing.T) {
		client := &fakeRepoClient{putSHA: "new"}
		svc := boxRepoService(boxCustomer(), store, client)
		svc.Users = &fakeUserReader{user: &workspace.UserInfo{DisplayName: "Alice", Email: "alice@example.com"}}
		put(svc)
		if client.putAuthor == nil || client.putAuthor.Name != "Alice" || client.putAuthor.Email != "alice@example.com" {
			t.Fatalf("want Alice as author, got %+v", client.putAuthor)
		}
	})

	t.Run("name falls back to the email local part", func(t *testing.T) {
		client := &fakeRepoClient{putSHA: "new"}
		svc := boxRepoService(boxCustomer(), store, client)
		svc.Users = &fakeUserReader{user: &workspace.UserInfo{Email: "bob@example.com"}}
		put(svc)
		if client.putAuthor == nil || client.putAuthor.Name != "bob" {
			t.Fatalf("want email-local-part name, got %+v", client.putAuthor)
		}
	})

	t.Run("no email, lookup error, or no reader keep platform attribution", func(t *testing.T) {
		for name, users := range map[string]workspace.UserReader{
			"no email":     &fakeUserReader{user: &workspace.UserInfo{DisplayName: "Alice"}},
			"lookup error": &fakeUserReader{err: errors.New("db down")},
			"no reader":    nil,
		} {
			client := &fakeRepoClient{putSHA: "new"}
			svc := boxRepoService(boxCustomer(), store, client)
			svc.Users = users
			put(svc)
			if client.putAuthor != nil {
				t.Fatalf("%s: want nil author, got %+v", name, client.putAuthor)
			}
		}
	})
}

func mirrorService(ws *workspace.Workspace, client *fakeMirrorClient) *workspace.BoxRepoService {
	svc := boxRepoService(ws, &fakeCredStore{cred: &workspace.BoxGitCredential{Username: "fairtier-admin", Token: "tok"}}, &fakeRepoClient{})
	svc.NewMirrorClient = func(string, string, string) workspace.RepoMirrorClient { return client }
	return svc
}

func TestBoxRepoService_PushMirrors(t *testing.T) {
	t.Run("set replaces any existing mirror", func(t *testing.T) {
		client := &fakeMirrorClient{mirrors: []workspace.PushMirror{{RemoteName: "remote_mirror_old", RemoteURL: "https://github.com/acme/old.git"}}}
		svc := mirrorService(boxCustomer(), client)
		err := svc.SetPushMirror(context.Background(), "u1", "rill", "https://github.com/acme/analytics.git", "acme", "ghp_token")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(client.deleted) != 1 || client.deleted[0] != "remote_mirror_old" {
			t.Fatalf("previous mirror not removed: %v", client.deleted)
		}
		if len(client.added) != 1 || client.added[0] != "https://github.com/acme/analytics.git" {
			t.Fatalf("mirror not added: %v", client.added)
		}
	})

	t.Run("set rejects bad remotes", func(t *testing.T) {
		svc := mirrorService(boxCustomer(), &fakeMirrorClient{})
		for _, tc := range []struct{ url, user, pass string }{
			{"http://github.com/acme/x.git", "acme", "tok"},          // not https
			{"https://user:pw@github.com/acme/x.git", "acme", "tok"}, // embedded creds
			{"https://github.com/acme/x.git", "", "tok"},             // no username
			{"https://github.com/acme/x.git", "acme", ""},            // no token
		} {
			err := svc.SetPushMirror(context.Background(), "u1", "rill", tc.url, tc.user, tc.pass)
			if _, ok := errors.AsType[*workspace.ErrInvalidSourceConfig](err); !ok {
				t.Fatalf("want validation error for %+v, got %v", tc, err)
			}
		}
	})

	t.Run("pipelines repo is mirrorable but not editable", func(t *testing.T) {
		svc := mirrorService(boxCustomer(), &fakeMirrorClient{})
		if err := svc.SyncPushMirror(context.Background(), "u1", "pipelines"); err != nil {
			t.Fatalf("pipelines must be mirrorable: %v", err)
		}
		_, err := svc.ListFiles(context.Background(), "u1", "pipelines")
		if _, ok := errors.AsType[*workspace.ErrInvalidSourceConfig](err); !ok {
			t.Fatalf("pipelines must not be file-editable, got %v", err)
		}
	})

	t.Run("get strips userinfo and reports status", func(t *testing.T) {
		client := &fakeMirrorClient{mirrors: []workspace.PushMirror{{
			RemoteName: "remote_mirror_1",
			RemoteURL:  "https://oauth2:secret@gitlab.com/acme/x.git",
			LastError:  "boom",
		}}}
		svc := mirrorService(boxCustomer(), client)
		m, ok, err := svc.GetPushMirror(context.Background(), "u1", "rill")
		if err != nil || !ok {
			t.Fatalf("unexpected: %v %v", ok, err)
		}
		if m.RemoteURL != "https://gitlab.com/acme/x.git" {
			t.Fatalf("userinfo not stripped: %q", m.RemoteURL)
		}
		if m.LastError != "boom" {
			t.Fatalf("status lost: %+v", m)
		}
	})

	t.Run("get reports unconfigured", func(t *testing.T) {
		svc := mirrorService(boxCustomer(), &fakeMirrorClient{})
		_, ok, err := svc.GetPushMirror(context.Background(), "u1", "transformations")
		if err != nil || ok {
			t.Fatalf("want unconfigured, got ok=%v err=%v", ok, err)
		}
	})

	t.Run("shared substrate is rejected", func(t *testing.T) {
		svc := mirrorService(&workspace.Workspace{Slug: "acme"}, &fakeMirrorClient{})
		err := svc.SetPushMirror(context.Background(), "u1", "rill", "https://github.com/acme/x.git", "acme", "tok")
		if !errors.Is(err, workspace.ErrBoxRepoUnavailable) {
			t.Fatalf("want ErrBoxRepoUnavailable, got %v", err)
		}
	})
}

func TestBoxRepoService_History(t *testing.T) {
	store := &fakeCredStore{cred: &workspace.BoxGitCredential{Username: "fairtier-admin", Token: "tok"}}

	t.Run("history lists commits", func(t *testing.T) {
		svc := boxRepoService(boxCustomer(), store, &fakeRepoClient{})
		commits, err := svc.ListFileHistory(context.Background(), "u1", "rill", "models/orders.sql")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(commits) != 1 || commits[0].AuthorName != "Alice" {
			t.Fatalf("bad history: %+v", commits)
		}
	})

	t.Run("history rejects platform-managed paths", func(t *testing.T) {
		svc := boxRepoService(boxCustomer(), store, &fakeRepoClient{})
		_, err := svc.ListFileHistory(context.Background(), "u1", "rill", "duckdb.yaml")
		if _, ok := errors.AsType[*workspace.ErrInvalidSourceConfig](err); !ok {
			t.Fatalf("want path validation error, got %v", err)
		}
	})

	t.Run("get at ref returns historical content", func(t *testing.T) {
		svc := boxRepoService(boxCustomer(), store, &fakeRepoClient{})
		content, err := svc.GetFileAtRef(context.Background(), "u1", "rill", "models/orders.sql", "c0ffee1234567")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if content != "old content" {
			t.Fatalf("bad content: %q", content)
		}
	})

	t.Run("get at ref rejects non-sha refs", func(t *testing.T) {
		svc := boxRepoService(boxCustomer(), store, &fakeRepoClient{})
		for _, ref := range []string{"main", "HEAD", "abc", "C0FFEE1234567", "12345g7890abc"} {
			_, err := svc.GetFileAtRef(context.Background(), "u1", "rill", "models/orders.sql", ref)
			var invalid *workspace.ErrInvalidSourceConfig
			if !errors.As(err, &invalid) || invalid.Field != "ref" {
				t.Fatalf("ref %q: want ref validation error, got %v", ref, err)
			}
		}
	})
}

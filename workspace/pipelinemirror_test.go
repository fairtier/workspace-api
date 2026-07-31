package workspace_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"filippo.io/age"
	"filippo.io/age/armor"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/crypto"
	"github.com/fairtier/workspace-api/workspace"
)

// fakeMirrorRepo is an in-memory stand-in for one box Gitea repo, faithful to
// the contents-API contract the mirror relies on: blob-sha guarded updates
// and deletes, create rejected when the file exists.
type fakeMirrorRepo struct {
	files map[string]string // path → content
	shas  map[string]string // path → blob sha
	seq   int

	puts, deletes, gets, trees int
	failNextPut                bool

	// authors records the author passed with each put/delete, keyed by path.
	authors map[string]*workspace.CommitAuthor
	// atRef is the content served for any GetContentsAt with a ref.
	atRef string
}

func newFakeMirrorRepo(files map[string]string) *fakeMirrorRepo {
	f := &fakeMirrorRepo{files: map[string]string{}, shas: map[string]string{}, authors: map[string]*workspace.CommitAuthor{}}
	for p, c := range files {
		f.files[p] = c
		f.seq++
		f.shas[p] = fmt.Sprintf("sha%d", f.seq)
	}
	return f
}

func (f *fakeMirrorRepo) ListTree(context.Context, string) ([]workspace.RepoFileEntry, error) {
	f.trees++
	var entries []workspace.RepoFileEntry
	for p := range f.files {
		entries = append(entries, workspace.RepoFileEntry{Path: p, SHA: f.shas[p]})
	}
	return entries, nil
}

func (f *fakeMirrorRepo) GetContents(_ context.Context, _, path string) (string, string, error) {
	f.gets++
	content, ok := f.files[path]
	if !ok {
		return "", "", fmt.Errorf("not found: %s", path)
	}
	return content, f.shas[path], nil
}

func (f *fakeMirrorRepo) PutContents(_ context.Context, _, path, content, sha, _ string, author *workspace.CommitAuthor) (string, error) {
	f.puts++
	f.authors[path] = author
	if f.failNextPut {
		f.failNextPut = false
		return "", workspace.ErrRepoFileChanged
	}
	current, exists := f.shas[path]
	if sha == "" && exists {
		return "", fmt.Errorf("already exists: %s", path)
	}
	if sha != "" && sha != current {
		return "", workspace.ErrRepoFileChanged
	}
	f.files[path] = content
	f.seq++
	f.shas[path] = fmt.Sprintf("sha%d", f.seq)
	return f.shas[path], nil
}

func (f *fakeMirrorRepo) ListCommits(context.Context, string, string, int) ([]workspace.RepoCommit, error) {
	return []workspace.RepoCommit{{SHA: "c0ffee1234567", Message: "Render via FairTier Console", AuthorName: "Alice", Date: "2026-07-19T00:00:00Z"}}, nil
}

// atRef, when set, is served for any GetContentsAt with a non-empty ref.
func (f *fakeMirrorRepo) GetContentsAt(ctx context.Context, repo, path, ref string) (string, string, error) {
	if ref != "" && f.atRef != "" {
		return f.atRef, "refsha", nil
	}
	return f.GetContents(ctx, repo, path)
}

func (f *fakeMirrorRepo) DeleteContents(_ context.Context, _, path, sha, _ string, author *workspace.CommitAuthor) error {
	f.deletes++
	f.authors[path] = author
	if sha != f.shas[path] {
		return workspace.ErrRepoFileChanged
	}
	delete(f.files, path)
	delete(f.shas, path)
	return nil
}

func mirrorFor(ws *workspace.Workspace, repo *fakeMirrorRepo, pipelines []workspace.Pipeline) *workspace.PipelineMirror {
	return &workspace.PipelineMirror{
		Workspaces: &mockCustomerReader{
			getBySlugFn: func(context.Context, string) (*workspace.Workspace, error) { return ws, nil },
		},
		Credentials: &fakeCredStore{cred: &workspace.BoxGitCredential{Username: "fairtier-admin", Token: "tok"}},
		Pipelines: &mockPipelineRepo{
			listPipelinesByCustomerFn: func(context.Context, string) ([]workspace.Pipeline, error) {
				return pipelines, nil
			},
		},
		NewClient: func(string, string, string) workspace.RepoFileClient { return repo },
	}
}

func TestPipelineMirror_Converge(t *testing.T) {
	pipe := func(id, name string) workspace.Pipeline {
		return workspace.Pipeline{
			ID:           workspace.PipelineID(id),
			Name:         name,
			SourceType:   "rest_api",
			SourceConfig: json.RawMessage(`{"url":"https://example.com","nested":{"page_size":100}}`),
			DatasetName:  "raw",
			Schedule:     "0 * * * *",
			Enabled:      true,
		}
	}

	t.Run("creates files for new pipelines, leaves root files alone", func(t *testing.T) {
		repo := newFakeMirrorRepo(map[string]string{"README.md": "readme"})
		m := mirrorFor(boxCustomer(), repo, []workspace.Pipeline{
			pipe("11111111-aaaa", "Orders Sync"), pipe("22222222-bbbb", "Users"),
		})

		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if _, ok := repo.files["pipelines/orders-sync.yaml"]; !ok {
			t.Fatalf("missing orders-sync.yaml, files: %v", repo.files)
		}
		if _, ok := repo.files["pipelines/users.yaml"]; !ok {
			t.Fatal("missing users.yaml")
		}
		if repo.files["README.md"] != "readme" {
			t.Fatal("README must not be touched")
		}
		content := repo.files["pipelines/orders-sync.yaml"]
		for _, want := range []string{"id: 11111111-aaaa", "name: Orders Sync", "page_size: 100", "enabled: true"} {
			if !strings.Contains(content, want) {
				t.Fatalf("rendered file missing %q:\n%s", want, content)
			}
		}
		if strings.Contains(content, "credential") {
			t.Fatalf("credentials must never be rendered:\n%s", content)
		}
	})

	t.Run("unchanged content produces no commit", func(t *testing.T) {
		p := pipe("11111111-aaaa", "Orders")
		repo := newFakeMirrorRepo(nil)
		m := mirrorFor(boxCustomer(), repo, []workspace.Pipeline{p})
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		putsAfterFirst := repo.puts
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if repo.puts != putsAfterFirst {
			t.Fatalf("second sync of identical state committed: %d → %d puts", putsAfterFirst, repo.puts)
		}
	})

	t.Run("rename moves the file, delete removes it", func(t *testing.T) {
		repo := newFakeMirrorRepo(nil)
		m := mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe("11111111-aaaa", "Old Name"), pipe("22222222-bbbb", "Doomed")})
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}

		m2 := mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe("11111111-aaaa", "New Name")})
		if err := m2.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if _, ok := repo.files["pipelines/new-name.yaml"]; !ok {
			t.Fatal("renamed file missing")
		}
		for _, gone := range []string{"pipelines/old-name.yaml", "pipelines/doomed.yaml"} {
			if _, ok := repo.files[gone]; ok {
				t.Fatalf("%s should have been deleted", gone)
			}
		}
	})

	t.Run("name collisions all get id suffixes", func(t *testing.T) {
		repo := newFakeMirrorRepo(nil)
		m := mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe("11111111-aaaa", "Sync"), pipe("22222222-bbbb", "sync!")})
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"pipelines/sync-11111111.yaml", "pipelines/sync-22222222.yaml"} {
			if _, ok := repo.files[want]; !ok {
				t.Fatalf("missing %s, files: %v", want, repo.files)
			}
		}
	})

	t.Run("conflict retries once with a fresh tree", func(t *testing.T) {
		repo := newFakeMirrorRepo(nil)
		repo.failNextPut = true
		m := mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe("11111111-aaaa", "Orders")})
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatalf("retry should have recovered: %v", err)
		}
		if repo.trees != 2 {
			t.Fatalf("want 2 tree reads (fresh tree on retry), got %d", repo.trees)
		}
		if _, ok := repo.files["pipelines/orders.yaml"]; !ok {
			t.Fatal("file missing after retry")
		}
	})

	t.Run("save-triggered sync carries the acting user as author", func(t *testing.T) {
		repo := newFakeMirrorRepo(map[string]string{"pipelines/stale.yaml": "old"})
		m := mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe("11111111-aaaa", "Orders")})

		author := &workspace.CommitAuthor{Name: "Alice", Email: "alice@example.com"}
		if err := m.SyncCustomer(context.Background(), "acme", author); err != nil {
			t.Fatal(err)
		}
		if got := repo.authors["pipelines/orders.yaml"]; got == nil || got.Email != "alice@example.com" {
			t.Fatalf("want acting user on rendered file, got %+v", got)
		}
		if got := repo.authors["pipelines/stale.yaml"]; got == nil || got.Email != "alice@example.com" {
			t.Fatalf("want acting user on stale-file delete, got %+v", got)
		}
	})

	t.Run("platform-initiated sync keeps platform attribution", func(t *testing.T) {
		repo := newFakeMirrorRepo(nil)
		m := mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe("11111111-aaaa", "Orders")})

		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if got, ok := repo.authors["pipelines/orders.yaml"]; !ok || got != nil {
			t.Fatalf("want recorded nil author, got %+v (ok=%v)", got, ok)
		}
	})
}

// failingMirror always errors — the save must succeed anyway. It runs on its
// own goroutine now (best-effort, non-blocking), so it closes done once
// invoked to let the test await it without racing the counter.
type failingMirror struct {
	calls atomic.Int32
	done  chan struct{}
}

func (f *failingMirror) SyncCustomer(context.Context, string, *workspace.CommitAuthor) error {
	f.calls.Add(1)
	close(f.done)
	return errors.New("box unreachable")
}

func TestPipelineService_MirrorFailureDoesNotFailSave(t *testing.T) {
	mirror := &failingMirror{done: make(chan struct{})}
	svc := &workspace.PipelineService{
		Workspaces: &mockCustomerReader{
			getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
				return &workspace.Workspace{Slug: "acme"}, nil
			},
		},
		Pipelines: &mockPipelineRepo{
			createPipelineFn: func(context.Context, *workspace.Pipeline) error { return nil },
		},
		Mirror: mirror,
	}

	p := &workspace.Pipeline{
		Name:              "test",
		SourceType:        "sql_database",
		SourceCredentials: json.RawMessage(`{"connection_string":"postgres://u:p@localhost/db"}`),
	}
	if _, err := svc.CreatePipeline(context.Background(), "user-1", p); err != nil {
		t.Fatalf("save must succeed despite mirror failure, got %v", err)
	}
	// The mirror is best-effort and now runs asynchronously; await its dispatch.
	select {
	case <-mirror.done:
	case <-time.After(2 * time.Second):
		t.Fatal("mirror was never attempted")
	}
	if got := mirror.calls.Load(); got != 1 {
		t.Fatalf("mirror should have been attempted once, got %d", got)
	}
}

// --- age credential files (pipelines-as-files Phase 3) ---

type fakeAgeKeyStore struct {
	key *workspace.BoxAgeKey
	err error
}

func (f *fakeAgeKeyStore) UpsertBoxAgeKey(context.Context, *workspace.BoxAgeKey) error { return nil }
func (f *fakeAgeKeyStore) GetBoxAgeKey(context.Context, string) (*workspace.BoxAgeKey, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.key, nil
}

type fakeRenderStore struct {
	rows    map[workspace.PipelineID]workspace.PipelineCredentialRender
	upserts int
}

func (f *fakeRenderStore) UpsertPipelineCredentialRender(_ context.Context, r *workspace.PipelineCredentialRender) error {
	if f.rows == nil {
		f.rows = map[workspace.PipelineID]workspace.PipelineCredentialRender{}
	}
	f.rows[r.PipelineID] = *r
	f.upserts++
	return nil
}

func (f *fakeRenderStore) GetPipelineCredentialRenders(context.Context, string) (map[workspace.PipelineID]workspace.PipelineCredentialRender, error) {
	return f.rows, nil
}

type fakeCredReader map[workspace.PipelineID]json.RawMessage

func (f fakeCredReader) ListPipelineCredentialsByCustomer(context.Context, string) (map[workspace.PipelineID]json.RawMessage, error) {
	return f, nil
}

// withAge arms a mirror for Phase 3 rendering; renders persists across
// syncs like the Postgres table does.
func withAge(m *workspace.PipelineMirror, recipient string, creds fakeCredReader, renders *fakeRenderStore) *workspace.PipelineMirror {
	m.AgeKeys = &fakeAgeKeyStore{key: &workspace.BoxAgeKey{PublicKey: recipient}}
	m.Renders = renders
	m.Fingerprint = crypto.SHA256Fingerprinter{}
	m.PipelineCredentials = creds
	return m
}

func decryptArmored(t *testing.T, identity *age.X25519Identity, content string) []byte {
	t.Helper()
	r, err := age.Decrypt(armor.NewReader(strings.NewReader(content)), identity)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	plaintext, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read decrypted: %v", err)
	}
	return plaintext
}

func TestPipelineMirror_AgeCredentials(t *testing.T) {
	pipe := func(id, name string) workspace.Pipeline {
		return workspace.Pipeline{
			ID:           workspace.PipelineID(id),
			Name:         name,
			SourceType:   "rest_api",
			SourceConfig: json.RawMessage(`{"url":"https://example.com"}`),
			DatasetName:  "raw",
			Schedule:     "0 * * * *",
			Enabled:      true,
		}
	}
	newIdentity := func(t *testing.T) (*age.X25519Identity, string) {
		t.Helper()
		identity, err := age.GenerateX25519Identity()
		if err != nil {
			t.Fatal(err)
		}
		return identity, identity.Recipient().String()
	}
	const credsJSON = `{"password":"s3cr3t"}`

	t.Run("renders armored credential file beside the yaml", func(t *testing.T) {
		identity, recipient := newIdentity(t)
		repo := newFakeMirrorRepo(map[string]string{"README.md": "readme"})
		m := withAge(
			mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe("11111111-aaaa", "Orders")}),
			recipient,
			fakeCredReader{"11111111-aaaa": json.RawMessage(credsJSON)},
			&fakeRenderStore{},
		)

		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		content, ok := repo.files["pipelines/orders.credentials.age"]
		if !ok {
			t.Fatalf("missing credential file, files: %v", sortedKeys(repo.files))
		}
		if !strings.HasPrefix(content, "-----BEGIN AGE ENCRYPTED FILE-----") {
			t.Fatalf("credential file is not armored:\n%s", content)
		}
		if got := string(decryptArmored(t, identity, content)); got != credsJSON {
			t.Fatalf("decrypted %q, want %q", got, credsJSON)
		}
		if strings.Contains(repo.files["pipelines/orders.yaml"], "s3cr3t") {
			t.Fatal("plaintext credentials leaked into the yaml")
		}
		if repo.files["README.md"] != "readme" {
			t.Fatal("README must not be touched")
		}
	})

	t.Run("empty credentials render no file", func(t *testing.T) {
		_, recipient := newIdentity(t)
		repo := newFakeMirrorRepo(nil)
		m := withAge(
			mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe("11111111-aaaa", "Orders")}),
			recipient,
			fakeCredReader{"11111111-aaaa": json.RawMessage(`{}`)},
			&fakeRenderStore{},
		)
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if _, ok := repo.files["pipelines/orders.credentials.age"]; ok {
			t.Fatal("empty credentials must not render a file")
		}
	})

	t.Run("no deposited key deletes stray credential files", func(t *testing.T) {
		repo := newFakeMirrorRepo(map[string]string{
			"pipelines/orders.credentials.age": "stale ciphertext",
			"README.md":                        "readme",
		})
		m := withAge(
			mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe("11111111-aaaa", "Orders")}),
			"", fakeCredReader{}, &fakeRenderStore{},
		)
		m.AgeKeys = &fakeAgeKeyStore{err: workspace.ErrBoxCredentialNotFound}

		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if _, ok := repo.files["pipelines/orders.credentials.age"]; ok {
			t.Fatal("stray credential file should be deleted when no key is deposited")
		}
		if repo.files["README.md"] != "readme" {
			t.Fatal("README must not be touched")
		}
	})

	t.Run("unchanged credentials make no commit", func(t *testing.T) {
		_, recipient := newIdentity(t)
		repo := newFakeMirrorRepo(nil)
		creds := fakeCredReader{"11111111-aaaa": json.RawMessage(credsJSON)}
		renders := &fakeRenderStore{}
		m := withAge(mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe("11111111-aaaa", "Orders")}), recipient, creds, renders)

		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		putsAfterFirst := repo.puts
		for range 2 {
			if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
				t.Fatal(err)
			}
		}
		if repo.puts != putsAfterFirst {
			t.Fatalf("re-encrypt churn: %d → %d puts on identical state", putsAfterFirst, repo.puts)
		}
	})

	t.Run("credential change re-renders exactly once", func(t *testing.T) {
		identity, recipient := newIdentity(t)
		repo := newFakeMirrorRepo(nil)
		creds := fakeCredReader{"11111111-aaaa": json.RawMessage(credsJSON)}
		renders := &fakeRenderStore{}
		m := withAge(mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe("11111111-aaaa", "Orders")}), recipient, creds, renders)
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}

		creds["11111111-aaaa"] = json.RawMessage(`{"password":"rotated"}`)
		putsBefore := repo.puts
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if repo.puts != putsBefore+1 {
			t.Fatalf("want exactly one put for the change, got %d", repo.puts-putsBefore)
		}
		if got := string(decryptArmored(t, identity, repo.files["pipelines/orders.credentials.age"])); !strings.Contains(got, "rotated") {
			t.Fatalf("file still holds old credentials: %s", got)
		}
		putsBefore = repo.puts
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if repo.puts != putsBefore {
			t.Fatal("stable state must not re-commit")
		}
	})

	t.Run("key rotation re-encrypts to the new recipient", func(t *testing.T) {
		_, oldRecipient := newIdentity(t)
		repo := newFakeMirrorRepo(nil)
		creds := fakeCredReader{"11111111-aaaa": json.RawMessage(credsJSON)}
		renders := &fakeRenderStore{}
		pipelines := []workspace.Pipeline{pipe("11111111-aaaa", "Orders")}
		m := withAge(mirrorFor(boxCustomer(), repo, pipelines), oldRecipient, creds, renders)
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}

		newIdent, newRecipient := newIdentity(t)
		m2 := withAge(mirrorFor(boxCustomer(), repo, pipelines), newRecipient, creds, renders)
		putsBefore := repo.puts
		if err := m2.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if repo.puts != putsBefore+1 {
			t.Fatalf("rotation must re-encrypt, got %d puts", repo.puts-putsBefore)
		}
		if got := string(decryptArmored(t, newIdent, repo.files["pipelines/orders.credentials.age"])); got != credsJSON {
			t.Fatalf("decrypt with rotated key: %q", got)
		}
	})

	t.Run("out-of-band edit is healed", func(t *testing.T) {
		identity, recipient := newIdentity(t)
		repo := newFakeMirrorRepo(nil)
		creds := fakeCredReader{"11111111-aaaa": json.RawMessage(credsJSON)}
		renders := &fakeRenderStore{}
		m := withAge(mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe("11111111-aaaa", "Orders")}), recipient, creds, renders)
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}

		// Simulate a manual Gitea edit: content and blob sha move on.
		repo.files["pipelines/orders.credentials.age"] = "tampered"
		repo.seq++
		repo.shas["pipelines/orders.credentials.age"] = fmt.Sprintf("sha%d", repo.seq)

		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if got := string(decryptArmored(t, identity, repo.files["pipelines/orders.credentials.age"])); got != credsJSON {
			t.Fatalf("tampered file not healed: %q", got)
		}
	})

	t.Run("cleared credentials delete the file", func(t *testing.T) {
		_, recipient := newIdentity(t)
		repo := newFakeMirrorRepo(nil)
		creds := fakeCredReader{"11111111-aaaa": json.RawMessage(credsJSON)}
		renders := &fakeRenderStore{}
		m := withAge(mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe("11111111-aaaa", "Orders")}), recipient, creds, renders)
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}

		delete(creds, "11111111-aaaa")
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if _, ok := repo.files["pipelines/orders.credentials.age"]; ok {
			t.Fatal("credential file should be deleted with its credentials")
		}
		if _, ok := repo.files["pipelines/orders.yaml"]; !ok {
			t.Fatal("definition must survive credential deletion")
		}
	})

	t.Run("rename moves the credential file with the yaml", func(t *testing.T) {
		identity, recipient := newIdentity(t)
		repo := newFakeMirrorRepo(nil)
		creds := fakeCredReader{"11111111-aaaa": json.RawMessage(credsJSON)}
		renders := &fakeRenderStore{}
		m := withAge(mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe("11111111-aaaa", "Old Name")}), recipient, creds, renders)
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}

		m2 := withAge(mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe("11111111-aaaa", "New Name")}), recipient, creds, renders)
		if err := m2.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		for _, gone := range []string{"pipelines/old-name.yaml", "pipelines/old-name.credentials.age"} {
			if _, ok := repo.files[gone]; ok {
				t.Fatalf("%s should have moved", gone)
			}
		}
		content, ok := repo.files["pipelines/new-name.credentials.age"]
		if !ok {
			t.Fatalf("renamed credential file missing, files: %v", sortedKeys(repo.files))
		}
		if got := string(decryptArmored(t, identity, content)); got != credsJSON {
			t.Fatalf("renamed file decrypts to %q", got)
		}
	})

	t.Run("lost render row re-renders once then stabilizes", func(t *testing.T) {
		_, recipient := newIdentity(t)
		repo := newFakeMirrorRepo(nil)
		creds := fakeCredReader{"11111111-aaaa": json.RawMessage(credsJSON)}
		renders := &fakeRenderStore{}
		m := withAge(mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe("11111111-aaaa", "Orders")}), recipient, creds, renders)
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}

		renders.rows = nil // cache loss must cost one re-encrypt, not correctness
		putsBefore := repo.puts
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if repo.puts != putsBefore+1 {
			t.Fatalf("want one healing put after row loss, got %d", repo.puts-putsBefore)
		}
		putsBefore = repo.puts
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if repo.puts != putsBefore {
			t.Fatal("must stabilize after the healing put")
		}
	})

	t.Run("oauth google_sheets file carries the injected client", func(t *testing.T) {
		identity, recipient := newIdentity(t)
		repo := newFakeMirrorRepo(nil)
		sheets := pipe("11111111-aaaa", "Sheet")
		sheets.SourceType = "google_sheets"
		m := withAge(
			mirrorFor(boxCustomer(), repo, []workspace.Pipeline{sheets}),
			recipient,
			fakeCredReader{"11111111-aaaa": json.RawMessage(`{"oauth":{"refresh_token":"rt","email":"e@example.com"}}`)},
			&fakeRenderStore{},
		)
		m.OAuthClientID = "central-client-id"
		m.OAuthClientSecret = "central-client-secret"

		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		got := string(decryptArmored(t, identity, repo.files["pipelines/sheet.credentials.age"]))
		for _, want := range []string{"central-client-id", "central-client-secret", `"rt"`} {
			if !strings.Contains(got, want) {
				t.Fatalf("decrypted OAuth credentials missing %q: %s", want, got)
			}
		}
	})
}

// boxCustomerWithStorage is a VM-box customer whose EffectiveS3 is provisioned,
// so file_upload pipelines can be resolved to their filesystem form.
func boxCustomerWithStorage() *workspace.Workspace {
	c := boxCustomer()
	c.EffectiveS3 = core.S3Config{
		Bucket:          "ft-acme",
		Endpoint:        "https://acct.r2.cloudflarestorage.com",
		Region:          "auto",
		AccessKeyID:     "AKIA",
		SecretAccessKey: "shh",
	}
	return c
}

func fileUploadPipe(id, name string, files string) workspace.Pipeline {
	return workspace.Pipeline{
		ID:               workspace.PipelineID(id),
		Name:             name,
		SourceType:       workspace.SourceTypeFileUpload,
		SourceConfig:     json.RawMessage(files),
		DatasetName:      "raw",
		WriteDisposition: "replace",
		Enabled:          true,
	}
}

func TestPipelineMirror_FileUpload(t *testing.T) {
	const withFiles = `{"files":[{"name":"orders","file":"orders.csv","size_bytes":10}]}`

	t.Run("rewrites file_upload to filesystem in the rendered yaml", func(t *testing.T) {
		repo := newFakeMirrorRepo(nil)
		m := mirrorFor(boxCustomerWithStorage(), repo, []workspace.Pipeline{
			fileUploadPipe("11111111-aaaa", "Sales Drop", withFiles),
		})
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		content, ok := repo.files["pipelines/sales-drop.yaml"]
		if !ok {
			t.Fatalf("missing rendered file, files: %v", sortedKeys(repo.files))
		}
		// The worker-facing type must be filesystem, never file_upload.
		if !strings.Contains(content, "source_type: filesystem") {
			t.Fatalf("file_upload not rewritten to filesystem:\n%s", content)
		}
		if strings.Contains(content, "file_upload") {
			t.Fatalf("file_upload leaked into the rendered file:\n%s", content)
		}
		// Config points at the pipeline's upload prefix, with the table mapped.
		for _, want := range []string{"s3://ft-acme/uploads/11111111-aaaa/", "name: orders", "file_glob: orders.csv"} {
			if !strings.Contains(content, want) {
				t.Fatalf("rendered config missing %q:\n%s", want, content)
			}
		}
	})

	t.Run("renders the storage credentials as an age file", func(t *testing.T) {
		identity, err := age.GenerateX25519Identity()
		if err != nil {
			t.Fatal(err)
		}
		recipient := identity.Recipient().String()
		repo := newFakeMirrorRepo(nil)
		m := withAge(
			mirrorFor(boxCustomerWithStorage(), repo, []workspace.Pipeline{
				fileUploadPipe("11111111-aaaa", "Sales Drop", withFiles),
			}),
			recipient,
			fakeCredReader{}, // file_upload has no stored credentials
			&fakeRenderStore{},
		)
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		cipher, ok := repo.files["pipelines/sales-drop.credentials.age"]
		if !ok {
			t.Fatalf("missing credential file, files: %v", sortedKeys(repo.files))
		}
		got := string(decryptArmored(t, identity, cipher))
		for _, want := range []string{`"access_key_id":"AKIA"`, `"secret_access_key":"shh"`, "acct.r2.cloudflarestorage.com"} {
			if !strings.Contains(got, want) {
				t.Fatalf("storage credentials missing %q: %s", want, got)
			}
		}
	})

	t.Run("no uploaded files renders nothing", func(t *testing.T) {
		repo := newFakeMirrorRepo(nil)
		m := mirrorFor(boxCustomerWithStorage(), repo, []workspace.Pipeline{
			fileUploadPipe("11111111-aaaa", "Empty Drop", `{}`),
		})
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if _, ok := repo.files["pipelines/empty-drop.yaml"]; ok {
			t.Fatal("an empty file_upload pipeline must not render a definition")
		}
	})

	t.Run("unprovisioned storage drops the pipeline", func(t *testing.T) {
		repo := newFakeMirrorRepo(nil)
		m := mirrorFor(boxCustomer(), repo, []workspace.Pipeline{ // no EffectiveS3
			fileUploadPipe("11111111-aaaa", "Sales Drop", withFiles),
		})
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if _, ok := repo.files["pipelines/sales-drop.yaml"]; ok {
			t.Fatal("file_upload must be skipped when storage is not provisioned")
		}
	})
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func TestPipelineMirror_Gates(t *testing.T) {
	pipelinesMustNotBeListed := &mockPipelineRepo{
		listPipelinesByCustomerFn: func(context.Context, string) ([]workspace.Pipeline, error) {
			return nil, errors.New("must not be called")
		},
	}

	t.Run("shared substrate is skipped", func(t *testing.T) {
		ws := &workspace.Workspace{Slug: "acme"} // OnVM=false = shared
		m := mirrorFor(ws, newFakeMirrorRepo(nil), nil)
		m.Pipelines = pipelinesMustNotBeListed
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatalf("shared substrate must be a silent skip, got %v", err)
		}
	})

	t.Run("missing deposited credential is skipped", func(t *testing.T) {
		m := mirrorFor(boxCustomer(), newFakeMirrorRepo(nil), nil)
		m.Pipelines = pipelinesMustNotBeListed
		m.Credentials = &fakeCredStore{err: workspace.ErrBoxCredentialNotFound}
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatalf("missing credential must be a silent skip, got %v", err)
		}
	})

	t.Run("unprovisioned domain is skipped", func(t *testing.T) {
		ws := boxCustomer()
		ws.CustomerDomain = ""
		m := mirrorFor(ws, newFakeMirrorRepo(nil), nil)
		m.Pipelines = pipelinesMustNotBeListed
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatalf("unprovisioned box must be a silent skip, got %v", err)
		}
	})
}

func TestPipelineMirror_Versions(t *testing.T) {
	pipe := workspace.Pipeline{
		ID:           "11111111-aaaa",
		Name:         "Orders",
		SourceType:   "rest_api",
		SourceConfig: json.RawMessage(`{"url":"https://example.com"}`),
		DatasetName:  "raw",
		Schedule:     "0 * * * *",
		Enabled:      true,
	}
	// Render once so version content is byte-exact with the real format.
	repo := newFakeMirrorRepo(nil)
	m := mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe})
	if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
		t.Fatal(err)
	}
	rendered := repo.files["pipelines/orders.yaml"]
	if rendered == "" {
		t.Fatal("rendered file missing")
	}

	t.Run("ListVersions returns history rows", func(t *testing.T) {
		versions, err := m.ListVersions(context.Background(), "acme", pipe.ID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(versions) != 1 || versions[0].AuthorName != "Alice" || versions[0].SHA == "" {
			t.Fatalf("bad versions: %+v", versions)
		}
	})

	t.Run("VersionAt round-trips the rendered file", func(t *testing.T) {
		repo.atRef = rendered
		p, err := m.VersionAt(context.Background(), "acme", pipe.ID, "c0ffee1234567")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.Name != "Orders" || p.SourceType != "rest_api" || p.DatasetName != "raw" ||
			p.Schedule != "0 * * * *" || !p.Enabled {
			t.Fatalf("bad round-trip: %+v", p)
		}
		if !strings.Contains(string(p.SourceConfig), "https://example.com") {
			t.Fatalf("source config lost: %s", p.SourceConfig)
		}
	})

	t.Run("VersionAt rejects a foreign pipeline's file", func(t *testing.T) {
		repo.atRef = strings.Replace(rendered, "11111111-aaaa", "99999999-bbbb", 1)
		_, err := m.VersionAt(context.Background(), "acme", pipe.ID, "c0ffee1234567")
		if !errors.Is(err, workspace.ErrPipelineVersionMismatch) {
			t.Fatalf("want ErrPipelineVersionMismatch, got %v", err)
		}
	})

	t.Run("VersionAt rejects non-sha refs", func(t *testing.T) {
		_, err := m.VersionAt(context.Background(), "acme", pipe.ID, "main")
		var invalid *workspace.ErrInvalidSourceConfig
		if !errors.As(err, &invalid) || invalid.Field != "sha" {
			t.Fatalf("want sha validation error, got %v", err)
		}
	})

	t.Run("unknown pipeline is not found", func(t *testing.T) {
		_, err := m.ListVersions(context.Background(), "acme", "no-such-id")
		if !errors.Is(err, workspace.ErrPipelineNotFound) {
			t.Fatalf("want ErrPipelineNotFound, got %v", err)
		}
	})

	t.Run("out-of-scope customer is unavailable", func(t *testing.T) {
		shared := boxCustomer()
		shared.OnVM = false
		m2 := mirrorFor(shared, newFakeMirrorRepo(nil), []workspace.Pipeline{pipe})
		_, err := m2.ListVersions(context.Background(), "acme", pipe.ID)
		if !errors.Is(err, workspace.ErrBoxRepoUnavailable) {
			t.Fatalf("want ErrBoxRepoUnavailable, got %v", err)
		}
	})
}

// fakeDefRenderStore is the in-memory PipelineDefinitionRenderStore.
type fakeDefRenderStore struct {
	rows map[workspace.PipelineID]workspace.PipelineDefinitionRender
}

func newFakeDefRenderStore() *fakeDefRenderStore {
	return &fakeDefRenderStore{rows: map[workspace.PipelineID]workspace.PipelineDefinitionRender{}}
}

func (f *fakeDefRenderStore) UpsertPipelineDefinitionRender(_ context.Context, r *workspace.PipelineDefinitionRender) error {
	f.rows[r.PipelineID] = *r
	return nil
}

func (f *fakeDefRenderStore) GetPipelineDefinitionRenders(context.Context, string) (map[workspace.PipelineID]workspace.PipelineDefinitionRender, error) {
	out := make(map[workspace.PipelineID]workspace.PipelineDefinitionRender, len(f.rows))
	for k, v := range f.rows {
		out[k] = v
	}
	return out, nil
}

func (f *fakeDefRenderStore) MarkPipelineDefinitionRefused(_ context.Context, id workspace.PipelineID, refusedSHA string) error {
	row := f.rows[id]
	row.PipelineID = id
	row.RefusedBlobSHA = refusedSHA
	f.rows[id] = row
	return nil
}

type fakeNotifier struct {
	notes []workspace.Notification
}

func (f *fakeNotifier) Notify(_ context.Context, n workspace.Notification) error {
	f.notes = append(f.notes, n)
	return nil
}

// tamper simulates an out-of-band commit: new content, new blob sha.
func (f *fakeMirrorRepo) tamper(path, content string) {
	f.files[path] = content
	f.seq++
	f.shas[path] = fmt.Sprintf("tampered%d", f.seq)
}

func TestPipelineMirror_DriftDetection(t *testing.T) {
	pipe := workspace.Pipeline{
		ID:           "11111111-aaaa",
		Name:         "Orders",
		SourceType:   "rest_api",
		SourceConfig: json.RawMessage(`{"url":"https://example.com"}`),
		DatasetName:  "raw",
		Enabled:      true,
	}
	const path = "pipelines/orders.yaml"

	setup := func(pipelines []workspace.Pipeline) (*fakeMirrorRepo, *fakeDefRenderStore, *fakeNotifier, *workspace.PipelineMirror) {
		repo := newFakeMirrorRepo(nil)
		store := newFakeDefRenderStore()
		notifier := &fakeNotifier{}
		m := mirrorFor(boxCustomer(), repo, pipelines)
		m.DefinitionRenders = store
		m.Notifications = notifier
		return repo, store, notifier, m
	}

	t.Run("out-of-band edit is overwritten and notified once", func(t *testing.T) {
		repo, store, notifier, m := setup([]workspace.Pipeline{pipe})
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		rendered := repo.files[path]

		repo.tamper(path, "schedule: '* * * * *' # sneaky")
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if repo.files[path] != rendered {
			t.Fatalf("drifted file must be reapplied, got:\n%s", repo.files[path])
		}
		if len(notifier.notes) != 1 || !strings.Contains(notifier.notes[0].Body, path) {
			t.Fatalf("want exactly one drift notification naming %s, got %+v", path, notifier.notes)
		}
		if store.rows[pipe.ID].BlobSHA != repo.shas[path] {
			t.Fatalf("row not re-stamped after overwrite")
		}

		// Converged again — no further notifications.
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if len(notifier.notes) != 1 {
			t.Fatalf("drift must notify once, got %d", len(notifier.notes))
		}
	})

	t.Run("console edit staleness does not notify", func(t *testing.T) {
		repo, store, notifier, m := setup([]workspace.Pipeline{pipe})
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}

		changed := pipe
		changed.Schedule = "0 6 * * *"
		m2 := mirrorFor(boxCustomer(), repo, []workspace.Pipeline{changed})
		m2.DefinitionRenders = store
		m2.Notifications = notifier
		if err := m2.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(repo.files[path], "0 6 * * *") {
			t.Fatal("console change not applied")
		}
		if len(notifier.notes) != 0 {
			t.Fatalf("a stale render is not drift, got %+v", notifier.notes)
		}
	})

	t.Run("no recorded row means no drift claim", func(t *testing.T) {
		repo := newFakeMirrorRepo(map[string]string{path: "pre-existing content"})
		store := newFakeDefRenderStore()
		notifier := &fakeNotifier{}
		m := mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe})
		m.DefinitionRenders = store
		m.Notifications = notifier
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if len(notifier.notes) != 0 {
			t.Fatalf("bootstrap overwrite must not notify, got %+v", notifier.notes)
		}
		if store.rows[pipe.ID].Path != path {
			t.Fatal("row not recorded on bootstrap overwrite")
		}
	})

	t.Run("out-of-band edit equal to the render is adopted silently", func(t *testing.T) {
		repo, store, notifier, m := setup([]workspace.Pipeline{pipe})
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		// Same content, new blob sha (e.g. a revert commit).
		repo.seq++
		repo.shas[path] = fmt.Sprintf("revert%d", repo.seq)

		putsBefore := repo.puts
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if repo.puts != putsBefore {
			t.Fatal("content-equal file must not be re-committed")
		}
		if len(notifier.notes) != 0 {
			t.Fatalf("nothing was overwritten — no notification, got %+v", notifier.notes)
		}
		if store.rows[pipe.ID].BlobSHA != repo.shas[path] {
			t.Fatal("row must adopt the new blob sha")
		}
	})
}

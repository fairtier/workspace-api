package gitea_test

// The git-as-source-of-truth soak: the four scenarios the pipelines plane is
// contractually held to — once the gate on the PIPELINES_GIT_PRIMARY flip,
// now simply the contract, the flip and its legacy alternative having been
// retired in the pipelines-as-files Phase 2.5 cleanup. Driven
// end-to-end against a REAL Gitea through the real
// gitea.Client — not the in-memory repo fakes the workspace unit tests use.
// It lives in the adapter package
// (not workspace) because the workspace plane must not import infra
// adapters (depguard); testing the real adapter is exactly this package's
// job. Run via scripts/gitea-soak.sh, which boots a throwaway Gitea
// matching the box image and provisions the owner + token; without
// GITEA_SOAK_URL/GITEA_SOAK_TOKEN the test skips.
//
// Scenarios:
//  1. git-first save — create/update commit the box repo synchronously
//  2. Gitea-down save failure — the save hard-fails and the cache row is
//     compensated back
//  3. hand-edit adoption — a foreign valid edit is adopted into the cache,
//     an unparseable one is refused with a once-per-commit notification,
//     and an explicit Console save still overwrites
//  4. foreign .age edit flips the pipeline to externally-managed
//     credentials; a Console credential edit reclaims the file

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"filippo.io/age"
	"filippo.io/age/armor"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/crypto"
	"github.com/fairtier/workspace-api/gitea"
	"github.com/fairtier/workspace-api/workspace"
)

// soakStore is a stateful in-memory PipelineRepository (+ credential reader
// and ownership store) standing in for central Postgres, so compensation
// and adoption are observable.
type soakStore struct {
	mu      sync.Mutex
	rows    map[workspace.PipelineID]*workspace.Pipeline
	seq     int
	renders *soakRenderStore
}

func newSoakStore(renders *soakRenderStore) *soakStore {
	return &soakStore{rows: map[workspace.PipelineID]*workspace.Pipeline{}, renders: renders}
}

func (s *soakStore) CreatePipeline(_ context.Context, p *workspace.Pipeline) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.ID == "" {
		s.seq++
		p.ID = workspace.PipelineID(fmt.Sprintf("s0ak%04d-0000-4000-8000-000000000000", s.seq))
	}
	cp := *p
	s.rows[p.ID] = &cp
	return nil
}

func (s *soakStore) GetPipeline(_ context.Context, id workspace.PipelineID) (*workspace.Pipeline, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[id]
	if !ok {
		return nil, workspace.ErrPipelineNotFound
	}
	cp := *row
	return &cp, nil
}

func (s *soakStore) ListPipelinesByCustomer(_ context.Context, customerSlug string) ([]workspace.Pipeline, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []workspace.Pipeline
	for _, row := range s.rows {
		if row.CustomerSlug == customerSlug {
			out = append(out, *row)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// UpdatePipeline preserves CredentialsExternal like the Postgres store does:
// the flag is owned by the ownership methods, never by a row update.
func (s *soakStore) UpdatePipeline(_ context.Context, p *workspace.Pipeline) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.rows[p.ID]
	if !ok {
		return workspace.ErrPipelineNotFound
	}
	cp := *p
	cp.CredentialsExternal = existing.CredentialsExternal
	s.rows[p.ID] = &cp
	return nil
}

func (s *soakStore) DeletePipeline(_ context.Context, id workspace.PipelineID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rows, id)
	delete(s.renders.rows, id) // FK cascade
	return nil
}

func (s *soakStore) GetEnabledPipelines(context.Context, string) ([]workspace.Pipeline, error) {
	return nil, nil
}
func (s *soakStore) CreatePipelineRun(context.Context, *workspace.PipelineRun) error { return nil }
func (s *soakStore) UpdatePipelineRun(context.Context, *workspace.PipelineRun) error { return nil }
func (s *soakStore) ListRecentRuns(context.Context, workspace.PipelineID, int) ([]workspace.PipelineRun, error) {
	return nil, nil
}

func (s *soakStore) GetPendingRun(context.Context, workspace.PipelineID) (*workspace.PipelineRun, error) {
	return nil, nil
}

func (s *soakStore) ListPipelineCredentialsByCustomer(_ context.Context, customerSlug string) (map[workspace.PipelineID]json.RawMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[workspace.PipelineID]json.RawMessage{}
	for id, row := range s.rows {
		if row.CustomerSlug == customerSlug && len(row.SourceCredentials) > 0 {
			out[id] = row.SourceCredentials
		}
	}
	return out, nil
}

func (s *soakStore) SetPipelineCredentialsExternal(_ context.Context, id workspace.PipelineID, external bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[id]
	if !ok {
		return workspace.ErrPipelineNotFound
	}
	row.CredentialsExternal = external
	return nil
}

func (s *soakStore) DeletePipelineCredentialRender(_ context.Context, id workspace.PipelineID) error {
	delete(s.renders.rows, id)
	return nil
}

// soakRenderStore is the in-memory PipelineCredentialRenderStore.
type soakRenderStore struct {
	rows map[workspace.PipelineID]workspace.PipelineCredentialRender
}

func (f *soakRenderStore) UpsertPipelineCredentialRender(_ context.Context, r *workspace.PipelineCredentialRender) error {
	f.rows[r.PipelineID] = *r
	return nil
}

func (f *soakRenderStore) GetPipelineCredentialRenders(context.Context, string) (map[workspace.PipelineID]workspace.PipelineCredentialRender, error) {
	return f.rows, nil
}

// soakDefRenderStore is the in-memory PipelineDefinitionRenderStore.
type soakDefRenderStore struct {
	rows map[workspace.PipelineID]workspace.PipelineDefinitionRender
}

func (f *soakDefRenderStore) UpsertPipelineDefinitionRender(_ context.Context, r *workspace.PipelineDefinitionRender) error {
	f.rows[r.PipelineID] = *r
	return nil
}

func (f *soakDefRenderStore) GetPipelineDefinitionRenders(context.Context, string) (map[workspace.PipelineID]workspace.PipelineDefinitionRender, error) {
	out := make(map[workspace.PipelineID]workspace.PipelineDefinitionRender, len(f.rows))
	maps.Copy(out, f.rows)
	return out, nil
}

func (f *soakDefRenderStore) MarkPipelineDefinitionRefused(_ context.Context, id workspace.PipelineID, refusedSHA string) error {
	row := f.rows[id]
	row.PipelineID = id
	row.RefusedBlobSHA = refusedSHA
	f.rows[id] = row
	return nil
}

type soakNotifier struct {
	notes []workspace.Notification
}

func (f *soakNotifier) Notify(_ context.Context, n workspace.Notification) error {
	f.notes = append(f.notes, n)
	return nil
}

// soakResolver serves one fixed workspace for both lookup shapes.
type soakResolver struct{ ws *workspace.Workspace }

func (r *soakResolver) GetWorkspace(context.Context, string) (*workspace.Workspace, error) {
	return r.ws, nil
}

func (r *soakResolver) GetWorkspaceByUser(context.Context, core.UserID) (*workspace.Workspace, error) {
	return r.ws, nil
}

type soakCredStore struct{ cred *workspace.BoxGitCredential }

func (f *soakCredStore) UpsertBoxGitCredential(context.Context, *workspace.BoxGitCredential) error {
	return nil
}

func (f *soakCredStore) GetBoxGitCredential(context.Context, string) (*workspace.BoxGitCredential, error) {
	return f.cred, nil
}

type soakAgeKeyStore struct{ key *workspace.BoxAgeKey }

func (f *soakAgeKeyStore) UpsertBoxAgeKey(context.Context, *workspace.BoxAgeKey) error { return nil }

func (f *soakAgeKeyStore) GetBoxAgeKey(context.Context, string) (*workspace.BoxAgeKey, error) {
	return f.key, nil
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

// soakGitea recreates a fairtier-admin repo (auto-init, like the box seed
// job) so every run starts from a clean tree.
func soakGitea(t *testing.T, baseURL, token, repo string) {
	t.Helper()
	do := func(method, path string, body any, want ...int) {
		t.Helper()
		var reqBody io.Reader
		if body != nil {
			payload, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			reqBody = bytes.NewReader(payload)
		}
		req, err := http.NewRequest(method, baseURL+path, reqBody)
		if err != nil {
			t.Fatal(err)
		}
		req.SetBasicAuth(gitea.Owner, token)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("gitea %s %s: %v", method, path, err)
		}
		defer resp.Body.Close()
		if slices.Contains(want, resp.StatusCode) {
			return
		}
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		t.Fatalf("gitea %s %s: status %d: %s", method, path, resp.StatusCode, snippet)
	}
	do(http.MethodDelete, "/api/v1/repos/"+gitea.Owner+"/"+repo, nil, http.StatusNoContent, http.StatusNotFound)
	do(http.MethodPost, "/api/v1/user/repos", map[string]any{"name": repo, "auto_init": true}, http.StatusCreated)
}

// deadPort returns a URL nothing listens on (bind, read the port, close).
func deadPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return "http://" + addr
}

func TestGiteaSoak_GitPrimary(t *testing.T) {
	baseURL := os.Getenv("GITEA_SOAK_URL")
	token := os.Getenv("GITEA_SOAK_TOKEN")
	if baseURL == "" || token == "" {
		t.Skip("GITEA_SOAK_URL / GITEA_SOAK_TOKEN not set — run scripts/gitea-soak.sh")
	}
	ctx := context.Background()
	soakGitea(t, baseURL, token, "pipelines")

	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	renders := &soakRenderStore{rows: map[workspace.PipelineID]workspace.PipelineCredentialRender{}}
	defRenders := &soakDefRenderStore{rows: map[workspace.PipelineID]workspace.PipelineDefinitionRender{}}
	notes := &soakNotifier{}
	store := newSoakStore(renders)
	ws := &workspace.Workspace{Slug: "acme"}
	ws.OnVM = true
	ws.CustomerDomain = "*.customer-acme.fairtier.com"
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// raw is the out-of-band editor: same real client, used to simulate a
	// customer committing directly in the box's Gitea.
	raw := &gitea.Client{BaseURL: baseURL, Username: gitea.Owner, Token: token}

	mirrorTo := func(url string) *workspace.PipelineMirror {
		return &workspace.PipelineMirror{
			Workspaces:          &soakResolver{ws: ws},
			Credentials:         &soakCredStore{cred: &workspace.BoxGitCredential{Username: gitea.Owner, Token: token}},
			Pipelines:           store,
			AgeKeys:             &soakAgeKeyStore{key: &workspace.BoxAgeKey{PublicKey: identity.Recipient().String()}},
			Renders:             renders,
			Fingerprint:         crypto.SHA256Fingerprinter{},
			PipelineCredentials: store,
			DefinitionRenders:   defRenders,
			Notifications:       notes,
			Ownership:           store,
			Logger:              logger,
			NewClient: func(_, username, tok string) workspace.RepoFileClient {
				return &gitea.Client{BaseURL: url, Username: username, Token: tok}
			},
		}
	}
	serviceWith := func(m *workspace.PipelineMirror) *workspace.PipelineService {
		return &workspace.PipelineService{
			Workspaces: &soakResolver{ws: ws},
			Pipelines:  store,
			Mirror:     m,
			Ownership:  store,
			Logger:     logger,
		}
	}
	mirror := mirrorTo(baseURL)
	svc := serviceWith(mirror)
	deadSvc := serviceWith(mirrorTo(deadPort(t)))

	repoFile := func(t *testing.T, path string) (string, string) {
		t.Helper()
		content, sha, err := raw.GetContents(ctx, "pipelines", path)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}
		return content, sha
	}
	notesWith := func(substr string) int {
		n := 0
		for _, note := range notes.notes {
			if strings.Contains(note.Title, substr) {
				n++
			}
		}
		return n
	}

	const yamlPath = "pipelines/orders-sync.yaml"
	var orders *workspace.Pipeline

	t.Run("git-first save commits the repo synchronously", func(t *testing.T) {
		p := &workspace.Pipeline{
			Name:         "Orders Sync",
			SourceType:   "rest_api",
			SourceConfig: json.RawMessage(`{"base_url":"https://api.example.com","resources":[{"name":"posts","endpoint":"/posts"}]}`),
			DatasetName:  "raw",
			Schedule:     "0 * * * *",
		}
		orders, err = svc.CreatePipeline(ctx, "user-1", p)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		content, _ := repoFile(t, yamlPath)
		for _, want := range []string{"id: " + string(orders.ID), "name: Orders Sync", "0 * * * *"} {
			if !strings.Contains(content, want) {
				t.Fatalf("rendered file missing %q:\n%s", want, content)
			}
		}

		upd := *orders
		upd.Schedule = "45 * * * *"
		if _, err := svc.UpdatePipeline(ctx, "user-1", &upd); err != nil {
			t.Fatalf("update: %v", err)
		}
		if content, _ := repoFile(t, yamlPath); !strings.Contains(content, "45 * * * *") {
			t.Fatalf("update not committed:\n%s", content)
		}
	})

	t.Run("gitea-down save hard-fails and compensates the row", func(t *testing.T) {
		doomed := &workspace.Pipeline{
			Name:         "Doomed",
			SourceType:   "rest_api",
			SourceConfig: json.RawMessage(`{"base_url":"https://api.example.com","resources":[{"name":"x","endpoint":"/x"}]}`),
			DatasetName:  "raw",
		}
		created, err := deadSvc.CreatePipeline(ctx, "user-1", doomed)
		if err == nil {
			t.Fatal("create must fail when the box is unreachable")
		}
		if created != nil {
			if _, err := store.GetPipeline(ctx, created.ID); err == nil {
				t.Fatal("failed create left its cache row behind")
			}
		}
		rows, _ := store.ListPipelinesByCustomer(ctx, "acme")
		for _, r := range rows {
			if r.Name == "Doomed" {
				t.Fatal("failed create left its cache row behind")
			}
		}

		upd := *orders
		upd.Schedule = "5 * * * *"
		if _, err := deadSvc.UpdatePipeline(ctx, "user-1", &upd); err == nil {
			t.Fatal("update must fail when the box is unreachable")
		}
		got, err := store.GetPipeline(ctx, orders.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Schedule != "45 * * * *" {
			t.Fatalf("failed update was not compensated: schedule %q", got.Schedule)
		}
	})

	t.Run("foreign valid edit is adopted into the cache", func(t *testing.T) {
		content, sha := repoFile(t, yamlPath)
		edited := strings.Replace(content, "45 * * * *", "30 * * * *", 1)
		if edited == content {
			t.Fatalf("schedule not found in rendered file:\n%s", content)
		}
		if _, err := raw.PutContents(ctx, "pipelines", yamlPath, edited, sha, "hand edit", nil); err != nil {
			t.Fatalf("hand edit: %v", err)
		}

		if err := mirror.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatalf("adopt: %v", err)
		}
		got, err := store.GetPipeline(ctx, orders.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Schedule != "30 * * * *" {
			t.Fatalf("foreign edit not adopted: schedule %q", got.Schedule)
		}
		if notesWith("Pipeline updated from your repo") != 1 {
			t.Fatalf("want exactly one adopted notification, notes: %+v", notes.notes)
		}
	})

	t.Run("unparseable edit is refused once and a Console save overwrites", func(t *testing.T) {
		_, sha := repoFile(t, yamlPath)
		if _, err := raw.PutContents(ctx, "pipelines", yamlPath, "{{ this is not yaml", sha, "break it", nil); err != nil {
			t.Fatalf("hand edit: %v", err)
		}

		if err := mirror.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatalf("adopt: %v", err)
		}
		got, _ := store.GetPipeline(ctx, orders.ID)
		if got.Schedule != "30 * * * *" {
			t.Fatalf("refused edit must not change the cache: schedule %q", got.Schedule)
		}
		if notesWith("Pipeline file edit could not be applied") != 1 {
			t.Fatalf("want exactly one refusal notification, notes: %+v", notes.notes)
		}
		// Second sweep over the same refused commit stays silent.
		if err := mirror.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatalf("adopt: %v", err)
		}
		if notesWith("Pipeline file edit could not be applied") != 1 {
			t.Fatal("refusal must notify once per commit")
		}

		// An explicit Console save is newer intent: it overwrites the file.
		upd := *got
		upd.Schedule = "15 * * * *"
		if _, err := svc.UpdatePipeline(ctx, "user-1", &upd); err != nil {
			t.Fatalf("update: %v", err)
		}
		content, _ := repoFile(t, yamlPath)
		if !strings.Contains(content, "15 * * * *") || strings.Contains(content, "not yaml") {
			t.Fatalf("Console save must overwrite the broken file:\n%s", content)
		}
	})

	const agePath = "pipelines/db-load.credentials.age"
	var dbLoad *workspace.Pipeline

	t.Run("credentials render as an age file the box key decrypts", func(t *testing.T) {
		p := &workspace.Pipeline{
			Name:              "DB Load",
			SourceType:        "sql_database",
			SourceConfig:      json.RawMessage(`{"tables":["users"]}`),
			SourceCredentials: json.RawMessage(`{"connection_string":"postgres://u:old@db/x"}`),
			DatasetName:       "raw",
		}
		dbLoad, err = svc.CreatePipeline(ctx, "user-1", p)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		content, _ := repoFile(t, agePath)
		if got := decryptArmored(t, identity, content); !strings.Contains(string(got), "postgres://u:old@db/x") {
			t.Fatalf("decrypted credentials wrong: %s", got)
		}
	})

	t.Run("foreign age edit flips ownership to the box", func(t *testing.T) {
		_, sha := repoFile(t, agePath)
		if _, err := raw.PutContents(ctx, "pipelines", agePath, "box-owned ciphertext", sha, "rotate on the box", nil); err != nil {
			t.Fatalf("hand edit: %v", err)
		}

		if err := mirror.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatalf("adopt: %v", err)
		}
		got, _ := store.GetPipeline(ctx, dbLoad.ID)
		if !got.CredentialsExternal {
			t.Fatal("foreign .age edit must flip the pipeline to externally-managed credentials")
		}
		if notesWith("Pipeline credentials are now managed in your repo") != 1 {
			t.Fatalf("want the external-credentials notification, notes: %+v", notes.notes)
		}

		// A rendering converge must now leave the box's file alone.
		if err := mirror.SyncCustomer(ctx, "acme", nil); err != nil {
			t.Fatalf("sync: %v", err)
		}
		if content, _ := repoFile(t, agePath); content != "box-owned ciphertext" {
			t.Fatalf("converge overwrote an externally-managed file:\n%s", content)
		}
	})

	t.Run("a Console credential edit reclaims the file", func(t *testing.T) {
		got, _ := store.GetPipeline(ctx, dbLoad.ID)
		upd := *got
		upd.SourceCredentials = json.RawMessage(`{"connection_string":"postgres://u:new@db/x"}`)
		if _, err := svc.UpdatePipeline(ctx, "user-1", &upd); err != nil {
			t.Fatalf("update: %v", err)
		}
		after, _ := store.GetPipeline(ctx, dbLoad.ID)
		if after.CredentialsExternal {
			t.Fatal("credential edit must reclaim ownership")
		}
		content, _ := repoFile(t, agePath)
		if got := decryptArmored(t, identity, content); !strings.Contains(string(got), "postgres://u:new@db/x") {
			t.Fatalf("reclaimed file must hold the fresh credentials: %s", got)
		}
	})

	t.Run("delete removes both files", func(t *testing.T) {
		if err := svc.DeletePipeline(ctx, "user-1", dbLoad.ID); err != nil {
			t.Fatalf("delete: %v", err)
		}
		for _, path := range []string{"pipelines/db-load.yaml", agePath} {
			if _, _, err := raw.GetContents(ctx, "pipelines", path); err == nil {
				t.Fatalf("%s must be deleted", path)
			}
		}
	})
}

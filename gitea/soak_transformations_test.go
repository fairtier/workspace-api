package gitea_test

// The TRANSFORMATIONS_GIT_PRIMARY rollout soak — the transformations twin
// of TestGiteaSoak_GitPrimary, driven
// against a REAL Gitea through the real gitea.Client. Same runner
// (scripts/gitea-soak.sh); skips without GITEA_SOAK_URL/GITEA_SOAK_TOKEN.
//
// Scenarios the flip is gated on:
//  1. git-first save — create/update commit transformations/<slug>.yaml
//     synchronously, leaving the dbt project files untouched
//  2. Gitea-down save failure — the save hard-fails and the cache row is
//     compensated back
//  3. hand-edit adoption — a foreign valid edit is adopted into the cache,
//     an unparseable one is refused with a once-per-commit notification,
//     and an explicit Console save still overwrites
//  4. delete removes the rendered file (and only it)

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/fairtier/workspace-api/gitea"
	"github.com/fairtier/workspace-api/workspace"
)

// soakTransformationStore is a stateful in-memory TransformationRepository,
// so compensation and adoption are observable.
type soakTransformationStore struct {
	mu   sync.Mutex
	rows map[workspace.TransformationID]*workspace.Transformation
	seq  int
}

func newSoakTransformationStore() *soakTransformationStore {
	return &soakTransformationStore{rows: map[workspace.TransformationID]*workspace.Transformation{}}
}

func (s *soakTransformationStore) CreateTransformation(_ context.Context, t *workspace.Transformation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.ID == "" {
		s.seq++
		t.ID = workspace.TransformationID(fmt.Sprintf("33333333-%04d", s.seq))
	}
	cp := *t
	s.rows[t.ID] = &cp
	return nil
}

func (s *soakTransformationStore) GetTransformation(_ context.Context, id workspace.TransformationID) (*workspace.Transformation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.rows[id]
	if !ok {
		return nil, workspace.ErrTransformationNotFound
	}
	cp := *t
	return &cp, nil
}

func (s *soakTransformationStore) ListTransformationsByCustomer(_ context.Context, customerSlug string) ([]workspace.Transformation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []workspace.Transformation
	for _, t := range s.rows {
		if t.CustomerSlug == customerSlug {
			out = append(out, *t)
		}
	}
	return out, nil
}

func (s *soakTransformationStore) UpdateTransformation(_ context.Context, t *workspace.Transformation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *t
	s.rows[t.ID] = &cp
	return nil
}

func (s *soakTransformationStore) DeleteTransformation(_ context.Context, id workspace.TransformationID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rows, id)
	return nil
}

func (s *soakTransformationStore) GetEnabledTransformations(context.Context, string) ([]workspace.Transformation, error) {
	return nil, nil
}

func (s *soakTransformationStore) CreateTransformationRun(context.Context, *workspace.TransformationRun) error {
	return nil
}

func (s *soakTransformationStore) UpdateTransformationRun(context.Context, *workspace.TransformationRun) error {
	return nil
}

func (s *soakTransformationStore) ListRecentTransformationRuns(context.Context, workspace.TransformationID, int) ([]workspace.TransformationRun, error) {
	return nil, nil
}

// soakTransformationRenderStore is the in-memory
// TransformationDefinitionRenderStore.
type soakTransformationRenderStore struct {
	mu   sync.Mutex
	rows map[workspace.TransformationID]workspace.TransformationDefinitionRender
}

func (f *soakTransformationRenderStore) UpsertTransformationDefinitionRender(_ context.Context, r *workspace.TransformationDefinitionRender) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows[r.TransformationID] = *r
	return nil
}

func (f *soakTransformationRenderStore) GetTransformationDefinitionRenders(context.Context, string) (map[workspace.TransformationID]workspace.TransformationDefinitionRender, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[workspace.TransformationID]workspace.TransformationDefinitionRender, len(f.rows))
	maps.Copy(out, f.rows)
	return out, nil
}

func (f *soakTransformationRenderStore) MarkTransformationDefinitionRefused(_ context.Context, id workspace.TransformationID, refusedSHA string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	row := f.rows[id]
	row.TransformationID = id
	row.RefusedBlobSHA = refusedSHA
	f.rows[id] = row
	return nil
}

// soakPipelineReader resolves the one pipeline id the trigger scenarios use.
type soakPipelineReader struct{}

func (soakPipelineReader) GetPipeline(_ context.Context, id workspace.PipelineID) (*workspace.Pipeline, error) {
	if id == "pipe-acme" {
		return &workspace.Pipeline{ID: id, CustomerSlug: "acme"}, nil
	}
	return nil, workspace.ErrPipelineNotFound
}

func TestGiteaSoak_TransformationsGitPrimary(t *testing.T) {
	baseURL := os.Getenv("GITEA_SOAK_URL")
	token := os.Getenv("GITEA_SOAK_TOKEN")
	if baseURL == "" || token == "" {
		t.Skip("GITEA_SOAK_URL / GITEA_SOAK_TOKEN not set — run scripts/gitea-soak.sh")
	}
	ctx := context.Background()
	soakGitea(t, baseURL, token, "transformations")

	store := newSoakTransformationStore()
	defRenders := &soakTransformationRenderStore{rows: map[workspace.TransformationID]workspace.TransformationDefinitionRender{}}
	notes := &soakNotifier{}
	ws := &workspace.Workspace{Slug: "acme"}
	ws.OnVM = true
	ws.CustomerDomain = "*.customer-acme.fairtier.com"
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// raw is the out-of-band editor: same real client, used to simulate a
	// customer committing directly in the box's Gitea. Also seeds a dbt file
	// the mirror must never touch.
	raw := &gitea.Client{BaseURL: baseURL, Username: gitea.Owner, Token: token}
	if _, err := raw.PutContents(ctx, "transformations", "models/marts/orders.sql", "select 1", "", "seed dbt model", nil); err != nil {
		t.Fatalf("seed dbt file: %v", err)
	}

	mirrorTo := func(url string) *workspace.TransformationMirror {
		return &workspace.TransformationMirror{
			Workspaces:        &soakResolver{ws: ws},
			Credentials:       &soakCredStore{cred: &workspace.BoxGitCredential{Username: gitea.Owner, Token: token}},
			Transformations:   store,
			Pipelines:         soakPipelineReader{},
			DefinitionRenders: defRenders,
			Notifications:     notes,
			Logger:            logger,
			NewClient: func(_, username, tok string) workspace.RepoFileClient {
				return &gitea.Client{BaseURL: url, Username: username, Token: tok}
			},
		}
	}
	serviceWith := func(m *workspace.TransformationMirror) *workspace.TransformationService {
		return &workspace.TransformationService{
			Workspaces:      &soakResolver{ws: ws},
			Transformations: store,
			Pipelines:       soakPipelineReader{},
			Mirror:          m,
			GitPrimary:      true,
			Logger:          logger,
		}
	}
	mirror := mirrorTo(baseURL)
	svc := serviceWith(mirror)
	deadSvc := serviceWith(mirrorTo(deadPort(t)))

	repoFile := func(t *testing.T, path string) (string, string) {
		t.Helper()
		content, sha, err := raw.GetContents(ctx, "transformations", path)
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

	const yamlPath = "transformations/nightly-marts.yaml"
	var nightly *workspace.Transformation

	t.Run("git-first save commits the repo synchronously", func(t *testing.T) {
		tr := &workspace.Transformation{
			Name:        "Nightly Marts",
			Schedule:    "0 2 * * *",
			DBTSelector: "marts",
		}
		var err error
		nightly, err = svc.CreateTransformation(ctx, "user-1", tr)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		content, _ := repoFile(t, yamlPath)
		for _, want := range []string{"id: " + string(nightly.ID), "name: Nightly Marts", "0 2 * * *", "dbt_selector: marts"} {
			if !strings.Contains(content, want) {
				t.Fatalf("rendered file missing %q:\n%s", want, content)
			}
		}
		if content, _ := repoFile(t, "models/marts/orders.sql"); content != "select 1" {
			t.Fatal("the dbt project files must never be touched")
		}

		upd := *nightly
		upd.Schedule = "0 4 * * *"
		if _, err := svc.UpdateTransformation(ctx, "user-1", &upd); err != nil {
			t.Fatalf("update: %v", err)
		}
		if content, _ := repoFile(t, yamlPath); !strings.Contains(content, "0 4 * * *") {
			t.Fatalf("update not committed:\n%s", content)
		}
	})

	t.Run("gitea-down save hard-fails and compensates the row", func(t *testing.T) {
		doomed := &workspace.Transformation{Name: "Doomed"}
		if _, err := deadSvc.CreateTransformation(ctx, "user-1", doomed); err == nil {
			t.Fatal("create must fail when the box is unreachable")
		}
		rows, _ := store.ListTransformationsByCustomer(ctx, "acme")
		for _, r := range rows {
			if r.Name == "Doomed" {
				t.Fatal("failed create left its cache row behind")
			}
		}

		upd := *nightly
		upd.Schedule = "0 5 * * *"
		if _, err := deadSvc.UpdateTransformation(ctx, "user-1", &upd); err == nil {
			t.Fatal("update must fail when the box is unreachable")
		}
		got, err := store.GetTransformation(ctx, nightly.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Schedule != "0 4 * * *" {
			t.Fatalf("failed update was not compensated: schedule %q", got.Schedule)
		}
	})

	t.Run("foreign valid edit is adopted into the cache", func(t *testing.T) {
		content, sha := repoFile(t, yamlPath)
		edited := strings.Replace(content, "0 4 * * *", "0 6 * * *", 1)
		edited = strings.Replace(edited, "enabled: true", "trigger_after_pipeline: pipe-acme\nenabled: true", 1)
		if edited == content {
			t.Fatalf("schedule not found in rendered file:\n%s", content)
		}
		if _, err := raw.PutContents(ctx, "transformations", yamlPath, edited, sha, "hand edit", nil); err != nil {
			t.Fatalf("hand edit: %v", err)
		}

		if err := mirror.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatalf("adopt: %v", err)
		}
		got, err := store.GetTransformation(ctx, nightly.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Schedule != "0 6 * * *" || got.TriggerAfterPipelineID != "pipe-acme" {
			t.Fatalf("foreign edit not adopted: %+v", got)
		}
		if notesWith("Transformation updated from your repo") != 1 {
			t.Fatalf("want exactly one adopted notification, notes: %+v", notes.notes)
		}
	})

	t.Run("unparseable edit is refused once and a Console save overwrites", func(t *testing.T) {
		_, sha := repoFile(t, yamlPath)
		if _, err := raw.PutContents(ctx, "transformations", yamlPath, "{{ this is not yaml", sha, "break it", nil); err != nil {
			t.Fatalf("hand edit: %v", err)
		}

		for range 2 {
			if err := mirror.AdoptCustomer(ctx, "acme"); err != nil {
				t.Fatalf("adopt: %v", err)
			}
		}
		got, _ := store.GetTransformation(ctx, nightly.ID)
		if got.Schedule != "0 6 * * *" {
			t.Fatalf("refused edit must not change the cache: schedule %q", got.Schedule)
		}
		if notesWith("Transformation file edit could not be applied") != 1 {
			t.Fatalf("want exactly one refusal notification, notes: %+v", notes.notes)
		}

		// An explicit Console save is newer intent: it overwrites the file.
		upd := *got
		upd.Schedule = "0 7 * * *"
		if _, err := svc.UpdateTransformation(ctx, "user-1", &upd); err != nil {
			t.Fatalf("update: %v", err)
		}
		content, _ := repoFile(t, yamlPath)
		if !strings.Contains(content, "0 7 * * *") || strings.Contains(content, "not yaml") {
			t.Fatalf("Console save must overwrite the broken file:\n%s", content)
		}
	})

	t.Run("delete removes the rendered file only", func(t *testing.T) {
		if err := svc.DeleteTransformation(ctx, "user-1", nightly.ID); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if _, _, err := raw.GetContents(ctx, "transformations", yamlPath); err == nil {
			t.Fatalf("%s must be deleted", yamlPath)
		}
		if content, _ := repoFile(t, "models/marts/orders.sql"); content != "select 1" {
			t.Fatal("delete must not touch the dbt project")
		}
	})
}

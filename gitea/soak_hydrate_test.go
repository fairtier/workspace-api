package gitea_test

// The hydration soak (control-plane/workspace-split Phase 3B): a box whose
// workspace database starts EMPTY must load the definitions its Gitea repos
// already carry, or it serves nothing. Driven against a REAL Gitea through the
// real gitea.Client, same runner as the other two soaks
// (scripts/gitea-soak.sh); skips without GITEA_SOAK_URL/GITEA_SOAK_TOKEN.
//
// The shape mirrors the live situation on a canary box: central rendered the
// repos while the box had no database, then the box comes up against them.
//
// Scenarios:
//  1. an empty database imports every rendered definition, ids intact, without
//     writing a single commit
//  2. a second sweep imports nothing (the render bookkeeping now tracks them)
//  3. a hand edit of an imported file is adopted by the box — hydration hands
//     the file over to the normal adopt path
//  4. an unusable file is skipped without failing the sweep or being touched

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"filippo.io/age"

	"github.com/fairtier/workspace-api/crypto"
	"github.com/fairtier/workspace-api/gitea"
	"github.com/fairtier/workspace-api/workspace"
)

// ImportPipeline is the hydration write path: the id comes from the file, and
// the name is unique per customer exactly as in Postgres.
func (s *soakStore) ImportPipeline(_ context.Context, p *workspace.Pipeline) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.rows[p.ID]; exists {
		return workspace.ErrPipelineAlreadyExists
	}
	for _, row := range s.rows {
		if row.CustomerSlug == p.CustomerSlug && row.Name == p.Name {
			return workspace.ErrPipelineAlreadyExists
		}
	}
	cp := *p
	s.rows[p.ID] = &cp
	return nil
}

func (s *soakTransformationStore) ImportTransformation(_ context.Context, t *workspace.Transformation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.rows[t.ID]; exists {
		return workspace.ErrTransformationAlreadyExists
	}
	for _, row := range s.rows {
		if row.CustomerSlug == t.CustomerSlug && row.Name == t.Name {
			return workspace.ErrTransformationAlreadyExists
		}
	}
	cp := *t
	s.rows[t.ID] = &cp
	return nil
}

// The ids central minted for the two definitions. They are real UUIDs because
// the box's Postgres primary keys are, and hydration preserves them.
const (
	soakHydratePipelineID       = "6b1f0d84-2c3e-4a97-8f5b-0e7d1a4c9b26"
	soakHydrateTransformationID = "0c9a7e13-58d4-4b62-a1f7-3e6b90d2c845"
)

func TestGiteaSoak_HydrateFromRepo(t *testing.T) {
	baseURL := os.Getenv("GITEA_SOAK_URL")
	token := os.Getenv("GITEA_SOAK_TOKEN")
	if baseURL == "" || token == "" {
		t.Skip("GITEA_SOAK_URL / GITEA_SOAK_TOKEN not set — run scripts/gitea-soak.sh")
	}
	ctx := context.Background()
	soakGitea(t, baseURL, token, "pipelines")
	soakGitea(t, baseURL, token, "transformations")

	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	ws := &workspace.Workspace{Slug: "acme"}
	ws.OnVM = true
	ws.CustomerDomain = "*.customer-acme.fairtier.com"
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	creds := &soakCredStore{cred: &workspace.BoxGitCredential{Username: gitea.Owner, Token: token}}
	newClient := func(_, username, tok string) workspace.RepoFileClient {
		return &gitea.Client{BaseURL: baseURL, Username: username, Token: tok}
	}
	// raw is the out-of-band editor: the customer committing in their Gitea.
	raw := &gitea.Client{BaseURL: baseURL, Username: gitea.Owner, Token: token}

	newPipelineMirror := func(store *soakStore, renders *soakRenderStore, defRenders *soakDefRenderStore, notes *soakNotifier) *workspace.PipelineMirror {
		return &workspace.PipelineMirror{
			Workspaces:          &soakResolver{ws: ws},
			Credentials:         creds,
			Pipelines:           store,
			AgeKeys:             &soakAgeKeyStore{key: &workspace.BoxAgeKey{PublicKey: identity.Recipient().String()}},
			Renders:             renders,
			Fingerprint:         crypto.SHA256Fingerprinter{},
			PipelineCredentials: store,
			DefinitionRenders:   defRenders,
			Notifications:       notes,
			Ownership:           store,
			Logger:              logger,
			NewClient:           newClient,
		}
	}
	newTransformationMirror := func(store *soakTransformationStore, pipelines workspace.PipelineReader, defRenders *soakTransformationRenderStore, notes *soakNotifier) *workspace.TransformationMirror {
		return &workspace.TransformationMirror{
			Workspaces:        &soakResolver{ws: ws},
			Credentials:       creds,
			Transformations:   store,
			Pipelines:         pipelines,
			DefinitionRenders: defRenders,
			Notifications:     notes,
			Logger:            logger,
			NewClient:         newClient,
		}
	}

	// ── Central renders both repos while the box has no database. ──────────
	centralPipelines := newSoakStore(&soakRenderStore{rows: map[workspace.PipelineID]workspace.PipelineCredentialRender{}})
	centralPipelineMirror := newPipelineMirror(
		centralPipelines,
		&soakRenderStore{rows: map[workspace.PipelineID]workspace.PipelineCredentialRender{}},
		&soakDefRenderStore{rows: map[workspace.PipelineID]workspace.PipelineDefinitionRender{}},
		&soakNotifier{},
	)
	err = centralPipelines.CreatePipeline(ctx, &workspace.Pipeline{
		ID:                soakHydratePipelineID,
		CustomerSlug:      "acme",
		Name:              "Orders Sync",
		SourceType:        "rest_api",
		SourceConfig:      json.RawMessage(`{"base_url":"https://api.example.com","resources":[{"name":"posts","endpoint":"/posts"}]}`),
		SourceCredentials: json.RawMessage(`{"api_key":"s3cr3t"}`),
		DatasetName:       "raw",
		Schedule:          "0 * * * *",
		Enabled:           true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := centralPipelineMirror.SyncCustomer(ctx, "acme", nil); err != nil {
		t.Fatalf("central pipeline render: %v", err)
	}

	centralTransformations := newSoakTransformationStore()
	centralTransformationMirror := newTransformationMirror(
		centralTransformations,
		centralPipelines,
		&soakTransformationRenderStore{rows: map[workspace.TransformationID]workspace.TransformationDefinitionRender{}},
		&soakNotifier{},
	)
	err = centralTransformations.CreateTransformation(ctx, &workspace.Transformation{
		ID:           soakHydrateTransformationID,
		CustomerSlug: "acme",
		Name:         "Nightly Marts",
		RepoRef:      "main",
		Schedule:     "0 2 * * *",
		DBTSelector:  "marts",
		Enabled:      true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := centralTransformationMirror.SyncCustomer(ctx, "acme", nil); err != nil {
		t.Fatalf("central transformation render: %v", err)
	}

	const (
		yamlPath           = "pipelines/orders-sync.yaml"
		agePath            = "pipelines/orders-sync.credentials.age"
		transformationPath = "transformations/nightly-marts.yaml"
	)
	shaOf := func(t *testing.T, repo, path string) string {
		t.Helper()
		_, sha, err := raw.GetContents(ctx, repo, path)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}
		return sha
	}
	before := map[string]string{
		yamlPath:           shaOf(t, "pipelines", yamlPath),
		agePath:            shaOf(t, "pipelines", agePath),
		transformationPath: shaOf(t, "transformations", transformationPath),
	}

	// ── The box comes up against those repos with an empty database. ───────
	boxPipelines := newSoakStore(&soakRenderStore{rows: map[workspace.PipelineID]workspace.PipelineCredentialRender{}})
	boxDefRenders := &soakDefRenderStore{rows: map[workspace.PipelineID]workspace.PipelineDefinitionRender{}}
	boxNotes := &soakNotifier{}
	boxPipelineMirror := newPipelineMirror(boxPipelines, boxPipelines.renders, boxDefRenders, boxNotes)
	boxPipelineMirror.Importer = boxPipelines

	boxTransformations := newSoakTransformationStore()
	boxTransformationRenders := &soakTransformationRenderStore{rows: map[workspace.TransformationID]workspace.TransformationDefinitionRender{}}
	boxTransformationMirror := newTransformationMirror(boxTransformations, boxPipelines, boxTransformationRenders, boxNotes)
	boxTransformationMirror.Importer = boxTransformations

	sweep := func(t *testing.T) {
		t.Helper()
		if err := boxPipelineMirror.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatalf("pipeline sweep: %v", err)
		}
		if err := boxTransformationMirror.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatalf("transformation sweep: %v", err)
		}
	}

	t.Run("an empty box database imports what the repos already hold", func(t *testing.T) {
		sweep(t)

		got, err := boxPipelines.GetPipeline(ctx, soakHydratePipelineID)
		if err != nil {
			t.Fatalf("pipeline not hydrated: %v", err)
		}
		if got.Name != "Orders Sync" || got.SourceType != "rest_api" || got.Schedule != "0 * * * *" || !got.Enabled {
			t.Errorf("imported pipeline does not match the rendered file: %+v", got)
		}
		if !strings.Contains(string(got.SourceConfig), "api.example.com") {
			t.Errorf("source config lost in import: %s", got.SourceConfig)
		}
		if !got.CredentialsExternal {
			t.Error("the .age file exists and this process cannot re-render it — the pipeline must import as externally-managed")
		}
		if row := boxDefRenders.rows[soakHydratePipelineID]; row.Path != yamlPath || row.BlobSHA != before[yamlPath] {
			t.Errorf("render bookkeeping not stamped from the real tree: %+v", row)
		}

		tr, err := boxTransformations.GetTransformation(ctx, soakHydrateTransformationID)
		if err != nil {
			t.Fatalf("transformation not hydrated: %v", err)
		}
		if tr.Name != "Nightly Marts" || tr.DBTSelector != "marts" || tr.Schedule != "0 2 * * *" || !tr.Enabled {
			t.Errorf("imported transformation does not match the rendered file: %+v", tr)
		}

		for path, sha := range before {
			repo := "pipelines"
			if strings.HasPrefix(path, "transformations/") {
				repo = "transformations"
			}
			if now := shaOf(t, repo, path); now != sha {
				t.Errorf("%s changed: hydration must stay read-only toward the repo", path)
			}
		}
	})

	t.Run("a second sweep imports nothing", func(t *testing.T) {
		sweep(t)

		if n := len(boxPipelines.rows); n != 1 {
			t.Errorf("pipeline rows = %d, want 1 — hydration must be idempotent", n)
		}
		if n := len(boxTransformations.rows); n != 1 {
			t.Errorf("transformation rows = %d, want 1 — hydration must be idempotent", n)
		}
	})

	t.Run("a hand edit of an imported file is adopted", func(t *testing.T) {
		content, sha, err := raw.GetContents(ctx, "pipelines", yamlPath)
		if err != nil {
			t.Fatal(err)
		}
		edited := strings.Replace(content, `schedule: 0 * * * *`, `schedule: 30 4 * * *`, 1)
		if edited == content {
			t.Fatalf("test edit did not apply to:\n%s", content)
		}
		if _, err := raw.PutContents(ctx, "pipelines", yamlPath, edited, sha, "hand edit", nil); err != nil {
			t.Fatalf("hand edit: %v", err)
		}

		sweep(t)

		got, err := boxPipelines.GetPipeline(ctx, soakHydratePipelineID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Schedule != "30 4 * * *" {
			t.Errorf("schedule = %q, want the hand-edited value — hydration must hand the file to the adopt path", got.Schedule)
		}
	})

	t.Run("an unusable file is skipped without failing the sweep", func(t *testing.T) {
		if _, err := raw.PutContents(ctx, "pipelines", "pipelines/broken.yaml", "{{{ not yaml", "", "hand add", nil); err != nil {
			t.Fatalf("add broken file: %v", err)
		}

		sweep(t)

		if n := len(boxPipelines.rows); n != 1 {
			t.Errorf("pipeline rows = %d, want 1 — an unusable file must not create one", n)
		}
		if _, _, err := raw.GetContents(ctx, "pipelines", "pipelines/broken.yaml"); err != nil {
			t.Error("a file that cannot be imported must be left in the repo")
		}
	})
}

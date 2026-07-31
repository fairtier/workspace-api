package workspace_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/fairtier/workspace-api/workspace"
)

// fakeImporter stands in for the hydration write path.
type fakeImporter struct {
	imported []workspace.Pipeline
}

func (f *fakeImporter) ImportPipeline(_ context.Context, p *workspace.Pipeline) error {
	f.imported = append(f.imported, *p)
	return nil
}

// importPipeline is the pipeline central rendered into the box repo before the
// box ever had a database. The id must be a real UUID — it is the primary key
// the import preserves.
const importPipelineID = "3f2b8c1e-0a4d-4b1e-9d3a-2c5e7f9a1b3d"

func importPipeline() workspace.Pipeline {
	return workspace.Pipeline{
		ID:           importPipelineID,
		Name:         "Orders",
		SourceType:   "rest_api",
		SourceConfig: json.RawMessage(`{"base_url":"https://example.com","resources":[{"name":"users","endpoint":"/users"}]}`),
		DatasetName:  "raw",
		Schedule:     "0 6 * * *",
		Enabled:      true,
	}
}

// renderedRepo returns a box repo holding the rendered definition and its age
// credential file — the state a box finds on its first boot, written by
// central while the box database did not exist yet.
func renderedRepo(t *testing.T, pipelines []workspace.Pipeline) *fakeMirrorRepo {
	t.Helper()
	repo := newFakeMirrorRepo(nil)
	creds := fakeCredReader{}
	for _, p := range pipelines {
		creds[p.ID] = json.RawMessage(`{"api_key":"s3cr3t"}`)
	}
	central := withAge(mirrorFor(boxCustomer(), repo, pipelines), testRecipient(t), creds, &fakeRenderStore{})
	central.DefinitionRenders = newFakeDefRenderStore()
	if err := central.SyncCustomer(context.Background(), "acme", nil); err != nil {
		t.Fatal(err)
	}
	return repo
}

// boxMirror is the box's mirror over an empty database: no pipelines, no
// render bookkeeping, no age material (the box holds only the public key, so
// it never renders credential files itself).
func boxMirror(repo *fakeMirrorRepo, pipelines []workspace.Pipeline, imp *fakeImporter) (*workspace.PipelineMirror, *fakeDefRenderStore) {
	store := newFakeDefRenderStore()
	m := mirrorFor(boxCustomer(), repo, pipelines)
	m.DefinitionRenders = store
	m.Logger = slog.New(slog.DiscardHandler)
	if imp != nil {
		m.Importer = imp
	}
	return m, store
}

func TestPipelineMirror_ImportUnrendered(t *testing.T) {
	ctx := context.Background()

	t.Run("hydrates an empty database from the repo", func(t *testing.T) {
		pipe := importPipeline()
		repo := renderedRepo(t, []workspace.Pipeline{pipe})
		writes := repo.puts + repo.deletes
		imp := &fakeImporter{}
		m, store := boxMirror(repo, nil, imp)

		if err := m.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatal(err)
		}

		if len(imp.imported) != 1 {
			t.Fatalf("imported = %d, want 1", len(imp.imported))
		}
		got := imp.imported[0]
		if got.ID != pipe.ID {
			t.Errorf("id = %q, want the id the file carries (%q)", got.ID, pipe.ID)
		}
		if got.CustomerSlug != "acme" {
			t.Errorf("customer slug = %q, want acme", got.CustomerSlug)
		}
		if got.Name != pipe.Name || got.SourceType != pipe.SourceType || got.Schedule != pipe.Schedule || !got.Enabled {
			t.Errorf("imported row does not match the file: %+v", got)
		}
		if !strings.Contains(string(got.SourceConfig), "example.com") {
			t.Errorf("source config lost in import: %s", got.SourceConfig)
		}
		if !got.CredentialsExternal {
			t.Error("a pipeline whose .age file exists must import as externally-managed — this process cannot re-render it, and the converge would delete it as stale")
		}
		if row := store.rows[pipe.ID]; row.Path != "pipelines/orders.yaml" || row.BlobSHA != repo.shas["pipelines/orders.yaml"] {
			t.Errorf("render bookkeeping not stamped for the imported file: %+v", row)
		}
		if repo.puts+repo.deletes != writes {
			t.Error("the import pass must stay read-only toward the repo")
		}
	})

	t.Run("a tracked file is not imported again", func(t *testing.T) {
		pipe := importPipeline()
		repo := renderedRepo(t, []workspace.Pipeline{pipe})
		imp := &fakeImporter{}
		m, _ := boxMirror(repo, nil, imp)

		if err := m.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatal(err)
		}
		// Second sweep: the row now exists and its render is tracked.
		m.Pipelines = &mockPipelineRepo{
			listPipelinesByCustomerFn: func(context.Context, string) ([]workspace.Pipeline, error) {
				return []workspace.Pipeline{imp.imported[0]}, nil
			},
		}
		if err := m.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatal(err)
		}

		if len(imp.imported) != 1 {
			t.Fatalf("imports = %d, want 1 — hydration must be idempotent", len(imp.imported))
		}
	})

	t.Run("no importer means no hydration", func(t *testing.T) {
		repo := renderedRepo(t, []workspace.Pipeline{importPipeline()})
		m, store := boxMirror(repo, nil, nil)

		if err := m.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatal(err)
		}

		if len(store.rows) != 0 {
			t.Fatalf("central semantics broken: an unwired importer must create nothing, got %+v", store.rows)
		}
	})

	t.Run("unusable files are skipped, the rest still import", func(t *testing.T) {
		pipe := importPipeline()
		repo := renderedRepo(t, []workspace.Pipeline{pipe})
		repo.files["pipelines/broken.yaml"] = "{{{ not yaml"
		repo.shas["pipelines/broken.yaml"] = "shabroken"
		repo.files["pipelines/no-id.yaml"] = "name: Ghost\nsource_type: rest_api\nenabled: true\n"
		repo.shas["pipelines/no-id.yaml"] = "shanoid"
		imp := &fakeImporter{}
		m, _ := boxMirror(repo, nil, imp)

		if err := m.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatalf("an unusable file must not fail the sweep: %v", err)
		}

		if len(imp.imported) != 1 || imp.imported[0].ID != pipe.ID {
			t.Fatalf("imported = %+v, want only the valid file", imp.imported)
		}
		if _, ok := repo.files["pipelines/broken.yaml"]; !ok {
			t.Error("a file that cannot be imported must be left in the repo")
		}
	})

	t.Run("a rendered file_upload pipeline imports back as file_upload", func(t *testing.T) {
		// Central renders file_upload in the rewritten "filesystem" form the
		// worker can load; importing it verbatim would change the pipeline's
		// type and lose the upload surface.
		const upload = "8d5c2f70-9b41-4e6a-83d2-5f1a7c04e9b8"
		repo := newFakeMirrorRepo(nil)
		central := mirrorFor(boxCustomerWithStorage(), repo, []workspace.Pipeline{
			fileUploadPipe(upload, "Sales Drop", `{"files":[{"name":"orders","file":"orders.csv","size_bytes":10}]}`),
		})
		central.DefinitionRenders = newFakeDefRenderStore()
		if err := central.SyncCustomer(ctx, "acme", nil); err != nil {
			t.Fatal(err)
		}
		if content := repo.files["pipelines/sales-drop.yaml"]; !strings.Contains(content, "source_type: filesystem") {
			t.Fatalf("precondition: the render must rewrite to filesystem:\n%s", content)
		}
		imp := &fakeImporter{}
		m, _ := boxMirror(repo, nil, imp)

		if err := m.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatal(err)
		}

		if len(imp.imported) != 1 {
			t.Fatalf("imported = %d, want 1", len(imp.imported))
		}
		got := imp.imported[0]
		if got.SourceType != workspace.SourceTypeFileUpload {
			t.Errorf("source type = %q, want file_upload restored from the rewrite", got.SourceType)
		}
		if !strings.Contains(string(got.SourceConfig), `"file":"orders.csv"`) ||
			!strings.Contains(string(got.SourceConfig), `"name":"orders"`) {
			t.Errorf("uploaded files lost in the round trip: %s", got.SourceConfig)
		}
	})

	t.Run("a real filesystem pipeline is not mistaken for a file drop", func(t *testing.T) {
		pipe := importPipeline()
		pipe.SourceType = "filesystem"
		// Same shape as a file drop, but pointing at another pipeline's
		// prefix — the marker is the pipeline's OWN id, so this stays plain.
		pipe.SourceConfig = json.RawMessage(`{"bucket_url":"s3://ft-acme/uploads/11111111-2222-4333-8444-555555555555/","file_glob":"*.csv"}`)
		repo := renderedRepo(t, []workspace.Pipeline{pipe})
		imp := &fakeImporter{}
		m, _ := boxMirror(repo, nil, imp)

		if err := m.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatal(err)
		}

		if len(imp.imported) != 1 || imp.imported[0].SourceType != "filesystem" {
			t.Fatalf("imported = %+v, want one plain filesystem pipeline", imp.imported)
		}
	})

	t.Run("a pipeline whose row exists but whose render is untracked is left alone", func(t *testing.T) {
		pipe := importPipeline()
		repo := renderedRepo(t, []workspace.Pipeline{pipe})
		imp := &fakeImporter{}
		// The row is known, the bookkeeping is not: importing would duplicate
		// the id, and adopting the content is the adopt pass's decision.
		m, _ := boxMirror(repo, []workspace.Pipeline{pipe}, imp)

		if err := m.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatal(err)
		}

		if len(imp.imported) != 0 {
			t.Fatalf("imported %+v, want nothing for an id that already has a row", imp.imported)
		}
	})
}

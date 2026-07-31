package workspace_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/fairtier/workspace-api/workspace"
)

// fakeTransformationImporter stands in for the hydration write path.
type fakeTransformationImporter struct {
	imported []workspace.Transformation
}

func (f *fakeTransformationImporter) ImportTransformation(_ context.Context, t *workspace.Transformation) error {
	f.imported = append(f.imported, *t)
	return nil
}

// importTransformationID must be a real UUID — it is the primary key the
// import preserves from the file.
const importTransformationID = "7a1c4d92-6f38-4e51-b0c7-1d84e26f9a05"

func importTransformation() workspace.Transformation {
	return workspace.Transformation{
		ID:           importTransformationID,
		CustomerSlug: "acme",
		Name:         "Nightly Marts",
		RepoURL:      "https://git.customer-acme.fairtier.com/fairtier-admin/transformations.git",
		RepoRef:      "main",
		Schedule:     "0 2 * * *",
		DBTSelector:  "marts",
		Enabled:      true,
	}
}

// renderedTransformationRepo returns a box repo holding the rendered execution
// config — what central wrote there before the box had a database.
func renderedTransformationRepo(t *testing.T, transformations []workspace.Transformation) *fakeMirrorRepo {
	t.Helper()
	repo := newFakeMirrorRepo(nil)
	set := transformations
	central := transformationMirrorFor(boxCustomer(), repo, newFakeTransformationRenderStore(), &fakeNotifier{}, &set)
	if err := central.SyncCustomer(context.Background(), "acme", nil); err != nil {
		t.Fatal(err)
	}
	return repo
}

func boxTransformationMirror(repo *fakeMirrorRepo, transformations []workspace.Transformation, imp *fakeTransformationImporter) (*workspace.TransformationMirror, *fakeTransformationRenderStore) {
	store := newFakeTransformationRenderStore()
	set := transformations
	m := transformationMirrorFor(boxCustomer(), repo, store, &fakeNotifier{}, &set)
	m.Logger = slog.New(slog.DiscardHandler)
	if imp != nil {
		m.Importer = imp
	}
	return m, store
}

func TestTransformationMirror_ImportUnrendered(t *testing.T) {
	ctx := context.Background()

	t.Run("hydrates an empty database from the repo", func(t *testing.T) {
		tr := importTransformation()
		repo := renderedTransformationRepo(t, []workspace.Transformation{tr})
		writes := repo.puts + repo.deletes
		imp := &fakeTransformationImporter{}
		m, store := boxTransformationMirror(repo, nil, imp)

		if err := m.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatal(err)
		}

		if len(imp.imported) != 1 {
			t.Fatalf("imported = %d, want 1", len(imp.imported))
		}
		got := imp.imported[0]
		if got.ID != tr.ID {
			t.Errorf("id = %q, want the id the file carries (%q)", got.ID, tr.ID)
		}
		if got.CustomerSlug != "acme" || got.Name != tr.Name || got.RepoURL != tr.RepoURL ||
			got.RepoRef != tr.RepoRef || got.Schedule != tr.Schedule || got.DBTSelector != tr.DBTSelector || !got.Enabled {
			t.Errorf("imported row does not match the file: %+v", got)
		}
		if row := store.rows[tr.ID]; row.Path != "transformations/nightly-marts.yaml" || row.BlobSHA != repo.shas["transformations/nightly-marts.yaml"] {
			t.Errorf("render bookkeeping not stamped for the imported file: %+v", row)
		}
		if repo.puts+repo.deletes != writes {
			t.Error("the import pass must stay read-only toward the repo")
		}
	})

	t.Run("a tracked file is not imported again", func(t *testing.T) {
		tr := importTransformation()
		repo := renderedTransformationRepo(t, []workspace.Transformation{tr})
		imp := &fakeTransformationImporter{}
		m, _ := boxTransformationMirror(repo, nil, imp)

		if err := m.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatal(err)
		}
		m.Transformations = &mockTransformationRepo{
			listTransformationsByCustomerFn: func(context.Context, string) ([]workspace.Transformation, error) {
				return []workspace.Transformation{imp.imported[0]}, nil
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
		repo := renderedTransformationRepo(t, []workspace.Transformation{importTransformation()})
		m, store := boxTransformationMirror(repo, nil, nil)

		if err := m.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatal(err)
		}

		if len(store.rows) != 0 {
			t.Fatalf("central semantics broken: an unwired importer must create nothing, got %+v", store.rows)
		}
	})

	t.Run("a file chained to an unknown pipeline is skipped", func(t *testing.T) {
		tr := importTransformation()
		tr.TriggerAfterPipelineID = "pipe-gone"
		repo := renderedTransformationRepo(t, []workspace.Transformation{tr})
		imp := &fakeTransformationImporter{}
		m, _ := boxTransformationMirror(repo, nil, imp)

		if err := m.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatalf("an unusable file must not fail the sweep: %v", err)
		}

		if len(imp.imported) != 0 {
			t.Fatalf("imported %+v, want nothing — the FK would reject it", imp.imported)
		}
	})

	t.Run("a file chained to another tenant's pipeline is skipped", func(t *testing.T) {
		tr := importTransformation()
		tr.TriggerAfterPipelineID = "pipe-other"
		repo := renderedTransformationRepo(t, []workspace.Transformation{tr})
		imp := &fakeTransformationImporter{}
		m, _ := boxTransformationMirror(repo, nil, imp)

		if err := m.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatal(err)
		}

		if len(imp.imported) != 0 {
			t.Fatalf("imported %+v, want nothing for a cross-tenant chain", imp.imported)
		}
	})

	t.Run("dbt project files outside transformations/ are never touched", func(t *testing.T) {
		repo := renderedTransformationRepo(t, []workspace.Transformation{importTransformation()})
		repo.files["models/stg_orders.sql"] = "select 1"
		repo.shas["models/stg_orders.sql"] = "shamodel"
		imp := &fakeTransformationImporter{}
		m, _ := boxTransformationMirror(repo, nil, imp)

		if err := m.AdoptCustomer(ctx, "acme"); err != nil {
			t.Fatal(err)
		}

		if len(imp.imported) != 1 {
			t.Fatalf("imported = %d, want only the execution config", len(imp.imported))
		}
	})
}

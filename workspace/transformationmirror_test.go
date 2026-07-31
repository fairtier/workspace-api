package workspace_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/fairtier/workspace-api/workspace"
)

// fakeTransformationRenderStore is the in-memory
// TransformationDefinitionRenderStore.
type fakeTransformationRenderStore struct {
	rows map[workspace.TransformationID]workspace.TransformationDefinitionRender
}

func newFakeTransformationRenderStore() *fakeTransformationRenderStore {
	return &fakeTransformationRenderStore{rows: map[workspace.TransformationID]workspace.TransformationDefinitionRender{}}
}

func (f *fakeTransformationRenderStore) UpsertTransformationDefinitionRender(_ context.Context, r *workspace.TransformationDefinitionRender) error {
	f.rows[r.TransformationID] = *r
	return nil
}

func (f *fakeTransformationRenderStore) GetTransformationDefinitionRenders(context.Context, string) (map[workspace.TransformationID]workspace.TransformationDefinitionRender, error) {
	out := make(map[workspace.TransformationID]workspace.TransformationDefinitionRender, len(f.rows))
	for k, v := range f.rows {
		out[k] = v
	}
	return out, nil
}

func (f *fakeTransformationRenderStore) MarkTransformationDefinitionRefused(_ context.Context, id workspace.TransformationID, refusedSHA string) error {
	row := f.rows[id]
	row.TransformationID = id
	row.RefusedBlobSHA = refusedSHA
	f.rows[id] = row
	return nil
}

func transformationMirrorFor(ws *workspace.Workspace, repo *fakeMirrorRepo, store *fakeTransformationRenderStore, notifier *fakeNotifier, transformations *[]workspace.Transformation) *workspace.TransformationMirror {
	return &workspace.TransformationMirror{
		Workspaces: &mockCustomerReader{
			getBySlugFn: func(context.Context, string) (*workspace.Workspace, error) { return ws, nil },
		},
		Credentials: &fakeCredStore{cred: &workspace.BoxGitCredential{Username: "fairtier-admin", Token: "tok"}},
		Transformations: &mockTransformationRepo{
			listTransformationsByCustomerFn: func(context.Context, string) ([]workspace.Transformation, error) {
				return *transformations, nil
			},
			updateTransformationFn: func(_ context.Context, t *workspace.Transformation) error {
				for i := range *transformations {
					if (*transformations)[i].ID == t.ID {
						(*transformations)[i] = *t
					}
				}
				return nil
			},
		},
		Pipelines: &mockPipelineReader{
			getPipelineFn: func(_ context.Context, id workspace.PipelineID) (*workspace.Pipeline, error) {
				if id == "pipe-acme" {
					return &workspace.Pipeline{ID: id, CustomerSlug: "acme"}, nil
				}
				if id == "pipe-other" {
					return &workspace.Pipeline{ID: id, CustomerSlug: "other"}, nil
				}
				return nil, workspace.ErrPipelineNotFound
			},
		},
		DefinitionRenders: store,
		Notifications:     notifier,
		NewClient: func(string, string, string) workspace.RepoFileClient {
			return repo
		},
	}
}

func nightlyMarts() workspace.Transformation {
	return workspace.Transformation{
		ID:           "22222222-bbbb",
		CustomerSlug: "acme",
		Name:         "Nightly Marts",
		RepoRef:      "main",
		Schedule:     "0 2 * * *",
		DBTSelector:  "marts",
		Enabled:      true,
	}
}

func TestTransformationMirror_SyncCustomer(t *testing.T) {
	const path = "transformations/nightly-marts.yaml"

	t.Run("renders configs and leaves the dbt project untouched", func(t *testing.T) {
		repo := newFakeMirrorRepo(map[string]string{
			"dbt_project.yml":          "name: acme",
			"models/marts/orders.sql":  "select 1",
			"transformations/old.yaml": "# Rendered by the FairTier Console — a Console save overwrites this file.\nid: gone\n",
		})
		store := newFakeTransformationRenderStore()
		transformations := []workspace.Transformation{nightlyMarts()}
		m := transformationMirrorFor(boxCustomer(), repo, store, &fakeNotifier{}, &transformations)

		author := &workspace.CommitAuthor{Name: "Alice", Email: "alice@example.com"}
		if err := m.SyncCustomer(context.Background(), "acme", author); err != nil {
			t.Fatal(err)
		}
		content, ok := repo.files[path]
		if !ok {
			t.Fatalf("missing %s; files: %v", path, repo.files)
		}
		for _, want := range []string{"id: 22222222-bbbb", "name: Nightly Marts", "schedule: 0 2 * * *", "dbt_selector: marts", "enabled: true"} {
			if !strings.Contains(content, want) {
				t.Errorf("rendered file missing %q:\n%s", want, content)
			}
		}
		if _, ok := repo.files["transformations/old.yaml"]; ok {
			t.Error("stale managed file must be deleted")
		}
		if _, ok := repo.files["dbt_project.yml"]; !ok {
			t.Error("dbt project files must never be touched")
		}
		if got := repo.authors[path]; got == nil || got.Name != "Alice" {
			t.Errorf("commit author = %v, want Alice", got)
		}
		if row := store.rows[nightlyMarts().ID]; row.Path != path || row.BlobSHA == "" {
			t.Errorf("render row not recorded: %+v", row)
		}
	})

	t.Run("out-of-scope customer is a silent no-op", func(t *testing.T) {
		repo := newFakeMirrorRepo(nil)
		transformations := []workspace.Transformation{nightlyMarts()}
		shared := &workspace.Workspace{Slug: "acme"} // OnVM=false
		m := transformationMirrorFor(shared, repo, newFakeTransformationRenderStore(), &fakeNotifier{}, &transformations)
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if repo.puts != 0 || repo.trees != 0 {
			t.Error("out-of-scope customer must not reach the repo")
		}
	})

	t.Run("save-converge overwrites drift and notifies", func(t *testing.T) {
		repo := newFakeMirrorRepo(nil)
		store := newFakeTransformationRenderStore()
		notifier := &fakeNotifier{}
		transformations := []workspace.Transformation{nightlyMarts()}
		m := transformationMirrorFor(boxCustomer(), repo, store, notifier, &transformations)
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}

		repo.tamper(path, "hand edit")
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(repo.files[path], "hand edit") {
			t.Error("converge must overwrite the out-of-band edit")
		}
		if len(notifier.notes) != 1 || !strings.Contains(notifier.notes[0].Title, "changed outside the Console") {
			t.Errorf("want one drift notification, got %+v", notifier.notes)
		}
	})

	t.Run("unchanged state makes no commits", func(t *testing.T) {
		repo := newFakeMirrorRepo(nil)
		store := newFakeTransformationRenderStore()
		transformations := []workspace.Transformation{nightlyMarts()}
		m := transformationMirrorFor(boxCustomer(), repo, store, &fakeNotifier{}, &transformations)
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		puts := repo.puts
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if repo.puts != puts || repo.deletes != 0 {
			t.Errorf("second sync must be a no-op (puts %d→%d, deletes %d)", puts, repo.puts, repo.deletes)
		}
	})
}

func TestTransformationMirror_AdoptCustomer(t *testing.T) {
	const path = "transformations/nightly-marts.yaml"

	setup := func(t *testing.T) (*fakeMirrorRepo, *fakeTransformationRenderStore, *fakeNotifier, *workspace.TransformationMirror, *[]workspace.Transformation) {
		t.Helper()
		repo := newFakeMirrorRepo(nil)
		store := newFakeTransformationRenderStore()
		notifier := &fakeNotifier{}
		transformations := []workspace.Transformation{nightlyMarts()}
		m := transformationMirrorFor(boxCustomer(), repo, store, notifier, &transformations)
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		notifier.notes = nil
		return repo, store, notifier, m, &transformations
	}

	t.Run("valid foreign edit is adopted into the cache", func(t *testing.T) {
		repo, store, notifier, m, transformations := setup(t)
		repo.tamper(path, "id: 22222222-bbbb\nname: Nightly Marts\nrepo_ref: main\nschedule: 0 4 * * *\ntrigger_after_pipeline: pipe-acme\ndbt_selector: marts+\nenabled: false\n")

		if err := m.AdoptCustomer(context.Background(), "acme"); err != nil {
			t.Fatal(err)
		}
		got := (*transformations)[0]
		if got.Schedule != "0 4 * * *" || got.DBTSelector != "marts+" || got.Enabled || got.TriggerAfterPipelineID != "pipe-acme" {
			t.Errorf("row not adopted: %+v", got)
		}
		if row := store.rows[got.ID]; row.BlobSHA != repo.shas[path] {
			t.Errorf("render row must adopt the foreign sha: %+v vs %s", row, repo.shas[path])
		}
		if len(notifier.notes) != 1 || !strings.Contains(notifier.notes[0].Title, "updated from your repo") {
			t.Errorf("want one adopted notification, got %+v", notifier.notes)
		}
		// The adopted state re-renders content-equal: no overwrite commit.
		puts := repo.puts
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(repo.files[path], "0 2 * * *") {
			t.Error("a later converge must keep the adopted state, not resurrect the old row")
		}
		_ = puts
	})

	t.Run("unparseable edit is refused once", func(t *testing.T) {
		repo, store, notifier, m, transformations := setup(t)
		repo.tamper(path, ": not yaml [")

		for range 2 {
			if err := m.AdoptCustomer(context.Background(), "acme"); err != nil {
				t.Fatal(err)
			}
		}
		if got := (*transformations)[0]; got.Schedule != "0 2 * * *" {
			t.Errorf("refused edit must not touch the row: %+v", got)
		}
		if len(notifier.notes) != 1 || !strings.Contains(notifier.notes[0].Title, "could not be applied") {
			t.Errorf("want exactly one refusal notification, got %+v", notifier.notes)
		}
		if row := store.rows[nightlyMarts().ID]; row.RefusedBlobSHA != repo.shas[path] {
			t.Errorf("refused sha not recorded: %+v", row)
		}
	})

	t.Run("foreign-tenant trigger pipeline is refused", func(t *testing.T) {
		repo, _, notifier, m, transformations := setup(t)
		repo.tamper(path, "id: 22222222-bbbb\nname: Nightly Marts\nrepo_ref: main\ntrigger_after_pipeline: pipe-other\nenabled: true\n")

		if err := m.AdoptCustomer(context.Background(), "acme"); err != nil {
			t.Fatal(err)
		}
		if got := (*transformations)[0]; got.TriggerAfterPipelineID != "" {
			t.Errorf("cross-tenant trigger must not be adopted: %+v", got)
		}
		if len(notifier.notes) != 1 || !strings.Contains(notifier.notes[0].Body, "trigger_after_pipeline") {
			t.Errorf("want a trigger refusal notification, got %+v", notifier.notes)
		}
	})

	t.Run("foreign id is refused", func(t *testing.T) {
		repo, _, notifier, m, transformations := setup(t)
		repo.tamper(path, "id: someone-else\nname: Evil\nrepo_ref: main\nenabled: true\n")

		if err := m.AdoptCustomer(context.Background(), "acme"); err != nil {
			t.Fatal(err)
		}
		if got := (*transformations)[0]; got.Name != "Nightly Marts" {
			t.Errorf("id-mismatch edit must not touch the row: %+v", got)
		}
		if len(notifier.notes) != 1 || !strings.Contains(notifier.notes[0].Body, "id field") {
			t.Errorf("want an id refusal notification, got %+v", notifier.notes)
		}
	})

	t.Run("in-sync repo is a no-op", func(t *testing.T) {
		_, _, notifier, m, transformations := setup(t)
		before := (*transformations)[0]
		if err := m.AdoptCustomer(context.Background(), "acme"); err != nil {
			t.Fatal(err)
		}
		if got := (*transformations)[0]; got.UpdatedAt != before.UpdatedAt {
			t.Error("no foreign commit must mean no row write")
		}
		if len(notifier.notes) != 0 {
			t.Errorf("no notifications expected, got %+v", notifier.notes)
		}
	})
}

func TestTransformationService_GitPrimary(t *testing.T) {
	t.Run("create compensates on mirror failure", func(t *testing.T) {
		var deleted bool
		svc := &workspace.TransformationService{
			Workspaces: acmeCustomerReader(),
			Transformations: &mockTransformationRepo{
				createTransformationFn: func(_ context.Context, tr *workspace.Transformation) error {
					tr.ID = "new-1"
					return nil
				},
				deleteTransformationFn: func(_ context.Context, id workspace.TransformationID) error {
					if id != "new-1" {
						t.Errorf("compensation deleted %q, want new-1", id)
					}
					deleted = true
					return nil
				},
			},
			Mirror:     &hardFailMirror{err: workspace.ErrBoxUnreachable},
			GitPrimary: true,
		}
		_, err := svc.CreateTransformation(context.Background(), "u1", &workspace.Transformation{Name: "T"})
		if !errors.Is(err, workspace.ErrBoxUnreachable) {
			t.Fatalf("err = %v, want ErrBoxUnreachable", err)
		}
		if !deleted {
			t.Fatal("failed commit must compensate the created cache row")
		}
	})

	t.Run("update restores previous row on mirror failure", func(t *testing.T) {
		existing := &workspace.Transformation{ID: "t1", CustomerSlug: "acme", Name: "old"}
		var updates []string
		svc := &workspace.TransformationService{
			Workspaces: acmeCustomerReader(),
			Transformations: &mockTransformationRepo{
				getTransformationFn: func(context.Context, workspace.TransformationID) (*workspace.Transformation, error) {
					return existing, nil
				},
				updateTransformationFn: func(_ context.Context, tr *workspace.Transformation) error {
					updates = append(updates, tr.Name)
					return nil
				},
			},
			Mirror:     &hardFailMirror{err: workspace.ErrBoxUnreachable},
			GitPrimary: true,
		}
		_, err := svc.UpdateTransformation(context.Background(), "u1", &workspace.Transformation{ID: "t1", Name: "new"})
		if !errors.Is(err, workspace.ErrBoxUnreachable) {
			t.Fatalf("err = %v, want ErrBoxUnreachable", err)
		}
		if len(updates) != 2 || updates[1] != "old" {
			t.Fatalf("updates = %v, want [new old] (compensation writes the previous row back)", updates)
		}
	})

	t.Run("delete recreates row on mirror failure", func(t *testing.T) {
		existing := &workspace.Transformation{ID: "t1", CustomerSlug: "acme", Name: "T"}
		var recreated bool
		svc := &workspace.TransformationService{
			Workspaces: acmeCustomerReader(),
			Transformations: &mockTransformationRepo{
				getTransformationFn: func(context.Context, workspace.TransformationID) (*workspace.Transformation, error) {
					return existing, nil
				},
				deleteTransformationFn: func(context.Context, workspace.TransformationID) error { return nil },
				createTransformationFn: func(_ context.Context, tr *workspace.Transformation) error {
					recreated = tr.Name == "T"
					return nil
				},
			},
			Mirror:     &hardFailMirror{err: workspace.ErrBoxUnreachable},
			GitPrimary: true,
		}
		err := svc.DeleteTransformation(context.Background(), "u1", "t1")
		if !errors.Is(err, workspace.ErrBoxUnreachable) {
			t.Fatalf("err = %v, want ErrBoxUnreachable", err)
		}
		if !recreated {
			t.Fatal("failed commit must re-insert the deleted config")
		}
	})

	t.Run("successful commit is synchronous and save succeeds", func(t *testing.T) {
		mirror := &hardFailMirror{} // nil err = success
		svc := &workspace.TransformationService{
			Workspaces: acmeCustomerReader(),
			Transformations: &mockTransformationRepo{
				createTransformationFn: func(context.Context, *workspace.Transformation) error { return nil },
			},
			Mirror:     mirror,
			GitPrimary: true,
		}
		if _, err := svc.CreateTransformation(context.Background(), "u1", &workspace.Transformation{Name: "T"}); err != nil {
			t.Fatalf("CreateTransformation: %v", err)
		}
		if mirror.calls != 1 {
			t.Fatalf("mirror calls = %d, want 1 (synchronous, on the request path)", mirror.calls)
		}
	})
}

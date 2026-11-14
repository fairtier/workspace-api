package workspace_test

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

// --- Fakes ---

// ImportWorkflow gives the in-memory store the hydration write path: the
// caller's id is kept.
func (f *fakeWorkflowStore) ImportWorkflow(_ context.Context, w *workspace.Workflow) error {
	if f.nameTaken(w) {
		return workspace.ErrWorkflowAlreadyExists
	}
	if _, ok := f.workflows[w.ID]; ok {
		return workspace.ErrWorkflowAlreadyExists
	}
	cp := *w
	cp.Steps = append([]workspace.WorkflowStep(nil), w.Steps...)
	f.workflows[w.ID] = &cp
	return nil
}

// fakeWorkflowTargets serves the workspace's pipelines and transformations,
// both by id (the validator) and as lists (file slugs).
type fakeWorkflowTargets struct {
	pipelines       []workspace.Pipeline
	transformations []workspace.Transformation
}

func (f *fakeWorkflowTargets) GetPipeline(_ context.Context, id workspace.PipelineID) (*workspace.Pipeline, error) {
	for i := range f.pipelines {
		if f.pipelines[i].ID == id {
			p := f.pipelines[i]
			return &p, nil
		}
	}
	return nil, workspace.ErrPipelineNotFound
}

func (f *fakeWorkflowTargets) ListPipelinesByCustomer(_ context.Context, slug string) ([]workspace.Pipeline, error) {
	var out []workspace.Pipeline
	for _, p := range f.pipelines {
		if p.CustomerSlug == slug {
			out = append(out, p)
		}
	}
	return out, nil
}

func (f *fakeWorkflowTargets) GetTransformation(_ context.Context, id workspace.TransformationID) (*workspace.Transformation, error) {
	for i := range f.transformations {
		if f.transformations[i].ID == id {
			t := f.transformations[i]
			return &t, nil
		}
	}
	return nil, workspace.ErrTransformationNotFound
}

func (f *fakeWorkflowTargets) ListTransformationsByCustomer(_ context.Context, slug string) ([]workspace.Transformation, error) {
	var out []workspace.Transformation
	for _, t := range f.transformations {
		if t.CustomerSlug == slug {
			out = append(out, t)
		}
	}
	return out, nil
}

// fakeWorkflowRenderStore is the in-memory WorkflowDefinitionRenderStore. It
// drops rows of deleted workflows on read, like the FK cascade + join.
type fakeWorkflowRenderStore struct {
	rows  map[workspace.WorkflowID]workspace.WorkflowDefinitionRender
	store *fakeWorkflowStore
}

func (f *fakeWorkflowRenderStore) UpsertWorkflowDefinitionRender(_ context.Context, r *workspace.WorkflowDefinitionRender) error {
	f.rows[r.WorkflowID] = *r
	return nil
}

func (f *fakeWorkflowRenderStore) GetWorkflowDefinitionRenders(context.Context, string) (map[workspace.WorkflowID]workspace.WorkflowDefinitionRender, error) {
	out := make(map[workspace.WorkflowID]workspace.WorkflowDefinitionRender, len(f.rows))
	for id, r := range f.rows {
		if _, live := f.store.workflows[id]; live {
			out[id] = r
		}
	}
	return out, nil
}

func (f *fakeWorkflowRenderStore) MarkWorkflowDefinitionRefused(_ context.Context, id workspace.WorkflowID, sha string) error {
	row := f.rows[id]
	row.WorkflowID, row.RefusedBlobSHA = id, sha
	f.rows[id] = row
	return nil
}

// workflowMirrorFixture is a box: workflow service + mirror over one
// in-memory transformations repo, with the repo as the source of truth
// (Importer wired).
type workflowMirrorFixture struct {
	svc     *workspace.WorkflowService
	mirror  *workspace.WorkflowMirror
	store   *fakeWorkflowStore
	renders *fakeWorkflowRenderStore
	targets *fakeWorkflowTargets
	repo    *fakeMirrorRepo
	notes   *fakeNotifier
}

const (
	nightlyPath = "workflows/nightly.yaml"
)

func newWorkflowMirrorFixture(t *testing.T) *workflowMirrorFixture {
	t.Helper()
	store := newFakeWorkflowStore()
	targets := &fakeWorkflowTargets{
		pipelines: []workspace.Pipeline{
			{ID: pipeAcme, CustomerSlug: "acme", Name: "Orders"},
			{ID: pipe2, CustomerSlug: "acme", Name: "Customers"},
			{ID: pipeEvil, CustomerSlug: "evil", Name: "Evil"},
		},
		transformations: []workspace.Transformation{{ID: trAcme, CustomerSlug: "acme", Name: "Nightly Marts"}},
	}
	renders := &fakeWorkflowRenderStore{rows: map[workspace.WorkflowID]workspace.WorkflowDefinitionRender{}, store: store}
	repo := newFakeMirrorRepo(map[string]string{"dbt_project.yml": "name: acme\n"})
	notes := &fakeNotifier{}
	workspaces := &mockCustomerReader{
		getBySlugFn: func(context.Context, string) (*workspace.Workspace, error) { return boxCustomer(), nil },
		getByUserIDFn: func(_ context.Context, id core.UserID) (*workspace.Workspace, error) {
			slug, _ := strings.CutPrefix(string(id), "user-")
			return &workspace.Workspace{Slug: slug}, nil
		},
	}
	mirror := &workspace.WorkflowMirror{
		Workspaces:        workspaces,
		Credentials:       &fakeCredStore{cred: &workspace.BoxGitCredential{Username: "fairtier-admin", Token: "tok"}},
		Workflows:         store,
		Pipelines:         targets,
		Transformations:   targets,
		DefinitionRenders: renders,
		Notifications:     notes,
		Importer:          store,
		NewClient:         func(string, string, string) workspace.RepoFileClient { return repo },
	}
	svc := &workspace.WorkflowService{
		Workspaces:      workspaces,
		Workflows:       store,
		Pipelines:       targets,
		Transformations: targets,
		Mirror:          mirror,
	}
	return &workflowMirrorFixture{svc: svc, mirror: mirror, store: store, renders: renders, targets: targets, repo: repo, notes: notes}
}

func (fx *workflowMirrorFixture) create(t *testing.T, name, schedule string, steps ...workspace.WorkflowStep) *workspace.Workflow {
	t.Helper()
	w, err := fx.svc.CreateWorkflow(context.Background(), "user-acme", &workspace.Workflow{Name: name, Schedule: schedule, Enabled: true, Steps: steps})
	if err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}
	return w
}

func (fx *workflowMirrorFixture) adopt(t *testing.T) {
	t.Helper()
	if err := fx.mirror.AdoptCustomer(context.Background(), "acme"); err != nil {
		t.Fatalf("AdoptCustomer: %v", err)
	}
}

func (fx *workflowMirrorFixture) sync(t *testing.T) {
	t.Helper()
	if err := fx.mirror.SyncCustomer(context.Background(), "acme", nil); err != nil {
		t.Fatalf("SyncCustomer: %v", err)
	}
}

// stored returns the only workflow in the store.
func (fx *workflowMirrorFixture) stored(t *testing.T) *workspace.Workflow {
	t.Helper()
	if len(fx.store.workflows) != 1 {
		t.Fatalf("store holds %d workflows, want 1", len(fx.store.workflows))
	}
	for _, w := range fx.store.workflows {
		return w
	}
	return nil
}

func titles(notes []workspace.Notification) []string {
	out := make([]string, 0, len(notes))
	for _, n := range notes {
		out = append(out, n.Title)
	}
	return out
}

func workflowFiles(repo *fakeMirrorRepo) []string {
	var out []string
	for p := range repo.files {
		if strings.HasPrefix(p, "workflows/") {
			out = append(out, p)
		}
	}
	slices.Sort(out)
	return out
}

// --- Tests ---

func TestWorkflowMirror_RoundTrip(t *testing.T) {
	fx := newWorkflowMirrorFixture(t)
	w := fx.create(t, "Nightly", "0 2 * * *", pipeStep(pipeAcme), pipeStep(pipe2), trStep(trAcme))

	content, ok := fx.repo.files[nightlyPath]
	if !ok {
		t.Fatalf("no %s; files: %v", nightlyPath, workflowFiles(fx.repo))
	}
	want := renderedHeader + "id: " + string(w.ID) + `
name: Nightly
schedule: 0 2 * * *
enabled: true
steps:
    - pipeline: orders
    - pipeline: customers
    - transformation: nightly-marts
`
	if content != want {
		t.Errorf("rendered file:\n%s\nwant:\n%s", content, want)
	}
	if row := fx.renders.rows[w.ID]; row.Path != nightlyPath || row.BlobSHA != fx.repo.shas[nightlyPath] {
		t.Errorf("render row = %+v", row)
	}
	if _, ok := fx.repo.files["dbt_project.yml"]; !ok {
		t.Error("the dbt project must never be touched")
	}

	// Loop safety: the mirror's own write is not a foreign commit.
	before := *fx.stored(t)
	fx.adopt(t)
	if got := fx.stored(t); got.UpdatedAt != before.UpdatedAt || len(fx.notes.notes) != 0 {
		t.Errorf("adopting our own write changed something: %+v, notes %v", got, titles(fx.notes.notes))
	}
	puts := fx.repo.puts
	fx.sync(t)
	if fx.repo.puts != puts || fx.repo.deletes != 0 {
		t.Error("an unchanged converge must not commit")
	}

	// Disable via the service: the file follows.
	if _, err := fx.svc.SetWorkflowEnabled(context.Background(), "user-acme", w.ID, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fx.repo.files[nightlyPath], "enabled: false") {
		t.Errorf("disable not mirrored:\n%s", fx.repo.files[nightlyPath])
	}
}

func TestWorkflowMirror_AdoptsHandEdit(t *testing.T) {
	fx := newWorkflowMirrorFixture(t)
	w := fx.create(t, "Nightly", "0 2 * * *", pipeStep(pipeAcme), trStep(trAcme))

	// What a person (or an agent) writes: no header, no id, reordered steps,
	// one added, a new schedule, disabled.
	fx.repo.tamper(nightlyPath, `name: Nightly
description: orders first, then the marts
schedule: 30 3 * * 1-5
enabled: false
steps:
  - pipeline: customers
  - pipeline: orders
  - transformation: nightly-marts
`)
	fx.adopt(t)

	got := fx.stored(t)
	if got.ID != w.ID || got.Schedule != "30 3 * * 1-5" || got.Enabled || got.Description != "orders first, then the marts" {
		t.Errorf("not adopted: %+v", got)
	}
	var refs []string
	for _, s := range got.Steps {
		refs = append(refs, s.RefID())
	}
	if want := []string{pipe2, pipeAcme, trAcme}; !slices.Equal(refs, want) {
		t.Errorf("steps = %v, want %v", refs, want)
	}
	if fx.renders.rows[w.ID].BlobSHA != fx.repo.shas[nightlyPath] {
		t.Error("the adopted commit must become the row's blob sha")
	}
	if got := titles(fx.notes.notes); !slices.Equal(got, []string{"Workflow updated from your repo"}) {
		t.Errorf("notifications = %v", got)
	}

	// A second pass sees nothing new; the next converge normalises the file
	// to the rendered form without calling it drift.
	fx.notes.notes = nil
	fx.adopt(t)
	fx.sync(t)
	if len(fx.notes.notes) != 0 {
		t.Errorf("no notification expected, got %v", titles(fx.notes.notes))
	}
	if c := fx.repo.files[nightlyPath]; !strings.HasPrefix(c, renderedHeader) || !strings.Contains(c, "id: "+string(w.ID)) {
		t.Errorf("converge should re-render the adopted file:\n%s", c)
	}
}

func TestWorkflowMirror_RefusesInvalidEdits(t *testing.T) {
	cases := []struct {
		name, content, reason string
	}{
		{"unresolvable pipeline", "name: Nightly\nsteps:\n  - pipeline: no-such-thing\n", `pipeline "no-such-thing", which does not exist`},
		{"another tenant's pipeline by id", "name: Nightly\nsteps:\n  - pipeline: " + pipeEvil + "\n", "which does not exist"},
		{"misspelt key", "name: Nightly\nsteps:\n  - pipline: orders\n", "field pipline not found"},
		{"duplicate target", "name: Nightly\nsteps:\n  - pipeline: orders\n  - pipeline: orders\n", "step 2: this pipeline is already step 1"},
		{"cron macro", "name: Nightly\nschedule: '@daily'\nsteps:\n  - pipeline: orders\n", `use "0 0 * * *" instead`},
		{"no steps", "name: Nightly\nsteps: []\n", "at least one step"},
		{"both kinds in a step", "name: Nightly\nsteps:\n  - pipeline: orders\n    transformation: nightly-marts\n", "names both"},
		{"foreign id", "id: " + missing + "\nname: Nightly\nsteps:\n  - pipeline: orders\n", "id field does not match"},
		{"not yaml", ": [", "not a valid workflow definition"},
		{"too many steps", "name: Nightly\nsteps:\n" + strings.Repeat("  - pipeline: orders\n", 21), "at most 20 steps"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newWorkflowMirrorFixture(t)
			fx.create(t, "Nightly", "0 2 * * *", pipeStep(pipeAcme))
			fx.repo.tamper(nightlyPath, tc.content)

			for range 2 {
				fx.adopt(t)
			}
			got := fx.stored(t)
			if got.Schedule != "0 2 * * *" || len(got.Steps) != 1 {
				t.Errorf("refused edit must leave the row alone: %+v", got)
			}
			if len(fx.notes.notes) != 1 {
				t.Fatalf("want exactly one notification, got %v", titles(fx.notes.notes))
			}
			n := fx.notes.notes[0]
			if n.Title != "Workflow file edit could not be applied" || !strings.Contains(n.Body, tc.reason) {
				t.Errorf("notification = %q / %q, want reason containing %q", n.Title, n.Body, tc.reason)
			}
			if _, ok := fx.repo.files[nightlyPath]; !ok {
				t.Error("a refused file stays in the repo")
			}
		})
	}
}

func TestWorkflowMirror_RefusesNameTakenByAnother(t *testing.T) {
	fx := newWorkflowMirrorFixture(t)
	fx.create(t, "Nightly", "", pipeStep(pipeAcme))
	fx.create(t, "Hourly", "", pipeStep(pipe2))
	fx.repo.tamper(nightlyPath, "name: Hourly\nsteps:\n  - pipeline: orders\n")
	fx.adopt(t)
	if len(fx.notes.notes) != 1 || !strings.Contains(fx.notes.notes[0].Body, "another workflow already uses that name") {
		t.Errorf("notifications = %+v", fx.notes.notes)
	}
}

func TestWorkflowMirror_AddedFileIsImported(t *testing.T) {
	fx := newWorkflowMirrorFixture(t)
	const path = "workflows/morning.yaml"
	fx.repo.files[path] = "name: Morning\nschedule: 0 6 * * *\nsteps:\n  - pipeline: orders\n  - transformation: nightly-marts\n"
	fx.repo.shas[path] = "agent1"

	fx.adopt(t)
	w := fx.stored(t)
	if w.Name != "Morning" || !w.Enabled || len(w.Steps) != 2 || uuid.Validate(string(w.ID)) != nil {
		t.Fatalf("imported = %+v", w)
	}
	if row := fx.renders.rows[w.ID]; row.Path != path || row.BlobSHA != "agent1" {
		t.Errorf("render row = %+v", row)
	}
	if got := titles(fx.notes.notes); !slices.Equal(got, []string{"Workflow added from your repo"}) {
		t.Errorf("notifications = %v", got)
	}

	// Loop safety: nothing to adopt twice; the next converge writes the
	// minted id into the file, and adopting that is a no-op.
	fx.notes.notes = nil
	fx.adopt(t)
	fx.sync(t)
	fx.adopt(t)
	if len(fx.store.workflows) != 1 || len(fx.notes.notes) != 0 {
		t.Errorf("workflows %d, notes %v", len(fx.store.workflows), titles(fx.notes.notes))
	}
	if !strings.Contains(fx.repo.files[path], "id: "+string(w.ID)) {
		t.Errorf("converge should record the minted id:\n%s", fx.repo.files[path])
	}
}

func TestWorkflowMirror_RefusedNewFileIsReportedOnceAndKept(t *testing.T) {
	fx := newWorkflowMirrorFixture(t)
	const path = "workflows/broken.yaml"
	fx.repo.files[path] = "name: Broken\nsteps:\n  - transformation: nope\n"
	fx.repo.shas[path] = "agent1"

	fx.adopt(t)
	fx.adopt(t)
	if len(fx.store.workflows) != 0 {
		t.Fatal("an unresolvable file must not create a workflow")
	}
	if len(fx.notes.notes) != 1 || !strings.Contains(fx.notes.notes[0].Body, `transformation "nope"`) {
		t.Errorf("want one refusal naming the bad key, got %+v", fx.notes.notes)
	}

	// A Console save elsewhere must not collect it: it is nobody's render.
	fx.create(t, "Nightly", "", pipeStep(pipeAcme))
	if _, ok := fx.repo.files[path]; !ok {
		t.Error("a refused hand-written file is the only copy and must survive a converge")
	}
}

func TestWorkflowMirror_NewFilesNeedTheImporter(t *testing.T) {
	fx := newWorkflowMirrorFixture(t)
	fx.mirror.Importer = nil
	fx.repo.files["workflows/morning.yaml"] = "name: Morning\nsteps:\n  - pipeline: orders\n"
	fx.repo.shas["workflows/morning.yaml"] = "agent1"
	fx.adopt(t)
	if len(fx.store.workflows) != 0 || len(fx.notes.notes) != 0 {
		t.Errorf("without an importer a new file is not adopted: %d workflows, %v", len(fx.store.workflows), titles(fx.notes.notes))
	}
}

func TestWorkflowMirror_Deletion(t *testing.T) {
	t.Run("file removed from git deletes the workflow", func(t *testing.T) {
		fx := newWorkflowMirrorFixture(t)
		fx.create(t, "Nightly", "", pipeStep(pipeAcme))
		delete(fx.repo.files, nightlyPath)
		delete(fx.repo.shas, nightlyPath)

		fx.adopt(t)
		if len(fx.store.workflows) != 0 {
			t.Fatal("workflow should be deleted")
		}
		if got := titles(fx.notes.notes); !slices.Equal(got, []string{"Workflow deleted from your repo"}) {
			t.Errorf("notifications = %v", got)
		}
	})

	t.Run("not without the importer", func(t *testing.T) {
		fx := newWorkflowMirrorFixture(t)
		fx.mirror.Importer = nil
		fx.create(t, "Nightly", "", pipeStep(pipeAcme))
		delete(fx.repo.files, nightlyPath)
		fx.adopt(t)
		if len(fx.store.workflows) != 1 {
			t.Fatal("central-style mirror must not delete")
		}
	})

	t.Run("not on an empty repo listing", func(t *testing.T) {
		fx := newWorkflowMirrorFixture(t)
		fx.create(t, "Nightly", "", pipeStep(pipeAcme))
		fx.repo.files, fx.repo.shas = map[string]string{}, map[string]string{}
		fx.adopt(t)
		if len(fx.store.workflows) != 1 {
			t.Fatal("an empty repo is a reset, not a deletion")
		}
	})

	t.Run("Console delete removes the file, hand-written or not", func(t *testing.T) {
		fx := newWorkflowMirrorFixture(t)
		const path = "workflows/morning.yaml"
		fx.repo.files[path] = "name: Morning\nsteps:\n  - pipeline: orders\n"
		fx.repo.shas[path] = "agent1"
		fx.adopt(t) // imported, file still has no header or id
		w := fx.stored(t)

		if err := fx.svc.DeleteWorkflow(context.Background(), "user-acme", w.ID); err != nil {
			t.Fatal(err)
		}
		if _, ok := fx.repo.files[path]; ok {
			t.Fatal("the deleted workflow's file must go, or the next pass re-imports it")
		}
		fx.adopt(t)
		if len(fx.store.workflows) != 0 {
			t.Error("deleted workflow came back")
		}
	})
}

func TestWorkflowMirror_Rename(t *testing.T) {
	t.Run("Console rename moves the file", func(t *testing.T) {
		fx := newWorkflowMirrorFixture(t)
		w := fx.create(t, "Nightly", "", pipeStep(pipeAcme))
		w.Name = "Night Owl"
		if _, err := fx.svc.UpdateWorkflow(context.Background(), "user-acme", w); err != nil {
			t.Fatal(err)
		}
		if got := workflowFiles(fx.repo); !slices.Equal(got, []string{"workflows/night-owl.yaml"}) {
			t.Errorf("files = %v", got)
		}
		fx.adopt(t)
		if len(fx.store.workflows) != 1 || len(fx.notes.notes) != 0 {
			t.Errorf("rename must not read as delete+add: %d workflows, %v", len(fx.store.workflows), titles(fx.notes.notes))
		}
	})

	t.Run("git move keeps the workflow", func(t *testing.T) {
		fx := newWorkflowMirrorFixture(t)
		w := fx.create(t, "Nightly", "", pipeStep(pipeAcme))
		content := fx.repo.files[nightlyPath]
		delete(fx.repo.files, nightlyPath)
		delete(fx.repo.shas, nightlyPath)
		const moved = "workflows/team/nightly.yaml"
		fx.repo.files[moved] = strings.Replace(content, "name: Nightly", "name: Nightly v2", 1)
		fx.repo.shas[moved] = "moved1"

		fx.adopt(t)
		got := fx.stored(t)
		if got.ID != w.ID || got.Name != "Nightly v2" {
			t.Fatalf("moved file not adopted as the same workflow: %+v", got)
		}
		if row := fx.renders.rows[w.ID]; row.Path != moved {
			t.Errorf("render row should follow the file: %+v", row)
		}
		// Paths follow names: the next converge renders it at its slug and
		// collects the moved copy (its id is ours).
		fx.sync(t)
		if got := workflowFiles(fx.repo); !slices.Equal(got, []string{"workflows/nightly-v2.yaml"}) {
			t.Errorf("files = %v", got)
		}
	})

	t.Run("a copy with the same id is refused, not adopted", func(t *testing.T) {
		fx := newWorkflowMirrorFixture(t)
		fx.create(t, "Nightly", "", pipeStep(pipeAcme))
		fx.repo.files["workflows/copy.yaml"] = fx.repo.files[nightlyPath]
		fx.repo.shas["workflows/copy.yaml"] = "copy1"
		fx.adopt(t)
		fx.adopt(t)
		if len(fx.notes.notes) != 1 || !strings.Contains(fx.notes.notes[0].Body, "remove the id line") {
			t.Errorf("notifications = %+v", fx.notes.notes)
		}
	})
}

func TestWorkflowMirror_TargetChangesRerenderWorkflowFiles(t *testing.T) {
	fx := newWorkflowMirrorFixture(t)
	w := fx.create(t, "Nightly", "", pipeStep(pipeAcme), pipeStep(pipe2))
	primary := &hardFailMirror{}
	then := &workspace.ThenSyncWorkflows{Primary: primary, Workflows: fx.mirror}

	// Rename a target: its key in the workflow file moves with it.
	fx.targets.pipelines[0].Name = "Orders EU"
	if err := then.SyncCustomer(context.Background(), "acme", nil); err != nil {
		t.Fatal(err)
	}
	if c := fx.repo.files[nightlyPath]; !strings.Contains(c, "pipeline: orders-eu") {
		t.Errorf("renamed target's key not re-rendered:\n%s", c)
	}

	// Delete a target: the FK cascade drops the step, the file follows.
	fx.targets.pipelines = slices.DeleteFunc(fx.targets.pipelines, func(p workspace.Pipeline) bool { return p.ID == pipe2 })
	stored := fx.store.workflows[w.ID]
	stored.Steps = stored.Steps[:1]
	if err := then.SyncCustomer(context.Background(), "acme", nil); err != nil {
		t.Fatal(err)
	}
	if c := fx.repo.files[nightlyPath]; strings.Contains(c, "customers") {
		t.Errorf("cascaded step still in the file:\n%s", c)
	}
	if len(fx.notes.notes) != 0 {
		t.Errorf("mirror writes are not drift: %v", titles(fx.notes.notes))
	}

	// The primary's failure is the save's failure; the workflow converge is
	// skipped.
	primary.err = workspace.ErrBoxUnreachable
	if err := then.SyncCustomer(context.Background(), "acme", nil); !errors.Is(err, workspace.ErrBoxUnreachable) {
		t.Errorf("err = %v", err)
	}
}

func TestWorkflowMirror_SaveOverwritesDrift(t *testing.T) {
	fx := newWorkflowMirrorFixture(t)
	w := fx.create(t, "Nightly", "", pipeStep(pipeAcme))
	fx.repo.tamper(nightlyPath, "name: Nightly\nsteps:\n  - pipeline: customers\n")
	// A Console save before the adopt pass ran: the save wins, and says so.
	w.Schedule = "0 1 * * *"
	if _, err := fx.svc.UpdateWorkflow(context.Background(), "user-acme", w); err != nil {
		t.Fatal(err)
	}
	if c := fx.repo.files[nightlyPath]; strings.Contains(c, "customers") || !strings.Contains(c, "0 1 * * *") {
		t.Errorf("save must overwrite the out-of-band edit:\n%s", c)
	}
	if got := titles(fx.notes.notes); !slices.Equal(got, []string{"Workflow file changed outside the Console"}) {
		t.Errorf("notifications = %v", got)
	}
}

// failingWorkflowMirror fails every converge.
type failingWorkflowMirror struct{ err error }

func (m *failingWorkflowMirror) SyncCustomer(context.Context, string, *workspace.CommitAuthor) error {
	return m.err
}

func (m *failingWorkflowMirror) SyncCustomerAfterDelete(context.Context, string, *workspace.CommitAuthor, *workspace.Workflow) error {
	return m.err
}

func TestWorkflowService_GitPrimary(t *testing.T) {
	ctx := context.Background()
	setup := func(t *testing.T) (*workflowMirrorFixture, *workspace.Workflow) {
		t.Helper()
		fx := newWorkflowMirrorFixture(t)
		w := fx.create(t, "Nightly", "0 2 * * *", pipeStep(pipeAcme))
		fx.svc.Mirror = &failingWorkflowMirror{err: workspace.ErrBoxUnreachable}
		return fx, w
	}

	t.Run("create compensates", func(t *testing.T) {
		fx, _ := setup(t)
		_, err := fx.svc.CreateWorkflow(ctx, "user-acme", &workspace.Workflow{Name: "Other", Steps: []workspace.WorkflowStep{pipeStep(pipe2)}})
		if !errors.Is(err, workspace.ErrBoxUnreachable) || len(fx.store.workflows) != 1 {
			t.Errorf("err = %v, workflows = %d", err, len(fx.store.workflows))
		}
	})

	t.Run("update restores the previous definition", func(t *testing.T) {
		fx, w := setup(t)
		w.Schedule = "0 5 * * *"
		if _, err := fx.svc.UpdateWorkflow(ctx, "user-acme", w); !errors.Is(err, workspace.ErrBoxUnreachable) {
			t.Fatalf("err = %v", err)
		}
		if got := fx.store.workflows[w.ID].Schedule; got != "0 2 * * *" {
			t.Errorf("schedule = %q, want the old one back", got)
		}
	})

	t.Run("enable toggle restores the flag", func(t *testing.T) {
		fx, w := setup(t)
		if _, err := fx.svc.SetWorkflowEnabled(ctx, "user-acme", w.ID, false); !errors.Is(err, workspace.ErrBoxUnreachable) {
			t.Fatalf("err = %v", err)
		}
		if !fx.store.workflows[w.ID].Enabled {
			t.Error("enabled flag must be restored")
		}
	})

	t.Run("delete re-inserts under the same id", func(t *testing.T) {
		fx, w := setup(t)
		if err := fx.svc.DeleteWorkflow(ctx, "user-acme", w.ID); !errors.Is(err, workspace.ErrBoxUnreachable) {
			t.Fatalf("err = %v", err)
		}
		if _, ok := fx.store.workflows[w.ID]; !ok {
			t.Errorf("workflow %s not restored; have %v", w.ID, slices.Collect(maps.Keys(fx.store.workflows)))
		}
	})
}

func TestWorkflowMirror_ImportedFileAtOtherPathMovesToItsSlug(t *testing.T) {
	fx := newWorkflowMirrorFixture(t)
	const path = "workflows/draft-by-agent.yaml"
	fx.repo.files[path] = "name: Morning\nsteps:\n  - pipeline: orders\n"
	fx.repo.shas[path] = "agent1"
	fx.adopt(t)
	fx.sync(t)
	if got := workflowFiles(fx.repo); !slices.Equal(got, []string{"workflows/morning.yaml"}) {
		t.Fatalf("files = %v, want the id-less draft collected and the workflow at its slug", got)
	}
	fx.notes.notes = nil
	fx.adopt(t)
	if len(fx.store.workflows) != 1 || len(fx.notes.notes) != 0 {
		t.Errorf("workflows %d, notes %v", len(fx.store.workflows), titles(fx.notes.notes))
	}
}

package workspace_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

// --- In-memory fakes ---

// fakeWorkflowStore is an in-memory WorkflowRepository with the same
// observable semantics as the postgres store: unique names per workspace,
// upsert-by-id that refuses to move a run to another workflow, step runs
// upserted by position.
type fakeWorkflowStore struct {
	workflows map[workspace.WorkflowID]*workspace.Workflow
	runs      map[string]*workspace.WorkflowRun
	nextID    int
	clock     time.Time
}

func newFakeWorkflowStore() *fakeWorkflowStore {
	return &fakeWorkflowStore{
		workflows: map[workspace.WorkflowID]*workspace.Workflow{},
		runs:      map[string]*workspace.WorkflowRun{},
		clock:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func (f *fakeWorkflowStore) newID() string {
	f.nextID++
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", f.nextID)
}

func (f *fakeWorkflowStore) tick() time.Time {
	f.clock = f.clock.Add(time.Second)
	return f.clock
}

func (f *fakeWorkflowStore) nameTaken(w *workspace.Workflow) bool {
	for _, other := range f.workflows {
		if other.ID != w.ID && other.CustomerSlug == w.CustomerSlug && other.Name == w.Name {
			return true
		}
	}
	return false
}

func (f *fakeWorkflowStore) CreateWorkflow(_ context.Context, w *workspace.Workflow) error {
	if f.nameTaken(w) {
		return workspace.ErrWorkflowAlreadyExists
	}
	w.ID = workspace.WorkflowID(f.newID())
	cp := *w
	cp.Steps = append([]workspace.WorkflowStep(nil), w.Steps...)
	f.workflows[w.ID] = &cp
	return nil
}

func (f *fakeWorkflowStore) GetWorkflow(_ context.Context, id workspace.WorkflowID) (*workspace.Workflow, error) {
	w, ok := f.workflows[id]
	if !ok {
		return nil, workspace.ErrWorkflowNotFound
	}
	cp := *w
	return &cp, nil
}

func (f *fakeWorkflowStore) ListWorkflowsByCustomer(_ context.Context, slug string) ([]workspace.Workflow, error) {
	var out []workspace.Workflow
	for _, w := range f.workflows {
		if w.CustomerSlug == slug {
			out = append(out, *w)
		}
	}
	return out, nil
}

func (f *fakeWorkflowStore) UpdateWorkflow(_ context.Context, w *workspace.Workflow) error {
	if _, ok := f.workflows[w.ID]; !ok {
		return workspace.ErrWorkflowNotFound
	}
	if f.nameTaken(w) {
		return workspace.ErrWorkflowAlreadyExists
	}
	cp := *w
	f.workflows[w.ID] = &cp
	return nil
}

func (f *fakeWorkflowStore) DeleteWorkflow(_ context.Context, id workspace.WorkflowID) error {
	if _, ok := f.workflows[id]; !ok {
		return workspace.ErrWorkflowNotFound
	}
	delete(f.workflows, id)
	for rid, r := range f.runs {
		if r.WorkflowID == id {
			delete(f.runs, rid)
		}
	}
	return nil
}

func (f *fakeWorkflowStore) GetEnabledWorkflows(ctx context.Context, slug string) ([]workspace.Workflow, error) {
	var out []workspace.Workflow
	for _, w := range f.workflows {
		if w.CustomerSlug != slug {
			continue
		}
		cp := *w
		if pending, _ := f.GetPendingWorkflowRun(ctx, w.ID); pending != nil {
			cp.TriggerNow, cp.PendingRunID = true, pending.ID
		}
		if cp.Enabled || cp.TriggerNow {
			out = append(out, cp)
		}
	}
	return out, nil
}

func (f *fakeWorkflowStore) sortedRuns(id workspace.WorkflowID) []workspace.WorkflowRun {
	var out []workspace.WorkflowRun
	for _, r := range f.runs {
		if r.WorkflowID == id {
			out = append(out, *r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		return out[i].ID > out[j].ID
	})
	return out
}

func (f *fakeWorkflowStore) GetPendingWorkflowRun(_ context.Context, id workspace.WorkflowID) (*workspace.WorkflowRun, error) {
	runs := f.sortedRuns(id)
	for i := len(runs) - 1; i >= 0; i-- {
		if runs[i].Status == workspace.RunStatusPending {
			return &runs[i], nil
		}
	}
	return nil, nil
}

func (f *fakeWorkflowStore) CreateWorkflowRun(_ context.Context, run *workspace.WorkflowRun) error {
	if run.ID == "" {
		run.ID = f.newID()
	}
	run.CreatedAt = f.tick()
	cp := *run
	f.runs[run.ID] = &cp
	return nil
}

func (f *fakeWorkflowStore) UpsertWorkflowRun(_ context.Context, run *workspace.WorkflowRun) (string, error) {
	existing, ok := f.runs[run.ID]
	if !ok {
		cp := *run
		cp.CreatedAt = f.tick()
		if cp.Trigger == "" {
			cp.Trigger = workspace.WorkflowTriggerSchedule
		}
		cp.StepRuns = append([]workspace.WorkflowStepRun(nil), run.StepRuns...)
		f.runs[run.ID] = &cp
		return "", nil
	}
	if existing.WorkflowID != run.WorkflowID {
		return "", workspace.ErrWorkflowRunNotFound
	}
	previous := existing.Status
	existing.Status = run.Status
	existing.CompletedAt = run.CompletedAt
	existing.ErrorMessage = run.ErrorMessage
	if run.Trigger != "" {
		existing.Trigger = run.Trigger
	}
	if run.StartedAt != nil {
		existing.StartedAt = run.StartedAt
	}
	for _, sr := range run.StepRuns {
		existing.StepRuns = upsertStepRun(existing.StepRuns, sr)
	}
	return previous, nil
}

func upsertStepRun(steps []workspace.WorkflowStepRun, sr workspace.WorkflowStepRun) []workspace.WorkflowStepRun {
	for i := range steps {
		if steps[i].Position == sr.Position {
			steps[i] = sr
			return steps
		}
	}
	steps = append(steps, sr)
	sort.Slice(steps, func(i, j int) bool { return steps[i].Position < steps[j].Position })
	return steps
}

func (f *fakeWorkflowStore) ListWorkflowRuns(_ context.Context, id workspace.WorkflowID, limit int, after *workspace.WorkflowRunCursor) ([]workspace.WorkflowRun, error) {
	var out []workspace.WorkflowRun
	for _, r := range f.sortedRuns(id) {
		if after != nil && !olderThan(r, after) {
			continue
		}
		out = append(out, r)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func olderThan(r workspace.WorkflowRun, c *workspace.WorkflowRunCursor) bool {
	if r.CreatedAt.Equal(c.CreatedAt) {
		return r.ID < c.ID
	}
	return r.CreatedAt.Before(c.CreatedAt)
}

// fakeRefs serves pipelines and transformations by id.
type fakeRefs struct {
	pipelines       map[workspace.PipelineID]string // id → customer slug
	transformations map[workspace.TransformationID]string
}

func (f *fakeRefs) GetPipeline(_ context.Context, id workspace.PipelineID) (*workspace.Pipeline, error) {
	slug, ok := f.pipelines[id]
	if !ok {
		return nil, workspace.ErrPipelineNotFound
	}
	return &workspace.Pipeline{ID: id, CustomerSlug: slug}, nil
}

func (f *fakeRefs) GetTransformation(_ context.Context, id workspace.TransformationID) (*workspace.Transformation, error) {
	slug, ok := f.transformations[id]
	if !ok {
		return nil, workspace.ErrTransformationNotFound
	}
	return &workspace.Transformation{ID: id, CustomerSlug: slug}, nil
}

type recordingNotifier struct{ notes []workspace.Notification }

func (r *recordingNotifier) Notify(_ context.Context, n workspace.Notification) error {
	r.notes = append(r.notes, n)
	return nil
}

// Fixture ids: acme owns pipeAcme and trAcme, evil owns pipeEvil.
const (
	pipeAcme = "11111111-1111-4111-8111-111111111111"
	pipe2    = "11111111-1111-4111-8111-222222222222"
	pipeEvil = "11111111-1111-4111-8111-999999999999"
	trAcme   = "22222222-2222-4222-8222-111111111111"
	missing  = "33333333-3333-4333-8333-333333333333"
	runA     = "44444444-4444-4444-8444-444444444444"
)

type workflowFixture struct {
	svc   *workspace.WorkflowService
	store *fakeWorkflowStore
	notes *recordingNotifier
}

func newWorkflowFixture() workflowFixture {
	store := newFakeWorkflowStore()
	refs := &fakeRefs{
		pipelines: map[workspace.PipelineID]string{
			pipeAcme: "acme", pipe2: "acme", pipeEvil: "evil",
		},
		transformations: map[workspace.TransformationID]string{trAcme: "acme"},
	}
	notes := &recordingNotifier{}
	return workflowFixture{
		svc: &workspace.WorkflowService{
			Workspaces: &mockCustomerReader{
				getByUserIDFn: func(_ context.Context, id core.UserID) (*workspace.Workspace, error) {
					slug, _ := strings.CutPrefix(string(id), "user-")
					return &workspace.Workspace{Slug: slug}, nil
				},
			},
			Workflows:       store,
			Pipelines:       refs,
			Transformations: refs,
			Notifications:   notes,
		},
		store: store,
		notes: notes,
	}
}

func pipeStep(id string) workspace.WorkflowStep {
	return workspace.NewWorkflowStep(workspace.StepKindPipeline, id)
}

func trStep(id string) workspace.WorkflowStep {
	return workspace.NewWorkflowStep(workspace.StepKindTransformation, id)
}

func (fx workflowFixture) create(t *testing.T, name string, steps ...workspace.WorkflowStep) *workspace.Workflow {
	t.Helper()
	w, err := fx.svc.CreateWorkflow(context.Background(), "user-acme", &workspace.Workflow{Name: name, Steps: steps})
	if err != nil {
		t.Fatalf("CreateWorkflow(%q) error = %v", name, err)
	}
	return w
}

func invalidField(t *testing.T, err error) string {
	t.Helper()
	var inv *workspace.ErrInvalidWorkflow
	if !errors.As(err, &inv) {
		t.Fatalf("error = %v, want *ErrInvalidWorkflow", err)
	}
	return inv.Field
}

// --- Tests ---

func TestWorkflowService_Create(t *testing.T) {
	fx := newWorkflowFixture()
	w := fx.create(t, "  nightly  ", pipeStep(pipeAcme), trStep(trAcme))

	if w.CustomerSlug != "acme" || w.Name != "nightly" || !w.Enabled {
		t.Fatalf("created = %+v, want acme/nightly/enabled", w)
	}
	if len(w.Steps) != 2 || w.Steps[0].Position != 0 || w.Steps[1].Position != 1 {
		t.Fatalf("steps = %+v, want positions 0,1", w.Steps)
	}
	if w.Steps[1].RefID() != trAcme {
		t.Fatalf("step 1 ref = %q", w.Steps[1].RefID())
	}
}

func TestWorkflowService_CreateValidation(t *testing.T) {
	tooMany := make([]workspace.WorkflowStep, workspace.MaxWorkflowSteps+1)
	for i := range tooMany {
		tooMany[i] = pipeStep(fmt.Sprintf("11111111-1111-4111-8111-%012d", i))
	}
	cases := []struct {
		name      string
		w         workspace.Workflow
		wantField string
	}{
		{"name required", workspace.Workflow{Name: " ", Steps: []workspace.WorkflowStep{pipeStep(pipeAcme)}}, "name"},
		{"no steps", workspace.Workflow{Name: "x"}, "steps"},
		{"too many steps", workspace.Workflow{Name: "x", Steps: tooMany}, "steps"},
		{"bad kind", workspace.Workflow{Name: "x", Steps: []workspace.WorkflowStep{{Kind: "sql"}}}, "steps[0].kind"},
		{"bad uuid", workspace.Workflow{Name: "x", Steps: []workspace.WorkflowStep{pipeStep("nope")}}, "steps[0].ref_id"},
		{"duplicate step", workspace.Workflow{Name: "x", Steps: []workspace.WorkflowStep{pipeStep(pipeAcme), trStep(trAcme), pipeStep(pipeAcme)}}, "steps[2].ref_id"},
		{"missing ref", workspace.Workflow{Name: "x", Steps: []workspace.WorkflowStep{pipeStep(missing)}}, "steps[0].ref_id"},
		{"kind/ref mismatch", workspace.Workflow{Name: "x", Steps: []workspace.WorkflowStep{trStep(pipeAcme)}}, "steps[0].ref_id"},
		{"cross-tenant ref", workspace.Workflow{Name: "x", Steps: []workspace.WorkflowStep{pipeStep(pipeAcme), pipeStep(pipeEvil)}}, "steps[1].ref_id"},
		{"bad cron", workspace.Workflow{Name: "x", Schedule: "every day", Steps: []workspace.WorkflowStep{pipeStep(pipeAcme)}}, "schedule"},
		{"long description", workspace.Workflow{Name: "x", Description: strings.Repeat("a", 2001), Steps: []workspace.WorkflowStep{pipeStep(pipeAcme)}}, "description"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newWorkflowFixture()
			w := tc.w
			_, err := fx.svc.CreateWorkflow(context.Background(), "user-acme", &w)
			if got := invalidField(t, err); got != tc.wantField {
				t.Fatalf("field = %q, want %q (err %v)", got, tc.wantField, err)
			}
			if len(fx.store.workflows) != 0 {
				t.Fatal("an invalid workflow was stored")
			}
		})
	}
}

func TestWorkflowService_DuplicateName(t *testing.T) {
	fx := newWorkflowFixture()
	fx.create(t, "nightly", pipeStep(pipeAcme))
	_, err := fx.svc.CreateWorkflow(context.Background(), "user-acme", &workspace.Workflow{Name: "nightly", Steps: []workspace.WorkflowStep{pipeStep(pipe2)}})
	if !errors.Is(err, workspace.ErrWorkflowAlreadyExists) {
		t.Fatalf("error = %v, want ErrWorkflowAlreadyExists", err)
	}
	// Names are unique per workspace, not globally.
	fx.store.workflows["other"] = &workspace.Workflow{ID: "other", CustomerSlug: "evil", Name: "elsewhere"}
	if _, err := fx.svc.CreateWorkflow(context.Background(), "user-evil", &workspace.Workflow{Name: "nightly", Steps: []workspace.WorkflowStep{pipeStep(pipeEvil)}}); err != nil {
		t.Fatalf("same name in another workspace: %v", err)
	}
}

func TestValidateCronSchedule(t *testing.T) {
	valid := []string{"", "@daily", "@HOURLY", "0 * * * *", "*/15 2-4 1,15 jan-mar mon-fri", "0 0 * * 7", "5 4 * * sun"}
	for _, s := range valid {
		if err := workspace.ValidateCronSchedule(s); err != nil {
			t.Errorf("ValidateCronSchedule(%q) = %v, want nil", s, err)
		}
	}
	invalid := []string{"daily", "* * * *", "* * * * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "* * * 13 *", "*/0 * * * *", "5-1 * * * *", "* * * * funday", "@reboot"}
	for _, s := range invalid {
		if err := workspace.ValidateCronSchedule(s); err == nil {
			t.Errorf("ValidateCronSchedule(%q) = nil, want error", s)
		}
	}
}

func TestWorkflowService_TenantIsolation(t *testing.T) {
	fx := newWorkflowFixture()
	w := fx.create(t, "nightly", pipeStep(pipeAcme))
	ctx := context.Background()

	if _, _, err := fx.svc.GetWorkflow(ctx, "user-evil", w.ID); !errors.Is(err, workspace.ErrWorkflowNotFound) {
		t.Errorf("Get: %v", err)
	}
	if _, err := fx.svc.UpdateWorkflow(ctx, "user-evil", &workspace.Workflow{ID: w.ID, Name: "x", Steps: []workspace.WorkflowStep{pipeStep(pipeEvil)}}); !errors.Is(err, workspace.ErrWorkflowNotFound) {
		t.Errorf("Update: %v", err)
	}
	if err := fx.svc.DeleteWorkflow(ctx, "user-evil", w.ID); !errors.Is(err, workspace.ErrWorkflowNotFound) {
		t.Errorf("Delete: %v", err)
	}
	if _, err := fx.svc.TriggerWorkflow(ctx, "user-evil", w.ID); !errors.Is(err, workspace.ErrWorkflowNotFound) {
		t.Errorf("Trigger: %v", err)
	}
	if _, _, err := fx.svc.ListWorkflowRuns(ctx, "user-evil", w.ID, 0, ""); !errors.Is(err, workspace.ErrWorkflowNotFound) {
		t.Errorf("ListRuns: %v", err)
	}
	if list, _ := fx.svc.ListWorkflows(ctx, "user-evil"); len(list) != 0 {
		t.Errorf("List leaked %d workflows", len(list))
	}
	if cfgs, _ := fx.svc.GetWorkflowConfigs(ctx, "evil"); len(cfgs) != 0 {
		t.Errorf("GetWorkflowConfigs leaked %d workflows", len(cfgs))
	}
	err := fx.svc.ReportWorkflowRun(ctx, "evil", &workspace.WorkflowRun{ID: runA, WorkflowID: w.ID, Status: "running"})
	if !errors.Is(err, workspace.ErrWorkflowNotFound) {
		t.Errorf("Report from another tenant: %v", err)
	}
	if len(fx.store.runs) != 0 {
		t.Error("a cross-tenant report was stored")
	}
}

func TestWorkflowService_Update(t *testing.T) {
	fx := newWorkflowFixture()
	w := fx.create(t, "nightly", pipeStep(pipeAcme))
	got, err := fx.svc.UpdateWorkflow(context.Background(), "user-acme", &workspace.Workflow{
		ID: w.ID, Name: "nightly", Schedule: "@daily", Enabled: false,
		Steps: []workspace.WorkflowStep{trStep(trAcme), pipeStep(pipeAcme)},
	})
	if err != nil {
		t.Fatalf("UpdateWorkflow() error = %v", err)
	}
	if got.Enabled || got.Schedule != "@daily" || got.Steps[0].Kind != workspace.StepKindTransformation || got.Steps[1].Position != 1 {
		t.Fatalf("updated = %+v", got)
	}
	if !got.CreatedAt.Equal(w.CreatedAt) {
		t.Error("CreatedAt changed on update")
	}
}

func TestWorkflowService_Trigger(t *testing.T) {
	fx := newWorkflowFixture()
	w := fx.create(t, "nightly", pipeStep(pipeAcme))
	ctx := context.Background()

	run, err := fx.svc.TriggerWorkflow(ctx, "user-acme", w.ID)
	if err != nil {
		t.Fatalf("TriggerWorkflow() error = %v", err)
	}
	if run.Status != "pending" || run.Trigger != "manual" || run.ID == "" {
		t.Fatalf("run = %+v, want a pending manual run", run)
	}
	again, err := fx.svc.TriggerWorkflow(ctx, "user-acme", w.ID)
	if err != nil || again.ID != run.ID {
		t.Fatalf("second trigger = %+v, %v; want the same pending run", again, err)
	}

	// A disabled workflow with a pending run is still served, flagged.
	fx.store.workflows[w.ID].Enabled = false
	cfgs, err := fx.svc.GetWorkflowConfigs(ctx, "acme")
	if err != nil || len(cfgs) != 1 || !cfgs[0].TriggerNow || cfgs[0].PendingRunID != run.ID {
		t.Fatalf("configs = %+v, %v; want the pending run served", cfgs, err)
	}

	// Reporting the pending id clears trigger_now.
	if err := fx.svc.ReportWorkflowRun(ctx, "acme", &workspace.WorkflowRun{ID: run.ID, WorkflowID: w.ID, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if cfgs, _ := fx.svc.GetWorkflowConfigs(ctx, "acme"); len(cfgs) != 0 {
		t.Fatalf("disabled workflow still served after its run started: %+v", cfgs)
	}
	if fx.store.runs[run.ID].Trigger != "manual" {
		t.Error("a report without trigger overwrote the stored one")
	}
}

func TestWorkflowService_StepsGoneWithTheirTargets(t *testing.T) {
	fx := newWorkflowFixture()
	w := fx.create(t, "nightly", pipeStep(pipeAcme))
	// What the FK cascade leaves behind when the only target is deleted.
	fx.store.workflows[w.ID].Steps = nil

	if _, err := fx.svc.TriggerWorkflow(context.Background(), "user-acme", w.ID); !errors.Is(err, workspace.ErrWorkflowHasNoSteps) {
		t.Fatalf("Trigger on a stepless workflow: %v, want ErrWorkflowHasNoSteps", err)
	}
	if cfgs, _ := fx.svc.GetWorkflowConfigs(context.Background(), "acme"); len(cfgs) != 0 {
		t.Fatalf("stepless workflow served to the worker: %+v", cfgs)
	}
}

func stepRun(pos int, kind workspace.WorkflowStepKind, ref, status string) workspace.WorkflowStepRun {
	return workspace.WorkflowStepRun{Position: pos, Kind: kind, RefID: ref, Name: "n", Status: status}
}

func TestWorkflowService_ReportIdempotent(t *testing.T) {
	fx := newWorkflowFixture()
	w := fx.create(t, "nightly", pipeStep(pipeAcme), trStep(trAcme))
	ctx := context.Background()

	report := func(status string, steps ...workspace.WorkflowStepRun) {
		t.Helper()
		err := fx.svc.ReportWorkflowRun(ctx, "acme", &workspace.WorkflowRun{
			ID: runA, WorkflowID: w.ID, Status: status, StepRuns: steps, ErrorMessage: "boom",
		})
		if err != nil {
			t.Fatalf("ReportWorkflowRun(%s) error = %v", status, err)
		}
	}
	report("running", stepRun(0, "pipeline", pipeAcme, "running"), stepRun(1, "transformation", trAcme, "pending"))
	failed := stepRun(0, "pipeline", pipeAcme, "failed")
	failed.ErrorMessage = "source down"
	report("failed", failed, stepRun(1, "transformation", trAcme, "skipped"))
	report("failed", failed, stepRun(1, "transformation", trAcme, "skipped"))

	if len(fx.store.runs) != 1 {
		t.Fatalf("runs = %d, want one row for one run id", len(fx.store.runs))
	}
	got := fx.store.runs[runA]
	if got.Status != "failed" || got.Trigger != "schedule" || len(got.StepRuns) != 2 || got.StepRuns[1].Status != "skipped" {
		t.Fatalf("stored run = %+v", got)
	}
	if len(fx.notes.notes) != 1 {
		t.Fatalf("notifications = %d, want exactly one for the transition into failed", len(fx.notes.notes))
	}
	n := fx.notes.notes[0]
	if n.Type != "workflow_run" || n.CustomerSlug != "acme" || !strings.Contains(n.Body, "source down") || !strings.Contains(n.Title, "nightly") {
		t.Fatalf("notification = %+v", n)
	}
}

func TestWorkflowService_ReportSuccessDoesNotNotify(t *testing.T) {
	fx := newWorkflowFixture()
	w := fx.create(t, "nightly", pipeStep(pipeAcme))
	if err := fx.svc.ReportWorkflowRun(context.Background(), "acme", &workspace.WorkflowRun{
		ID: runA, WorkflowID: w.ID, Status: "success", StepRuns: []workspace.WorkflowStepRun{stepRun(0, "pipeline", pipeAcme, "success")},
	}); err != nil {
		t.Fatal(err)
	}
	if len(fx.notes.notes) != 0 {
		t.Fatalf("notifications = %+v, want none", fx.notes.notes)
	}
}

func TestWorkflowService_ReportRunIDBelongsToAnotherWorkflow(t *testing.T) {
	fx := newWorkflowFixture()
	a := fx.create(t, "a", pipeStep(pipeAcme))
	b := fx.create(t, "b", pipeStep(pipe2))
	ctx := context.Background()
	if err := fx.svc.ReportWorkflowRun(ctx, "acme", &workspace.WorkflowRun{ID: runA, WorkflowID: a.ID, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	err := fx.svc.ReportWorkflowRun(ctx, "acme", &workspace.WorkflowRun{ID: runA, WorkflowID: b.ID, Status: "running"})
	if !errors.Is(err, workspace.ErrWorkflowRunNotFound) {
		t.Fatalf("error = %v, want ErrWorkflowRunNotFound", err)
	}
}

func TestWorkflowService_ReportValidation(t *testing.T) {
	fx := newWorkflowFixture()
	w := fx.create(t, "nightly", pipeStep(pipeAcme))
	cases := []struct {
		name  string
		run   workspace.WorkflowRun
		field string
	}{
		{"run id", workspace.WorkflowRun{ID: "x", Status: "running"}, "run_id"},
		{"pending is not reportable", workspace.WorkflowRun{ID: runA, Status: "pending"}, "status"},
		{"trigger", workspace.WorkflowRun{ID: runA, Status: "running", Trigger: "chained"}, "trigger"},
		{"step status", workspace.WorkflowRun{ID: runA, Status: "running", StepRuns: []workspace.WorkflowStepRun{stepRun(0, "pipeline", pipeAcme, "done")}}, "step_runs[0].status"},
		{"step position", workspace.WorkflowRun{ID: runA, Status: "running", StepRuns: []workspace.WorkflowStepRun{stepRun(-1, "pipeline", pipeAcme, "running")}}, "step_runs[0].position"},
		{"duplicate position", workspace.WorkflowRun{ID: runA, Status: "running", StepRuns: []workspace.WorkflowStepRun{
			stepRun(0, "pipeline", pipeAcme, "running"), stepRun(0, "pipeline", pipeAcme, "running"),
		}}, "step_runs[1].position"},
		{"step kind", workspace.WorkflowRun{ID: runA, Status: "running", StepRuns: []workspace.WorkflowStepRun{stepRun(0, "sql", pipeAcme, "running")}}, "step_runs[0].kind"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := tc.run
			run.WorkflowID = w.ID
			err := fx.svc.ReportWorkflowRun(context.Background(), "acme", &run)
			if got := invalidField(t, err); got != tc.field {
				t.Fatalf("field = %q, want %q", got, tc.field)
			}
		})
	}
}

func TestWorkflowService_ListRunsPagination(t *testing.T) {
	fx := newWorkflowFixture()
	w := fx.create(t, "nightly", pipeStep(pipeAcme))
	ctx := context.Background()
	for i := range 5 {
		id := fmt.Sprintf("55555555-5555-4555-8555-%012d", i)
		if err := fx.svc.ReportWorkflowRun(ctx, "acme", &workspace.WorkflowRun{ID: id, WorkflowID: w.ID, Status: "success"}); err != nil {
			t.Fatal(err)
		}
	}

	var seen []string
	token := ""
	for page := 0; ; page++ {
		runs, next, err := fx.svc.ListWorkflowRuns(ctx, "user-acme", w.ID, 2, token)
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		for _, r := range runs {
			seen = append(seen, r.ID)
		}
		if next == "" {
			break
		}
		token = next
		if page > 5 {
			t.Fatal("pagination does not terminate")
		}
	}
	if len(seen) != 5 || seen[0] != "55555555-5555-4555-8555-000000000004" || seen[4] != "55555555-5555-4555-8555-000000000000" {
		t.Fatalf("pages = %v, want all five newest first", seen)
	}

	if _, _, err := fx.svc.ListWorkflowRuns(ctx, "user-acme", w.ID, 2, "garbage!"); !errors.Is(err, workspace.ErrInvalidPageToken) {
		t.Fatalf("bad token: %v, want ErrInvalidPageToken", err)
	}
}

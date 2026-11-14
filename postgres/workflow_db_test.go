package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/fairtier/workspace-api/workspace"
)

// The workflow store's contract lives in SQL — the upsert that must not move
// a run between workflows, the cascade that removes a step with its target,
// the keyset pagination — so it is tested against a real database, like the
// rewrap sweep (TEST_DATABASE_URL, skipped when unset).

type workflowDBFixture struct {
	repo  *Repository
	slug  string
	pipe  workspace.PipelineID
	pipe2 workspace.PipelineID
	tr    workspace.TransformationID
}

func newWorkflowDBFixture(t *testing.T) workflowDBFixture {
	t.Helper()
	db := testDB(t)
	if err := Migrate(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	repo := &Repository{DB: db}
	ctx := context.Background()
	fx := workflowDBFixture{repo: repo, slug: "wf-" + uuid.NewString()[:8]}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM workflows WHERE customer_slug = $1`, fx.slug)
		_, _ = db.Exec(`DELETE FROM transformations WHERE customer_slug = $1`, fx.slug)
		_, _ = db.Exec(`DELETE FROM pipelines WHERE customer_slug = $1`, fx.slug)
	})

	now := time.Now()
	for _, name := range []string{"orders", "customers"} {
		p := &workspace.Pipeline{CustomerSlug: fx.slug, Name: name, SourceType: "rest_api", SourceConfig: []byte(`{}`), SourceCredentials: []byte(`{}`), DatasetName: "raw", Enabled: true, CreatedAt: now, UpdatedAt: now}
		if err := repo.CreatePipeline(ctx, p); err != nil {
			t.Fatalf("create pipeline: %v", err)
		}
		if fx.pipe == "" {
			fx.pipe = p.ID
		} else {
			fx.pipe2 = p.ID
		}
	}
	tr := &workspace.Transformation{CustomerSlug: fx.slug, Name: "marts", RepoRef: "main", Enabled: false, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateTransformation(ctx, tr); err != nil {
		t.Fatalf("create transformation: %v", err)
	}
	fx.tr = tr.ID
	return fx
}

func (fx workflowDBFixture) createWorkflow(t *testing.T, name string, steps ...workspace.WorkflowStep) *workspace.Workflow {
	t.Helper()
	for i := range steps {
		steps[i].Position = i
	}
	now := time.Now()
	w := &workspace.Workflow{CustomerSlug: fx.slug, Name: name, Schedule: "0 0 * * *", Enabled: true, Steps: steps, CreatedAt: now, UpdatedAt: now}
	if err := fx.repo.CreateWorkflow(context.Background(), w); err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	return w
}

func TestWorkflowStore_CRUD(t *testing.T) {
	fx := newWorkflowDBFixture(t)
	ctx := context.Background()
	w := fx.createWorkflow(t, "nightly",
		workspace.NewWorkflowStep(workspace.StepKindPipeline, string(fx.pipe)),
		workspace.NewWorkflowStep(workspace.StepKindTransformation, string(fx.tr)))

	got, err := fx.repo.GetWorkflow(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Steps) != 2 || got.Steps[0].Name != "orders" || !got.Steps[0].RefEnabled ||
		got.Steps[1].Name != "marts" || got.Steps[1].RefEnabled || got.Steps[1].RefID() != string(fx.tr) {
		t.Fatalf("steps = %+v", got.Steps)
	}

	dup := &workspace.Workflow{CustomerSlug: fx.slug, Name: "nightly", Steps: got.Steps[:1]}
	if err := fx.repo.CreateWorkflow(ctx, dup); !errors.Is(err, workspace.ErrWorkflowAlreadyExists) {
		t.Fatalf("duplicate name: %v", err)
	}

	got.Steps = []workspace.WorkflowStep{workspace.NewWorkflowStep(workspace.StepKindPipeline, string(fx.pipe2))}
	got.Enabled = false
	got.UpdatedAt = time.Now()
	if err := fx.repo.UpdateWorkflow(ctx, got); err != nil {
		t.Fatal(err)
	}
	again, _ := fx.repo.GetWorkflow(ctx, w.ID)
	if again.Enabled || len(again.Steps) != 1 || again.Steps[0].Name != "customers" {
		t.Fatalf("after update = %+v", again)
	}

	list, err := fx.repo.ListWorkflowsByCustomer(ctx, fx.slug)
	if err != nil || len(list) != 1 || len(list[0].Steps) != 1 {
		t.Fatalf("list = %+v, %v", list, err)
	}

	if err := fx.repo.DeleteWorkflow(ctx, w.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := fx.repo.GetWorkflow(ctx, w.ID); !errors.Is(err, workspace.ErrWorkflowNotFound) {
		t.Fatalf("after delete: %v", err)
	}
}

func TestWorkflowStore_DeletingATargetRemovesTheStep(t *testing.T) {
	fx := newWorkflowDBFixture(t)
	ctx := context.Background()
	w := fx.createWorkflow(t, "nightly",
		workspace.NewWorkflowStep(workspace.StepKindPipeline, string(fx.pipe)),
		workspace.NewWorkflowStep(workspace.StepKindPipeline, string(fx.pipe2)))

	run := &workspace.WorkflowRun{
		ID: uuid.NewString(), WorkflowID: w.ID, Status: "success",
		StepRuns: []workspace.WorkflowStepRun{{Position: 0, Kind: "pipeline", RefID: string(fx.pipe), Name: "orders", Status: "success"}},
	}
	if _, err := fx.repo.UpsertWorkflowRun(ctx, run); err != nil {
		t.Fatal(err)
	}

	if err := fx.repo.DeletePipeline(ctx, fx.pipe); err != nil {
		t.Fatalf("delete pipeline referenced by a workflow: %v", err)
	}
	got, _ := fx.repo.GetWorkflow(ctx, w.ID)
	if len(got.Steps) != 1 || got.Steps[0].PipelineID != fx.pipe2 || got.Steps[0].Position != 1 {
		t.Fatalf("steps after delete = %+v, want only the second step", got.Steps)
	}
	runs, _ := fx.repo.ListWorkflowRuns(ctx, w.ID, 10, nil)
	if len(runs) != 1 || len(runs[0].StepRuns) != 1 || runs[0].StepRuns[0].Name != "orders" {
		t.Fatalf("history after delete = %+v, want the snapshot kept", runs)
	}
}

func TestWorkflowStore_TriggerAndUpsert(t *testing.T) {
	fx := newWorkflowDBFixture(t)
	ctx := context.Background()
	w := fx.createWorkflow(t, "nightly", workspace.NewWorkflowStep(workspace.StepKindPipeline, string(fx.pipe)))

	pending := &workspace.WorkflowRun{WorkflowID: w.ID, Status: "pending", Trigger: "manual"}
	if err := fx.repo.CreateWorkflowRun(ctx, pending); err != nil {
		t.Fatal(err)
	}
	found, err := fx.repo.GetPendingWorkflowRun(ctx, w.ID)
	if err != nil || found == nil || found.ID != pending.ID {
		t.Fatalf("pending = %+v, %v", found, err)
	}

	// Disabled + pending is still served.
	w.Enabled = false
	w.UpdatedAt = time.Now()
	if err := fx.repo.UpdateWorkflow(ctx, w); err != nil {
		t.Fatal(err)
	}
	cfgs, err := fx.repo.GetEnabledWorkflows(ctx, fx.slug)
	if err != nil || len(cfgs) != 1 || !cfgs[0].TriggerNow || cfgs[0].PendingRunID != pending.ID || cfgs[0].LastRunAt != nil {
		t.Fatalf("configs = %+v, %v", cfgs, err)
	}

	started := time.Now().UTC().Truncate(time.Second)
	report := &workspace.WorkflowRun{
		ID: pending.ID, WorkflowID: w.ID, Status: "running", StartedAt: &started,
		StepRuns: []workspace.WorkflowStepRun{{Position: 0, Kind: "pipeline", RefID: string(fx.pipe), Name: "orders", Status: "running", PipelineRunID: uuid.NewString()}},
	}
	prev, err := fx.repo.UpsertWorkflowRun(ctx, report)
	if err != nil || prev != "pending" {
		t.Fatalf("upsert running: prev=%q err=%v", prev, err)
	}

	report.Status = "failed"
	report.StartedAt = nil // a later report without it keeps the stored value
	report.StepRuns[0].Status = "failed"
	report.StepRuns[0].PipelineRunID = ""
	for range 2 {
		if _, err := fx.repo.UpsertWorkflowRun(ctx, report); err != nil {
			t.Fatal(err)
		}
	}
	runs, err := fx.repo.ListWorkflowRuns(ctx, w.ID, 10, nil)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs = %+v, %v", runs, err)
	}
	r := runs[0]
	if r.Status != "failed" || r.Trigger != "manual" || r.StartedAt == nil || !r.StartedAt.Equal(started) ||
		len(r.StepRuns) != 1 || r.StepRuns[0].Status != "failed" || r.StepRuns[0].PipelineRunID == "" {
		t.Fatalf("stored run = %+v / %+v", r, r.StepRuns)
	}

	cfgs, _ = fx.repo.GetEnabledWorkflows(ctx, fx.slug)
	if len(cfgs) != 0 {
		t.Fatalf("disabled workflow served after its trigger was consumed: %+v", cfgs)
	}

	other := fx.createWorkflow(t, "other", workspace.NewWorkflowStep(workspace.StepKindPipeline, string(fx.pipe2)))
	_, err = fx.repo.UpsertWorkflowRun(ctx, &workspace.WorkflowRun{ID: pending.ID, WorkflowID: other.ID, Status: "running"})
	if !errors.Is(err, workspace.ErrWorkflowRunNotFound) {
		t.Fatalf("moving a run to another workflow: %v, want ErrWorkflowRunNotFound", err)
	}
}

func TestWorkflowStore_RunPagination(t *testing.T) {
	fx := newWorkflowDBFixture(t)
	ctx := context.Background()
	w := fx.createWorkflow(t, "nightly", workspace.NewWorkflowStep(workspace.StepKindPipeline, string(fx.pipe)))

	base := time.Now().Add(-time.Hour)
	var want []string
	for i := range 5 {
		run := &workspace.WorkflowRun{WorkflowID: w.ID, Status: "success", CreatedAt: base.Add(time.Duration(i) * time.Minute)}
		if err := fx.repo.CreateWorkflowRun(ctx, run); err != nil {
			t.Fatal(err)
		}
		want = append([]string{run.ID}, want...)
	}

	var got []string
	var after *workspace.WorkflowRunCursor
	for {
		page, err := fx.repo.ListWorkflowRuns(ctx, w.ID, 2, after)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, r := range page {
			got = append(got, r.ID)
		}
		last := page[len(page)-1]
		after = &workspace.WorkflowRunCursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	if len(got) != len(want) {
		t.Fatalf("paged %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("paged %v, want %v", got, want)
		}
	}

	last, err := fx.repo.ListWorkflowsByCustomer(ctx, fx.slug)
	if err != nil || len(last) != 1 || last[0].LastRunStatus != "success" || last[0].LastRunAt == nil {
		t.Fatalf("list last run = %+v, %v", last, err)
	}
}

func TestWorkflowStore_FailureSummarySetEnabledAndStuckRuns(t *testing.T) {
	fx := newWorkflowDBFixture(t)
	ctx := context.Background()
	w := fx.createWorkflow(t, "nightly",
		workspace.NewWorkflowStep(workspace.StepKindPipeline, string(fx.pipe)),
		workspace.NewWorkflowStep(workspace.StepKindTransformation, string(fx.tr)))

	old := time.Now().Add(-9 * time.Hour)
	failedID := uuid.NewString()
	if _, err := fx.repo.UpsertWorkflowRun(ctx, &workspace.WorkflowRun{
		ID: failedID, WorkflowID: w.ID, Status: workspace.RunStatusFailed, StartedAt: &old, ErrorMessage: "run error",
		StepRuns: []workspace.WorkflowStepRun{
			{Position: 0, Kind: workspace.StepKindPipeline, RefID: string(fx.pipe), Name: "orders", Status: workspace.RunStatusSuccess},
			{Position: 1, Kind: workspace.StepKindTransformation, RefID: string(fx.tr), Name: "marts", Status: workspace.RunStatusFailed, ErrorMessage: "dbt failed\nmore"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	list, err := fx.repo.ListWorkflowsByCustomer(ctx, fx.slug)
	if err != nil || len(list) != 1 {
		t.Fatalf("list = %+v, %v", list, err)
	}
	got := list[0]
	if got.LastRunError != "dbt failed\nmore" || got.LastFailedStepPosition == nil || *got.LastFailedStepPosition != 1 || got.LastFailedStepName != "marts" {
		t.Fatalf("summary = %q %v %q", got.LastRunError, got.LastFailedStepPosition, got.LastFailedStepName)
	}
	// The worker query carries no summary but still scans.
	if _, err := fx.repo.GetEnabledWorkflows(ctx, fx.slug); err != nil {
		t.Fatal(err)
	}

	if err := fx.repo.SetWorkflowEnabled(ctx, w.ID, false, time.Now()); err != nil {
		t.Fatal(err)
	}
	if after, _ := fx.repo.GetWorkflow(ctx, w.ID); after.Enabled || len(after.Steps) != 2 {
		t.Fatalf("after SetWorkflowEnabled: %+v", after)
	}
	if err := fx.repo.SetWorkflowEnabled(ctx, workspace.WorkflowID(uuid.NewString()), true, time.Now()); !errors.Is(err, workspace.ErrWorkflowNotFound) {
		t.Fatalf("missing workflow: %v", err)
	}

	stuckID := uuid.NewString()
	fresh := time.Now()
	for id, started := range map[string]*time.Time{stuckID: &old, uuid.NewString(): &fresh} {
		if _, err := fx.repo.UpsertWorkflowRun(ctx, &workspace.WorkflowRun{
			ID: id, WorkflowID: w.ID, Status: workspace.RunStatusRunning, StartedAt: started,
			StepRuns: []workspace.WorkflowStepRun{{Position: 0, Kind: workspace.StepKindPipeline, RefID: string(fx.pipe), Name: "orders", Status: workspace.RunStatusRunning}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	stuck, err := fx.repo.ListStuckWorkflowRuns(ctx, fx.slug, 8*time.Hour)
	if err != nil || len(stuck) != 1 || stuck[0].ID != stuckID || len(stuck[0].StepRuns) != 1 {
		t.Fatalf("stuck = %+v, %v", stuck, err)
	}
	if other, _ := fx.repo.ListStuckWorkflowRuns(ctx, "someone-else", time.Minute); len(other) != 0 {
		t.Fatalf("another workspace sees %d stuck runs", len(other))
	}
}

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/fairtier/workspace-api/workspace"
)

func TestWorkflowDefinitionRenders(t *testing.T) {
	fx := newWorkflowDBFixture(t)
	ctx := context.Background()
	w := fx.createWorkflow(t, "nightly", workspace.NewWorkflowStep(workspace.StepKindPipeline, string(fx.pipe)))

	if err := fx.repo.UpsertWorkflowDefinitionRender(ctx, &workspace.WorkflowDefinitionRender{WorkflowID: w.ID, Path: "workflows/nightly.yaml", BlobSHA: "a1"}); err != nil {
		t.Fatal(err)
	}
	if err := fx.repo.MarkWorkflowDefinitionRefused(ctx, w.ID, "b2"); err != nil {
		t.Fatal(err)
	}
	rows, err := fx.repo.GetWorkflowDefinitionRenders(ctx, fx.slug)
	if err != nil {
		t.Fatal(err)
	}
	if got := rows[w.ID]; got.Path != "workflows/nightly.yaml" || got.BlobSHA != "a1" || got.RefusedBlobSHA != "b2" {
		t.Errorf("row = %+v", got)
	}
	// A new render clears the refusal.
	if err := fx.repo.UpsertWorkflowDefinitionRender(ctx, &workspace.WorkflowDefinitionRender{WorkflowID: w.ID, Path: "workflows/nightly.yaml", BlobSHA: "c3"}); err != nil {
		t.Fatal(err)
	}
	rows, _ = fx.repo.GetWorkflowDefinitionRenders(ctx, fx.slug)
	if got := rows[w.ID]; got.BlobSHA != "c3" || got.RefusedBlobSHA != "" {
		t.Errorf("row after re-render = %+v", got)
	}
	other, err := fx.repo.GetWorkflowDefinitionRenders(ctx, "someone-else")
	if err != nil || len(other) != 0 {
		t.Errorf("other tenant sees %v (%v)", other, err)
	}

	// Rows die with their workflow.
	if err := fx.repo.DeleteWorkflow(ctx, w.ID); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := fx.repo.DB.QueryRow(`SELECT count(*) FROM workflow_definition_renders WHERE workflow_id = $1`, w.ID).Scan(&n); err != nil || n != 0 {
		t.Errorf("render rows left: %d (%v)", n, err)
	}
}

func TestImportWorkflow(t *testing.T) {
	fx := newWorkflowDBFixture(t)
	ctx := context.Background()
	now := time.Now()
	id := workspace.WorkflowID(uuid.NewString())
	w := &workspace.Workflow{
		ID: id, CustomerSlug: fx.slug, Name: "from-file", Schedule: "0 3 * * *", Enabled: false,
		Steps: []workspace.WorkflowStep{
			{Position: 0, Kind: workspace.StepKindTransformation, TransformationID: fx.tr},
			{Position: 1, Kind: workspace.StepKindPipeline, PipelineID: fx.pipe2},
		},
		CreatedAt: now, UpdatedAt: now,
	}
	if err := fx.repo.ImportWorkflow(ctx, w); err != nil {
		t.Fatal(err)
	}
	got, err := fx.repo.GetWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "from-file" || got.Enabled || len(got.Steps) != 2 || got.Steps[0].TransformationID != fx.tr {
		t.Errorf("imported = %+v", got)
	}
	dup := *w
	dup.ID = workspace.WorkflowID(uuid.NewString())
	if err := fx.repo.ImportWorkflow(ctx, &dup); !errors.Is(err, workspace.ErrWorkflowAlreadyExists) {
		t.Errorf("duplicate name: err = %v", err)
	}
}

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/fairtier/workspace-api/workspace"
)

func TestTransformationStore_GetPendingTransformationRun(t *testing.T) {
	fx := newWorkflowDBFixture(t)
	ctx := context.Background()

	if run, err := fx.repo.GetPendingTransformationRun(ctx, fx.tr); err != nil || run != nil {
		t.Fatalf("no runs: run %+v, err %v", run, err)
	}
	now := time.Now()
	for i, status := range []string{"success", "pending", "pending"} {
		run := &workspace.TransformationRun{TransformationID: fx.tr, Status: status, CreatedAt: now.Add(time.Duration(i) * time.Second)}
		if err := fx.repo.CreateTransformationRun(ctx, run); err != nil {
			t.Fatalf("create run: %v", err)
		}
	}
	run, err := fx.repo.GetPendingTransformationRun(ctx, fx.tr)
	if err != nil || run == nil || run.Status != "pending" || !run.CreatedAt.Equal(now.Add(time.Second).Truncate(time.Microsecond)) {
		t.Fatalf("oldest pending: run %+v, err %v", run, err)
	}
}

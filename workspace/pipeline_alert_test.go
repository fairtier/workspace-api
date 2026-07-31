package workspace_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/fairtier/workspace-api/workspace"
)

// mockAlerter records AlertPipelineFailure calls (workspace.PipelineAlerter).
type mockAlerter struct {
	calls []string
}

func (m *mockAlerter) AlertPipelineFailure(_ context.Context, _, pipelineName, _ string) error {
	m.calls = append(m.calls, pipelineName)
	return nil
}

// notifyRun (via ReportPipelineRun) must only reach the alerter on failures.
// The email alerter implementation itself is control-plane
// (domain.EmailAlertService) and covered by its own tests.
func TestPipelineService_ReportPipelineRun_EmailAlerts(t *testing.T) {
	pipe := &workspace.Pipeline{ID: "pipe-1", CustomerSlug: "acme", Name: "nightly"}

	newPipelines := func() *mockPipelineRepo {
		return &mockPipelineRepo{
			getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
				return pipe, nil
			},
			createPipelineRunFn: func(context.Context, *workspace.PipelineRun) error { return nil },
		}
	}

	t.Run("failed run triggers an alert", func(t *testing.T) {
		alerter := &mockAlerter{}
		svc := &workspace.PipelineService{
			Pipelines: newPipelines(),
			Alerts:    alerter,
			Logger:    slog.Default(),
		}

		run := &workspace.PipelineRun{PipelineID: "pipe-1", Status: "failed", ErrorMessage: "boom"}
		if err := svc.ReportPipelineRun(context.Background(), "", run); err != nil {
			t.Fatalf("ReportPipelineRun() error = %v", err)
		}
		if len(alerter.calls) != 1 {
			t.Fatalf("alert calls = %d, want 1", len(alerter.calls))
		}
	})

	t.Run("success run never reaches the alerter", func(t *testing.T) {
		alerter := &mockAlerter{}
		svc := &workspace.PipelineService{
			Pipelines: newPipelines(),
			Alerts:    alerter,
			Logger:    slog.Default(),
		}

		run := &workspace.PipelineRun{PipelineID: "pipe-1", Status: "success", RowsLoaded: 5}
		if err := svc.ReportPipelineRun(context.Background(), "", run); err != nil {
			t.Fatalf("ReportPipelineRun() error = %v", err)
		}
		if len(alerter.calls) != 0 {
			t.Errorf("alert calls = %d, want 0 on success", len(alerter.calls))
		}
	})
}

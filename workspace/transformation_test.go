package workspace_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

// --- Mock implementations ---

type mockTransformationRepo struct {
	createTransformationFn          func(ctx context.Context, t *workspace.Transformation) error
	getTransformationFn             func(ctx context.Context, id workspace.TransformationID) (*workspace.Transformation, error)
	listTransformationsByCustomerFn func(ctx context.Context, customerSlug string) ([]workspace.Transformation, error)
	updateTransformationFn          func(ctx context.Context, t *workspace.Transformation) error
	deleteTransformationFn          func(ctx context.Context, id workspace.TransformationID) error
	getEnabledTransformationsFn     func(ctx context.Context, customerSlug string) ([]workspace.Transformation, error)
	createTransformationRunFn       func(ctx context.Context, run *workspace.TransformationRun) error
	updateTransformationRunFn       func(ctx context.Context, run *workspace.TransformationRun) error
	listRecentTransformationRunsFn  func(ctx context.Context, id workspace.TransformationID, limit int) ([]workspace.TransformationRun, error)
}

func (m *mockTransformationRepo) CreateTransformation(ctx context.Context, t *workspace.Transformation) error {
	return m.createTransformationFn(ctx, t)
}

func (m *mockTransformationRepo) GetTransformation(ctx context.Context, id workspace.TransformationID) (*workspace.Transformation, error) {
	return m.getTransformationFn(ctx, id)
}

func (m *mockTransformationRepo) ListTransformationsByCustomer(ctx context.Context, customerSlug string) ([]workspace.Transformation, error) {
	return m.listTransformationsByCustomerFn(ctx, customerSlug)
}

func (m *mockTransformationRepo) UpdateTransformation(ctx context.Context, t *workspace.Transformation) error {
	return m.updateTransformationFn(ctx, t)
}

func (m *mockTransformationRepo) DeleteTransformation(ctx context.Context, id workspace.TransformationID) error {
	return m.deleteTransformationFn(ctx, id)
}

func (m *mockTransformationRepo) GetEnabledTransformations(ctx context.Context, customerSlug string) ([]workspace.Transformation, error) {
	return m.getEnabledTransformationsFn(ctx, customerSlug)
}

func (m *mockTransformationRepo) CreateTransformationRun(ctx context.Context, run *workspace.TransformationRun) error {
	return m.createTransformationRunFn(ctx, run)
}

func (m *mockTransformationRepo) UpdateTransformationRun(ctx context.Context, run *workspace.TransformationRun) error {
	return m.updateTransformationRunFn(ctx, run)
}

func (m *mockTransformationRepo) ListRecentTransformationRuns(ctx context.Context, id workspace.TransformationID, limit int) ([]workspace.TransformationRun, error) {
	return m.listRecentTransformationRunsFn(ctx, id, limit)
}

type mockPipelineReader struct {
	getPipelineFn func(ctx context.Context, id workspace.PipelineID) (*workspace.Pipeline, error)
}

func (m *mockPipelineReader) GetPipeline(ctx context.Context, id workspace.PipelineID) (*workspace.Pipeline, error) {
	return m.getPipelineFn(ctx, id)
}

func acmeCustomerReader() *mockCustomerReader {
	return &mockCustomerReader{
		getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
			return &workspace.Workspace{Slug: "acme"}, nil
		},
	}
}

// --- Tests ---

func TestTransformationService_CreateTransformation(t *testing.T) {
	t.Run("defaults main ref and enabled", func(t *testing.T) {
		var created *workspace.Transformation

		svc := &workspace.TransformationService{
			Workspaces: acmeCustomerReader(),
			Transformations: &mockTransformationRepo{
				createTransformationFn: func(_ context.Context, tr *workspace.Transformation) error {
					created = tr
					return nil
				},
			},
			Logger: slog.Default(),
		}

		result, err := svc.CreateTransformation(context.Background(), "user-1", &workspace.Transformation{Name: "nightly"})
		if err != nil {
			t.Fatalf("CreateTransformation() error = %v", err)
		}
		if result.CustomerSlug != "acme" {
			t.Errorf("CustomerSlug = %q, want %q", result.CustomerSlug, "acme")
		}
		if created.RepoRef != "main" {
			t.Errorf("RepoRef = %q, want %q", created.RepoRef, "main")
		}
		if !created.Enabled {
			t.Error("Enabled should be true by default")
		}
		if created.CreatedAt.IsZero() {
			t.Error("CreatedAt should be set")
		}
	})

	t.Run("rejects cross-tenant trigger pipeline", func(t *testing.T) {
		svc := &workspace.TransformationService{
			Workspaces:      acmeCustomerReader(),
			Transformations: &mockTransformationRepo{},
			Pipelines: &mockPipelineReader{
				getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
					return &workspace.Pipeline{ID: "pipe-1", CustomerSlug: "evil"}, nil
				},
			},
			Logger: slog.Default(),
		}

		_, err := svc.CreateTransformation(context.Background(), "user-1", &workspace.Transformation{
			Name:                   "nightly",
			TriggerAfterPipelineID: "pipe-1",
		})
		if !errors.Is(err, workspace.ErrPipelineNotFound) {
			t.Fatalf("CreateTransformation() error = %v, want ErrPipelineNotFound", err)
		}
	})

	t.Run("accepts same-tenant trigger pipeline", func(t *testing.T) {
		svc := &workspace.TransformationService{
			Workspaces: acmeCustomerReader(),
			Transformations: &mockTransformationRepo{
				createTransformationFn: func(context.Context, *workspace.Transformation) error { return nil },
			},
			Pipelines: &mockPipelineReader{
				getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
					return &workspace.Pipeline{ID: "pipe-1", CustomerSlug: "acme"}, nil
				},
			},
			Logger: slog.Default(),
		}

		_, err := svc.CreateTransformation(context.Background(), "user-1", &workspace.Transformation{
			Name:                   "nightly",
			TriggerAfterPipelineID: "pipe-1",
		})
		if err != nil {
			t.Fatalf("CreateTransformation() error = %v", err)
		}
	})
}

func TestTransformationService_UpdateTransformation(t *testing.T) {
	existingCreds := json.RawMessage(`{"token":"secret"}`)

	existing := &workspace.Transformation{
		ID:             "tf-1",
		CustomerSlug:   "acme",
		GitCredentials: existingCreds,
	}

	t.Run("preserves creds when empty", func(t *testing.T) {
		var updated *workspace.Transformation

		svc := &workspace.TransformationService{
			Workspaces: acmeCustomerReader(),
			Transformations: &mockTransformationRepo{
				getTransformationFn: func(context.Context, workspace.TransformationID) (*workspace.Transformation, error) {
					return existing, nil
				},
				updateTransformationFn: func(_ context.Context, tr *workspace.Transformation) error {
					updated = tr
					return nil
				},
			},
			Logger: slog.Default(),
		}

		tr := &workspace.Transformation{ID: "tf-1", Name: "nightly", GitCredentials: json.RawMessage("{}")}
		_, err := svc.UpdateTransformation(context.Background(), "user-1", tr)
		if err != nil {
			t.Fatalf("UpdateTransformation() error = %v", err)
		}
		if string(updated.GitCredentials) != string(existingCreds) {
			t.Errorf("GitCredentials = %s, want %s", updated.GitCredentials, existingCreds)
		}
	})

	t.Run("cross-tenant update is not found", func(t *testing.T) {
		svc := &workspace.TransformationService{
			Workspaces: acmeCustomerReader(),
			Transformations: &mockTransformationRepo{
				getTransformationFn: func(context.Context, workspace.TransformationID) (*workspace.Transformation, error) {
					return &workspace.Transformation{ID: "tf-1", CustomerSlug: "evil"}, nil
				},
			},
			Logger: slog.Default(),
		}

		_, err := svc.UpdateTransformation(context.Background(), "user-1", &workspace.Transformation{ID: "tf-1"})
		if !errors.Is(err, workspace.ErrTransformationNotFound) {
			t.Fatalf("UpdateTransformation() error = %v, want ErrTransformationNotFound", err)
		}
	})
}

func TestTransformationService_ReportTransformationRun(t *testing.T) {
	acmeTransformation := &workspace.Transformation{
		ID:           "tf-1",
		CustomerSlug: "acme",
		Name:         "nightly",
	}

	t.Run("caller tenant must own the transformation", func(t *testing.T) {
		svc := &workspace.TransformationService{
			Transformations: &mockTransformationRepo{
				getTransformationFn: func(context.Context, workspace.TransformationID) (*workspace.Transformation, error) {
					return acmeTransformation, nil
				},
				createTransformationRunFn: func(context.Context, *workspace.TransformationRun) error { return nil },
			},
			Logger: slog.Default(),
		}

		run := &workspace.TransformationRun{TransformationID: "tf-1", Status: "success"}
		if err := svc.ReportTransformationRun(context.Background(), "evil", run); !errors.Is(err, workspace.ErrTransformationNotFound) {
			t.Fatalf("ReportTransformationRun() error = %v, want ErrTransformationNotFound", err)
		}
		if err := svc.ReportTransformationRun(context.Background(), "acme", run); err != nil {
			t.Fatalf("ReportTransformationRun() error = %v, want nil for the owning tenant", err)
		}
	})

	t.Run("updates existing run when ID set", func(t *testing.T) {
		var updated *workspace.TransformationRun

		svc := &workspace.TransformationService{
			Transformations: &mockTransformationRepo{
				getTransformationFn: func(context.Context, workspace.TransformationID) (*workspace.Transformation, error) {
					return acmeTransformation, nil
				},
				updateTransformationRunFn: func(_ context.Context, run *workspace.TransformationRun) error {
					updated = run
					return nil
				},
			},
			Logger: slog.Default(),
		}

		run := &workspace.TransformationRun{ID: "run-1", TransformationID: "tf-1", Status: "failed"}
		if err := svc.ReportTransformationRun(context.Background(), "acme", run); err != nil {
			t.Fatalf("ReportTransformationRun() error = %v", err)
		}
		if updated == nil || updated.ID != "run-1" {
			t.Fatalf("expected UpdateTransformationRun with run-1, got %+v", updated)
		}
	})

	t.Run("notifies on completion", func(t *testing.T) {
		var notified *workspace.Notification

		svc := &workspace.TransformationService{
			Transformations: &mockTransformationRepo{
				getTransformationFn: func(context.Context, workspace.TransformationID) (*workspace.Transformation, error) {
					return acmeTransformation, nil
				},
				createTransformationRunFn: func(context.Context, *workspace.TransformationRun) error { return nil },
			},
			Notifications: notifierFunc(func(_ context.Context, n workspace.Notification) error {
				notified = &n
				return nil
			}),
			Logger: slog.Default(),
		}

		run := &workspace.TransformationRun{TransformationID: "tf-1", Status: "failed", ErrorMessage: "boom"}
		if err := svc.ReportTransformationRun(context.Background(), "acme", run); err != nil {
			t.Fatalf("ReportTransformationRun() error = %v", err)
		}
		if notified == nil {
			t.Fatal("expected a notification")
		}
		if notified.Type != "transformation_run" {
			t.Errorf("Type = %q, want %q", notified.Type, "transformation_run")
		}
		if notified.CustomerSlug != "acme" {
			t.Errorf("CustomerSlug = %q, want %q", notified.CustomerSlug, "acme")
		}
		if notified.Body != "boom" {
			t.Errorf("Body = %q, want %q", notified.Body, "boom")
		}
	})
}

// notifierFunc adapts a function to the Notifier interface.
type notifierFunc func(ctx context.Context, n workspace.Notification) error

func (f notifierFunc) Notify(ctx context.Context, n workspace.Notification) error { return f(ctx, n) }

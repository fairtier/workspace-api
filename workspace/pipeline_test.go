package workspace_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

// --- Mock implementations ---

type mockPipelineRepo struct {
	createPipelineFn          func(ctx context.Context, p *workspace.Pipeline) error
	getPipelineFn             func(ctx context.Context, id workspace.PipelineID) (*workspace.Pipeline, error)
	listPipelinesByCustomerFn func(ctx context.Context, customerSlug string) ([]workspace.Pipeline, error)
	updatePipelineFn          func(ctx context.Context, p *workspace.Pipeline) error
	deletePipelineFn          func(ctx context.Context, id workspace.PipelineID) error
	getEnabledPipelinesFn     func(ctx context.Context, customerSlug string) ([]workspace.Pipeline, error)
	createPipelineRunFn       func(ctx context.Context, run *workspace.PipelineRun) error
	updatePipelineRunFn       func(ctx context.Context, run *workspace.PipelineRun) error
	listRecentRunsFn          func(ctx context.Context, pipelineID workspace.PipelineID, limit int) ([]workspace.PipelineRun, error)
	getPendingRunFn           func(ctx context.Context, pipelineID workspace.PipelineID) (*workspace.PipelineRun, error)
}

func (m *mockPipelineRepo) CreatePipeline(ctx context.Context, p *workspace.Pipeline) error {
	return m.createPipelineFn(ctx, p)
}

func (m *mockPipelineRepo) GetPipeline(ctx context.Context, id workspace.PipelineID) (*workspace.Pipeline, error) {
	return m.getPipelineFn(ctx, id)
}

func (m *mockPipelineRepo) ListPipelinesByCustomer(ctx context.Context, customerSlug string) ([]workspace.Pipeline, error) {
	return m.listPipelinesByCustomerFn(ctx, customerSlug)
}

func (m *mockPipelineRepo) UpdatePipeline(ctx context.Context, p *workspace.Pipeline) error {
	return m.updatePipelineFn(ctx, p)
}

func (m *mockPipelineRepo) DeletePipeline(ctx context.Context, id workspace.PipelineID) error {
	return m.deletePipelineFn(ctx, id)
}

func (m *mockPipelineRepo) GetEnabledPipelines(ctx context.Context, customerSlug string) ([]workspace.Pipeline, error) {
	return m.getEnabledPipelinesFn(ctx, customerSlug)
}

func (m *mockPipelineRepo) CreatePipelineRun(ctx context.Context, run *workspace.PipelineRun) error {
	return m.createPipelineRunFn(ctx, run)
}

func (m *mockPipelineRepo) UpdatePipelineRun(ctx context.Context, run *workspace.PipelineRun) error {
	return m.updatePipelineRunFn(ctx, run)
}

func (m *mockPipelineRepo) GetPendingRun(ctx context.Context, pipelineID workspace.PipelineID) (*workspace.PipelineRun, error) {
	if m.getPendingRunFn == nil {
		return nil, nil
	}
	return m.getPendingRunFn(ctx, pipelineID)
}

func (m *mockPipelineRepo) ListRecentRuns(ctx context.Context, pipelineID workspace.PipelineID, limit int) ([]workspace.PipelineRun, error) {
	return m.listRecentRunsFn(ctx, pipelineID, limit)
}

type mockCustomerReader struct {
	getByUserIDFn func(ctx context.Context, userID core.UserID) (*workspace.Workspace, error)
	getBySlugFn   func(ctx context.Context, slug string) (*workspace.Workspace, error)
}

func (m *mockCustomerReader) GetWorkspace(ctx context.Context, slug string) (*workspace.Workspace, error) {
	if m.getBySlugFn == nil {
		return nil, errors.New("not implemented")
	}
	return m.getBySlugFn(ctx, slug)
}

func (m *mockCustomerReader) GetWorkspaceByUser(ctx context.Context, userID core.UserID) (*workspace.Workspace, error) {
	return m.getByUserIDFn(ctx, userID)
}

// --- Tests ---

func TestPipelineService_CreatePipeline(t *testing.T) {
	t.Run("defaults append and enabled", func(t *testing.T) {
		var created *workspace.Pipeline

		svc := &workspace.PipelineService{
			Workspaces: &mockCustomerReader{
				getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
					return &workspace.Workspace{Slug: "acme"}, nil
				},
			},
			Pipelines: &mockPipelineRepo{
				createPipelineFn: func(_ context.Context, p *workspace.Pipeline) error {
					created = p
					return nil
				},
			},
			Logger: slog.Default(),
		}

		p := &workspace.Pipeline{
			Name:              "test",
			SourceType:        "sql_database",
			SourceCredentials: json.RawMessage(`{"connection_string":"postgres://u:p@localhost/db"}`),
		}
		result, err := svc.CreatePipeline(context.Background(), "user-1", p)
		if err != nil {
			t.Fatalf("CreatePipeline() error = %v", err)
		}
		if result.CustomerSlug != "acme" {
			t.Errorf("CustomerSlug = %q, want %q", result.CustomerSlug, "acme")
		}
		if created.WriteDisposition != "append" {
			t.Errorf("WriteDisposition = %q, want %q", created.WriteDisposition, "append")
		}
		if !created.Enabled {
			t.Error("Enabled should be true by default")
		}
		if created.CreatedAt.IsZero() {
			t.Error("CreatedAt should be set")
		}
	})

	t.Run("preserves explicit write_disposition", func(t *testing.T) {
		var created *workspace.Pipeline

		svc := &workspace.PipelineService{
			Workspaces: &mockCustomerReader{
				getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
					return &workspace.Workspace{Slug: "acme"}, nil
				},
			},
			Pipelines: &mockPipelineRepo{
				createPipelineFn: func(_ context.Context, p *workspace.Pipeline) error {
					created = p
					return nil
				},
			},
			Logger: slog.Default(),
		}

		p := &workspace.Pipeline{
			Name:              "test",
			SourceType:        "sql_database",
			SourceCredentials: json.RawMessage(`{"connection_string":"postgres://u:p@localhost/db"}`),
			WriteDisposition:  "replace",
		}
		_, err := svc.CreatePipeline(context.Background(), "user-1", p)
		if err != nil {
			t.Fatalf("CreatePipeline() error = %v", err)
		}
		if created.WriteDisposition != "replace" {
			t.Errorf("WriteDisposition = %q, want %q", created.WriteDisposition, "replace")
		}
	})

	t.Run("customer not found", func(t *testing.T) {
		errNotFound := errors.New("customer not found")
		svc := &workspace.PipelineService{
			Workspaces: &mockCustomerReader{
				getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
					return nil, errNotFound
				},
			},
			Pipelines: &mockPipelineRepo{},
			Logger:    slog.Default(),
		}

		_, err := svc.CreatePipeline(context.Background(), "user-1", &workspace.Pipeline{})
		if !errors.Is(err, errNotFound) {
			t.Fatalf("CreatePipeline() error = %v, want %v", err, errNotFound)
		}
	})
}

func TestPipelineService_UpdatePipeline(t *testing.T) {
	existingCreds := json.RawMessage(`{"password":"secret"}`)

	existing := &workspace.Pipeline{
		ID:                "pipe-1",
		CustomerSlug:      "acme",
		SourceCredentials: existingCreds,
	}

	newCustomerReader := func() *mockCustomerReader {
		return &mockCustomerReader{
			getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
				return &workspace.Workspace{Slug: "acme"}, nil
			},
		}
	}

	t.Run("preserves creds when empty", func(t *testing.T) {
		var updated *workspace.Pipeline

		svc := &workspace.PipelineService{
			Workspaces: newCustomerReader(),
			Pipelines: &mockPipelineRepo{
				getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
					return existing, nil
				},
				updatePipelineFn: func(_ context.Context, p *workspace.Pipeline) error {
					updated = p
					return nil
				},
			},
			Logger: slog.Default(),
		}

		p := &workspace.Pipeline{ID: "pipe-1", SourceType: "sql_database", SourceCredentials: nil}
		_, err := svc.UpdatePipeline(context.Background(), "user-1", p)
		if err != nil {
			t.Fatalf("UpdatePipeline() error = %v", err)
		}
		if string(updated.SourceCredentials) != string(existingCreds) {
			t.Errorf("SourceCredentials = %s, want %s", updated.SourceCredentials, existingCreds)
		}
	})

	t.Run("preserves creds when empty object", func(t *testing.T) {
		var updated *workspace.Pipeline

		svc := &workspace.PipelineService{
			Workspaces: newCustomerReader(),
			Pipelines: &mockPipelineRepo{
				getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
					return existing, nil
				},
				updatePipelineFn: func(_ context.Context, p *workspace.Pipeline) error {
					updated = p
					return nil
				},
			},
			Logger: slog.Default(),
		}

		p := &workspace.Pipeline{ID: "pipe-1", SourceType: "sql_database", SourceCredentials: json.RawMessage(`{}`)}
		_, err := svc.UpdatePipeline(context.Background(), "user-1", p)
		if err != nil {
			t.Fatalf("UpdatePipeline() error = %v", err)
		}
		if string(updated.SourceCredentials) != string(existingCreds) {
			t.Errorf("SourceCredentials = %s, want %s", updated.SourceCredentials, existingCreds)
		}
	})

	t.Run("preserves creds when null", func(t *testing.T) {
		var updated *workspace.Pipeline

		svc := &workspace.PipelineService{
			Workspaces: newCustomerReader(),
			Pipelines: &mockPipelineRepo{
				getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
					return existing, nil
				},
				updatePipelineFn: func(_ context.Context, p *workspace.Pipeline) error {
					updated = p
					return nil
				},
			},
			Logger: slog.Default(),
		}

		p := &workspace.Pipeline{ID: "pipe-1", SourceType: "sql_database", SourceCredentials: json.RawMessage(`null`)}
		_, err := svc.UpdatePipeline(context.Background(), "user-1", p)
		if err != nil {
			t.Fatalf("UpdatePipeline() error = %v", err)
		}
		if string(updated.SourceCredentials) != string(existingCreds) {
			t.Errorf("SourceCredentials = %s, want %s", updated.SourceCredentials, existingCreds)
		}
	})

	// Detach: the one way to say "drop them", as opposed to the empty-means-
	// keep default. Without it a pipeline can never let go of a workspace
	// Connection, so the connection's in-use guard can never be satisfied.
	t.Run("clears creds when asked", func(t *testing.T) {
		var updated *workspace.Pipeline

		svc := &workspace.PipelineService{
			Workspaces: newCustomerReader(),
			Pipelines: &mockPipelineRepo{
				getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
					return existing, nil
				},
				updatePipelineFn: func(_ context.Context, p *workspace.Pipeline) error {
					updated = p
					return nil
				},
			},
			Logger: slog.Default(),
		}

		p := &workspace.Pipeline{ID: "pipe-1", SourceType: "sql_database"}
		if _, err := svc.UpdatePipeline(context.Background(), "user-1", p, workspace.ClearCredentials()); err != nil {
			t.Fatalf("UpdatePipeline() error = %v", err)
		}
		if len(updated.SourceCredentials) != 0 {
			t.Errorf("SourceCredentials = %s, want empty", updated.SourceCredentials)
		}
	})

	t.Run("rejects clear together with new creds", func(t *testing.T) {
		svc := &workspace.PipelineService{
			Workspaces: newCustomerReader(),
			Pipelines:  &mockPipelineRepo{},
			Logger:     slog.Default(),
		}

		p := &workspace.Pipeline{
			ID: "pipe-1", SourceType: "sql_database",
			SourceCredentials: json.RawMessage(`{"connection_string":"postgres://u:p@localhost/db"}`),
		}
		if _, err := svc.UpdatePipeline(context.Background(), "user-1", p, workspace.ClearCredentials()); err == nil {
			t.Fatal("expected a contradictory save to be rejected")
		}
	})

	t.Run("replaces creds when provided", func(t *testing.T) {
		var updated *workspace.Pipeline
		newCreds := json.RawMessage(`{"connection_string":"postgres://u:p@localhost/newdb"}`)

		svc := &workspace.PipelineService{
			Workspaces: newCustomerReader(),
			Pipelines: &mockPipelineRepo{
				getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
					return existing, nil
				},
				updatePipelineFn: func(_ context.Context, p *workspace.Pipeline) error {
					updated = p
					return nil
				},
			},
			Logger: slog.Default(),
		}

		p := &workspace.Pipeline{ID: "pipe-1", SourceType: "sql_database", SourceCredentials: newCreds}
		_, err := svc.UpdatePipeline(context.Background(), "user-1", p)
		if err != nil {
			t.Fatalf("UpdatePipeline() error = %v", err)
		}
		if string(updated.SourceCredentials) != string(newCreds) {
			t.Errorf("SourceCredentials = %s, want %s", updated.SourceCredentials, newCreds)
		}
	})

	t.Run("wrong customer returns ErrPipelineNotFound", func(t *testing.T) {
		svc := &workspace.PipelineService{
			Workspaces: &mockCustomerReader{
				getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
					return &workspace.Workspace{Slug: "other-co"}, nil
				},
			},
			Pipelines: &mockPipelineRepo{
				getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
					return existing, nil
				},
			},
			Logger: slog.Default(),
		}

		_, err := svc.UpdatePipeline(context.Background(), "user-1", &workspace.Pipeline{ID: "pipe-1"})
		if !errors.Is(err, workspace.ErrPipelineNotFound) {
			t.Fatalf("UpdatePipeline() error = %v, want %v", err, workspace.ErrPipelineNotFound)
		}
	})
}

func TestPipelineService_DeletePipeline(t *testing.T) {
	t.Run("deletes owned pipeline", func(t *testing.T) {
		deleteCalled := false

		svc := &workspace.PipelineService{
			Workspaces: &mockCustomerReader{
				getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
					return &workspace.Workspace{Slug: "acme"}, nil
				},
			},
			Pipelines: &mockPipelineRepo{
				getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
					return &workspace.Pipeline{ID: "pipe-1", CustomerSlug: "acme"}, nil
				},
				deletePipelineFn: func(context.Context, workspace.PipelineID) error {
					deleteCalled = true
					return nil
				},
			},
			Logger: slog.Default(),
		}

		if err := svc.DeletePipeline(context.Background(), "user-1", "pipe-1"); err != nil {
			t.Fatalf("DeletePipeline() error = %v", err)
		}
		if !deleteCalled {
			t.Error("DeletePipeline should have been called on repository")
		}
	})

	t.Run("wrong customer returns ErrPipelineNotFound", func(t *testing.T) {
		svc := &workspace.PipelineService{
			Workspaces: &mockCustomerReader{
				getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
					return &workspace.Workspace{Slug: "other-co"}, nil
				},
			},
			Pipelines: &mockPipelineRepo{
				getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
					return &workspace.Pipeline{ID: "pipe-1", CustomerSlug: "acme"}, nil
				},
			},
			Logger: slog.Default(),
		}

		err := svc.DeletePipeline(context.Background(), "user-1", "pipe-1")
		if !errors.Is(err, workspace.ErrPipelineNotFound) {
			t.Fatalf("DeletePipeline() error = %v, want %v", err, workspace.ErrPipelineNotFound)
		}
	})
}

func TestPipelineService_TriggerPipeline(t *testing.T) {
	t.Run("creates pending run", func(t *testing.T) {
		var createdRun *workspace.PipelineRun

		svc := &workspace.PipelineService{
			Workspaces: &mockCustomerReader{
				getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
					return &workspace.Workspace{Slug: "acme"}, nil
				},
			},
			Pipelines: &mockPipelineRepo{
				getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
					return &workspace.Pipeline{ID: "pipe-1", CustomerSlug: "acme"}, nil
				},
				createPipelineRunFn: func(_ context.Context, run *workspace.PipelineRun) error {
					createdRun = run
					return nil
				},
			},
			Logger: slog.Default(),
		}

		run, err := svc.TriggerPipeline(context.Background(), "user-1", "pipe-1")
		if err != nil {
			t.Fatalf("TriggerPipeline() error = %v", err)
		}
		if run.Status != "pending" {
			t.Errorf("Status = %q, want %q", run.Status, "pending")
		}
		if createdRun.PipelineID != "pipe-1" {
			t.Errorf("PipelineID = %q, want %q", createdRun.PipelineID, "pipe-1")
		}
		if createdRun.CreatedAt.IsZero() {
			t.Error("CreatedAt should be set")
		}
	})

	t.Run("wrong customer returns ErrPipelineNotFound", func(t *testing.T) {
		svc := &workspace.PipelineService{
			Workspaces: &mockCustomerReader{
				getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
					return &workspace.Workspace{Slug: "other-co"}, nil
				},
			},
			Pipelines: &mockPipelineRepo{
				getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
					return &workspace.Pipeline{ID: "pipe-1", CustomerSlug: "acme"}, nil
				},
			},
			Logger: slog.Default(),
		}

		_, err := svc.TriggerPipeline(context.Background(), "user-1", "pipe-1")
		if !errors.Is(err, workspace.ErrPipelineNotFound) {
			t.Fatalf("TriggerPipeline() error = %v, want %v", err, workspace.ErrPipelineNotFound)
		}
	})

	t.Run("idempotent: returns existing pending run without creating a duplicate", func(t *testing.T) {
		createCalled := false
		queued := &workspace.PipelineRun{ID: "run-existing", PipelineID: "pipe-1", Status: "pending"}

		svc := &workspace.PipelineService{
			Workspaces: &mockCustomerReader{
				getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
					return &workspace.Workspace{Slug: "acme"}, nil
				},
			},
			Pipelines: &mockPipelineRepo{
				getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
					return &workspace.Pipeline{ID: "pipe-1", CustomerSlug: "acme"}, nil
				},
				getPendingRunFn: func(context.Context, workspace.PipelineID) (*workspace.PipelineRun, error) {
					return queued, nil
				},
				createPipelineRunFn: func(context.Context, *workspace.PipelineRun) error {
					createCalled = true
					return nil
				},
			},
			Logger: slog.Default(),
		}

		run, err := svc.TriggerPipeline(context.Background(), "user-1", "pipe-1")
		if err != nil {
			t.Fatalf("TriggerPipeline() error = %v", err)
		}
		if run.ID != "run-existing" {
			t.Errorf("run ID = %q, want the already-queued run %q", run.ID, "run-existing")
		}
		if createCalled {
			t.Error("CreatePipelineRun should NOT be called when a run is already pending")
		}
	})

	t.Run("triggers a paused pipeline (enabled flag is not checked)", func(t *testing.T) {
		var createdRun *workspace.PipelineRun

		svc := &workspace.PipelineService{
			Workspaces: &mockCustomerReader{
				getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
					return &workspace.Workspace{Slug: "acme"}, nil
				},
			},
			Pipelines: &mockPipelineRepo{
				getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
					return &workspace.Pipeline{ID: "pipe-1", CustomerSlug: "acme", Enabled: false}, nil
				},
				createPipelineRunFn: func(_ context.Context, run *workspace.PipelineRun) error {
					createdRun = run
					return nil
				},
			},
			Logger: slog.Default(),
		}

		run, err := svc.TriggerPipeline(context.Background(), "user-1", "pipe-1")
		if err != nil {
			t.Fatalf("TriggerPipeline() error = %v", err)
		}
		if run.Status != "pending" || createdRun == nil {
			t.Errorf("expected a pending run to be created for a paused pipeline, got %+v", run)
		}
	})
}

func TestPipelineService_ReportPipelineRun(t *testing.T) {
	t.Run("updates existing run when ID set", func(t *testing.T) {
		updateCalled := false
		createCalled := false

		svc := &workspace.PipelineService{
			Pipelines: &mockPipelineRepo{
				updatePipelineRunFn: func(context.Context, *workspace.PipelineRun) error {
					updateCalled = true
					return nil
				},
				createPipelineRunFn: func(context.Context, *workspace.PipelineRun) error {
					createCalled = true
					return nil
				},
			},
			Logger: slog.Default(),
		}

		run := &workspace.PipelineRun{ID: "run-1", PipelineID: "pipe-1", Status: "success"}
		if err := svc.ReportPipelineRun(context.Background(), "", run); err != nil {
			t.Fatalf("ReportPipelineRun() error = %v", err)
		}
		if !updateCalled {
			t.Error("UpdatePipelineRun should have been called")
		}
		if createCalled {
			t.Error("CreatePipelineRun should NOT have been called")
		}
	})

	// The box worker records a run locally and reports the same id. If this
	// database has never seen that id, the row must be created UNDER it —
	// minting a second one would show the customer two rows for one run.
	t.Run("creates under the reported ID when the run is unknown", func(t *testing.T) {
		var createdID string

		svc := &workspace.PipelineService{
			Pipelines: &mockPipelineRepo{
				updatePipelineRunFn: func(context.Context, *workspace.PipelineRun) error {
					return workspace.ErrPipelineRunNotFound
				},
				createPipelineRunFn: func(_ context.Context, run *workspace.PipelineRun) error {
					createdID = run.ID
					return nil
				},
			},
			Logger: slog.Default(),
		}

		run := &workspace.PipelineRun{ID: "run-local-1", PipelineID: "pipe-1", Status: "success"}
		if err := svc.ReportPipelineRun(context.Background(), "", run); err != nil {
			t.Fatalf("ReportPipelineRun() error = %v", err)
		}
		if createdID != "run-local-1" {
			t.Errorf("created run id = %q, want the reported id preserved", createdID)
		}
	})

	t.Run("creates new run when ID empty", func(t *testing.T) {
		updateCalled := false
		createCalled := false

		svc := &workspace.PipelineService{
			Pipelines: &mockPipelineRepo{
				updatePipelineRunFn: func(context.Context, *workspace.PipelineRun) error {
					updateCalled = true
					return nil
				},
				createPipelineRunFn: func(context.Context, *workspace.PipelineRun) error {
					createCalled = true
					return nil
				},
			},
			Logger: slog.Default(),
		}

		run := &workspace.PipelineRun{PipelineID: "pipe-1", Status: "success"}
		if err := svc.ReportPipelineRun(context.Background(), "", run); err != nil {
			t.Fatalf("ReportPipelineRun() error = %v", err)
		}
		if updateCalled {
			t.Error("UpdatePipelineRun should NOT have been called")
		}
		if !createCalled {
			t.Error("CreatePipelineRun should have been called")
		}
	})

	t.Run("caller slug must own the pipeline", func(t *testing.T) {
		createCalled := false

		svc := &workspace.PipelineService{
			Pipelines: &mockPipelineRepo{
				getPipelineFn: func(_ context.Context, id workspace.PipelineID) (*workspace.Pipeline, error) {
					return &workspace.Pipeline{ID: id, CustomerSlug: "acme"}, nil
				},
				createPipelineRunFn: func(context.Context, *workspace.PipelineRun) error {
					createCalled = true
					return nil
				},
			},
			Logger: slog.Default(),
		}

		run := &workspace.PipelineRun{PipelineID: "pipe-1", Status: "success"}
		if err := svc.ReportPipelineRun(context.Background(), "evil", run); !errors.Is(err, workspace.ErrPipelineNotFound) {
			t.Fatalf("ReportPipelineRun() error = %v, want ErrPipelineNotFound", err)
		}
		if createCalled {
			t.Error("CreatePipelineRun should NOT have been called for a foreign pipeline")
		}

		if err := svc.ReportPipelineRun(context.Background(), "acme", run); err != nil {
			t.Fatalf("ReportPipelineRun() error = %v, want nil for the owning tenant", err)
		}
		if !createCalled {
			t.Error("CreatePipelineRun should have been called for the owning tenant")
		}
	})

	t.Run("caller slug on unknown pipeline propagates not found", func(t *testing.T) {
		svc := &workspace.PipelineService{
			Pipelines: &mockPipelineRepo{
				getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
					return nil, workspace.ErrPipelineNotFound
				},
			},
			Logger: slog.Default(),
		}

		run := &workspace.PipelineRun{PipelineID: "pipe-404", Status: "success"}
		if err := svc.ReportPipelineRun(context.Background(), "acme", run); !errors.Is(err, workspace.ErrPipelineNotFound) {
			t.Fatalf("ReportPipelineRun() error = %v, want ErrPipelineNotFound", err)
		}
	})
}

// blockingMirror is a PipelineMirrorer whose SyncCustomer blocks until it is
// released — it stands in for a box whose Gitea is unreachable and hangs the
// dual-write. It signals when it starts and finishes so a test can prove the
// save neither waits for nor is blocked by it.
type blockingMirror struct {
	started chan struct{}
	release chan struct{}
	done    chan struct{}
}

func (m *blockingMirror) SyncCustomer(context.Context, string, *workspace.CommitAuthor) error {
	close(m.started)
	<-m.release
	close(m.done)
	return nil
}

// TestPipelineService_UpdatePipeline_mirrorAsync is the regression guard for
// Fix A: a Console save must return as soon as Postgres is written and must
// never block on the best-effort box mirror — an unreachable box hung the
// mirror long enough to trip Cloudflare's timeout and 504 the RPC.
func TestPipelineService_UpdatePipeline_mirrorAsync(t *testing.T) {
	mirror := &blockingMirror{
		started: make(chan struct{}),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
	svc := &workspace.PipelineService{
		Workspaces: &mockCustomerReader{
			getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
				return &workspace.Workspace{Slug: "acme"}, nil
			},
		},
		Pipelines: &mockPipelineRepo{
			getPipelineFn: func(_ context.Context, id workspace.PipelineID) (*workspace.Pipeline, error) {
				return &workspace.Pipeline{ID: id, CustomerSlug: "acme"}, nil
			},
			updatePipelineFn: func(context.Context, *workspace.Pipeline) error { return nil },
		},
		Mirror: mirror,
		Logger: slog.Default(),
	}

	// Let the blocked mirror goroutine finish only once the test is done, so
	// it exits cleanly rather than leaking past the test run.
	t.Cleanup(func() {
		close(mirror.release)
		select {
		case <-mirror.done:
		case <-time.After(2 * time.Second):
			t.Error("mirror goroutine never finished after release")
		}
	})

	returned := make(chan error, 1)
	go func() {
		_, err := svc.UpdatePipeline(context.Background(), "user-1",
			&workspace.Pipeline{ID: "pipe-1", SourceType: "sql_database"})
		returned <- err
	}()

	select {
	case err := <-returned:
		if err != nil {
			t.Fatalf("UpdatePipeline() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("UpdatePipeline blocked on the mirror instead of returning (Fix A regression)")
	}

	// The mirror was dispatched (async) but is still blocked — proving the RPC
	// returned without waiting for the box round-trip.
	select {
	case <-mirror.started:
	case <-time.After(2 * time.Second):
		t.Fatal("mirror was never invoked")
	}
	select {
	case <-mirror.done:
		t.Fatal("mirror ran to completion synchronously; the save must not wait for it")
	default:
	}
}

// fakeVersioner serves canned version data (the mirror's role).
type fakeVersioner struct {
	restored *workspace.Pipeline
}

func (f *fakeVersioner) ListVersions(context.Context, string, workspace.PipelineID) ([]workspace.PipelineVersion, error) {
	return []workspace.PipelineVersion{{SHA: "c0ffee1234567", AuthorName: "Alice", Date: "2026-07-19T00:00:00Z"}}, nil
}

func (f *fakeVersioner) VersionAt(context.Context, string, workspace.PipelineID, string) (*workspace.Pipeline, error) {
	return f.restored, nil
}

func TestPipelineService_RestoreVersion(t *testing.T) {
	existingCreds := json.RawMessage(`{"api_key":"secret"}`)
	restored := &workspace.Pipeline{
		ID:           "p1",
		Name:         "Orders (yesterday)",
		SourceType:   "rest_api",
		SourceConfig: json.RawMessage(`{"base_url":"https://example.com","resources":[{"name":"orders","endpoint":"/orders"}]}`),
		DatasetName:  "raw",
		Enabled:      true,
	}

	var updated *workspace.Pipeline
	svc := &workspace.PipelineService{
		Workspaces: &mockCustomerReader{
			getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
				return &workspace.Workspace{Slug: "acme"}, nil
			},
		},
		Pipelines: &mockPipelineRepo{
			getPipelineFn: func(_ context.Context, id workspace.PipelineID) (*workspace.Pipeline, error) {
				// Same source type as the restored version — a restore never
				// changes a pipeline's type (guarded in RestorePipelineVersion).
				return &workspace.Pipeline{ID: id, CustomerSlug: "acme", SourceType: "rest_api", SourceCredentials: existingCreds}, nil
			},
			updatePipelineFn: func(_ context.Context, p *workspace.Pipeline) error {
				updated = p
				return nil
			},
		},
		Versions: &fakeVersioner{restored: restored},
	}

	t.Run("restore applies through the normal update path", func(t *testing.T) {
		out, err := svc.RestorePipelineVersion(context.Background(), "u1", "p1", "c0ffee1234567")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if updated == nil || updated.Name != "Orders (yesterday)" || out.Name != updated.Name {
			t.Fatalf("restore did not save the historical state: %+v", updated)
		}
		if string(updated.SourceCredentials) != string(existingCreds) {
			t.Fatalf("credentials must be preserved, got %s", updated.SourceCredentials)
		}
	})

	t.Run("versions list requires the mirror", func(t *testing.T) {
		bare := &workspace.PipelineService{Workspaces: svc.Workspaces, Pipelines: svc.Pipelines}
		if _, err := bare.ListPipelineVersions(context.Background(), "u1", "p1"); !errors.Is(err, workspace.ErrBoxRepoUnavailable) {
			t.Fatalf("want ErrBoxRepoUnavailable, got %v", err)
		}
		if _, err := bare.RestorePipelineVersion(context.Background(), "u1", "p1", "c0ffee1234567"); !errors.Is(err, workspace.ErrBoxRepoUnavailable) {
			t.Fatalf("want ErrBoxRepoUnavailable, got %v", err)
		}
	})

	t.Run("restore refuses to change the source type", func(t *testing.T) {
		// A file_upload pipeline whose history renders as its filesystem form
		// must not be restored into a filesystem pipeline (that would drop its
		// file-drop identity and leave it credential-less).
		var saved bool
		guarded := &workspace.PipelineService{
			Workspaces: svc.Workspaces,
			Pipelines: &mockPipelineRepo{
				getPipelineFn: func(_ context.Context, id workspace.PipelineID) (*workspace.Pipeline, error) {
					return &workspace.Pipeline{ID: id, CustomerSlug: "acme", SourceType: workspace.SourceTypeFileUpload}, nil
				},
				updatePipelineFn: func(context.Context, *workspace.Pipeline) error { saved = true; return nil },
			},
			Versions: &fakeVersioner{restored: &workspace.Pipeline{ID: "p1", SourceType: "filesystem"}},
		}
		_, err := guarded.RestorePipelineVersion(context.Background(), "u1", "p1", "c0ffee1234567")
		var invalid *workspace.ErrInvalidSourceConfig
		if !errors.As(err, &invalid) || invalid.Field != "source_type" {
			t.Fatalf("want source_type validation error, got %v", err)
		}
		if saved {
			t.Fatal("a type-changing restore must not be persisted")
		}
	})
}

// hardFailMirror is a PipelineMirrorer that always fails; recorder counts calls.
type hardFailMirror struct {
	calls int
	err   error
}

func (m *hardFailMirror) SyncCustomer(context.Context, string, *workspace.CommitAuthor) error {
	m.calls++
	return m.err
}

// Git-first mode (Phase 2A): a failed box-repo commit hard-fails the save
// and compensates the cache row; a successful commit happens synchronously.
func TestPipelineService_GitPrimary(t *testing.T) {
	acmeResolver := &mockCustomerReader{
		getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
			return &workspace.Workspace{Slug: "acme"}, nil
		},
	}

	t.Run("create compensates on mirror failure", func(t *testing.T) {
		var deleted bool
		mirror := &hardFailMirror{err: workspace.ErrBoxUnreachable}
		svc := &workspace.PipelineService{
			Workspaces: acmeResolver,
			Pipelines: &mockPipelineRepo{
				createPipelineFn: func(_ context.Context, p *workspace.Pipeline) error {
					p.ID = "new-1"
					return nil
				},
				deletePipelineFn: func(_ context.Context, id workspace.PipelineID) error {
					if id != "new-1" {
						t.Errorf("compensation deleted %q, want new-1", id)
					}
					deleted = true
					return nil
				},
			},
			Mirror:     mirror,
			GitPrimary: true,
			Logger:     slog.Default(),
		}
		_, err := svc.CreatePipeline(context.Background(), "u1", &workspace.Pipeline{
			SourceType:   "rest_api",
			SourceConfig: json.RawMessage(`{"base_url":"https://x","resources":[{"name":"users","endpoint":"/users"}]}`),
		})
		if !errors.Is(err, workspace.ErrBoxUnreachable) {
			t.Fatalf("err = %v, want ErrBoxUnreachable", err)
		}
		if !deleted {
			t.Fatal("failed commit must compensate the created cache row")
		}
	})

	t.Run("update restores previous row on mirror failure", func(t *testing.T) {
		existing := &workspace.Pipeline{ID: "p1", CustomerSlug: "acme", Name: "old",
			SourceType: "rest_api", SourceConfig: json.RawMessage(`{"base_url":"https://old","resources":[{"name":"users","endpoint":"/users"}]}`)}
		var updates []string
		svc := &workspace.PipelineService{
			Workspaces: acmeResolver,
			Pipelines: &mockPipelineRepo{
				getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
					return existing, nil
				},
				updatePipelineFn: func(_ context.Context, p *workspace.Pipeline) error {
					updates = append(updates, p.Name)
					return nil
				},
			},
			Mirror:     &hardFailMirror{err: workspace.ErrBoxUnreachable},
			GitPrimary: true,
			Logger:     slog.Default(),
		}
		_, err := svc.UpdatePipeline(context.Background(), "u1", &workspace.Pipeline{ID: "p1", Name: "new",
			SourceType: "rest_api", SourceConfig: json.RawMessage(`{"base_url":"https://new","resources":[{"name":"users","endpoint":"/users"}]}`)})
		if !errors.Is(err, workspace.ErrBoxUnreachable) {
			t.Fatalf("err = %v, want ErrBoxUnreachable", err)
		}
		if len(updates) != 2 || updates[1] != "old" {
			t.Fatalf("updates = %v, want [new old] (compensation writes the previous row back)", updates)
		}
	})

	t.Run("successful commit is synchronous and save succeeds", func(t *testing.T) {
		mirror := &hardFailMirror{} // nil err = success
		svc := &workspace.PipelineService{
			Workspaces: acmeResolver,
			Pipelines: &mockPipelineRepo{
				createPipelineFn: func(context.Context, *workspace.Pipeline) error { return nil },
			},
			Mirror:     mirror,
			GitPrimary: true,
			Logger:     slog.Default(),
		}
		_, err := svc.CreatePipeline(context.Background(), "u1", &workspace.Pipeline{
			SourceType:   "rest_api",
			SourceConfig: json.RawMessage(`{"base_url":"https://x","resources":[{"name":"users","endpoint":"/users"}]}`),
		})
		if err != nil {
			t.Fatalf("CreatePipeline: %v", err)
		}
		if mirror.calls != 1 {
			t.Fatalf("mirror calls = %d, want 1 (synchronous, on the request path)", mirror.calls)
		}
	})
}

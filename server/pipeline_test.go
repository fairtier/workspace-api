package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"connectrpc.com/connect"

	pipelinev1 "github.com/fairtier/workspace-api/proto/pipeline/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// stubPipelineRepo overrides just the worker-facing reads; everything else
// panics via the embedded nil interface if unexpectedly called.
type stubPipelineRepo struct {
	workspace.PipelineRepository
	enabled []workspace.Pipeline
}

func (s stubPipelineRepo) GetEnabledPipelines(context.Context, string) ([]workspace.Pipeline, error) {
	return s.enabled, nil
}

// withInternalCaller mirrors what NewInternalAuthInterceptor puts in context
// for a central (shared-substrate) service token.
func withInternalCaller(app string) context.Context {
	return context.WithValue(context.Background(), internalCallerKey, InternalCaller{App: app})
}

// withBoxCaller mirrors a box token: the slug is bound from the verified
// issuer host, the app name is the box's base-named Casdoor app.
func withBoxCaller(app, slug string) context.Context {
	return context.WithValue(context.Background(), internalCallerKey, InternalCaller{
		App: app, Slug: slug, Issuer: "https://auth.customer-" + slug + ".fairtier.com",
	})
}

func newTestPipelineService() *workspace.PipelineService {
	return &workspace.PipelineService{
		Pipelines: stubPipelineRepo{enabled: []workspace.Pipeline{{ID: "pipe-1", CustomerSlug: "acme"}}},
	}
}

func TestGetPipelineConfigs_TenantBinding(t *testing.T) {
	srv := NewInternalPipelineServer(newTestPipelineService(), InternalAuthEnforce)
	req := connect.NewRequest(&pipelinev1.GetPipelineConfigsRequest{CustomerSlug: "acme"})

	t.Run("matching dlt-worker token", func(t *testing.T) {
		resp, err := srv.GetPipelineConfigs(withInternalCaller("dlt-worker-acme"), req)
		if err != nil {
			t.Fatalf("GetPipelineConfigs() error = %v", err)
		}
		if len(resp.Msg.Pipelines) != 1 {
			t.Errorf("got %d pipelines, want 1", len(resp.Msg.Pipelines))
		}
	})

	t.Run("foreign tenant token denied", func(t *testing.T) {
		_, err := srv.GetPipelineConfigs(withInternalCaller("dlt-worker-evil"), req)
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("GetPipelineConfigs() error = %v, want PermissionDenied", err)
		}
	})

	t.Run("non-dlt service token denied", func(t *testing.T) {
		_, err := srv.GetPipelineConfigs(withInternalCaller("duckflight-acme"), req)
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("GetPipelineConfigs() error = %v, want PermissionDenied", err)
		}
	})

	t.Run("unauthenticated denied under enforce", func(t *testing.T) {
		_, err := srv.GetPipelineConfigs(context.Background(), req)
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Fatalf("GetPipelineConfigs() error = %v, want Unauthenticated", err)
		}
	})

	t.Run("unauthenticated allowed under log mode", func(t *testing.T) {
		logSrv := NewInternalPipelineServer(newTestPipelineService(), InternalAuthLog)
		if _, err := logSrv.GetPipelineConfigs(context.Background(), req); err != nil {
			t.Fatalf("GetPipelineConfigs() error = %v", err)
		}
	})

	t.Run("matching box token", func(t *testing.T) {
		resp, err := srv.GetPipelineConfigs(withBoxCaller("dlt-worker", "acme"), req)
		if err != nil {
			t.Fatalf("GetPipelineConfigs() error = %v", err)
		}
		if len(resp.Msg.Pipelines) != 1 {
			t.Errorf("got %d pipelines, want 1", len(resp.Msg.Pipelines))
		}
	})

	t.Run("foreign box token denied", func(t *testing.T) {
		_, err := srv.GetPipelineConfigs(withBoxCaller("dlt-worker", "evil"), req)
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("GetPipelineConfigs() error = %v, want PermissionDenied", err)
		}
	})

	t.Run("box token app name is advisory", func(t *testing.T) {
		// The box is customer-controlled: any app it mints binds to its own
		// slug, so an unexpected app name must not widen or deny access.
		resp, err := srv.GetPipelineConfigs(withBoxCaller("anything", "acme"), req)
		if err != nil {
			t.Fatalf("GetPipelineConfigs() error = %v", err)
		}
		if len(resp.Msg.Pipelines) != 1 {
			t.Errorf("got %d pipelines, want 1", len(resp.Msg.Pipelines))
		}
	})
}

func TestJsonOrEmpty(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "empty string returns {}", input: "", want: "{}"},
		{name: "json passthrough", input: `{"k":"v"}`, want: `{"k":"v"}`},
		{name: "arbitrary string passthrough", input: "hello", want: "hello"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := jsonOrEmpty(tt.input)
			if string(got) != tt.want {
				t.Errorf("jsonOrEmpty(%q) = %q, want %q", tt.input, string(got), tt.want)
			}
		})
	}
}

func TestParseTimePtr(t *testing.T) {
	t.Run("empty string returns nil", func(t *testing.T) {
		got, err := parseTimePtr("")
		if err != nil {
			t.Fatalf("parseTimePtr(\"\") error = %v", err)
		}
		if got != nil {
			t.Errorf("parseTimePtr(\"\") = %v, want nil", got)
		}
	})

	t.Run("valid RFC3339", func(t *testing.T) {
		got, err := parseTimePtr("2025-01-15T10:30:00Z")
		if err != nil {
			t.Fatalf("parseTimePtr() error = %v", err)
		}
		if got == nil {
			t.Fatal("parseTimePtr() returned nil, want non-nil")
		}
		want := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Errorf("parseTimePtr() = %v, want %v", got, want)
		}
	})

	t.Run("invalid string returns error", func(t *testing.T) {
		_, err := parseTimePtr("not-a-date")
		if err == nil {
			t.Fatal("parseTimePtr(\"not-a-date\") should return error")
		}
	})
}

func TestPipelineToPB(t *testing.T) {
	now := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)

	p := &workspace.Pipeline{
		ID:               "pipe-1",
		CustomerSlug:     "acme",
		Name:             "my-pipeline",
		SourceType:       "sql_database",
		SourceConfig:     json.RawMessage(`{"tables":["orders"]}`),
		DatasetName:      "raw",
		Schedule:         "*/5 * * * *",
		WriteDisposition: "append",
		MergeStrategy:    "delete-insert",
		Enabled:          true,
		CreatedAt:        now,
		UpdatedAt:        now.Add(time.Hour),
	}

	pb := pipelineToPB(p)

	if pb.Id != "pipe-1" {
		t.Errorf("Id = %q, want %q", pb.Id, "pipe-1")
	}
	if pb.CustomerSlug != "acme" {
		t.Errorf("CustomerSlug = %q, want %q", pb.CustomerSlug, "acme")
	}
	if pb.Name != "my-pipeline" {
		t.Errorf("Name = %q, want %q", pb.Name, "my-pipeline")
	}
	if pb.SourceType != "sql_database" {
		t.Errorf("SourceType = %q, want %q", pb.SourceType, "sql_database")
	}
	if pb.SourceConfig != `{"tables":["orders"]}` {
		t.Errorf("SourceConfig = %q", pb.SourceConfig)
	}
	if pb.DatasetName != "raw" {
		t.Errorf("DatasetName = %q, want %q", pb.DatasetName, "raw")
	}
	if pb.Schedule != "*/5 * * * *" {
		t.Errorf("Schedule = %q, want %q", pb.Schedule, "*/5 * * * *")
	}
	if pb.WriteDisposition != "append" {
		t.Errorf("WriteDisposition = %q, want %q", pb.WriteDisposition, "append")
	}
	if pb.MergeStrategy != "delete-insert" {
		t.Errorf("MergeStrategy = %q, want %q", pb.MergeStrategy, "delete-insert")
	}
	if !pb.Enabled {
		t.Error("Enabled should be true")
	}
	if pb.CreatedAt != "2025-06-01T12:00:00Z" {
		t.Errorf("CreatedAt = %q, want %q", pb.CreatedAt, "2025-06-01T12:00:00Z")
	}
	if pb.UpdatedAt != "2025-06-01T13:00:00Z" {
		t.Errorf("UpdatedAt = %q, want %q", pb.UpdatedAt, "2025-06-01T13:00:00Z")
	}
	// No run yet → last-run fields empty.
	if pb.LastRunAt != "" || pb.LastRunStatus != "" {
		t.Errorf("expected empty last-run fields, got at=%q status=%q", pb.LastRunAt, pb.LastRunStatus)
	}
}

func TestPipelineToPB_LastRun(t *testing.T) {
	runAt := time.Date(2025, 6, 2, 9, 30, 0, 0, time.UTC)
	p := &workspace.Pipeline{
		ID:            "pipe-1",
		LastRunTime:   &runAt,
		LastRunStatus: "failed",
	}
	pb := pipelineToPB(p)
	if pb.LastRunAt != "2025-06-02T09:30:00Z" {
		t.Errorf("LastRunAt = %q, want %q", pb.LastRunAt, "2025-06-02T09:30:00Z")
	}
	if pb.LastRunStatus != "failed" {
		t.Errorf("LastRunStatus = %q, want %q", pb.LastRunStatus, "failed")
	}
}

func TestPipelineRunToPB(t *testing.T) {
	t.Run("all times set", func(t *testing.T) {
		now := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
		started := now.Add(time.Minute)
		completed := now.Add(5 * time.Minute)

		r := &workspace.PipelineRun{
			ID:           "run-1",
			PipelineID:   "pipe-1",
			Status:       "success",
			StartedAt:    &started,
			CompletedAt:  &completed,
			RowsLoaded:   42,
			ErrorMessage: "",
			CreatedAt:    now,
		}

		pb := pipelineRunToPB(r)

		if pb.Id != "run-1" {
			t.Errorf("Id = %q, want %q", pb.Id, "run-1")
		}
		if pb.PipelineId != "pipe-1" {
			t.Errorf("PipelineId = %q, want %q", pb.PipelineId, "pipe-1")
		}
		if pb.Status != "success" {
			t.Errorf("Status = %q, want %q", pb.Status, "success")
		}
		if pb.StartedAt != "2025-06-01T12:01:00Z" {
			t.Errorf("StartedAt = %q, want %q", pb.StartedAt, "2025-06-01T12:01:00Z")
		}
		if pb.CompletedAt != "2025-06-01T12:05:00Z" {
			t.Errorf("CompletedAt = %q, want %q", pb.CompletedAt, "2025-06-01T12:05:00Z")
		}
		if pb.RowsLoaded != 42 {
			t.Errorf("RowsLoaded = %d, want %d", pb.RowsLoaded, 42)
		}
		if pb.CreatedAt != "2025-06-01T12:00:00Z" {
			t.Errorf("CreatedAt = %q, want %q", pb.CreatedAt, "2025-06-01T12:00:00Z")
		}
	})

	t.Run("nil times produce empty strings", func(t *testing.T) {
		now := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)

		r := &workspace.PipelineRun{
			ID:         "run-2",
			PipelineID: "pipe-1",
			Status:     "pending",
			CreatedAt:  now,
		}

		pb := pipelineRunToPB(r)

		if pb.StartedAt != "" {
			t.Errorf("StartedAt = %q, want empty", pb.StartedAt)
		}
		if pb.CompletedAt != "" {
			t.Errorf("CompletedAt = %q, want empty", pb.CompletedAt)
		}
	})
}

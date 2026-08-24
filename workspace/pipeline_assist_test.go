package workspace_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

type mockDrafter struct {
	fn func(ctx context.Context, prompt string) (*workspace.PipelineDraft, error)
}

func (m *mockDrafter) DraftPipeline(ctx context.Context, prompt string) (*workspace.PipelineDraft, error) {
	return m.fn(ctx, prompt)
}

func acmeReader() *mockCustomerReader {
	return &mockCustomerReader{
		getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
			return &workspace.Workspace{Slug: "acme"}, nil
		},
	}
}

func TestPipelineAssistService_DraftPipeline(t *testing.T) {
	validCfg := json.RawMessage(`{"base_url":"https://api.example.com","resources":[{"name":"charges","endpoint":"/charges"}]}`)

	t.Run("not configured when drafter nil", func(t *testing.T) {
		svc := &workspace.PipelineAssistService{Workspaces: acmeReader()}
		_, err := svc.DraftPipeline(context.Background(), "u1", "load stripe")
		if !errors.Is(err, workspace.ErrDraftNotConfigured) {
			t.Fatalf("want ErrDraftNotConfigured, got %v", err)
		}
	})

	t.Run("empty prompt rejected", func(t *testing.T) {
		svc := &workspace.PipelineAssistService{
			Workspaces: acmeReader(),
			Drafter:    &mockDrafter{fn: func(context.Context, string) (*workspace.PipelineDraft, error) { return nil, nil }},
		}
		_, err := svc.DraftPipeline(context.Background(), "u1", "   ")
		var invalid *workspace.ErrInvalidSourceConfig
		if !errors.As(err, &invalid) || invalid.Field != "prompt" {
			t.Fatalf("want invalid prompt field, got %v", err)
		}
	})

	t.Run("happy path defaults write_disposition", func(t *testing.T) {
		svc := &workspace.PipelineAssistService{
			Workspaces: acmeReader(),
			Drafter: &mockDrafter{fn: func(context.Context, string) (*workspace.PipelineDraft, error) {
				return &workspace.PipelineDraft{
					Name:         "Stripe",
					SourceType:   "rest_api",
					DatasetName:  "stripe",
					SourceConfig: validCfg,
					Notes:        "Provide your Stripe API key.",
				}, nil
			}},
		}
		draft, err := svc.DraftPipeline(context.Background(), "u1", "load stripe charges")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if draft.WriteDisposition != "append" {
			t.Fatalf("want write_disposition defaulted to append, got %q", draft.WriteDisposition)
		}
	})

	t.Run("invalid drafted config surfaces as validation error", func(t *testing.T) {
		svc := &workspace.PipelineAssistService{
			Workspaces: acmeReader(),
			Drafter: &mockDrafter{fn: func(context.Context, string) (*workspace.PipelineDraft, error) {
				// rest_api without base_url is invalid
				return &workspace.PipelineDraft{
					SourceType:   "rest_api",
					SourceConfig: json.RawMessage(`{"resources":[{"name":"x","endpoint":"/x"}]}`),
				}, nil
			}},
		}
		_, err := svc.DraftPipeline(context.Background(), "u1", "load something")
		var invalid *workspace.ErrInvalidSourceConfig
		if !errors.As(err, &invalid) || invalid.Field != "base_url" {
			t.Fatalf("want base_url validation error, got %v", err)
		}
	})

	t.Run("unsupported request returns reason and skips validation", func(t *testing.T) {
		svc := &workspace.PipelineAssistService{
			Workspaces: acmeReader(),
			Drafter: &mockDrafter{fn: func(context.Context, string) (*workspace.PipelineDraft, error) {
				// A refusal draft carries no config at all — validation of the
				// (absent) source_config must not reject it.
				return &workspace.PipelineDraft{
					SourceType:        "unsupported",
					UnsupportedReason: "Oracle databases are not supported; only PostgreSQL is reachable.",
					Notes:             "Consider exporting to CSV and using file upload.",
				}, nil
			}},
		}
		draft, err := svc.DraftPipeline(context.Background(), "u1", "extract from our Oracle DB")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if draft.UnsupportedReason == "" {
			t.Fatal("want UnsupportedReason set")
		}
		if draft.SourceType != "" || draft.WriteDisposition != "" {
			t.Fatalf("refusal draft must carry no pre-fill, got source_type=%q write_disposition=%q", draft.SourceType, draft.WriteDisposition)
		}
		if draft.Notes == "" {
			t.Fatal("want notes preserved on refusal")
		}
	})

	t.Run("unsupported source_type without reason gets a generic one", func(t *testing.T) {
		svc := &workspace.PipelineAssistService{
			Workspaces: acmeReader(),
			Drafter: &mockDrafter{fn: func(context.Context, string) (*workspace.PipelineDraft, error) {
				return &workspace.PipelineDraft{SourceType: "unsupported"}, nil
			}},
		}
		draft, err := svc.DraftPipeline(context.Background(), "u1", "do the impossible")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if draft.UnsupportedReason == "" {
			t.Fatal("want a generic UnsupportedReason when the model omitted one")
		}
	})

	t.Run("rate limited", func(t *testing.T) {
		svc := &workspace.PipelineAssistService{
			Workspaces: acmeReader(),
			Limiter:    workspace.NewMemoryRateLimiter(1, time.Minute),
			Drafter: &mockDrafter{fn: func(context.Context, string) (*workspace.PipelineDraft, error) {
				return &workspace.PipelineDraft{SourceType: "rest_api", SourceConfig: validCfg}, nil
			}},
		}
		if _, err := svc.DraftPipeline(context.Background(), "u1", "p"); err != nil {
			t.Fatalf("first call should pass: %v", err)
		}
		_, err := svc.DraftPipeline(context.Background(), "u1", "p")
		if !errors.Is(err, workspace.ErrDraftRateLimited) {
			t.Fatalf("want ErrDraftRateLimited, got %v", err)
		}
	})
}

func TestMemoryRateLimiter(t *testing.T) {
	t.Run("non-positive max disables limiting", func(t *testing.T) {
		l := workspace.NewMemoryRateLimiter(0, time.Minute)
		for range 5 {
			if !l.Allow("k") {
				t.Fatalf("disabled limiter must always allow")
			}
		}
	})

	t.Run("caps per window then resets", func(t *testing.T) {
		l := workspace.NewMemoryRateLimiter(2, time.Minute)
		if !l.Allow("k") {
			t.Fatal("first allowed")
		}
		if !l.Allow("k") {
			t.Fatal("second allowed")
		}
		if l.Allow("k") {
			t.Fatal("third within window should be blocked")
		}
		// separate key has its own bucket
		if !l.Allow("other") {
			t.Fatal("distinct key should be allowed")
		}
	})
}

package workspace_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/fairtier/workspace-api/workspace"
)

type mockTransformationDrafter struct {
	fn func(ctx context.Context, prompt string) (*workspace.TransformationDraft, error)
}

func (m *mockTransformationDrafter) DraftTransformation(ctx context.Context, prompt string) (*workspace.TransformationDraft, error) {
	return m.fn(ctx, prompt)
}

type mockRillDrafter struct {
	fn func(ctx context.Context, prompt string, existingPaths []string) (*workspace.RillDraft, error)
}

func (m *mockRillDrafter) DraftRillDashboard(ctx context.Context, prompt string, existingPaths []string) (*workspace.RillDraft, error) {
	return m.fn(ctx, prompt, existingPaths)
}

func validTransformationDraft() *workspace.TransformationDraft {
	return &workspace.TransformationDraft{
		Name: "Revenue mart",
		Files: []workspace.DraftFile{
			{Path: "models/marts/revenue_daily.sql", Content: "SELECT 1"},
			{Path: "models/marts/schema.yml", Content: "version: 2"},
		},
		Notes: "Assumes a stripe charges source.",
	}
}

func TestAssistService_DraftTransformation(t *testing.T) {
	t.Run("not configured when drafter nil", func(t *testing.T) {
		svc := &workspace.AssistService{Workspaces: acmeReader()}
		_, err := svc.DraftTransformation(context.Background(), "u1", "build a mart")
		if !errors.Is(err, workspace.ErrDraftNotConfigured) {
			t.Fatalf("want ErrDraftNotConfigured, got %v", err)
		}
	})

	t.Run("empty prompt rejected", func(t *testing.T) {
		svc := &workspace.AssistService{
			Workspaces: acmeReader(),
			Transformations: &mockTransformationDrafter{fn: func(context.Context, string) (*workspace.TransformationDraft, error) {
				return validTransformationDraft(), nil
			}},
		}
		_, err := svc.DraftTransformation(context.Background(), "u1", "  ")
		var invalid *workspace.ErrInvalidSourceConfig
		if !errors.As(err, &invalid) || invalid.Field != "prompt" {
			t.Fatalf("want invalid prompt field, got %v", err)
		}
	})

	t.Run("happy path", func(t *testing.T) {
		svc := &workspace.AssistService{
			Workspaces: acmeReader(),
			Transformations: &mockTransformationDrafter{fn: func(context.Context, string) (*workspace.TransformationDraft, error) {
				return validTransformationDraft(), nil
			}},
		}
		draft, err := svc.DraftTransformation(context.Background(), "u1", "build a revenue mart")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(draft.Files) != 2 {
			t.Fatalf("want 2 files, got %d", len(draft.Files))
		}
	})

	t.Run("missing name rejected", func(t *testing.T) {
		svc := &workspace.AssistService{
			Workspaces: acmeReader(),
			Transformations: &mockTransformationDrafter{fn: func(context.Context, string) (*workspace.TransformationDraft, error) {
				d := validTransformationDraft()
				d.Name = " "
				return d, nil
			}},
		}
		_, err := svc.DraftTransformation(context.Background(), "u1", "p")
		var invalid *workspace.ErrInvalidSourceConfig
		if !errors.As(err, &invalid) || invalid.Field != "name" {
			t.Fatalf("want name validation error, got %v", err)
		}
	})

	for _, tc := range []struct {
		name string
		path string
	}{
		{"path traversal", "models/../secrets.sql"},
		{"absolute path", "/etc/passwd.sql"},
		{"outside models", "macros/evil.sql"},
		{"bare root", "models/"},
		{"wrong extension", "models/marts/run.sh"},
	} {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			svc := &workspace.AssistService{
				Workspaces: acmeReader(),
				Transformations: &mockTransformationDrafter{fn: func(context.Context, string) (*workspace.TransformationDraft, error) {
					d := validTransformationDraft()
					d.Files = []workspace.DraftFile{{Path: tc.path, Content: "x"}}
					return d, nil
				}},
			}
			_, err := svc.DraftTransformation(context.Background(), "u1", "p")
			if _, ok := errors.AsType[*workspace.ErrInvalidSourceConfig](err); !ok {
				t.Fatalf("want validation error for %q, got %v", tc.path, err)
			}
		})
	}

	t.Run("rejects oversized draft", func(t *testing.T) {
		svc := &workspace.AssistService{
			Workspaces: acmeReader(),
			Transformations: &mockTransformationDrafter{fn: func(context.Context, string) (*workspace.TransformationDraft, error) {
				d := validTransformationDraft()
				d.Files = []workspace.DraftFile{{Path: "models/big.sql", Content: strings.Repeat("x", 64*1024+1)}}
				return d, nil
			}},
		}
		if _, err := svc.DraftTransformation(context.Background(), "u1", "p"); err == nil {
			t.Fatal("want size validation error")
		}
	})

	t.Run("rate limit shared across draft kinds", func(t *testing.T) {
		limiter := workspace.NewMemoryRateLimiter(1, time.Minute)
		svc := &workspace.AssistService{
			Workspaces: acmeReader(),
			Limiter:    limiter,
			Transformations: &mockTransformationDrafter{fn: func(context.Context, string) (*workspace.TransformationDraft, error) {
				return validTransformationDraft(), nil
			}},
			Rill: &mockRillDrafter{fn: func(context.Context, string, []string) (*workspace.RillDraft, error) {
				return &workspace.RillDraft{Files: []workspace.DraftFile{{Path: "metrics/m.yaml", Content: "type: metrics_view"}}}, nil
			}},
		}
		if _, err := svc.DraftTransformation(context.Background(), "u1", "p"); err != nil {
			t.Fatalf("first call should pass: %v", err)
		}
		_, err := svc.DraftRillDashboard(context.Background(), "u1", "p", nil)
		if !errors.Is(err, workspace.ErrDraftRateLimited) {
			t.Fatalf("want ErrDraftRateLimited on second draft (shared limiter), got %v", err)
		}
	})
}

func TestAssistService_DraftRillDashboard(t *testing.T) {
	t.Run("not configured when drafter nil", func(t *testing.T) {
		svc := &workspace.AssistService{Workspaces: acmeReader()}
		_, err := svc.DraftRillDashboard(context.Background(), "u1", "a dashboard", nil)
		if !errors.Is(err, workspace.ErrDraftNotConfigured) {
			t.Fatalf("want ErrDraftNotConfigured, got %v", err)
		}
	})

	t.Run("happy path passes existing paths through", func(t *testing.T) {
		var gotPaths []string
		svc := &workspace.AssistService{
			Workspaces: acmeReader(),
			Rill: &mockRillDrafter{fn: func(_ context.Context, _ string, paths []string) (*workspace.RillDraft, error) {
				gotPaths = paths
				return &workspace.RillDraft{
					Files: []workspace.DraftFile{
						{Path: "metrics/revenue.yaml", Content: "type: metrics_view\nmodel: revenue"},
						{Path: "dashboards/revenue.yaml", Content: "type: explore\nmetrics_view: revenue"},
						{Path: "models/revenue.sql", Content: "SELECT * FROM lk.stripe.charges"},
					},
					Notes: "ok",
				}, nil
			}},
		}
		draft, err := svc.DraftRillDashboard(context.Background(), "u1", "revenue dashboard", []string{"models/orders.sql"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(draft.Files) != 3 {
			t.Fatalf("want 3 files, got %d", len(draft.Files))
		}
		if len(gotPaths) != 1 || gotPaths[0] != "models/orders.sql" {
			t.Fatalf("existing paths not passed through: %v", gotPaths)
		}
	})

	t.Run("empty draft rejected", func(t *testing.T) {
		svc := &workspace.AssistService{
			Workspaces: acmeReader(),
			Rill: &mockRillDrafter{fn: func(context.Context, string, []string) (*workspace.RillDraft, error) {
				return &workspace.RillDraft{}, nil
			}},
		}
		if _, err := svc.DraftRillDashboard(context.Background(), "u1", "p", nil); err == nil {
			t.Fatal("want validation error for empty draft")
		}
	})

	t.Run("invalid YAML rejected", func(t *testing.T) {
		svc := &workspace.AssistService{
			Workspaces: acmeReader(),
			Rill: &mockRillDrafter{fn: func(context.Context, string, []string) (*workspace.RillDraft, error) {
				return &workspace.RillDraft{
					Files: []workspace.DraftFile{{Path: "metrics/bad.yaml", Content: "type: [unclosed"}},
				}, nil
			}},
		}
		_, err := svc.DraftRillDashboard(context.Background(), "u1", "p", nil)
		var invalid *workspace.ErrInvalidSourceConfig
		if !errors.As(err, &invalid) || !strings.Contains(invalid.Msg, "not valid YAML") {
			t.Fatalf("want YAML validation error, got %v", err)
		}
	})
}

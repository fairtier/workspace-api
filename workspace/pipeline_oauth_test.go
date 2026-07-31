package workspace_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

type mockGrantStore struct {
	consumeFn func(ctx context.Context, grantID, customerSlug string) (*workspace.GoogleOAuthGrant, error)
}

func (m *mockGrantStore) CreateGoogleOAuthGrant(context.Context, *workspace.GoogleOAuthGrant) error {
	return nil
}

func (m *mockGrantStore) ConsumeGoogleOAuthGrant(ctx context.Context, grantID, customerSlug string) (*workspace.GoogleOAuthGrant, error) {
	return m.consumeFn(ctx, grantID, customerSlug)
}

func (m *mockGrantStore) DeleteExpiredGoogleOAuthGrants(context.Context) (int64, error) {
	return 0, nil
}

func acmeCustomers() *mockCustomerReader {
	return &mockCustomerReader{
		getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
			return &workspace.Workspace{Slug: "acme"}, nil
		},
	}
}

func TestCreatePipeline_SwapsGoogleSheetsGrant(t *testing.T) {
	var stored *workspace.Pipeline
	var gotGrantID, gotSlug string

	svc := &workspace.PipelineService{
		Workspaces: acmeCustomers(),
		Pipelines: &mockPipelineRepo{
			createPipelineFn: func(_ context.Context, p *workspace.Pipeline) error {
				stored = p
				return nil
			},
		},
		GoogleOAuth: &mockGrantStore{
			consumeFn: func(_ context.Context, grantID, customerSlug string) (*workspace.GoogleOAuthGrant, error) {
				gotGrantID, gotSlug = grantID, customerSlug
				return &workspace.GoogleOAuthGrant{
					GrantID:      grantID,
					CustomerSlug: customerSlug,
					RefreshToken: "1//refresh",
					Email:        "user@gmail.com",
					ExpiresAt:    time.Now().Add(time.Minute),
				}, nil
			},
		},
		Logger: slog.Default(),
	}

	p := &workspace.Pipeline{
		Name:              "sheet",
		SourceType:        "google_sheets",
		SourceConfig:      json.RawMessage(`{"spreadsheet_url_or_id":"abc123"}`),
		SourceCredentials: json.RawMessage(`{"oauth":{"grant_id":"g-1"}}`),
		DatasetName:       "ds",
	}
	if _, err := svc.CreatePipeline(context.Background(), "user-1", p); err != nil {
		t.Fatalf("CreatePipeline() error = %v", err)
	}
	if gotGrantID != "g-1" || gotSlug != "acme" {
		t.Fatalf("consume called with grantID=%q slug=%q", gotGrantID, gotSlug)
	}
	// Stored creds must carry the refresh token + email, and NOT the grant_id.
	got := string(stored.SourceCredentials)
	if !strings.Contains(got, `"refresh_token":"1//refresh"`) || !strings.Contains(got, `"email":"user@gmail.com"`) {
		t.Fatalf("stored creds missing refresh token/email: %s", got)
	}
	if strings.Contains(got, "grant_id") {
		t.Fatalf("stored creds still carry grant_id: %s", got)
	}
}

func TestCreatePipeline_GrantExpiredOrUsed(t *testing.T) {
	svc := &workspace.PipelineService{
		Workspaces: acmeCustomers(),
		Pipelines: &mockPipelineRepo{
			createPipelineFn: func(context.Context, *workspace.Pipeline) error {
				t.Fatal("pipeline must not be created when the grant is invalid")
				return nil
			},
		},
		GoogleOAuth: &mockGrantStore{
			consumeFn: func(context.Context, string, string) (*workspace.GoogleOAuthGrant, error) {
				return nil, workspace.ErrOAuthGrantNotFound
			},
		},
		Logger: slog.Default(),
	}

	p := &workspace.Pipeline{
		Name:              "sheet",
		SourceType:        "google_sheets",
		SourceConfig:      json.RawMessage(`{"spreadsheet_url_or_id":"abc123"}`),
		SourceCredentials: json.RawMessage(`{"oauth":{"grant_id":"g-1"}}`),
		DatasetName:       "ds",
	}
	_, err := svc.CreatePipeline(context.Background(), "user-1", p)
	var credErr *workspace.ErrInvalidSourceCredentials
	if !errors.As(err, &credErr) {
		t.Fatalf("want ErrInvalidSourceCredentials, got %v", err)
	}
}

func TestCreatePipeline_OAuthDisabled(t *testing.T) {
	svc := &workspace.PipelineService{
		Workspaces: acmeCustomers(),
		Pipelines: &mockPipelineRepo{
			createPipelineFn: func(context.Context, *workspace.Pipeline) error {
				t.Fatal("pipeline must not be created when OAuth is disabled")
				return nil
			},
		},
		// GoogleOAuth nil = disabled
		Logger: slog.Default(),
	}

	p := &workspace.Pipeline{
		Name:              "sheet",
		SourceType:        "google_sheets",
		SourceConfig:      json.RawMessage(`{"spreadsheet_url_or_id":"abc123"}`),
		SourceCredentials: json.RawMessage(`{"oauth":{"grant_id":"g-1"}}`),
		DatasetName:       "ds",
	}
	if _, err := svc.CreatePipeline(context.Background(), "user-1", p); err == nil {
		t.Fatal("expected error when OAuth is disabled")
	}
}

func TestGetEnabledPipelines_InjectsOAuthClient(t *testing.T) {
	svc := &workspace.PipelineService{
		Workspaces: acmeCustomers(),
		Pipelines: &mockPipelineRepo{
			getEnabledPipelinesFn: func(context.Context, string) ([]workspace.Pipeline, error) {
				return []workspace.Pipeline{
					{
						ID:                "p-oauth",
						SourceType:        "google_sheets",
						SourceCredentials: json.RawMessage(`{"oauth":{"refresh_token":"1//refresh","email":"u@gmail.com"}}`),
					},
					{
						ID:                "p-sa",
						SourceType:        "google_sheets",
						SourceCredentials: json.RawMessage(`{"service_account_key":{"client_email":"x@y.iam","private_key":"k"}}`),
					},
				}, nil
			},
		},
		OAuthClientID:     "central-cid",
		OAuthClientSecret: "central-csecret",
		Logger:            slog.Default(),
	}

	got, err := svc.GetEnabledPipelines(context.Background(), "acme")
	if err != nil {
		t.Fatalf("GetEnabledPipelines() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 pipelines, got %d", len(got))
	}

	oauth := string(got[0].SourceCredentials)
	if !strings.Contains(oauth, `"client_id":"central-cid"`) || !strings.Contains(oauth, `"client_secret":"central-csecret"`) {
		t.Fatalf("oauth pipeline missing injected client creds: %s", oauth)
	}
	if !strings.Contains(oauth, `"refresh_token":"1//refresh"`) {
		t.Fatalf("oauth pipeline lost refresh token: %s", oauth)
	}

	// Service-account pipeline is left untouched (no client_id injected).
	if strings.Contains(string(got[1].SourceCredentials), "client_id") {
		t.Fatalf("service-account pipeline was wrongly injected: %s", got[1].SourceCredentials)
	}
}

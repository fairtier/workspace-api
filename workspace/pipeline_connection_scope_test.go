package workspace_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

// mockConnectionStore serves one workspace connection by id; every other id is
// not found.
type mockConnectionStore struct{ conn *workspace.Connection }

func (m *mockConnectionStore) CreateConnection(context.Context, *workspace.Connection) error {
	return nil
}

func (m *mockConnectionStore) ReauthorizeConnection(context.Context, string, string, json.RawMessage) error {
	return nil
}

func (m *mockConnectionStore) GetConnection(_ context.Context, _, id string) (*workspace.Connection, error) {
	if m.conn == nil || m.conn.ID != id {
		return nil, workspace.ErrConnectionNotFound
	}
	return m.conn, nil
}

func (m *mockConnectionStore) ListConnections(context.Context, string) ([]workspace.Connection, error) {
	if m.conn == nil {
		return nil, nil
	}
	return []workspace.Connection{*m.conn}, nil
}

func (m *mockConnectionStore) DeleteConnection(context.Context, string, string) error { return nil }

// googleConnection builds a stored google connection whose consent granted
// exactly the given scopes (none = a connection minted before scopes were
// recorded).
func googleConnection(id string, scopes ...string) *mockConnectionStore {
	creds := map[string]any{"refresh_token": "1//refresh", "email": "user@gmail.com", "client_id": "acme-cid"}
	if len(scopes) > 0 {
		creds["scopes"] = scopes
	}
	raw, err := json.Marshal(creds)
	if err != nil {
		panic(err)
	}
	return &mockConnectionStore{conn: &workspace.Connection{
		ID:           id,
		CustomerSlug: "acme",
		Type:         workspace.ConnectionTypeGoogle,
		Name:         "user@gmail.com",
		Status:       "active",
		Credentials:  raw,
	}}
}

// driveP is a duckdb/gdrive pipeline referencing a workspace connection —
// exactly what the Console's connection picker saves.
func driveP(connID string) *workspace.Pipeline {
	return &workspace.Pipeline{
		Name:              "drive invoices",
		SourceType:        "duckdb",
		SourceConfig:      json.RawMessage(`{"extension":"gdrive","tables":[{"name":"invoices","query":"SELECT page, text FROM read_pdf('gdrive://id:abc')"}]}`),
		SourceCredentials: json.RawMessage(`{"oauth":{"connection_id":"` + connID + `"}}`),
		DatasetName:       "ds",
	}
}

func scopeSvc(conns *mockConnectionStore, stored **workspace.Pipeline) *workspace.PipelineService {
	return &workspace.PipelineService{
		Workspaces: acmeCustomers(),
		Pipelines: &mockPipelineRepo{
			createPipelineFn: func(_ context.Context, p *workspace.Pipeline) error {
				*stored = p
				return nil
			},
		},
		Connections: conns,
		Logger:      slog.Default(),
	}
}

// A Drive pipeline pointed at an account that only ever consented to Sheets is
// refused where the user can still fix it. Without this the token is stored,
// the pipeline saves clean, and Google returns 403 inside a scheduled run on
// the box hours later.
func TestCreatePipeline_RefusesDriveOnSheetsOnlyConnection(t *testing.T) {
	var stored *workspace.Pipeline
	svc := scopeSvc(googleConnection("c-1", "openid", "email", core.GoogleSheetsReadonlyScope), &stored)

	_, err := svc.CreatePipeline(context.Background(), "user-1", driveP("c-1"))
	if err == nil {
		t.Fatal("expected a Sheets-only connection to be refused for a Drive pipeline")
	}
	var invalid *workspace.ErrInvalidSourceCredentials
	if !errors.As(err, &invalid) || invalid.Field != "oauth" {
		t.Fatalf("want a field error on oauth, got %#v", err)
	}
	// The message has to name the fix, not the scope string.
	if !strings.Contains(invalid.Msg, "reconnect") {
		t.Fatalf("refusal does not name the fix: %q", invalid.Msg)
	}
	if stored != nil {
		t.Fatal("pipeline was stored despite the refusal")
	}
}

func TestCreatePipeline_AcceptsDriveConnection(t *testing.T) {
	var stored *workspace.Pipeline
	svc := scopeSvc(googleConnection("c-1", "openid", "email",
		core.GoogleSheetsReadonlyScope, core.GoogleDriveFileScope), &stored)

	if _, err := svc.CreatePipeline(context.Background(), "user-1", driveP("c-1")); err != nil {
		t.Fatalf("CreatePipeline() error = %v", err)
	}
	// The reference is stored as-is: resolution happens at serve/render time
	// so the pipeline follows the connection.
	if got := string(stored.SourceCredentials); !strings.Contains(got, `"connection_id":"c-1"`) {
		t.Fatalf("stored creds lost the connection reference: %s", got)
	}
}

// A connection granted before scopes were recorded carries none, and that has
// to read as "unknown", not as "nothing granted" — refusing it would break a
// working pipeline over a measurement we never took.
func TestCreatePipeline_AcceptsConnectionWithNoRecordedScopes(t *testing.T) {
	var stored *workspace.Pipeline
	svc := scopeSvc(googleConnection("c-1"), &stored)

	if _, err := svc.CreatePipeline(context.Background(), "user-1", driveP("c-1")); err != nil {
		t.Fatalf("CreatePipeline() error = %v", err)
	}
}

// google_sheets needs nothing beyond the base consent, so a Sheets-only
// connection must stay perfectly valid for it.
func TestCreatePipeline_SheetsConnectionNeedsNoDriveScope(t *testing.T) {
	var stored *workspace.Pipeline
	svc := scopeSvc(googleConnection("c-1", "openid", "email", core.GoogleSheetsReadonlyScope), &stored)

	p := &workspace.Pipeline{
		Name:              "sheet",
		SourceType:        "google_sheets",
		SourceConfig:      json.RawMessage(`{"spreadsheet_url_or_id":"abc"}`),
		SourceCredentials: json.RawMessage(`{"oauth":{"connection_id":"c-1"}}`),
		DatasetName:       "ds",
	}
	if _, err := svc.CreatePipeline(context.Background(), "user-1", p); err != nil {
		t.Fatalf("CreatePipeline() error = %v", err)
	}
}

package workspace_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

// mockResolver satisfies workspace.Resolver.
type mockResolver struct {
	ws *workspace.Workspace
}

func (m *mockResolver) GetWorkspace(context.Context, string) (*workspace.Workspace, error) {
	return m.ws, nil
}

func (m *mockResolver) GetWorkspaceByUser(context.Context, core.UserID) (*workspace.Workspace, error) {
	return m.ws, nil
}

// mockCasdoorAppManager satisfies workspace.CasdoorAppManager.
type mockCasdoorAppManager struct {
	listAppsFn func(ctx context.Context, org string) ([]core.CasdoorApp, error)
}

func (m *mockCasdoorAppManager) AddApp(context.Context, string, string) (*core.CasdoorApp, error) {
	return &core.CasdoorApp{Name: "lk-acme-jane", ClientID: "app-cid", ClientSecret: "app-secret"}, nil
}

func (m *mockCasdoorAppManager) DeleteApp(context.Context, string, string) error { return nil }

func (m *mockCasdoorAppManager) ListApps(ctx context.Context, org string) ([]core.CasdoorApp, error) {
	return m.listAppsFn(ctx, org)
}

// mockTokenProvider satisfies workspace.TokenProvider.
type mockTokenProvider struct{}

func (m *mockTokenProvider) GetClientToken(context.Context, string, string, string) (string, error) {
	return "tok", nil
}

// mockLakekeeperClient satisfies workspace.LakekeeperClient.
type mockLakekeeperClient struct {
	listWarehousesFn          func(ctx context.Context, url, token string) ([]core.Warehouse, error)
	getWarehouseAssignmentsFn func(ctx context.Context, url, token, warehouseID string) ([]core.WarehouseAssignment, error)
}

func (m *mockLakekeeperClient) CreateWarehouse(context.Context, string, string, string, core.S3Config) (string, error) {
	return "", nil
}

func (m *mockLakekeeperClient) ListWarehouses(ctx context.Context, url, token string) ([]core.Warehouse, error) {
	return m.listWarehousesFn(ctx, url, token)
}

func (m *mockLakekeeperClient) GetWarehouseID(context.Context, string, string, string) (string, error) {
	return "", nil
}

func (m *mockLakekeeperClient) CreateUser(context.Context, string, string, string, string) error {
	return nil
}

func (m *mockLakekeeperClient) DeleteUser(context.Context, string, string, string) error {
	return nil
}

func (m *mockLakekeeperClient) AssignWarehouseRole(context.Context, string, string, string, string, string) error {
	return nil
}

func (m *mockLakekeeperClient) RemoveWarehouseRole(context.Context, string, string, string, string) error {
	return nil
}

func (m *mockLakekeeperClient) GetWarehouseAssignments(ctx context.Context, url, token, warehouseID string) ([]core.WarehouseAssignment, error) {
	return m.getWarehouseAssignmentsFn(ctx, url, token, warehouseID)
}

// TestAddUser_NilAudienceUpdater exercises the syncAudiences nil guard: a
// non-VM workspace with no AudienceUpdater wired (the box binary never sets
// one) must complete AddUser without panicking — there is no shared-cluster
// Secret to converge.
func TestAddUser_NilAudienceUpdater(t *testing.T) {
	ws := &workspace.Workspace{
		Slug:          "acme",
		OnVM:          false,
		LakekeeperURL: "https://lk.example.com",
		CasdoorOrg:    "customer-acme",
	}

	svc := &workspace.LakekeeperUserService{
		Workspaces:  &mockResolver{ws: ws},
		CasdoorApps: &mockCasdoorAppManager{},
		Tokens:      &mockTokenProvider{},
		Lakekeeper:  &mockLakekeeperClient{},
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	got, err := svc.AddUser(context.Background(), "caller", "jane", "reader", "default")
	if err != nil {
		t.Fatalf("AddUser: %v", err)
	}
	if got.ClientID != "app-cid" {
		t.Errorf("ClientID = %q, want app-cid", got.ClientID)
	}
}

func TestListUsers_WarehousePerUser(t *testing.T) {
	ws := &workspace.Workspace{
		Slug:                "acme",
		Namespace:           "customer-acme",
		CasdoorOrg:          "customer-acme",
		LakekeeperURL:       "https://lk.example.com",
		OIDCClientID:        "cid",
		OIDCClientSecret:    "secret",
		LakekeeperWarehouse: "default",
	}

	svc := &workspace.LakekeeperUserService{
		Workspaces: &mockResolver{ws: ws},
		CasdoorApps: &mockCasdoorAppManager{
			listAppsFn: func(context.Context, string) ([]core.CasdoorApp, error) {
				return []core.CasdoorApp{
					{Name: "lk-acme-jane"},
					{Name: "lk-acme-bob"},
					{Name: "lk-acme-nobody"},
				}, nil
			},
		},
		Tokens: &mockTokenProvider{},
		Lakekeeper: &mockLakekeeperClient{
			listWarehousesFn: func(context.Context, string, string) ([]core.Warehouse, error) {
				// Return non-default first to verify default-first reordering.
				return []core.Warehouse{
					{ID: "wh-analytics", Name: "analytics"},
					{ID: "wh-default", Name: "default"},
				}, nil
			},
			getWarehouseAssignmentsFn: func(_ context.Context, _, _, warehouseID string) ([]core.WarehouseAssignment, error) {
				switch warehouseID {
				case "wh-default":
					return []core.WarehouseAssignment{
						{UserID: "oidc~admin/lk-acme-jane", Relation: "ownership"},
					}, nil
				case "wh-analytics":
					return []core.WarehouseAssignment{
						{UserID: "oidc~admin/lk-acme-bob", Relation: "select"},
						// jane also on analytics, but default must win.
						{UserID: "oidc~admin/lk-acme-jane", Relation: "select"},
					}, nil
				}
				return nil, nil
			},
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	results, err := svc.ListUsers(context.Background(), "caller")
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}

	byName := map[string]workspace.LakekeeperUserResult{}
	for _, r := range results {
		byName[r.Name] = r
	}

	if got := byName["jane"]; got.Warehouse != "default" || got.Role != "admin" {
		t.Errorf("jane = %+v, want warehouse=default role=admin", got)
	}
	if got := byName["bob"]; got.Warehouse != "analytics" || got.Role != "reader" {
		t.Errorf("bob = %+v, want warehouse=analytics role=reader", got)
	}
	if got := byName["nobody"]; got.Warehouse != "" || got.Role != "" {
		t.Errorf("nobody = %+v, want empty warehouse/role", got)
	}
}

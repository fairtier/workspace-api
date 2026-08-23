package workspace_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
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
	listAppsFn  func(ctx context.Context, org string) ([]core.CasdoorApp, error)
	addedApps   []string
	deletedApps []string
}

func (m *mockCasdoorAppManager) AddApp(_ context.Context, _, name string) (*core.CasdoorApp, error) {
	m.addedApps = append(m.addedApps, name)
	return &core.CasdoorApp{Name: "lk-acme-jane", ClientID: "app-cid", ClientSecret: "app-secret"}, nil
}

func (m *mockCasdoorAppManager) DeleteApp(_ context.Context, _, name string) error {
	m.deletedApps = append(m.deletedApps, name)
	return nil
}

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
	deletedUsers              []string
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

func (m *mockLakekeeperClient) DeleteUser(_ context.Context, _, _, id string) error {
	m.deletedUsers = append(m.deletedUsers, id)
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
		Slug:             "acme",
		OnVM:             false,
		LakekeeperURL:    "https://lk.example.com",
		CasdoorOrg:       "customer-acme",
		OIDCClientID:     "cid",
		OIDCClientSecret: "secret",
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

// TestRemoveUser_RejectsForeignAppNames pins the RemoveUser authorization
// boundary: the Casdoor app deletion is keyed on the name alone, so any
// A box whose WORKSPACE_OIDC_CLIENT_ID/_SECRET Secret exists but is empty can
// reach neither Casdoor's admin API nor Lakekeeper. Before the guard, every
// call failed as an opaque CodeInternal carrying whatever the Casdoor SDK
// said; it must now be a typed, mappable precondition — and, crucially, must
// not touch Casdoor or Lakekeeper on the way out.
func TestLakekeeperUsers_UnavailableWithoutOIDCCredentials(t *testing.T) {
	newService := func() (*workspace.LakekeeperUserService, *mockCasdoorAppManager, *mockLakekeeperClient) {
		ws := &workspace.Workspace{
			Slug:          "acme",
			LakekeeperURL: "https://lk.example.com",
			CasdoorOrg:    "customer-acme",
			// OIDCClientID / OIDCClientSecret deliberately unset.
		}
		casdoor := &mockCasdoorAppManager{}
		lakekeeper := &mockLakekeeperClient{}
		return &workspace.LakekeeperUserService{
			Workspaces:  &mockResolver{ws: ws},
			CasdoorApps: casdoor,
			Tokens:      &mockTokenProvider{},
			Lakekeeper:  lakekeeper,
			Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		}, casdoor, lakekeeper
	}

	t.Run("AddUser", func(t *testing.T) {
		svc, casdoor, lakekeeper := newService()
		_, err := svc.AddUser(context.Background(), "caller", "jane", "reader", "default")
		if !errors.Is(err, workspace.ErrLakekeeperUsersUnavailable) {
			t.Errorf("AddUser = %v, want ErrLakekeeperUsersUnavailable", err)
		}
		if len(casdoor.addedApps) != 0 || len(lakekeeper.deletedUsers) != 0 {
			t.Errorf("touched Casdoor/Lakekeeper: apps=%v users=%v", casdoor.addedApps, lakekeeper.deletedUsers)
		}
	})

	t.Run("RemoveUser", func(t *testing.T) {
		svc, casdoor, _ := newService()
		err := svc.RemoveUser(context.Background(), "caller", "lk-acme-jane")
		if !errors.Is(err, workspace.ErrLakekeeperUsersUnavailable) {
			t.Errorf("RemoveUser = %v, want ErrLakekeeperUsersUnavailable", err)
		}
		if len(casdoor.deletedApps) != 0 {
			t.Errorf("Casdoor apps deleted: %v, want none", casdoor.deletedApps)
		}
	})

	t.Run("ListUsers", func(t *testing.T) {
		svc, _, _ := newService()
		if _, err := svc.ListUsers(context.Background(), "caller"); !errors.Is(err, workspace.ErrLakekeeperUsersUnavailable) {
			t.Errorf("ListUsers = %v, want ErrLakekeeperUsersUnavailable", err)
		}
	})

	// An unprovisioned customer must still report *that*, not the new error:
	// "service account management is not available" would send an operator
	// looking at the box's Casdoor Secret instead of at provisioning.
	t.Run("no LakekeeperURL still reports not provisioned", func(t *testing.T) {
		ws := &workspace.Workspace{Slug: "acme", CasdoorOrg: "customer-acme"}
		svc := &workspace.LakekeeperUserService{
			Workspaces:  &mockResolver{ws: ws},
			CasdoorApps: &mockCasdoorAppManager{},
			Tokens:      &mockTokenProvider{},
			Lakekeeper:  &mockLakekeeperClient{},
			Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		if _, err := svc.ListUsers(context.Background(), "caller"); !errors.Is(err, workspace.ErrCustomerNotProvisioned) {
			t.Errorf("ListUsers = %v, want ErrCustomerNotProvisioned", err)
		}
	})
}

// user_id outside the workspace's own "lk-<slug>-" namespace must be
// rejected before Lakekeeper roles or the Casdoor app are touched.
func TestRemoveUser_RejectsForeignAppNames(t *testing.T) {
	for _, userID := range []string{
		"dlt-worker-acme", // the pipeline worker's service account
		"acme-console",    // the workspace's own OIDC application
		"lk-other-jane",   // another tenant's namespace on a shared Casdoor
		"lk-acme-",        // namespace prefix with an empty user name
		"lk-acme",         // prefix without the trailing separator
		"",
	} {
		t.Run(userID, func(t *testing.T) {
			ws := &workspace.Workspace{
				Slug:             "acme",
				LakekeeperURL:    "https://lk.example.com",
				CasdoorOrg:       "customer-acme",
				OIDCClientID:     "cid",
				OIDCClientSecret: "secret",
			}
			casdoor := &mockCasdoorAppManager{}
			lakekeeper := &mockLakekeeperClient{
				listWarehousesFn: func(context.Context, string, string) ([]core.Warehouse, error) {
					return []core.Warehouse{{ID: "wh-default", Name: "default"}}, nil
				},
			}
			svc := &workspace.LakekeeperUserService{
				Workspaces:  &mockResolver{ws: ws},
				CasdoorApps: casdoor,
				Tokens:      &mockTokenProvider{},
				Lakekeeper:  lakekeeper,
				Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
			}

			err := svc.RemoveUser(context.Background(), "caller", userID)
			if !errors.Is(err, core.ErrUserNotFound) {
				t.Errorf("RemoveUser(%q) = %v, want core.ErrUserNotFound", userID, err)
			}
			if len(casdoor.deletedApps) != 0 {
				t.Errorf("Casdoor apps deleted: %v, want none", casdoor.deletedApps)
			}
			if len(lakekeeper.deletedUsers) != 0 {
				t.Errorf("Lakekeeper users deleted: %v, want none", lakekeeper.deletedUsers)
			}
		})
	}
}

func TestRemoveUser_OwnNamespace(t *testing.T) {
	ws := &workspace.Workspace{
		Slug:             "acme",
		LakekeeperURL:    "https://lk.example.com",
		CasdoorOrg:       "customer-acme",
		OIDCClientID:     "cid",
		OIDCClientSecret: "secret",
	}
	casdoor := &mockCasdoorAppManager{}
	lakekeeper := &mockLakekeeperClient{
		listWarehousesFn: func(context.Context, string, string) ([]core.Warehouse, error) {
			return []core.Warehouse{{ID: "wh-default", Name: "default"}}, nil
		},
	}
	svc := &workspace.LakekeeperUserService{
		Workspaces:  &mockResolver{ws: ws},
		CasdoorApps: casdoor,
		Tokens:      &mockTokenProvider{},
		Lakekeeper:  lakekeeper,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	if err := svc.RemoveUser(context.Background(), "caller", "lk-acme-jane"); err != nil {
		t.Fatalf("RemoveUser: %v", err)
	}
	if got, want := casdoor.deletedApps, []string{"lk-acme-jane"}; !slices.Equal(got, want) {
		t.Errorf("Casdoor apps deleted = %v, want %v", got, want)
	}
	if got, want := lakekeeper.deletedUsers, []string{"oidc~admin/lk-acme-jane"}; !slices.Equal(got, want) {
		t.Errorf("Lakekeeper users deleted = %v, want %v", got, want)
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

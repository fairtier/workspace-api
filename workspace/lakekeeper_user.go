package workspace

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/fairtier/workspace-api/core"
)

// LakekeeperUserService orchestrates user management across Casdoor and Lakekeeper.
type LakekeeperUserService struct {
	Workspaces  Resolver
	CasdoorApps CasdoorAppManager
	// CasdoorAppsFor, when set, resolves the app manager per workspace: VM
	// boxes run their own Casdoor (Workspace.CasdoorIssuer), so their
	// service-account apps must be created there, not centrally. Falls back
	// to CasdoorApps when nil or when it returns nil.
	CasdoorAppsFor func(ws *Workspace) CasdoorAppManager
	Lakekeeper     LakekeeperClient
	Tokens         TokenProvider
	Audiences      AudienceUpdater
	Logger         *slog.Logger
}

// casdoorAppsFor picks the Casdoor app manager for a customer (box-local on
// VM substrate, central otherwise).
func (s *LakekeeperUserService) casdoorAppsFor(ws *Workspace) CasdoorAppManager {
	if s.CasdoorAppsFor != nil {
		if m := s.CasdoorAppsFor(ws); m != nil {
			return m
		}
	}
	return s.CasdoorApps
}

// LakekeeperUserResult is returned from AddUser and ListUsers.
type LakekeeperUserResult struct {
	ID           string // Casdoor app name (e.g. "lk-acme-jane-doe")
	Name         string
	Role         string
	Warehouse    string // Lakekeeper warehouse the user has access to
	ClientID     string // Only set on AddUser
	ClientSecret string // Only set on AddUser
}

// appName returns the Casdoor application name for a data platform user.
func appName(slug, userName string) string {
	return "lk-" + slug + "-" + userName
}

// displayName strips the "lk-{slug}-" prefix from a Casdoor app name
// to recover the user-friendly name that was originally passed to AddUser.
func displayName(slug, casdoorAppName string) string {
	prefix := "lk-" + slug + "-"
	if after, ok := strings.CutPrefix(casdoorAppName, prefix); ok {
		return after
	}
	return casdoorAppName
}

// isDataPlatformApp reports whether casdoorAppName is a data platform user
// app that AddUser could have created for the given workspace — i.e. it lies
// in the workspace's own "lk-<slug>-" namespace and has a non-empty user
// name. It is the authorization boundary for RemoveUser: Casdoor deletes an
// application by owner-qualified name alone, so any other name (the
// workspace's OIDC app, the dlt-worker's service account, another tenant's
// namespace on a shared Casdoor) must never reach the delete call.
func isDataPlatformApp(slug, casdoorAppName string) bool {
	name, ok := strings.CutPrefix(casdoorAppName, "lk-"+slug+"-")
	return ok && name != ""
}

// lakekeeperID returns the Lakekeeper principal ID for a Casdoor application.
// Casdoor client_credentials JWT has sub = "admin/<app_name>".
func lakekeeperID(appName string) string {
	return "oidc~admin/" + appName
}

// AddUser creates a per-user Casdoor application and registers it in Lakekeeper with the given role on the specified warehouse.
func (s *LakekeeperUserService) AddUser(ctx context.Context, callerID core.UserID, name, role, warehouseName string) (*LakekeeperUserResult, error) {
	if err := validateRole(role); err != nil {
		return nil, err
	}

	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}

	if ws.LakekeeperURL == "" {
		return nil, ErrCustomerNotProvisioned
	}

	org := ws.CasdoorOrg
	an := appName(ws.Slug, name)

	app, err := s.casdoorAppsFor(ws).AddApp(ctx, org, an)
	if err != nil {
		return nil, fmt.Errorf("casdoor add app: %w", err)
	}

	// Compensate: delete the Casdoor app if any Lakekeeper step fails.
	cleanup := func() {
		if err := s.casdoorAppsFor(ws).DeleteApp(ctx, org, an); err != nil {
			s.Logger.ErrorContext(ctx, "failed to clean up Casdoor app after Lakekeeper error", "app", an, "error", err)
		}
	}

	token, err := s.Tokens.GetClientToken(ctx, ws.CasdoorIssuer, ws.OIDCClientID, ws.OIDCClientSecret)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("get lakekeeper token: %w", err)
	}

	lkID := lakekeeperID(an)

	if err := s.registerLakekeeperUser(ctx, ws, token, lkID, name, role, warehouseName); err != nil {
		cleanup()
		return nil, err
	}

	if err := s.syncAudiences(ctx, ws); err != nil {
		// Clean up both the Lakekeeper user and the Casdoor app.
		if delErr := s.Lakekeeper.DeleteUser(ctx, ws.LakekeeperServiceURL(), token, lkID); delErr != nil {
			s.Logger.ErrorContext(ctx, "failed to clean up Lakekeeper user after audience sync error", "user", lkID, "error", delErr)
		}
		cleanup()
		return nil, fmt.Errorf("sync audiences: %w", err)
	}

	return &LakekeeperUserResult{
		ID:           an,
		Name:         name,
		Role:         role,
		Warehouse:    warehouseName,
		ClientID:     app.ClientID,
		ClientSecret: app.ClientSecret,
	}, nil
}

// registerLakekeeperUser creates the Lakekeeper user and assigns it the given
// role on the named warehouse. On failure the caller is responsible for cleanup.
func (s *LakekeeperUserService) registerLakekeeperUser(ctx context.Context, ws *Workspace, token, lkID, name, role, warehouseName string) error {
	serviceURL := ws.LakekeeperServiceURL()

	if err := s.Lakekeeper.CreateUser(ctx, serviceURL, token, lkID, name); err != nil {
		return fmt.Errorf("lakekeeper create user: %w", err)
	}

	warehouseID, err := s.Lakekeeper.GetWarehouseID(ctx, serviceURL, token, warehouseName)
	if err != nil {
		return fmt.Errorf("get warehouse ID: %w", err)
	}

	if err := s.Lakekeeper.AssignWarehouseRole(ctx, serviceURL, token, warehouseID, lkID, role); err != nil {
		return fmt.Errorf("assign warehouse role: %w", err)
	}

	return nil
}

// RemoveUser removes a user from Lakekeeper and Casdoor.
// Removes warehouse roles across all warehouses to ensure full cleanup.
func (s *LakekeeperUserService) RemoveUser(ctx context.Context, callerID core.UserID, userID string) error {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return fmt.Errorf("get customer: %w", err)
	}

	if ws.LakekeeperURL == "" {
		return ErrCustomerNotProvisioned
	}

	if !isDataPlatformApp(ws.Slug, userID) {
		return core.ErrUserNotFound
	}

	token, err := s.Tokens.GetClientToken(ctx, ws.CasdoorIssuer, ws.OIDCClientID, ws.OIDCClientSecret)
	if err != nil {
		return fmt.Errorf("get lakekeeper token: %w", err)
	}

	lkID := lakekeeperID(userID)

	if err := s.removeWarehouseRoles(ctx, ws, token, lkID); err != nil {
		return err
	}

	if err := s.Lakekeeper.DeleteUser(ctx, ws.LakekeeperServiceURL(), token, lkID); err != nil {
		return fmt.Errorf("lakekeeper delete user: %w", err)
	}

	org := ws.CasdoorOrg
	if err := s.casdoorAppsFor(ws).DeleteApp(ctx, org, userID); err != nil {
		return fmt.Errorf("casdoor delete app: %w", err)
	}

	// Best-effort audience sync — the user is already deleted, so don't fail
	// the whole operation. The audience list will be corrected on next add/remove.
	if err := s.syncAudiences(ctx, ws); err != nil {
		s.Logger.ErrorContext(ctx, "failed to sync audiences after user removal", "customer", ws.Slug, "error", err)
	}

	return nil
}

// removeWarehouseRoles strips the principal's role from every warehouse,
// ensuring full cleanup before the principal itself is deleted.
func (s *LakekeeperUserService) removeWarehouseRoles(ctx context.Context, ws *Workspace, token, lkID string) error {
	warehouses, err := s.Lakekeeper.ListWarehouses(ctx, ws.LakekeeperServiceURL(), token)
	if err != nil {
		return fmt.Errorf("list warehouses: %w", err)
	}

	for _, w := range warehouses {
		if err := s.Lakekeeper.RemoveWarehouseRole(ctx, ws.LakekeeperServiceURL(), token, w.ID, lkID); err != nil {
			return fmt.Errorf("remove warehouse role for %s: %w", w.Name, err)
		}
	}
	return nil
}

// ListUsers returns all data platform users for the caller's ws,
// including their roles on the default warehouse.
func (s *LakekeeperUserService) ListUsers(ctx context.Context, callerID core.UserID) ([]LakekeeperUserResult, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}

	if ws.LakekeeperURL == "" {
		return nil, ErrCustomerNotProvisioned
	}

	org := ws.CasdoorOrg

	apps, err := s.casdoorAppsFor(ws).ListApps(ctx, org)
	if err != nil {
		return nil, fmt.Errorf("list casdoor apps: %w", err)
	}

	// Build maps of Lakekeeper principal ID → role and → warehouse name from
	// warehouse assignments.
	roleByPrincipal := make(map[string]string)
	warehouseByPrincipal := make(map[string]string)

	s.fillWarehouseAssignments(ctx, ws, roleByPrincipal, warehouseByPrincipal)

	results := make([]LakekeeperUserResult, 0, len(apps))
	for _, a := range apps {
		principalID := lakekeeperID(a.Name)
		results = append(results, LakekeeperUserResult{
			ID:        a.Name,
			Name:      displayName(ws.Slug, a.Name),
			Role:      roleByPrincipal[principalID],
			Warehouse: warehouseByPrincipal[principalID],
		})
	}
	return results, nil
}

// fillWarehouseAssignments populates roleByPrincipal and warehouseByPrincipal
// with each user's derived role and the warehouse it applies to. The customer's
// default warehouse is checked first so that, for users with a role on multiple
// warehouses, the default wins (stable with the historic single-warehouse
// behaviour). Errors are logged as warnings; on failure the maps are left as-is.
func (s *LakekeeperUserService) fillWarehouseAssignments(ctx context.Context, ws *Workspace, roleByPrincipal, warehouseByPrincipal map[string]string) {
	token, err := s.Tokens.GetClientToken(ctx, ws.CasdoorIssuer, ws.OIDCClientID, ws.OIDCClientSecret)
	if err != nil {
		s.Logger.WarnContext(ctx, "failed to get token for role lookup, roles will be empty", "error", err)
		return
	}

	serviceURL := ws.LakekeeperServiceURL()

	warehouses, err := s.Lakekeeper.ListWarehouses(ctx, serviceURL, token)
	if err != nil {
		s.Logger.WarnContext(ctx, "failed to list warehouses for role lookup, roles will be empty", "error", err)
		return
	}

	defaultWh := ws.LakekeeperWarehouse
	if defaultWh == "" {
		defaultWh = "default"
	}
	// Reorder so the default warehouse is inspected first.
	ordered := make([]core.Warehouse, 0, len(warehouses))
	for _, w := range warehouses {
		if w.Name == defaultWh {
			ordered = append([]core.Warehouse{w}, ordered...)
		} else {
			ordered = append(ordered, w)
		}
	}

	for _, w := range ordered {
		assignments, err := s.Lakekeeper.GetWarehouseAssignments(ctx, serviceURL, token, w.ID)
		if err != nil {
			s.Logger.WarnContext(ctx, "failed to get warehouse assignments, skipping warehouse", "warehouse", w.Name, "error", err)
			continue
		}
		applyWarehouseAssignments(w.Name, assignments, roleByPrincipal, warehouseByPrincipal)
	}
}

// applyWarehouseAssignments records each user's derived role for one warehouse,
// keeping the first (default-first) warehouse where a user has a role.
func applyWarehouseAssignments(warehouseName string, assignments []core.WarehouseAssignment, roleByPrincipal, warehouseByPrincipal map[string]string) {
	relsByUser := make(map[string][]string)
	for _, a := range assignments {
		relsByUser[a.UserID] = append(relsByUser[a.UserID], a.Relation)
	}
	for uid, rels := range relsByUser {
		role := relationsToRole(rels)
		if role == "" {
			continue
		}
		// First warehouse (default-first) where the user has a role wins.
		if _, seen := roleByPrincipal[uid]; !seen {
			roleByPrincipal[uid] = role
			warehouseByPrincipal[uid] = warehouseName
		}
	}
}

// syncAudiences collects all service account client IDs for this customer
// and updates the Lakekeeper OIDC audience Secret + triggers a restart.
//
// VM boxes are skipped: the audience Secret lives on the box, converged by
// the box's casdoor-audience-sync CronJob from the box's own
// Casdoor — the kube.AudienceUpdater below only reaches the shared cluster.
// The box CronJob picks up a new/removed service account within one tick.
//
// A nil Audiences means this deployment has no shared-cluster Secret to
// converge (the box binary never wires one), so there is nothing to sync.
func (s *LakekeeperUserService) syncAudiences(ctx context.Context, ws *Workspace) error {
	if s.Audiences == nil || ws.OnVM {
		return nil
	}

	org := ws.CasdoorOrg

	apps, err := s.casdoorAppsFor(ws).ListApps(ctx, org)
	if err != nil {
		return fmt.Errorf("list casdoor apps: %w", err)
	}

	// Main app audience first, then all service account audiences.
	audiences := make([]string, 0, len(apps)+1)
	audiences = append(audiences, ws.OIDCClientID)
	for _, a := range apps {
		audiences = append(audiences, a.ClientID)
	}

	return s.Audiences.UpdateAudiences(ctx, ws.Namespace, audiences)
}

func validateRole(role string) error {
	switch role {
	case "reader", "writer", "admin":
		return nil
	default:
		return ErrInvalidRole
	}
}

// relationsToRole maps a set of Lakekeeper warehouse relations to a role name.
// This is the reverse of roleToAssignments in the lakekeeper client.
func relationsToRole(relations []string) string {
	set := make(map[string]bool, len(relations))
	for _, r := range relations {
		set[r] = true
	}

	if set["ownership"] {
		return "admin"
	}
	if set["create"] || set["modify"] {
		return "writer"
	}
	if set["describe"] || set["select"] {
		return "reader"
	}
	return ""
}

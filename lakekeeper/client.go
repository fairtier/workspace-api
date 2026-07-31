package lakekeeper

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	lkclient "github.com/lakekeeper/go-lakekeeper/pkg/client"
	"github.com/lakekeeper/go-lakekeeper/pkg/permissions"

	managementv1 "github.com/lakekeeper/go-lakekeeper/pkg/apis/management/v1"
	credentialv1 "github.com/lakekeeper/go-lakekeeper/pkg/storage/credential"
	profilev1 "github.com/lakekeeper/go-lakekeeper/pkg/storage/profile"

	"github.com/fairtier/workspace-api/core"
)

// defaultProjectID is the nil UUID used by Lakekeeper when ENABLE_DEFAULT_PROJECT=true.
const defaultProjectID = "00000000-0000-0000-0000-000000000000"

// Client implements core.LakekeeperClient via the go-lakekeeper SDK.
type Client struct {
	// HTTPClient is the HTTP client used for WaitForReady (raw HTTP).
	// If nil, http.DefaultClient is used.
	HTTPClient *http.Client
}

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

// sdkClient creates a new go-lakekeeper SDK client for the given URL and token.
func (c *Client) sdkClient(ctx context.Context, lakekeeperURL, token string) (*lkclient.Client, error) {
	return lkclient.New(ctx, lakekeeperURL, token, lkclient.WithoutRetries())
}

// WaitForReady polls the Lakekeeper health endpoint until it responds with 200
// or the context is cancelled. This ensures the pod is fully started before
// attempting bootstrap or other management operations.
func (c *Client) WaitForReady(ctx context.Context, lakekeeperURL string) error {
	url := strings.TrimRight(lakekeeperURL, "/") + "/health"
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("build health request: %w", err)
		}

		resp, err := c.httpClient().Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("lakekeeper not ready: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// Bootstrap performs the one-time initialization of a Lakekeeper catalog.
// Idempotent — an already-bootstrapped server is treated as success.
// POST /management/v1/bootstrap
func (c *Client) Bootstrap(ctx context.Context, lakekeeperURL string, token string) error {
	sdk, err := c.sdkClient(ctx, lakekeeperURL, token)
	if err != nil {
		return fmt.Errorf("bootstrap: create SDK client: %w", err)
	}

	_, err = sdk.ServerAPI.Bootstrap(ctx).BootstrapRequest(*managementv1.NewBootstrapRequest(true)).Execute()
	if err != nil {
		// Already-bootstrapped is the idempotent happy path. Lakekeeper
		// reports it as HTTP 400 with error type "CatalogAlreadyBootstrapped"
		// (not 409), so we must match on the error type, not the status code.
		if isAlreadyBootstrapped(err) {
			return nil
		}
		return fmt.Errorf("bootstrap: %w", err)
	}
	return nil
}

// isAlreadyBootstrapped reports whether err is Lakekeeper's
// "CatalogAlreadyBootstrapped" error, which is the idempotent success case for
// Bootstrap. The SDK surfaces it as a GenericOpenAPIError whose decoded model
// carries the error type (the human-readable message is only the bare HTTP
// status, so we cannot match on that).
func isAlreadyBootstrapped(err error) bool {
	var apiErr *managementv1.GenericOpenAPIError
	if !errors.As(err, &apiErr) {
		return false
	}
	model, ok := apiErr.Model().(managementv1.IcebergErrorResponse)
	if !ok {
		return false
	}
	return model.Error.Type == "CatalogAlreadyBootstrapped"
}

// CreateWarehouse creates a named warehouse in the Lakekeeper catalog.
// POST /management/v1/warehouse
func (c *Client) CreateWarehouse(ctx context.Context, lakekeeperURL string, token string, name string, s3 core.S3Config) (string, error) {
	sdk, err := c.sdkClient(ctx, lakekeeperURL, token)
	if err != nil {
		return "", fmt.Errorf("create warehouse: create SDK client: %w", err)
	}

	storageProfile, storageCred := buildStorageConfig(s3)

	req := managementv1.NewCreateWarehouseRequest(storageProfile, name)
	// Scope to the default project via the body field. It's marked deprecated
	// upstream in favor of the x-project-id header, but the generated
	// CreateWarehouse request exposes no XProjectId builder — the body field is
	// the only per-request lever for this op.
	req.ProjectId = new(defaultProjectID)
	req.SetStorageCredential(storageCred)

	if _, err := sdk.Warehouses.Create(ctx, req); err != nil {
		return "", fmt.Errorf("create warehouse: %w", err)
	}

	return name, nil
}

// UpdateWarehouseStorage replaces the warehouse storage profile (and
// credential, in the same call — Lakekeeper accepts both atomically).
// Used at finalize of a managed↔BYOS migration once bergrebase has
// rewritten table metadata to point at the new bucket.
// POST /management/v1/warehouse/{id}/storage
func (c *Client) UpdateWarehouseStorage(ctx context.Context, lakekeeperURL, token, warehouseID string, s3 core.S3Config) error {
	sdk, err := c.sdkClient(ctx, lakekeeperURL, token)
	if err != nil {
		return fmt.Errorf("update warehouse storage: create SDK client: %w", err)
	}

	storageProfile, storageCred := buildStorageConfig(s3)

	req := managementv1.NewUpdateWarehouseStorageRequest(storageProfile)
	req.SetStorageCredential(storageCred)

	if _, _, err := sdk.WarehouseAPI.UpdateStorageProfile(ctx, warehouseID).
		UpdateWarehouseStorageRequest(*req).Execute(); err != nil {
		return fmt.Errorf("update warehouse storage: %w", err)
	}
	return nil
}

// UpdateWarehouseCredential replaces only the storage credential — used
// when rotating creds without changing the bucket.
// POST /management/v1/warehouse/{id}/storage-credential
func (c *Client) UpdateWarehouseCredential(ctx context.Context, lakekeeperURL, token, warehouseID string, s3 core.S3Config) error {
	sdk, err := c.sdkClient(ctx, lakekeeperURL, token)
	if err != nil {
		return fmt.Errorf("update warehouse credential: create SDK client: %w", err)
	}

	_, storageCred := buildStorageConfig(s3)

	req := managementv1.NewUpdateWarehouseCredentialRequest()
	req.SetNewStorageCredential(storageCred)

	if _, _, err := sdk.WarehouseAPI.UpdateStorageCredential(ctx, warehouseID).
		UpdateWarehouseCredentialRequest(*req).Execute(); err != nil {
		return fmt.Errorf("update warehouse credential: %w", err)
	}
	return nil
}

// buildStorageConfig creates the SDK storage profile and credential based on
// the credential delegation mode.
func buildStorageConfig(s3 core.S3Config) (managementv1.StorageProfile, managementv1.StorageCredential) {
	stsEnabled, remoteSigningEnabled := resolveDelegationFlags(s3)

	opts := s3ProfileOptions(s3, stsEnabled, remoteSigningEnabled)
	profile := profilev1.NewS3Profile(s3.Bucket, s3.Region, opts...)

	// Use cloudflare-r2 credential type when vending with R2 credentials.
	isR2 := s3.CloudflareAPIToken != "" && s3.CloudflareAccountID != ""
	if isR2 && s3.CredentialDelegationMode == "vended" {
		cred := credentialv1.NewS3CloudflareR2(
			s3.AccessKeyID, s3.SecretAccessKey,
			s3.CloudflareAccountID, s3.CloudflareAPIToken,
		)
		return profile, cred
	}

	cred := credentialv1.NewS3AccessKey(s3.AccessKeyID, s3.SecretAccessKey)
	return profile, cred
}

// resolveDelegationFlags derives the STS/remote-signing flags from the S3
// config, letting the explicit CredentialDelegationMode override the raw flags.
func resolveDelegationFlags(s3 core.S3Config) (stsEnabled, remoteSigningEnabled bool) {
	stsEnabled = derefBool(s3.STSEnabled, false)
	remoteSigningEnabled = derefBool(s3.RemoteSigningEnabled, true)

	switch s3.CredentialDelegationMode {
	case "vended":
		stsEnabled = true
		remoteSigningEnabled = false
	case "remote-signing":
		stsEnabled = false
		remoteSigningEnabled = true
	case "none":
		stsEnabled = false
		remoteSigningEnabled = false
	}
	return stsEnabled, remoteSigningEnabled
}

// s3ProfileOptions builds the SDK S3 profile options for the given config and
// resolved delegation flags.
func s3ProfileOptions(s3 core.S3Config, stsEnabled, remoteSigningEnabled bool) []profilev1.S3Option {
	var opts []profilev1.S3Option
	// Set flavor based on storage provider.
	if s3.StorageProvider == "aws" {
		opts = append(opts, profilev1.WithS3Flavor(managementv1.S3FlavorAws))
	} else {
		opts = append(opts, profilev1.WithS3Flavor(managementv1.S3FlavorS3Compat))
	}
	if s3.Endpoint != "" {
		opts = append(opts, profilev1.WithS3Endpoint(s3.Endpoint))
	}
	if s3.KeyPrefix != "" {
		opts = append(opts, profilev1.WithS3KeyPrefix(s3.KeyPrefix))
	}
	if derefBool(s3.PathStyleAccess, false) {
		opts = append(opts, profilev1.WithS3PathStyleAccess())
	}
	if stsEnabled {
		opts = append(opts, profilev1.WithS3STSEnabled())
	}
	if s3.AssumeRoleARN != "" {
		opts = append(opts, profilev1.WithS3AssumeRoleARN(s3.AssumeRoleARN))
	}
	opts = append(opts, profilev1.WithS3RemoteSigningEnabled(remoteSigningEnabled))
	return opts
}

// ListWarehouses returns all warehouses in the Lakekeeper catalog.
// GET /management/v1/warehouse
func (c *Client) ListWarehouses(ctx context.Context, lakekeeperURL, token string) ([]core.Warehouse, error) {
	sdk, err := c.sdkClient(ctx, lakekeeperURL, token)
	if err != nil {
		return nil, fmt.Errorf("list warehouses: create SDK client: %w", err)
	}

	// The façade scopes by the projectId query param. It's deprecated upstream
	// in favor of the x-project-id header, but ListWarehouses exposes no
	// XProjectId builder, so the query param is the only per-request lever.
	resp, err := sdk.Warehouses.List(ctx, defaultProjectID)
	if err != nil {
		return nil, fmt.Errorf("list warehouses: %w", err)
	}

	warehouses := make([]core.Warehouse, 0, len(resp.Warehouses))
	for _, w := range resp.Warehouses {
		warehouses = append(warehouses, core.Warehouse{
			ID:   w.WarehouseId,
			Name: w.Name,
		})
	}
	return warehouses, nil
}

// GetWarehouseID returns the UUID of a warehouse by name.
func (c *Client) GetWarehouseID(ctx context.Context, lakekeeperURL, token, warehouseName string) (string, error) {
	warehouses, err := c.ListWarehouses(ctx, lakekeeperURL, token)
	if err != nil {
		return "", err
	}
	for _, w := range warehouses {
		if w.Name == warehouseName {
			return w.ID, nil
		}
	}
	return "", fmt.Errorf("warehouse %q not found", warehouseName)
}

// CreateUser registers a user in Lakekeeper.
// POST /management/v1/user
func (c *Client) CreateUser(ctx context.Context, lakekeeperURL, token, userID, name string) error {
	sdk, err := c.sdkClient(ctx, lakekeeperURL, token)
	if err != nil {
		return fmt.Errorf("create user: create SDK client: %w", err)
	}

	req := managementv1.NewCreateUserRequest()
	req.Id = new(userID)
	req.Name = new(name)
	req.SetUserType(managementv1.UserTypeApplication)

	_, resp, err := sdk.UserAPI.CreateUser(ctx).CreateUserRequest(*req).Execute()
	if err != nil {
		// 409 = already exists — success (idempotent).
		if resp == nil || resp.StatusCode != http.StatusConflict {
			return fmt.Errorf("create user: %w", err)
		}
	}
	return nil
}

// DeleteUser removes a user from Lakekeeper.
// DELETE /management/v1/user/<userID>
func (c *Client) DeleteUser(ctx context.Context, lakekeeperURL, token, userID string) error {
	sdk, err := c.sdkClient(ctx, lakekeeperURL, token)
	if err != nil {
		return fmt.Errorf("delete user: create SDK client: %w", err)
	}

	resp, err := sdk.UserAPI.DeleteUser(ctx, userID).Execute()
	if err != nil {
		// 404 = already gone — success.
		if resp == nil || resp.StatusCode != http.StatusNotFound {
			return fmt.Errorf("delete user: %w", err)
		}
	}
	return nil
}

// ListUsers returns all users registered in Lakekeeper.
// GET /management/v1/user
func (c *Client) ListUsers(ctx context.Context, lakekeeperURL, token string) ([]core.LakekeeperUser, error) {
	sdk, err := c.sdkClient(ctx, lakekeeperURL, token)
	if err != nil {
		return nil, fmt.Errorf("list users: create SDK client: %w", err)
	}

	resp, err := sdk.Users.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}

	users := make([]core.LakekeeperUser, 0, len(resp.Users))
	for _, u := range resp.Users {
		users = append(users, core.LakekeeperUser{
			ID:   u.Id,
			Name: u.Name,
		})
	}
	return users, nil
}

// AssignServerRole grants a server-level role (admin or operator) to a principal.
// POST /management/v1/permissions/server/assignments
func (c *Client) AssignServerRole(ctx context.Context, lakekeeperURL, token, principalID, role string) error {
	sdk, err := c.sdkClient(ctx, lakekeeperURL, token)
	if err != nil {
		return fmt.Errorf("assign server role: create SDK client: %w", err)
	}

	assignment, err := permissions.BuildAssignment[managementv1.ServerAssignment](role, permissions.PrincipalUser, principalID)
	if err != nil {
		return fmt.Errorf("assign server role: %w", err)
	}

	req := managementv1.UpdateServerAssignmentsRequest{
		Writes: []managementv1.ServerAssignment{assignment},
	}

	if _, err := sdk.PermissionsOpenfgaAPI.UpdateServerAssignments(ctx).
		UpdateServerAssignmentsRequest(req).Execute(); err != nil {
		return fmt.Errorf("assign server role: %w", err)
	}
	return nil
}

// AssignWarehouseRole upserts a user's warehouse role to exactly the relations
// that role grants. It computes the diff against the user's current assignments
// and sends only the writes/deletes needed — so it's safe to call repeatedly
// and works for role *upgrades* (reader → writer) and *downgrades* alike.
//
// The naive "just Writes the new set" approach is silently broken on upgrades:
// OpenFGA writes are transactional, so if any tuple in the batch already
// exists (e.g. `describe` carried over from the old reader role), the whole
// batch is rolled back and the new tuples (`create`, `modify`) never land.
//
// Relations outside the role-managed set (`ownership`, `pass_grants`,
// `manage_grants`) are deliberately left untouched — those are admin
// concerns and shouldn't be stripped by a reader/writer assignment.
//
// POST /management/v1/permissions/warehouse/{warehouse_id}/assignments
func (c *Client) AssignWarehouseRole(ctx context.Context, lakekeeperURL, token, warehouseID, userID, role string) error {
	desired, err := warehouseRelations(role)
	if err != nil {
		return err
	}
	desiredSet := make(map[string]struct{}, len(desired))
	for _, rel := range desired {
		desiredSet[rel] = struct{}{}
	}

	sdk, err := c.sdkClient(ctx, lakekeeperURL, token)
	if err != nil {
		return fmt.Errorf("assign warehouse role: create SDK client: %w", err)
	}

	// Pull existing assignments for this user so we can compute the diff.
	resp, _, err := sdk.PermissionsOpenfgaAPI.GetWarehouseAssignmentsById(ctx, warehouseID).Execute()
	if err != nil {
		return fmt.Errorf("assign warehouse role: get current assignments: %w", err)
	}
	currentSet := currentUserRelations(resp.Assignments, userID)

	// Writes: desired tuples not already present.
	writes, err := warehouseAssignmentDiff(desiredSet, currentSet, userID)
	if err != nil {
		return err
	}

	// Deletes: role-managed tuples the user currently has but doesn't need.
	// Only consider the relations roles actually manage (see roleManagedRelations).
	deletes, err := warehouseAssignmentDiff(currentSet, desiredSet, userID, roleManagedRelations)
	if err != nil {
		return err
	}

	if len(writes) == 0 && len(deletes) == 0 {
		return nil
	}

	req := managementv1.UpdateWarehouseAssignmentsRequest{
		Writes:  writes,
		Deletes: deletes,
	}

	if _, err := sdk.PermissionsOpenfgaAPI.UpdateWarehouseAssignmentsById(ctx, warehouseID).
		UpdateWarehouseAssignmentsRequest(req).Execute(); err != nil {
		return fmt.Errorf("assign warehouse role: %w", err)
	}
	return nil
}

// currentUserRelations collects the set of relations currently assigned to
// userID (as a user principal), ignoring assignments for other principals.
func currentUserRelations[T any](assignments []T, userID string) map[string]struct{} {
	currentSet := make(map[string]struct{})
	for _, a := range assignments {
		row, ok := permissions.DescribeAssignment(a)
		if !ok || row.PrincipalType != "user" || row.PrincipalID != userID {
			continue
		}
		currentSet[row.Relation] = struct{}{}
	}
	return currentSet
}

// warehouseAssignmentDiff builds warehouse assignments for every relation in
// have that is absent from want. When an optional managed-relation filter is
// supplied, relations outside it are skipped (used to bound deletes to
// role-managed relations).
func warehouseAssignmentDiff(have, want map[string]struct{}, userID string, managed ...map[string]struct{}) ([]managementv1.WarehouseAssignment, error) {
	out := make([]managementv1.WarehouseAssignment, 0, len(have))
	for rel := range have {
		if len(managed) > 0 {
			if _, ok := managed[0][rel]; !ok {
				continue
			}
		}
		if _, ok := want[rel]; ok {
			continue
		}
		a, err := permissions.BuildAssignment[managementv1.WarehouseAssignment](rel, permissions.PrincipalUser, userID)
		if err != nil {
			return nil, fmt.Errorf("assign warehouse role: %w", err)
		}
		out = append(out, a)
	}
	return out, nil
}

// roleManagedRelations is the set of warehouse relations that reader/writer/admin
// roles assign. Relations outside this set (e.g. pass_grants granted
// out-of-band) are deliberately left alone by AssignWarehouseRole.
var roleManagedRelations = map[string]struct{}{
	string(managementv1.WarehouseRelationDescribe):  {},
	string(managementv1.WarehouseRelationSelect):    {},
	string(managementv1.WarehouseRelationCreate):    {},
	string(managementv1.WarehouseRelationModify):    {},
	string(managementv1.WarehouseRelationOwnership): {},
}

// RemoveWarehouseRole removes all warehouse permissions from a user.
// POST /management/v1/permissions/warehouse/{warehouse_id}/assignments
func (c *Client) RemoveWarehouseRole(ctx context.Context, lakekeeperURL, token, warehouseID, userID string) error {
	sdk, err := c.sdkClient(ctx, lakekeeperURL, token)
	if err != nil {
		return fmt.Errorf("remove warehouse role: create SDK client: %w", err)
	}

	allRelations := []managementv1.WarehouseRelation{
		managementv1.WarehouseRelationOwnership,
		managementv1.WarehouseRelationPassGrants,
		managementv1.WarehouseRelationManageGrants,
		managementv1.WarehouseRelationDescribe,
		managementv1.WarehouseRelationSelect,
		managementv1.WarehouseRelationCreate,
		managementv1.WarehouseRelationModify,
	}

	deletes := make([]managementv1.WarehouseAssignment, 0, len(allRelations))
	for _, rel := range allRelations {
		a, err := permissions.BuildAssignment[managementv1.WarehouseAssignment](string(rel), permissions.PrincipalUser, userID)
		if err != nil {
			return fmt.Errorf("remove warehouse role: %w", err)
		}
		deletes = append(deletes, a)
	}

	req := managementv1.UpdateWarehouseAssignmentsRequest{Deletes: deletes}

	resp, err := sdk.PermissionsOpenfgaAPI.UpdateWarehouseAssignmentsById(ctx, warehouseID).
		UpdateWarehouseAssignmentsRequest(req).Execute()
	if err != nil {
		// If the batch fails because some tuples don't exist, treat 404 as success.
		if resp == nil || resp.StatusCode != http.StatusNotFound {
			return fmt.Errorf("remove warehouse role: %w", err)
		}
	}
	return nil
}

// GetWarehouseAssignments returns all user-to-relation assignments for a warehouse.
// GET /management/v1/permissions/warehouse/{warehouse_id}/assignments
func (c *Client) GetWarehouseAssignments(ctx context.Context, lakekeeperURL, token, warehouseID string) ([]core.WarehouseAssignment, error) {
	sdk, err := c.sdkClient(ctx, lakekeeperURL, token)
	if err != nil {
		return nil, fmt.Errorf("get warehouse assignments: create SDK client: %w", err)
	}

	resp, _, err := sdk.PermissionsOpenfgaAPI.GetWarehouseAssignmentsById(ctx, warehouseID).Execute()
	if err != nil {
		return nil, fmt.Errorf("get warehouse assignments: %w", err)
	}

	assignments := make([]core.WarehouseAssignment, 0, len(resp.Assignments))
	for _, a := range resp.Assignments {
		row, ok := permissions.DescribeAssignment(a)
		if !ok || row.PrincipalType != "user" {
			continue // skip role-based assignments, we only track users
		}
		assignments = append(assignments, core.WarehouseAssignment{
			UserID:   row.PrincipalID,
			Relation: row.Relation,
		})
	}
	return assignments, nil
}

// warehouseRelations maps a role name to the warehouse relations it grants.
func warehouseRelations(role string) ([]string, error) {
	switch role {
	case "reader":
		return []string{
			string(managementv1.WarehouseRelationDescribe),
			string(managementv1.WarehouseRelationSelect),
		}, nil
	case "writer":
		return []string{
			string(managementv1.WarehouseRelationDescribe),
			string(managementv1.WarehouseRelationSelect),
			string(managementv1.WarehouseRelationCreate),
			string(managementv1.WarehouseRelationModify),
		}, nil
	case "admin":
		return []string{
			string(managementv1.WarehouseRelationOwnership),
		}, nil
	default:
		return nil, fmt.Errorf("unknown role %q", role)
	}
}

// derefBool returns the value pointed to by p, or def if p is nil.
func derefBool(p *bool, def bool) bool {
	if p != nil {
		return *p
	}
	return def
}

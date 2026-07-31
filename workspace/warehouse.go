package workspace

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/fairtier/workspace-api/core"
)

// warehouseNameRegex validates warehouse names: lowercase alphanumeric with hyphens, 1-63 chars.
var warehouseNameRegex = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

// WarehouseService orchestrates warehouse management in Lakekeeper.
type WarehouseService struct {
	Workspaces Resolver
	Lakekeeper LakekeeperClient
	Tokens     TokenProvider
	Logger     *slog.Logger
}

// ListWarehouses returns all warehouses for the caller's customer.
func (s *WarehouseService) ListWarehouses(ctx context.Context, callerID core.UserID) ([]core.Warehouse, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}

	if ws.LakekeeperURL == "" {
		return nil, ErrCustomerNotProvisioned
	}

	token, err := s.Tokens.GetClientToken(ctx, ws.CasdoorIssuer, ws.OIDCClientID, ws.OIDCClientSecret)
	if err != nil {
		return nil, fmt.Errorf("get lakekeeper token: %w", err)
	}

	warehouses, err := s.Lakekeeper.ListWarehouses(ctx, ws.LakekeeperServiceURL(), token)
	if err != nil {
		return nil, fmt.Errorf("list warehouses: %w", err)
	}

	return warehouses, nil
}

// CreateWarehouse creates a new warehouse in the caller's Lakekeeper instance.
// If customS3 is nil, the workspace's effective S3 config is used.
// If keyPrefix is empty, it defaults to the warehouse name.
func (s *WarehouseService) CreateWarehouse(ctx context.Context, callerID core.UserID, name, keyPrefix string, customS3 *core.S3Config) (*core.Warehouse, error) {
	if err := validateCreateWarehouseInput(name, keyPrefix); err != nil {
		return nil, err
	}

	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}

	if ws.LakekeeperURL == "" {
		return nil, ErrCustomerNotProvisioned
	}

	token, err := s.Tokens.GetClientToken(ctx, ws.CasdoorIssuer, ws.OIDCClientID, ws.OIDCClientSecret)
	if err != nil {
		return nil, fmt.Errorf("get lakekeeper token: %w", err)
	}

	s3 := resolveWarehouseS3(ws, name, keyPrefix, customS3)

	warehouseName, err := s.Lakekeeper.CreateWarehouse(ctx, ws.LakekeeperServiceURL(), token, name, s3)
	if err != nil {
		return nil, fmt.Errorf("create warehouse: %w", err)
	}

	warehouseID, err := s.Lakekeeper.GetWarehouseID(ctx, ws.LakekeeperServiceURL(), token, warehouseName)
	if err != nil {
		return nil, fmt.Errorf("get warehouse ID: %w", err)
	}

	return &core.Warehouse{
		ID:   warehouseID,
		Name: warehouseName,
	}, nil
}

// validateCreateWarehouseInput validates the warehouse name and, when set, the key prefix.
func validateCreateWarehouseInput(name, keyPrefix string) error {
	if err := validateWarehouseName(name); err != nil {
		return err
	}
	if keyPrefix != "" {
		if err := validateKeyPrefix(keyPrefix); err != nil {
			return err
		}
	}
	return nil
}

// resolveWarehouseS3 resolves the S3 config for a new warehouse: use customS3 if
// provided, otherwise inherit from the workspace, then apply the key-prefix for
// data isolation within the bucket (defaulting to the warehouse name).
func resolveWarehouseS3(ws *Workspace, name, keyPrefix string, customS3 *core.S3Config) core.S3Config {
	var s3 core.S3Config
	if customS3 != nil {
		s3 = *customS3
	} else {
		s3 = ws.EffectiveS3
	}
	if keyPrefix == "" {
		keyPrefix = name
	}
	s3.KeyPrefix = keyPrefix
	return s3
}

func validateWarehouseName(name string) error {
	if len(name) == 0 || len(name) > 63 {
		return ErrInvalidWarehouseName
	}
	if !warehouseNameRegex.MatchString(name) {
		return ErrInvalidWarehouseName
	}
	return nil
}

func validateKeyPrefix(prefix string) error {
	if len(prefix) > 63 {
		return ErrInvalidKeyPrefix
	}
	if !warehouseNameRegex.MatchString(prefix) {
		return ErrInvalidKeyPrefix
	}
	return nil
}

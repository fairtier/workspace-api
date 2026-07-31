package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	"github.com/fairtier/workspace-api/core"
	warehousev1 "github.com/fairtier/workspace-api/proto/warehouse/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// WarehouseServer implements the ConnectRPC WarehouseService handler.
type WarehouseServer struct {
	Service *workspace.WarehouseService
}

func (s *WarehouseServer) ListWarehouses(ctx context.Context, _ *connect.Request[warehousev1.ListWarehousesRequest]) (*connect.Response[warehousev1.ListWarehousesResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	warehouses, err := s.Service.ListWarehouses(ctx, callerID)
	if err != nil {
		return nil, domainError(err)
	}

	out := make([]*warehousev1.Warehouse, 0, len(warehouses))
	for _, w := range warehouses {
		out = append(out, &warehousev1.Warehouse{
			Id:   w.ID,
			Name: w.Name,
		})
	}

	return connect.NewResponse(&warehousev1.ListWarehousesResponse{
		Warehouses: out,
	}), nil
}

func (s *WarehouseServer) CreateWarehouse(ctx context.Context, req *connect.Request[warehousev1.CreateWarehouseRequest]) (*connect.Response[warehousev1.CreateWarehouseResponse], error) {
	if req.Msg.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("name is required"))
	}

	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	var customS3 *core.S3Config
	if req.Msg.S3 != nil {
		s3 := warehouseS3ToCore(req.Msg.S3)
		customS3 = &s3
	}

	warehouse, err := s.Service.CreateWarehouse(ctx, callerID, req.Msg.Name, req.Msg.KeyPrefix, customS3)
	if err != nil {
		return nil, domainError(err)
	}

	return connect.NewResponse(&warehousev1.CreateWarehouseResponse{
		Warehouse: &warehousev1.Warehouse{
			Id:   warehouse.ID,
			Name: warehouse.Name,
		},
	}), nil
}

// warehouseS3ToCore converts the warehouse proto's S3Config (a wire-compatible
// mirror of provisioning.v1.S3Config) to the core type.
func warehouseS3ToCore(s *warehousev1.S3Config) core.S3Config {
	return core.S3Config{
		Bucket:                   s.Bucket,
		KeyPrefix:                s.KeyPrefix,
		Endpoint:                 s.Endpoint,
		Region:                   s.Region,
		AccessKeyID:              s.AccessKeyId,
		SecretAccessKey:          s.SecretAccessKey,
		PathStyleAccess:          s.PathStyleAccess,
		STSEnabled:               s.StsEnabled,
		RemoteSigningEnabled:     s.RemoteSigningEnabled,
		CloudflareAPIToken:       s.CloudflareApiToken,
		CloudflareAccountID:      s.CloudflareAccountId,
		CredentialDelegationMode: warehouseCredModeToString(s.CredentialDelegationMode),
		StorageProvider:          warehouseStorageProviderToString(s.StorageProvider),
		AssumeRoleARN:            s.AssumeRoleArn,
		StorageMode:              core.StorageMode(s.StorageMode),
	}
}

func warehouseCredModeToString(m warehousev1.CredentialDelegationMode) string {
	switch m {
	case warehousev1.CredentialDelegationMode_CREDENTIAL_DELEGATION_MODE_VENDED:
		return "vended"
	case warehousev1.CredentialDelegationMode_CREDENTIAL_DELEGATION_MODE_REMOTE_SIGNING:
		return "remote-signing"
	case warehousev1.CredentialDelegationMode_CREDENTIAL_DELEGATION_MODE_NONE:
		return "none"
	default:
		return ""
	}
}

func warehouseStorageProviderToString(p warehousev1.StorageProvider) string {
	switch p {
	case warehousev1.StorageProvider_STORAGE_PROVIDER_AWS:
		return "aws"
	case warehousev1.StorageProvider_STORAGE_PROVIDER_CLOUDFLARE_R2:
		return "cloudflare-r2"
	case warehousev1.StorageProvider_STORAGE_PROVIDER_S3_COMPAT:
		return "s3-compat"
	default:
		return ""
	}
}

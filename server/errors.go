package server

import (
	"errors"

	"connectrpc.com/connect"

	"github.com/fairtier/workspace-api/core"
	pipelinev1 "github.com/fairtier/workspace-api/proto/pipeline/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// domainErrorCodes maps the workspace plane's sentinel errors to Connect
// codes. The control plane keeps its own (wider) mapper over the same shape
// for the services it hosts.
var domainErrorCodes = []struct {
	err  error
	code connect.Code
}{
	{core.ErrUserAlreadyExists, connect.CodeAlreadyExists},
	{core.ErrUserNotFound, connect.CodeNotFound},
	{workspace.ErrNotFileUploadPipeline, connect.CodeFailedPrecondition},
	{workspace.ErrInvalidRole, connect.CodeInvalidArgument},
	{workspace.ErrInvalidWarehouseName, connect.CodeInvalidArgument},
	{workspace.ErrInvalidKeyPrefix, connect.CodeInvalidArgument},
	{workspace.ErrInvalidS3Endpoint, connect.CodeInvalidArgument},
	{workspace.ErrCustomerNotProvisioned, connect.CodeFailedPrecondition},
	{workspace.ErrLakekeeperUsersUnavailable, connect.CodeFailedPrecondition},
	{workspace.ErrPipelineNotFound, connect.CodeNotFound},
	{workspace.ErrPipelineAlreadyExists, connect.CodeAlreadyExists},
	{workspace.ErrPipelineRunNotFound, connect.CodeNotFound},
	{workspace.ErrTransformationNotFound, connect.CodeNotFound},
	{workspace.ErrTransformationAlreadyExists, connect.CodeAlreadyExists},
	{workspace.ErrTransformationRunNotFound, connect.CodeNotFound},
	{workspace.ErrWorkspaceNotFound, connect.CodeNotFound},
	{workspace.ErrBoxRepoUnavailable, connect.CodeFailedPrecondition},
	{workspace.ErrBoxUnreachable, connect.CodeUnavailable},
	{workspace.ErrBoxCredentialNotFound, connect.CodeFailedPrecondition},
	{workspace.ErrPipelineVersionMismatch, connect.CodeFailedPrecondition},
	{workspace.ErrDemoNotConfigured, connect.CodeUnimplemented},
	{workspace.ErrDemoAlreadyLoaded, connect.CodeAlreadyExists},
	{workspace.ErrDemoNotLoaded, connect.CodeNotFound},
	{workspace.ErrSourceTestNotFound, connect.CodeNotFound},
	{workspace.ErrSourceTestUnsupported, connect.CodeFailedPrecondition},
}

func domainError(err error) error {
	if cerr, ok := validationDomainError(err); ok {
		return cerr
	}

	for _, m := range domainErrorCodes {
		if errors.Is(err, m.err) {
			return connect.NewError(m.code, err)
		}
	}

	return connect.NewError(connect.CodeInternal, err)
}

// validationDomainError handles the typed validation errors that carry a
// structured field/message detail (checked before the sentinel mappings). The
// bool reports whether err matched one of these typed errors.
func validationDomainError(err error) (error, bool) {
	var invalidCfg *workspace.ErrInvalidSourceConfig
	var invalidCreds *workspace.ErrInvalidSourceCredentials
	var invalidUpload *workspace.ErrInvalidUploadFile

	switch {
	case errors.As(err, &invalidCfg):
		return validationError(err, invalidCfg.Field, invalidCfg.Msg), true
	case errors.As(err, &invalidCreds):
		return validationError(err, invalidCreds.Field, invalidCreds.Msg), true
	case errors.As(err, &invalidUpload):
		return connect.NewError(connect.CodeInvalidArgument, err), true
	default:
		return nil, false
	}
}

// validationError builds an INVALID_ARGUMENT error carrying a structured
// ValidationErrors detail (gRPC richer error model) so clients can map the
// failure to a specific input. The plain message is preserved for clients that
// ignore details.
func validationError(err error, field, msg string) error {
	cerr := connect.NewError(connect.CodeInvalidArgument, err)
	detail, derr := connect.NewErrorDetail(&pipelinev1.ValidationErrors{
		Violations: []*pipelinev1.FieldViolation{
			{Field: field, Description: msg},
		},
	})
	if derr == nil {
		cerr.AddDetail(detail)
	}
	return cerr
}

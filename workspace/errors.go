package workspace

import "errors"

// Workspace-plane error sentinels. Names and messages are unchanged from
// their previous domain-package homes so Connect error mapping and Console
// behavior stay byte-identical across the plane split.
var (
	// ErrWorkspaceNotFound means the requested slug does not address a
	// workspace this deployment serves. Centrally the Resolver returns the
	// control plane's not-found instead; this sentinel exists for box-local
	// resolvers (StaticResolver), which know exactly one workspace.
	ErrWorkspaceNotFound = errors.New("workspace not found")

	ErrCustomerNotProvisioned = errors.New("customer not provisioned")
	ErrInvalidRole            = errors.New("invalid role: must be reader, writer, or admin")
	ErrInvalidWarehouseName   = errors.New("invalid warehouse name: must be lowercase alphanumeric with hyphens, 1-63 characters")
	ErrInvalidKeyPrefix       = errors.New("invalid key prefix: must be lowercase alphanumeric with hyphens, 1-63 characters")

	ErrPipelineNotFound            = errors.New("pipeline not found")
	ErrPipelineAlreadyExists       = errors.New("pipeline already exists")
	ErrPipelineRunNotFound         = errors.New("pipeline run not found")
	ErrTransformationNotFound      = errors.New("transformation not found")
	ErrTransformationAlreadyExists = errors.New("transformation already exists")
	ErrTransformationRunNotFound   = errors.New("transformation run not found")
	ErrDraftNotConfigured          = errors.New("AI pipeline drafting is not configured on this server")
	ErrDraftRateLimited            = errors.New("too many drafting requests; please wait and try again")
	ErrOAuthGrantNotFound          = errors.New("oauth grant not found, already used, or expired")
)

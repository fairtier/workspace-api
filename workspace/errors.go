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
	ErrInvalidS3Endpoint      = errors.New("invalid S3 endpoint: must be an http(s) URL with a public host")

	ErrPipelineNotFound            = errors.New("pipeline not found")
	ErrPipelineAlreadyExists       = errors.New("pipeline already exists")
	ErrPipelineRunNotFound         = errors.New("pipeline run not found")
	ErrTransformationNotFound      = errors.New("transformation not found")
	ErrTransformationAlreadyExists = errors.New("transformation already exists")
	ErrTransformationRunNotFound   = errors.New("transformation run not found")
	ErrDraftNotConfigured          = errors.New("AI pipeline drafting is not configured on this server")
	ErrDraftRateLimited            = errors.New("too many drafting requests; please wait and try again")
	ErrOAuthGrantNotFound          = errors.New("oauth grant not found, already used, or expired")

	// ErrOAuthClientNotFound means this customer has not connected an OAuth
	// application for the provider yet. It is a precondition, not a failure:
	// the Console turns it into "connect your Google app first" rather than an
	// error toast.
	ErrOAuthClientNotFound = errors.New("no OAuth client configured for this workspace")

	// ErrInvalidOAuthClient rejects a malformed client pair on save.
	ErrInvalidOAuthClient = errors.New("invalid OAuth client: client id and client secret are both required")

	// ErrUnsupportedOAuthProvider guards the provider column: the store shape is
	// generic, the flow behind it is not.
	ErrUnsupportedOAuthProvider = errors.New("unsupported OAuth provider")
)

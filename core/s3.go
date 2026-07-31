package core

// StorageMode describes the storage lifecycle phase for a customer.
// The provisioning layer handles the corresponding infrastructure changes.
//
// The lifecycle is bidirectional:
//
//	managed ──save BYOS──► migrating-to-byos    ──finalize──► byos
//	byos    ──save managed──► migrating-to-managed ──finalize──► managed
//
// During the migrating-* phases Lakekeeper keeps reading from the source
// bucket (R2 in →byos, BYOS in →managed); writers (dlt-worker) are scaled
// to zero; rclone copies data; metadata URIs are rewritten before finalize.
type StorageMode string

const (
	StorageModeManaged            StorageMode = "managed"
	StorageModeMigratingToBYOS    StorageMode = "migrating-to-byos"
	StorageModeMigratingToManaged StorageMode = "migrating-to-managed"
	StorageModeBYOS               StorageMode = "byos"
)

// IsMigrating reports whether the mode is one of the transient migrating states.
func (m StorageMode) IsMigrating() bool {
	return m == StorageModeMigratingToBYOS || m == StorageModeMigratingToManaged
}

// HasManagedBucket reports whether a managed `ft-{slug}` R2 bucket is held
// in Terraform state in this mode: managed, plus both migrating phases (the
// bucket is kept during →byos and re-imported during →managed). False for
// byos (the bucket was orphaned at finalize) and for the empty mode of a
// never-provisioned customer.
func (m StorageMode) HasManagedBucket() bool {
	return m == StorageModeManaged || m.IsMigrating()
}

// MigrationTarget returns the terminal mode the customer is migrating toward,
// or empty if the mode is not a migrating state.
func (m StorageMode) MigrationTarget() StorageMode {
	switch m {
	case StorageModeMigratingToBYOS:
		return StorageModeBYOS
	case StorageModeMigratingToManaged:
		return StorageModeManaged
	default:
		return ""
	}
}

type S3Config struct {
	Bucket               string `json:"bucket"`
	KeyPrefix            string `json:"key_prefix"`
	Endpoint             string `json:"endpoint"`
	Region               string `json:"region"`
	AccessKeyID          string `json:"access_key_id"`
	SecretAccessKey      string `json:"secret_access_key"`
	PathStyleAccess      *bool  `json:"path_style_access,omitempty"`
	STSEnabled           *bool  `json:"sts_enabled,omitempty"`
	RemoteSigningEnabled *bool  `json:"remote_signing_enabled,omitempty"`

	// R2-specific (for vended credentials)
	CloudflareAPIToken  string `json:"cloudflare_api_token,omitempty"`
	CloudflareAccountID string `json:"cloudflare_account_id,omitempty"`

	// Credential delegation mode: "vended", "remote-signing", "none"
	CredentialDelegationMode string `json:"credential_delegation_mode,omitempty"`

	StorageMode StorageMode `json:"storage_mode,omitempty"`

	// Storage provider: "aws", "cloudflare-r2", "minio", "s3-compat"
	StorageProvider string `json:"storage_provider,omitempty"`
	// AWS STS assume-role ARN (only for AWS + vended)
	AssumeRoleARN string `json:"assume_role_arn,omitempty"`
}

package workspace

import (
	"context"
	"time"
)

// BoxAgeKey is the box's deposited age PUBLIC key (X25519 recipient,
// "age1..."). The matching private key exists only on the box (Secret
// dlt-age); the pipeline mirror encrypts source credentials to this key and
// commits them as pipelines/<name>.credentials.age (pipelines-as-files
// Phase 3). Public keys are not secret — stored plaintext.
type BoxAgeKey struct {
	CustomerSlug string
	PublicKey    string
	Note         string
	UpdatedAt    time.Time
}

// BoxAgeKeyStore persists deposited box age public keys.
type BoxAgeKeyStore interface {
	// UpsertBoxAgeKey stores or replaces the key for a slug.
	UpsertBoxAgeKey(ctx context.Context, key *BoxAgeKey) error
	// GetBoxAgeKey returns the key for a slug, or ErrBoxCredentialNotFound.
	GetBoxAgeKey(ctx context.Context, customerSlug string) (*BoxAgeKey, error)
}

// PipelineCredentialRender is the mirror's bookkeeping for one rendered
// pipelines/<name>.credentials.age file. age ciphertext is non-deterministic
// (fresh ephemeral key per encryption), so "is the committed file current?"
// cannot be answered by content comparison: the mirror compares Fingerprint
// (a keyed hash of recipient+plaintext) and BlobSHA (the Gitea blob sha of
// the last write, detecting out-of-band edits) instead. The repo tree stays
// the existence truth — this row is a cache, safe to lose (one redundant
// re-encrypt commit, never corruption).
type PipelineCredentialRender struct {
	PipelineID  PipelineID
	Fingerprint string
	BlobSHA     string
}

// PipelineCredentialRenderStore persists render bookkeeping. Rows are
// removed by the pipeline-delete FK cascade; the mirror only ever upserts.
type PipelineCredentialRenderStore interface {
	UpsertPipelineCredentialRender(ctx context.Context, r *PipelineCredentialRender) error
	// GetPipelineCredentialRenders returns all rows for a customer's
	// pipelines, keyed by pipeline id (one batch read per converge).
	GetPipelineCredentialRenders(ctx context.Context, customerSlug string) (map[PipelineID]PipelineCredentialRender, error)
}

// PipelineDefinitionRender is the mirror's bookkeeping for one rendered
// pipelines/<slug>.yaml: the path and Gitea blob sha of the mirror's last
// write. Content comparison alone cannot distinguish "stale because the
// pipeline changed in the Console" (the normal update case) from "changed
// out-of-band in the repo"; a tree blob sha differing from the recorded one
// means a commit the mirror did not make — the drift signal behind
// overwrite-and-notify (git-centric gaps #4). Like the credential rows,
// this is a cache: a lost row costs one missed drift notification, never
// correctness.
type PipelineDefinitionRender struct {
	PipelineID PipelineID
	Path       string
	BlobSHA    string
	// RefusedBlobSHA is the last repo blob the adopt pass refused to take
	// into the central cache (unparseable, foreign id, source-type change).
	// It suppresses repeat notifications for the same refused commit; a
	// successful render or adoption clears it (Phase 2B).
	RefusedBlobSHA string
}

// PipelineDefinitionRenderStore persists definition-render bookkeeping.
// Rows are removed by the pipeline-delete FK cascade; the mirror only ever
// upserts.
type PipelineDefinitionRenderStore interface {
	UpsertPipelineDefinitionRender(ctx context.Context, r *PipelineDefinitionRender) error
	// GetPipelineDefinitionRenders returns all rows for a customer's
	// pipelines, keyed by pipeline id (one batch read per converge).
	GetPipelineDefinitionRenders(ctx context.Context, customerSlug string) (map[PipelineID]PipelineDefinitionRender, error)
	// MarkPipelineDefinitionRefused stamps the refused blob sha without
	// touching the last-render bookkeeping (Phase 2B adopt pass).
	MarkPipelineDefinitionRefused(ctx context.Context, id PipelineID, refusedSHA string) error
}

// CredentialFingerprinter produces the deterministic fingerprint the mirror
// stores per rendered credential file. Implemented by crypto
// (HMAC-SHA256 keyed with CREDENTIAL_ENCRYPTION_KEY; plain SHA-256 in
// keyless dev) — keyed so a leaked fingerprint is no guessing oracle for
// low-entropy credentials.
type CredentialFingerprinter interface {
	// Fingerprint hashes the parts (with unambiguous separation) to a hex
	// string.
	Fingerprint(parts ...[]byte) string
}

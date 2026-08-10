package workspace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"filippo.io/age"
	"filippo.io/age/armor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/yaml.v3"

	"github.com/fairtier/workspace-api/core"
)

// PipelineCredentialReader is the mirror's credential read path — separate
// from PipelineRepository so the Console list path never grows a
// credential-bearing method.
type PipelineCredentialReader interface {
	// ListPipelineCredentialsByCustomer returns decrypted source
	// credentials keyed by pipeline id.
	ListPipelineCredentialsByCustomer(ctx context.Context, customerSlug string) (map[PipelineID]json.RawMessage, error)
}

// PipelineMirror renders a customer's pipeline definitions into the box's
// `pipelines` Gitea repo — pipelines-as-files Phase 1 (dual-write). The
// central Postgres row stays the source of truth; every Console save calls
// SyncCustomer, which converges the repo to the full desired state, so a
// single save also backfills pre-existing pipelines and heals missed events.
//
// Source credentials are rendered ONLY as armored age ciphertext
// (pipelines/<name>.credentials.age, Phase 3), encrypted to the box's
// deposited public key — never in the yaml, never in plaintext. Boxes that
// have not deposited a key get definitions only.
type PipelineMirror struct {
	Workspaces  Resolver
	Credentials BoxGitCredentialStore
	Pipelines   PipelineRepository
	// Age credential rendering (Phase 3). All four must be set to render
	// pipelines/<name>.credentials.age files; when any is nil the mirror
	// manages definitions only and never touches .age paths.
	AgeKeys             BoxAgeKeyStore
	Renders             PipelineCredentialRenderStore
	Fingerprint         CredentialFingerprinter
	PipelineCredentials PipelineCredentialReader
	// DefinitionRenders, when set, keeps per-file blob-sha bookkeeping for
	// rendered pipelines/<slug>.yaml files — the drift signal behind
	// overwrite-and-notify (git-centric gaps #4). Optional: nil disables
	// drift detection, never the converge itself.
	DefinitionRenders PipelineDefinitionRenderStore
	// Notifications, when set, raises the in-app notification when a
	// converge overwrites an out-of-band repo edit. Optional.
	Notifications Notifier
	// Ownership, when set, lets the mirror mark a pipeline's credentials
	// externally-managed on a foreign .age commit (Phase 2B): central cannot
	// decrypt the box-encrypted file, so instead of clobbering it the file
	// is left to the box until a Console credential edit reclaims it.
	// Optional: nil keeps the pre-2B re-render behavior.
	Ownership PipelineCredentialOwnershipStore
	// Importer, when set, lets the adopt pass CREATE rows for definition
	// files this database does not track yet — hydration for a box whose
	// workspace database starts empty (Phase 3B, see
	// pipelinemirror_import.go). Optional, and deliberately left nil
	// centrally, where the Console is the only create path.
	Importer PipelineImporter
	// OAuthClients mirrors PipelineService's serve-time injection for OAuth
	// google_sheets credentials: the stored row lacks the client pair, but
	// the worker needs it to refresh tokens, so the rendered credential file
	// carries it too (encrypted to the box).
	//
	// The pair is the CUSTOMER's own app, resolved per tenant. That is what
	// makes writing it into their repo correct rather than a leak: a single
	// shared FairTier app would put our Google identity, able to impersonate
	// us on every other customer's consent screen, on every box that has one
	// Sheets pipeline. Nil = no injection.
	OAuthClients OAuthClientStore
	// NewClient builds a Gitea client for a box (same factory shape as
	// BoxRepoService.NewClient).
	NewClient func(baseURL, username, token string) RepoFileClient
	Logger    *slog.Logger

	// importSkips dedupes the import pass's "could not import" warnings
	// across sweeps.
	importSkips importSkips
	// staleOAuth dedupes the "reconnect this Sheets pipeline" notification
	// across converges, keyed by pipeline → the client id it is stale against.
	// Connecting a different app re-reports, which is right: the pipeline is
	// stale against the new app too.
	staleOAuth importSkips
}

// credentialFingerprintContext domain-separates the mirror's fingerprints
// from any other use of the shared HMAC key.
const credentialFingerprintContext = "pipeline-credentials-v1"

// credentialFile is one desired pipelines/<slug>.credentials.age file.
// external marks a file whose content is owned by the box (foreign edit the
// central plane cannot decrypt): the converge keeps the path but never
// writes it.
type credentialFile struct {
	pipelineID PipelineID
	plaintext  json.RawMessage
	external   bool
}

// pipelinesRepo is the box Gitea repo holding rendered definitions (seeded by
// the box's deploy-time seed job).
const pipelinesRepo = "pipelines"

// pipelineFileHeader marks every rendered file as Console-owned.
const pipelineFileHeader = "# Rendered by the FairTier Console — a Console save overwrites this file.\n"

// SyncCustomer converges the box's pipelines repo to the customer's current
// pipeline set. Customers outside the mirror's scope (shared substrate, box
// not provisioned, no deposited credential yet) are skipped silently — the
// mirror is a box/VM-substrate feature. A concurrent commit (stale sha) is
// retried once with a fresh tree; conflicting writes are never forced.
//
// author is the acting Console user when the sync was triggered by a save,
// nil for platform-initiated syncs (age-key deposit). It becomes the git
// author of every commit this converge makes — full-state convergence means
// a save can also backfill or heal other files, but those changes are still
// the result of that user's save; the committer stays the platform identity.
func (m *PipelineMirror) SyncCustomer(ctx context.Context, customerSlug string, author *CommitAuthor) (err error) {
	// With git-primary on, this converge is on the request path of every
	// Console save, so its span is the one that explains a slow save — the
	// child client spans show whether the time went to Gitea or to Postgres.
	ctx, span := tracer.Start(ctx, "PipelineMirror.SyncCustomer", trace.WithAttributes(
		attrSlug.String(customerSlug),
		attrRepoPlane.String(planePipelines),
	))
	started, inScope := time.Now(), false
	defer func() { recordSync(ctx, span, planePipelines, started, err, inScope) }()

	client, ws, ok, err := m.clientFor(ctx, customerSlug)
	if err != nil || !ok {
		return err
	}
	inScope = true

	pipelines, err := m.Pipelines.ListPipelinesByCustomer(ctx, customerSlug)
	if err != nil {
		return fmt.Errorf("list pipelines: %w", err)
	}
	// Rewrite file_upload pipelines into the plain "filesystem" form the
	// dlt-worker understands before rendering — the box reads its pipeline
	// config from this checkout (POLL_SOURCE_CREDENTIALS=off), and the worker
	// cannot load a "file_upload" source. Parity with the poll path's
	// GetEnabledPipelines. The injected storage credentials are returned to be
	// rendered as the pipeline's .credentials.age file.
	pipelines, fileUploadCreds := m.resolveFileUploads(ctx, ws, pipelines)

	slugs := pipelineFileSlugs(pipelines)
	desired, err := renderPipelineFiles(pipelines, slugs)
	if err != nil {
		return err
	}
	credentials, recipient, renders, err := m.desiredCredentialFiles(ctx, customerSlug, pipelines, slugs, fileUploadCreds)
	if err != nil {
		return err
	}
	defRenders, err := m.definitionRenderRows(ctx, customerSlug)
	if err != nil {
		return err
	}
	span.SetAttributes(
		attribute.Int("workspace.repo.definition_files", len(desired)),
		attribute.Int("workspace.repo.credential_files", len(credentials)),
	)

	if err := m.converge(ctx, client, customerSlug, desired, credentials, recipient, renders, defRenders, author); errors.Is(err, ErrRepoFileChanged) {
		// Something else committed between our tree read and write (e.g. the
		// seed job, or a manual edit). One fresh-tree retry; a second
		// conflict is surfaced, never overridden. Credential render rows are
		// written only after a successful put, so the retry just
		// re-encrypts. defRenders is deliberately NOT re-read: files the
		// first attempt already converged compare content-equal on the retry
		// and adopt the new sha silently, so no double drift notification.
		//
		// The event is what makes a rising conflict rate visible: both
		// attempts land inside one span, and a retry costs a second full tree
		// read the duration histogram alone would not explain.
		span.AddEvent("workspace.repo.converge_retry")
		return m.converge(ctx, client, customerSlug, desired, credentials, recipient, renders, defRenders, author)
	} else if err != nil {
		return err
	}
	return nil
}

// resolveFileUploads rewrites every file_upload pipeline in place into the
// plain "filesystem" form the dlt-worker understands (parity with the poll
// path's GetEnabledPipelines / resolveFileUploadPipeline), so the box checkout
// never carries a source_type the worker cannot load. It returns the filtered
// pipeline list — a file_upload pipeline with no uploaded files, or one whose
// customer storage is not provisioned, is dropped (nothing to load, matching
// the poll) — and the injected storage credentials keyed by pipeline id, which
// the caller renders as each pipeline's .credentials.age file. These are the
// customer's own storage credentials, which the box already holds; encrypting
// them to the box's age key adds no new exposure.
func (m *PipelineMirror) resolveFileUploads(ctx context.Context, ws *Workspace, pipelines []Pipeline) ([]Pipeline, map[PipelineID]json.RawMessage) {
	creds := make(map[PipelineID]json.RawMessage)
	s3, storageOK := m.resolveUploadStorage(ctx, ws, pipelines)
	out := pipelines[:0]
	for _, p := range pipelines {
		if p.SourceType != SourceTypeFileUpload {
			out = append(out, p)
			continue
		}
		if !storageOK {
			continue
		}
		ok, err := resolveFileUploadPipeline(&p, s3)
		if err != nil {
			if m.Logger != nil {
				m.Logger.WarnContext(ctx, "file_upload pipeline skipped in mirror", "pipeline_id", p.ID, "err", err)
			}
			continue
		}
		if !ok {
			continue // no files yet — nothing to load
		}
		creds[p.ID] = p.SourceCredentials
		out = append(out, p)
	}
	return out, creds
}

// resolveUploadStorage returns the customer's upload storage config, resolved
// once for the whole batch. It reports storageOK=false — dropping every
// file_upload pipeline — when no pipeline needs it or the storage is not
// provisioned. Non-file_upload batches never touch storage.
func (m *PipelineMirror) resolveUploadStorage(ctx context.Context, ws *Workspace, pipelines []Pipeline) (core.S3Config, bool) {
	if !slices.ContainsFunc(pipelines, func(p Pipeline) bool { return p.SourceType == SourceTypeFileUpload }) {
		return core.S3Config{}, false
	}
	cfg, err := uploadStorage(ws)
	if err != nil {
		if m.Logger != nil {
			m.Logger.WarnContext(ctx, "file_upload pipelines skipped: storage not provisioned", "customer", ws.Slug)
		}
		return core.S3Config{}, false
	}
	return cfg, true
}

// desiredCredentialFiles computes the .age files the repo should hold:
// one per pipeline with non-empty credentials, sharing the definition
// file's slug. extraCreds carries credentials injected by the mirror itself
// (file_upload → filesystem storage creds) that are not in the stored
// credential set. Returns a nil map when age rendering is not wired (fields
// unset — .age paths are then never touched) and an empty map when the box
// has no deposited key (full-state: strays get deleted).
func (m *PipelineMirror) desiredCredentialFiles(ctx context.Context, customerSlug string, pipelines []Pipeline, slugs map[PipelineID]string, extraCreds map[PipelineID]json.RawMessage) (map[string]credentialFile, string, map[PipelineID]PipelineCredentialRender, error) {
	if m.AgeKeys == nil || m.Renders == nil || m.Fingerprint == nil || m.PipelineCredentials == nil {
		return nil, "", nil, nil
	}
	key, err := m.AgeKeys.GetBoxAgeKey(ctx, customerSlug)
	if errors.Is(err, ErrBoxCredentialNotFound) {
		return map[string]credentialFile{}, "", nil, nil
	}
	if err != nil {
		return nil, "", nil, fmt.Errorf("get box age key: %w", err)
	}

	credentials, renders, err := m.loadCredentialState(ctx, customerSlug)
	if err != nil {
		return nil, "", nil, err
	}
	maps.Copy(credentials, extraCreds)
	return m.buildCredentialFiles(ctx, customerSlug, pipelines, slugs, credentials), key.PublicKey, renders, nil
}

func (m *PipelineMirror) buildCredentialFiles(ctx context.Context, customerSlug string, pipelines []Pipeline, slugs map[PipelineID]string, credentials map[PipelineID]json.RawMessage) map[string]credentialFile {
	oauth := newOAuthClientResolver(m.OAuthClients, customerSlug)
	files := make(map[string]credentialFile)
	for _, p := range pipelines {
		filePath := "pipelines/" + slugs[p.ID] + ".credentials.age"
		if p.CredentialsExternal {
			// Externally-managed: keep the box's file (it must not be
			// deleted as stale) but never write it.
			files[filePath] = credentialFile{pipelineID: p.ID, external: true}
			continue
		}
		raw, ok := credentials[p.ID]
		if !ok || isEmptyJSON(raw) {
			continue
		}
		files[filePath] = credentialFile{
			pipelineID: p.ID,
			plaintext:  m.credentialPlaintext(ctx, oauth, &p, raw),
		}
		m.notifyStaleOAuthCredential(ctx, oauth, customerSlug, &p, raw)
	}
	return files
}

// notifyStaleOAuthCredential tells the customer to reconnect a Sheets pipeline
// whose refresh token was minted by an OAuth app they no longer use — including
// the legacy case of a credential that records no app at all, from when one
// shared FairTier app served every customer.
//
// Without this the failure surfaces as a run error from inside the worker's
// Google client, which names neither the pipeline nor the fix. Best-effort and
// deduped: the converge runs on every save.
func (m *PipelineMirror) notifyStaleOAuthCredential(ctx context.Context, oauth *oauthClientResolver, customerSlug string, p *Pipeline, raw json.RawMessage) {
	if m.Notifications == nil {
		return
	}
	probe := *p
	probe.SourceCredentials = raw
	if !oauth.staleClientID(ctx, &probe) {
		return
	}
	if !m.staleOAuth.firstReport(string(p.ID), oauth.clientID) {
		return
	}
	n := Notification{
		CustomerSlug: customerSlug,
		Type:         "info",
		Title:        "Reconnect " + p.Name + " to Google",
		Body:         fmt.Sprintf("%s was connected with a different Google OAuth app than the one this workspace uses now, so its access can no longer be refreshed. Open the pipeline and sign in with Google again.", p.Name),
		Link:         "/pipelines?pipeline=" + string(p.ID),
	}
	if err := m.Notifications.Notify(ctx, n); err != nil && m.Logger != nil {
		m.Logger.WarnContext(ctx, "notify stale oauth credential", "pipeline", p.ID, "err", err)
	}
}

func (m *PipelineMirror) loadCredentialState(ctx context.Context, customerSlug string) (map[PipelineID]json.RawMessage, map[PipelineID]PipelineCredentialRender, error) {
	credentials, err := m.PipelineCredentials.ListPipelineCredentialsByCustomer(ctx, customerSlug)
	if err != nil {
		return nil, nil, fmt.Errorf("list pipeline credentials: %w", err)
	}
	renders, err := m.Renders.GetPipelineCredentialRenders(ctx, customerSlug)
	if err != nil {
		return nil, nil, fmt.Errorf("get credential renders: %w", err)
	}
	return credentials, renders, nil
}

// credentialPlaintext applies serve-time parity to a stored credential:
// OAuth google_sheets rows store only the refresh token, but the worker
// also needs the customer's client pair to refresh access tokens, so the
// rendered file carries it too.
//
// When the pair cannot be resolved — no app connected, or the credential was
// minted by a different one — the file is rendered WITHOUT it rather than with
// a mismatched pair. The run then fails on a missing client_id, which is the
// honest signal; the reconnect prompt is raised separately by
// notifyStaleOAuthCredentials.
func (m *PipelineMirror) credentialPlaintext(ctx context.Context, oauth *oauthClientResolver, p *Pipeline, raw json.RawMessage) json.RawMessage {
	pipeline := *p
	pipeline.SourceCredentials = raw
	if injected, ok := oauth.inject(ctx, &pipeline); ok {
		return injected
	}
	return raw
}

// PipelineVersion is one historical rendering of a pipeline definition in
// the box's pipelines repo — a Console "version history" row.
type PipelineVersion struct {
	SHA         string
	Message     string
	AuthorName  string
	AuthorEmail string
	// Date is the author time as an RFC 3339 string — displayed, never
	// computed on.
	Date string
}

// ErrPipelineVersionMismatch means the file at the requested sha records a
// different pipeline — the file path was reused (a rename moved this
// pipeline's file); that history is not restorable from here.
var ErrPipelineVersionMismatch = errors.New("the selected version does not belong to this pipeline")

// ListVersions returns the newest-first history of a pipeline's rendered
// definition file. Follows the current file path only — history from before
// a rename stays attached to the old path. ErrBoxRepoUnavailable when the
// customer is out of mirror scope (shared substrate, no deposited
// credential).
func (m *PipelineMirror) ListVersions(ctx context.Context, customerSlug string, id PipelineID) ([]PipelineVersion, error) {
	client, filePath, err := m.versionClient(ctx, customerSlug, id)
	if err != nil {
		return nil, err
	}
	commits, err := client.ListCommits(ctx, pipelinesRepo, filePath, fileHistoryLimit)
	if err != nil {
		return nil, fmt.Errorf("list %s history: %w", filePath, err)
	}
	versions := make([]PipelineVersion, 0, len(commits))
	for _, c := range commits {
		versions = append(versions, PipelineVersion(c))
	}
	return versions, nil
}

// VersionAt returns the pipeline state recorded at sha, parsed back from the
// rendered YAML. Credentials are never part of the file, so a restore never
// touches them.
func (m *PipelineMirror) VersionAt(ctx context.Context, customerSlug string, id PipelineID, sha string) (*Pipeline, error) {
	if !isCommitSHA(sha) {
		return nil, &ErrInvalidSourceConfig{Field: "sha", Msg: "version must be a commit sha"}
	}
	client, filePath, err := m.versionClient(ctx, customerSlug, id)
	if err != nil {
		return nil, err
	}
	content, _, err := client.GetContentsAt(ctx, pipelinesRepo, filePath, sha)
	if err != nil {
		return nil, fmt.Errorf("get %s at %s: %w", filePath, sha, err)
	}

	var f pipelineFile
	if err := yaml.Unmarshal([]byte(content), &f); err != nil {
		return nil, fmt.Errorf("parse version %s: %w", sha, err)
	}
	// The path can have belonged to another pipeline at that sha (slugs
	// follow display names); the rendered id is the stable guard.
	if f.ID != string(id) {
		return nil, ErrPipelineVersionMismatch
	}
	return pipelineFromFile(id, f)
}

// pipelineFromFile maps a parsed rendered definition back onto a Pipeline.
// Credentials are never part of the file, so they are never touched.
func pipelineFromFile(id PipelineID, f pipelineFile) (*Pipeline, error) {
	p := &Pipeline{
		ID:               id,
		Name:             f.Name,
		SourceType:       f.SourceType,
		DatasetName:      f.DatasetName,
		Schedule:         f.Schedule,
		WriteDisposition: f.WriteDisposition,
		MergeStrategy:    f.MergeStrategy,
		Enabled:          f.Enabled,
	}
	if len(f.SourceConfig) > 0 {
		raw, err := json.Marshal(f.SourceConfig)
		if err != nil {
			return nil, fmt.Errorf("re-encode source config: %w", err)
		}
		p.SourceConfig = raw
	}
	return p, nil
}

// versionClient resolves the mirror client and the pipeline's current
// rendered file path, mapping out-of-scope customers to
// ErrBoxRepoUnavailable and unknown ids to ErrPipelineNotFound (the id must
// be in the caller's own pipeline set, which also enforces tenancy).
func (m *PipelineMirror) versionClient(ctx context.Context, customerSlug string, id PipelineID) (RepoFileClient, string, error) {
	client, _, ok, err := m.clientFor(ctx, customerSlug)
	if err != nil {
		return nil, "", err
	}
	if !ok {
		return nil, "", ErrBoxRepoUnavailable
	}
	pipelines, err := m.Pipelines.ListPipelinesByCustomer(ctx, customerSlug)
	if err != nil {
		return nil, "", fmt.Errorf("list pipelines: %w", err)
	}
	slug, ok := pipelineFileSlugs(pipelines)[id]
	if !ok {
		return nil, "", ErrPipelineNotFound
	}
	return client, "pipelines/" + slug + ".yaml", nil
}

// clientFor runs the mirror's gates (slug-keyed twin of
// BoxRepoService.clientFor) and returns ok=false when the customer is out of
// scope for mirroring. The resolved customer is returned too, so the caller
// can reach EffectiveS3 (for file_upload → filesystem rewriting) without a
// second lookup.
func (m *PipelineMirror) clientFor(ctx context.Context, customerSlug string) (RepoFileClient, *Workspace, bool, error) {
	return boxMirrorClientFor(ctx, m.Workspaces, m.Credentials, m.NewClient, customerSlug)
}

// boxMirrorClientFor is the scope gate shared by the pipeline and
// transformation mirrors: VM substrate, a resolvable box domain, and a
// deposited box git credential. ok=false means the customer is out of mirror
// scope (silently skipped).
func boxMirrorClientFor(ctx context.Context, workspaces Resolver, credentials BoxGitCredentialStore, newClient func(baseURL, username, token string) RepoFileClient, customerSlug string) (RepoFileClient, *Workspace, bool, error) {
	ws, err := workspaces.GetWorkspace(ctx, customerSlug)
	if err != nil {
		return nil, nil, false, fmt.Errorf("get customer: %w", err)
	}
	if !ws.OnVM {
		return nil, nil, false, nil
	}
	domainName := strings.TrimPrefix(ws.CustomerDomain, "*.")
	if domainName == "" {
		return nil, nil, false, nil
	}
	cred, err := credentials.GetBoxGitCredential(ctx, ws.Slug)
	if errors.Is(err, ErrBoxCredentialNotFound) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	return newClient("https://git."+domainName, cred.Username, cred.Token), ws, true, nil
}

// converge diffs the repo's pipelines/ directory against desired and applies
// the minimum set of put/delete commits. Definition files (*.yaml) compare
// by content; credential files (*.credentials.age) compare by stored
// fingerprint + blob sha, because age ciphertext is non-deterministic and
// re-encrypting would otherwise commit on every sync. Files outside those
// two shapes (README) are never touched; a nil credentials map means age
// rendering is not wired and .age paths are left alone entirely.
func (m *PipelineMirror) converge(ctx context.Context, client RepoFileClient, customerSlug string, desired map[string]definitionFile, credentials map[string]credentialFile, recipient string, renders map[PipelineID]PipelineCredentialRender, defRenders map[PipelineID]PipelineDefinitionRender, author *CommitAuthor) error {
	tree, err := client.ListTree(ctx, pipelinesRepo)
	if err != nil {
		return fmt.Errorf("list %s tree: %w", pipelinesRepo, err)
	}
	existing, existingAge := partitionPipelineTree(tree)

	// Deterministic order keeps commit history stable across syncs.
	for _, filePath := range sortedPaths(desired) {
		if err := m.upsertFile(ctx, client, customerSlug, filePath, desired[filePath], existing, defRenders, author); err != nil {
			return err
		}
	}
	if err := deleteStaleRendered(ctx, client, pipelinesRepo, existing, desired, author); err != nil {
		return err
	}

	if credentials == nil {
		return nil
	}
	for _, filePath := range sortedPaths(credentials) {
		if err := m.upsertCredentialFile(ctx, client, filePath, credentials[filePath], recipient, renders, existingAge, author); err != nil {
			return err
		}
	}
	return deleteStale(ctx, client, pipelinesRepo, existingAge, credentials, author)
}

// partitionPipelineTree splits the repo tree into definition and credential
// paths (path → blob sha). Anything else (README) is left unmanaged.
func partitionPipelineTree(tree []RepoFileEntry) (yaml, age map[string]string) {
	yaml = make(map[string]string)
	age = make(map[string]string)
	for _, e := range tree {
		switch {
		case !strings.HasPrefix(e.Path, "pipelines/"):
		case strings.HasSuffix(e.Path, ".credentials.age"):
			age[e.Path] = e.SHA
		case strings.HasSuffix(e.Path, ".yaml"):
			yaml[e.Path] = e.SHA
		}
	}
	return yaml, age
}

// deleteStale removes every existing file whose path is no longer desired.
//
// Use deleteStaleRendered for definition files. This unconditional form is
// correct only where the desired set already accounts for files the mirror did
// not write — as the credential set does, via CredentialsExternal.
func deleteStale[V any](ctx context.Context, client RepoFileClient, repo string, existing map[string]string, desired map[string]V, author *CommitAuthor) error {
	for _, filePath := range sortedPaths(existing) {
		if _, ok := desired[filePath]; ok {
			continue
		}
		msg := fmt.Sprintf("Delete %s via FairTier Console", filePath)
		if err := client.DeleteContents(ctx, repo, filePath, existing[filePath], msg, author); err != nil {
			return fmt.Errorf("delete %s: %w", filePath, err)
		}
		recordCommit(ctx, repo, "delete", filePath)
	}
	return nil
}

// deleteStaleRendered removes stale definition files, but only the ones this
// mirror can prove it wrote — the file still opens with the rendered header.
//
// Deleting on absence alone was safe while central was the only writer: every
// file in the repo had been rendered from a central row, so "no row" really did
// mean "deleted in the Console". It stops being safe the moment the box's own
// database is the writer (control-plane/workspace split Phase 3). Hydration
// creates a row for every definition file it can parse and REFUSES the rest —
// bad YAML, no id, a name already taken — so a refused file is absent from
// every desired set that follows, and the next converge would delete the
// customer's only copy of it, on their own box, with no one having asked.
//
// Scoping to the render bookkeeping instead is the tempting fix and is wrong in
// the other direction: those rows go away with the pipeline-delete FK cascade,
// so a deleted pipeline's file would never be collected at all, and a rename's
// old path survives only in the snapshot read before the converge. The header
// is the durable marker — every file this mirror has written begins with it,
// and a file it never wrote cannot accidentally acquire one.
//
// A customer who hand-edits a rendered file keeps the header, and so keeps the
// old overwrite-and-delete behaviour. That is exactly what the header text
// promises them, and git history keeps the content recoverable either way.
func deleteStaleRendered[V any](ctx context.Context, client RepoFileClient, repo string, existing map[string]string, desired map[string]V, author *CommitAuthor) error {
	for _, filePath := range sortedPaths(existing) {
		if _, ok := desired[filePath]; ok {
			continue
		}
		content, _, err := client.GetContents(ctx, repo, filePath)
		if err != nil {
			return fmt.Errorf("get %s: %w", filePath, err)
		}
		if !strings.HasPrefix(content, pipelineFileHeader) {
			continue // not ours to delete
		}
		msg := fmt.Sprintf("Delete %s via FairTier Console", filePath)
		if err := client.DeleteContents(ctx, repo, filePath, existing[filePath], msg, author); err != nil {
			return fmt.Errorf("delete %s: %w", filePath, err)
		}
		recordCommit(ctx, repo, "delete", filePath)
	}
	return nil
}

// upsertCredentialFile writes the age-encrypted credential file when it is
// missing or stale. Staleness = the stored render row's fingerprint
// (recipient+plaintext) or blob sha no longer matches — covering credential
// edits, key rotation, out-of-band file edits, and a lost row alike. The
// row is upserted only after a successful put; a row-write failure is
// logged, costing at most one redundant re-encrypt on the next sync.
func (m *PipelineMirror) upsertCredentialFile(ctx context.Context, client RepoFileClient, filePath string, cf credentialFile, recipient string, renders map[PipelineID]PipelineCredentialRender, existing map[string]string, author *CommitAuthor) error {
	if cf.external {
		return nil
	}
	fp := m.Fingerprint.Fingerprint([]byte(credentialFingerprintContext), []byte(recipient), cf.plaintext)
	treeSHA, inTree := existing[filePath]
	if inTree && m.skipTrackedCredentialFile(ctx, filePath, cf, fp, treeSHA, renders) {
		return nil
	}

	ciphertext, err := encryptAge(recipient, cf.plaintext)
	if err != nil {
		return fmt.Errorf("encrypt credentials for pipeline %s: %w", cf.pipelineID, err)
	}
	sha := ""
	if inTree {
		sha = treeSHA
	}
	msg := fmt.Sprintf("Render %s via FairTier Console", filePath)
	newSHA, err := client.PutContents(ctx, pipelinesRepo, filePath, ciphertext, sha, msg, author)
	if err != nil {
		return fmt.Errorf("put %s: %w", filePath, err)
	}
	recordCommit(ctx, planePipelines, "upsert", filePath)
	err = m.Renders.UpsertPipelineCredentialRender(ctx, &PipelineCredentialRender{
		PipelineID:  cf.pipelineID,
		Fingerprint: fp,
		BlobSHA:     newSHA,
	})
	if err != nil && m.Logger != nil {
		m.Logger.WarnContext(ctx, "record credential render", "pipeline", cf.pipelineID, "err", err)
	}
	return nil
}

// skipTrackedCredentialFile reports whether the tracked .age file needs no
// write: either it is current (fingerprint + blob sha match), or it carries
// a foreign commit — which, with an ownership store wired, flips the
// pipeline to box-managed credentials instead of being clobbered (Phase 2B).
func (m *PipelineMirror) skipTrackedCredentialFile(ctx context.Context, filePath string, cf credentialFile, fp, treeSHA string, renders map[PipelineID]PipelineCredentialRender) bool {
	row, ok := renders[cf.pipelineID]
	if !ok {
		return false
	}
	if row.Fingerprint == fp && row.BlobSHA == treeSHA {
		return true
	}
	if row.BlobSHA != treeSHA && m.Ownership != nil {
		// A commit the mirror did not make, on a file only the box can
		// decrypt: hand ownership to the box. A Console credential edit
		// reclaims it.
		m.markCredentialsExternal(ctx, filePath, cf.pipelineID)
		return true
	}
	return false
}

// markCredentialsExternal flips a pipeline to box-managed credentials after
// a foreign .age commit and tells the user. Best-effort: on a failed flag
// write the converge simply re-detects the foreign sha next time.
func (m *PipelineMirror) markCredentialsExternal(ctx context.Context, filePath string, id PipelineID) {
	if err := m.Ownership.SetPipelineCredentialsExternal(ctx, id, true); err != nil {
		if m.Logger != nil {
			m.Logger.WarnContext(ctx, "mark credentials external", "pipeline", id, "err", err)
		}
		return
	}
	// A one-way ownership handover the user has to undo deliberately: worth a
	// counter, because "the Console stopped updating my credentials" is
	// otherwise reported as a bug with nothing in the logs to point at.
	recordAdoption(ctx, planePipelines, outcomeExternal,
		attrRepoPath.String(filePath), attrPipelineID.String(string(id)))
	if m.Notifications == nil {
		return
	}
	n := Notification{
		Type:  "info",
		Title: "Pipeline credentials are now managed in your repo",
		Body:  fmt.Sprintf("%s was changed outside the Console. The Console will no longer re-render this file; edit the pipeline's credentials in the Console to take ownership back.", filePath),
		Link:  "/pipelines?pipeline=" + string(id),
	}
	if err := m.notifyForPipeline(ctx, id, n); err != nil && m.Logger != nil {
		m.Logger.WarnContext(ctx, "notify external credentials", "pipeline", id, "err", err)
	}
}

// notifyForPipeline resolves the pipeline's tenant and raises n for it.
func (m *PipelineMirror) notifyForPipeline(ctx context.Context, id PipelineID, n Notification) error {
	p, err := m.Pipelines.GetPipeline(ctx, id)
	if err != nil {
		return err
	}
	n.CustomerSlug = p.CustomerSlug
	return m.Notifications.Notify(ctx, n)
}

// encryptAge armors plaintext to a single X25519 recipient — the only
// form credentials ever take in the repo.
func encryptAge(recipient string, plaintext []byte) (string, error) {
	rec, err := age.ParseX25519Recipient(recipient)
	if err != nil {
		return "", fmt.Errorf("parse age recipient: %w", err)
	}
	var buf bytes.Buffer
	aw := armor.NewWriter(&buf)
	w, err := age.Encrypt(aw, rec)
	if err != nil {
		return "", fmt.Errorf("age encrypt: %w", err)
	}
	if _, err := w.Write(plaintext); err != nil {
		return "", fmt.Errorf("age encrypt: %w", err)
	}
	if err := w.Close(); err != nil {
		return "", fmt.Errorf("age encrypt: %w", err)
	}
	if err := aw.Close(); err != nil {
		return "", fmt.Errorf("age armor: %w", err)
	}
	return buf.String(), nil
}

// upsertFile creates filePath when absent from existing, or updates it when
// its content differs from the desired render. A file whose content already
// matches is left untouched, so no empty commit is made.
//
// Drift (git-centric gaps #4): a tree blob sha that differs from the last
// recorded render means a commit the mirror did not make. The file is still
// overwritten — Postgres is the truth and the converge stays convergent —
// but the overwrite raises a notification. No recorded row (bootstrap, or a
// lost row) means no drift claim; the row is (re)stamped either way, so the
// signal fires at most once per out-of-band edit.
func (m *PipelineMirror) upsertFile(ctx context.Context, client RepoFileClient, customerSlug, filePath string, df definitionFile, existing map[string]string, defRenders map[PipelineID]PipelineDefinitionRender, author *CommitAuthor) error {
	msg := fmt.Sprintf("Render %s via FairTier Console", filePath)
	treeSHA, inTree := existing[filePath]
	if !inTree {
		newSHA, err := client.PutContents(ctx, pipelinesRepo, filePath, df.content, "", msg, author)
		if err != nil {
			return fmt.Errorf("put %s: %w", filePath, err)
		}
		recordCommit(ctx, planePipelines, "upsert", filePath)
		m.recordDefinitionRender(ctx, df.pipelineID, filePath, newSHA)
		return nil
	}
	current, sha, err := client.GetContents(ctx, pipelinesRepo, filePath)
	if err != nil {
		return fmt.Errorf("get %s: %w", filePath, err)
	}
	row, tracked := defRenders[df.pipelineID]
	if current == df.content {
		m.adoptInSyncRender(ctx, df.pipelineID, filePath, treeSHA, row, tracked)
		return nil
	}
	drifted := tracked && row.Path == filePath && row.BlobSHA != treeSHA
	newSHA, err := client.PutContents(ctx, pipelinesRepo, filePath, df.content, sha, msg, author)
	if err != nil {
		return fmt.Errorf("put %s: %w", filePath, err)
	}
	recordCommit(ctx, planePipelines, "upsert", filePath)
	m.recordDefinitionRender(ctx, df.pipelineID, filePath, newSHA)
	if drifted {
		m.notifyDefinitionDrift(ctx, customerSlug, filePath, df.pipelineID)
	}
	return nil
}

// adoptInSyncRender stamps the drift row when the repo file already equals the
// rendered content but the bookkeeping does not yet reflect it. Adopting an
// untracked or foreign blob sha silently is correct here: either bootstrap
// (the row predates this feature / was lost) or an out-of-band edit that
// landed exactly on the rendered state — nothing was overwritten, so there is
// nothing to report.
func (m *PipelineMirror) adoptInSyncRender(ctx context.Context, id PipelineID, filePath, treeSHA string, row PipelineDefinitionRender, tracked bool) {
	if !tracked || row.BlobSHA != treeSHA || row.Path != filePath {
		m.recordDefinitionRender(ctx, id, filePath, treeSHA)
	}
}

// definitionRenderRows loads the drift bookkeeping for a customer; nil when
// the store is not wired (drift detection off).
func (m *PipelineMirror) definitionRenderRows(ctx context.Context, customerSlug string) (map[PipelineID]PipelineDefinitionRender, error) {
	if m.DefinitionRenders == nil {
		return nil, nil
	}
	rows, err := m.DefinitionRenders.GetPipelineDefinitionRenders(ctx, customerSlug)
	if err != nil {
		return nil, fmt.Errorf("get definition renders: %w", err)
	}
	return rows, nil
}

// recordDefinitionRender best-effort stamps the row after a write (same
// contract as the credential render rows: a lost write costs one missed
// drift signal, never correctness).
func (m *PipelineMirror) recordDefinitionRender(ctx context.Context, id PipelineID, filePath, blobSHA string) {
	if m.DefinitionRenders == nil {
		return
	}
	err := m.DefinitionRenders.UpsertPipelineDefinitionRender(ctx, &PipelineDefinitionRender{
		PipelineID: id,
		Path:       filePath,
		BlobSHA:    blobSHA,
	})
	if err != nil && m.Logger != nil {
		m.Logger.WarnContext(ctx, "record definition render", "pipeline", id, "err", err)
	}
}

// notifyDefinitionDrift raises the in-app notification for an out-of-band
// repo edit the converge just overwrote (overwrite-and-notify, never
// silent). Best-effort.
func (m *PipelineMirror) notifyDefinitionDrift(ctx context.Context, customerSlug, filePath string, id PipelineID) {
	// Recorded before the nil check: the overwrite happened whether or not a
	// notifier is wired to tell the user about it.
	trace.SpanFromContext(ctx).AddEvent("workspace.repo.drift_overwritten", trace.WithAttributes(
		attrRepoPath.String(filePath),
		attrPipelineID.String(string(id)),
	))
	if m.Notifications == nil {
		return
	}
	n := Notification{
		CustomerSlug: customerSlug,
		Type:         "info",
		Title:        "Pipeline file changed outside the Console",
		Body:         fmt.Sprintf("%s in your workspace repo was edited outside the Console; the Console version has been reapplied. The overwritten change remains in the file's git history.", filePath),
		Link:         "/pipelines?pipeline=" + string(id),
	}
	if err := m.Notifications.Notify(ctx, n); err != nil && m.Logger != nil {
		m.Logger.WarnContext(ctx, "notify definition drift", "pipeline", id, "err", err)
	}
}

// pipelineFile is the rendered YAML shape of one pipeline definition. The id
// is the stable key (file names follow the display name and can move on
// rename); credentials are deliberately absent.
type pipelineFile struct {
	ID               string         `yaml:"id"`
	Name             string         `yaml:"name"`
	SourceType       string         `yaml:"source_type"`
	SourceConfig     map[string]any `yaml:"source_config,omitempty"`
	DatasetName      string         `yaml:"dataset_name"`
	Schedule         string         `yaml:"schedule,omitempty"`
	WriteDisposition string         `yaml:"write_disposition,omitempty"`
	MergeStrategy    string         `yaml:"merge_strategy,omitempty"`
	Enabled          bool           `yaml:"enabled"`
}

// pipelineFileSlugs assigns each pipeline its file slug, shared by the
// definition (<slug>.yaml) and credential (<slug>.credentials.age) paths so
// a rename always moves the pair together. Every member of a collision
// group gets an -<id[:8]> suffix (not just the later one), so which
// pipeline was created first never changes an existing path.
func pipelineFileSlugs(pipelines []Pipeline) map[PipelineID]string {
	baseCount := make(map[string]int, len(pipelines))
	for _, p := range pipelines {
		baseCount[pipelineFileSlug(p)]++
	}

	slugs := make(map[PipelineID]string, len(pipelines))
	for _, p := range pipelines {
		slug := pipelineFileSlug(p)
		if baseCount[slug] > 1 {
			slug += "-" + shortID(string(p.ID))
		}
		slugs[p.ID] = slug
	}
	return slugs
}

// definitionFile is one desired pipelines/<slug>.yaml: the rendered content
// plus the owning pipeline (for render bookkeeping and drift attribution).
type definitionFile struct {
	pipelineID PipelineID
	content    string
}

// renderPipelineFiles maps pipelines to path → desired definition file.
func renderPipelineFiles(pipelines []Pipeline, slugs map[PipelineID]string) (map[string]definitionFile, error) {
	files := make(map[string]definitionFile, len(pipelines))
	for _, p := range pipelines {
		content, err := renderPipelineFile(&p)
		if err != nil {
			return nil, fmt.Errorf("render pipeline %s: %w", p.ID, err)
		}
		files["pipelines/"+slugs[p.ID]+".yaml"] = definitionFile{pipelineID: p.ID, content: content}
	}
	return files, nil
}

func renderPipelineFile(p *Pipeline) (string, error) {
	f := pipelineFile{
		ID:               string(p.ID),
		Name:             p.Name,
		SourceType:       p.SourceType,
		DatasetName:      p.DatasetName,
		Schedule:         p.Schedule,
		WriteDisposition: p.WriteDisposition,
		MergeStrategy:    p.MergeStrategy,
		Enabled:          p.Enabled,
	}
	if !isEmptyJSON(p.SourceConfig) {
		if err := json.Unmarshal(p.SourceConfig, &f.SourceConfig); err != nil {
			return "", fmt.Errorf("unmarshal source config: %w", err)
		}
	}
	out, err := yaml.Marshal(f)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}
	return pipelineFileHeader + string(out), nil
}

// pipelineFileSlug derives a file-name-safe slug from the pipeline name,
// falling back to the id for names with no usable characters.
func pipelineFileSlug(p Pipeline) string {
	if slug := nameFileSlug(p.Name); slug != "" {
		return slug
	}
	return shortID(string(p.ID))
}

// nameFileSlug lowercases a display name into a file-name-safe slug; ""
// means the name has no usable characters (the caller falls back to the id).
func nameFileSlug(name string) string {
	var b strings.Builder
	lastDash := true // no leading dash
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z' || r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		case !lastDash:
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func sortedPaths[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

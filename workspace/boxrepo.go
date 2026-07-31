package workspace

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/fairtier/workspace-api/core"
)

// BoxGitCredential is the box-minted, write-scoped Gitea token a box deposits
// via BoxCredentialService (the "credential inversion": the box pushes the
// credential up). It authenticates central's Gitea REST calls when the Console
// edits box repos; central never holds box admin credentials.
type BoxGitCredential struct {
	CustomerSlug string
	Username     string
	Token        string
	Note         string
	UpdatedAt    time.Time
}

// BoxGitCredentialStore persists deposited box git credentials (token
// encrypted at rest).
type BoxGitCredentialStore interface {
	// UpsertBoxGitCredential stores or replaces the credential for a slug.
	UpsertBoxGitCredential(ctx context.Context, cred *BoxGitCredential) error
	// GetBoxGitCredential returns the credential for a slug, or
	// ErrBoxCredentialNotFound.
	GetBoxGitCredential(ctx context.Context, customerSlug string) (*BoxGitCredential, error)
}

// ErrBoxCredentialNotFound means the box has not deposited an editor
// credential yet (seed job not run / not yet rolled out).
var ErrBoxCredentialNotFound = errors.New("the box has not published editor credentials yet")

// BoxSnapshotCredential is the box-minted bearer token guarding the box's
// published rill-snapshot endpoint (the sidecar's AUTH_TOKEN) — the same
// credential inversion as BoxGitCredential. Central presents it when
// proxying a Console Save (SnapshotService) to the box.
type BoxSnapshotCredential struct {
	CustomerSlug string
	Token        string
	Note         string
	UpdatedAt    time.Time
}

// BoxSnapshotCredentialStore persists deposited box snapshot tokens (token
// encrypted at rest).
type BoxSnapshotCredentialStore interface {
	// UpsertBoxSnapshotCredential stores or replaces the credential for a slug.
	UpsertBoxSnapshotCredential(ctx context.Context, cred *BoxSnapshotCredential) error
	// GetBoxSnapshotCredential returns the credential for a slug, or
	// ErrBoxCredentialNotFound.
	GetBoxSnapshotCredential(ctx context.Context, customerSlug string) (*BoxSnapshotCredential, error)
}

// RepoFileEntry is one file in a box repo tree.
type RepoFileEntry struct {
	Path string
	SHA  string
}

// RepoCommit is one commit in a box-repo file's history — the Console's
// "version history" row (git-centric gaps #2).
type RepoCommit struct {
	SHA         string
	Message     string
	AuthorName  string
	AuthorEmail string
	// Date is the author time as Gitea's RFC 3339 string — displayed,
	// never computed on (same policy as PushMirror times).
	Date string
}

// ErrRepoFileChanged means a PutFile was based on a stale blob sha — the file
// changed outside the Console (e.g. in the app's own UI). Refresh and retry.
var ErrRepoFileChanged = errors.New("the file changed outside the Console; refresh and try again")

// CommitAuthor is the git author identity of a Console-originated box-repo
// commit: the acting user. The committer stays a fixed platform identity
// (gitea.platformCommitter), so history reads "authored by <user>,
// committed by the platform". Nil means no acting user (a platform-
// initiated change), which keeps the plain token-owner attribution.
type CommitAuthor struct {
	Name  string
	Email string
}

// resolveCommitAuthor best-effort maps a caller to a git author. Attribution
// is never worth failing a save: a nil reader, lookup error, or a user
// without an email all yield nil (platform identity) — Gitea only honors an
// author override that carries an email.
func resolveCommitAuthor(ctx context.Context, users UserReader, callerID core.UserID) *CommitAuthor {
	if users == nil || callerID == "" {
		return nil
	}
	user, err := users.GetCommitUser(ctx, callerID)
	if err != nil || user.Email == "" {
		return nil
	}
	name := cmp.Or(user.DisplayName, user.Name)
	if name == "" {
		name, _, _ = strings.Cut(user.Email, "@")
	}
	return &CommitAuthor{Name: name, Email: user.Email}
}

// RepoFileClient is the per-box Gitea contents-API surface BoxRepoService
// drives. Implementations (gitea.Client) are constructed per call with the
// tenant's deposited credential.
type RepoFileClient interface {
	ListTree(ctx context.Context, repo string) ([]RepoFileEntry, error)
	GetContents(ctx context.Context, repo, path string) (content string, sha string, err error)
	// PutContents creates (sha == "") or updates (sha == current blob sha) a
	// file as a commit on the default branch, returning the new blob sha. A
	// stale sha yields ErrRepoFileChanged. author (nil-able) becomes the git
	// author; the committer is always the platform token's owner.
	PutContents(ctx context.Context, repo, path, content, sha, message string, author *CommitAuthor) (newSHA string, err error)
	// DeleteContents deletes a file as a commit on the default branch,
	// guarded by the current blob sha (stale sha yields ErrRepoFileChanged).
	// Used by PipelineMirror only; the Console editor never deletes files.
	DeleteContents(ctx context.Context, repo, path, sha, message string, author *CommitAuthor) error
	// ListCommits returns the newest-first commits touching path (whole repo
	// when empty), at most limit entries. Follows the current path only.
	ListCommits(ctx context.Context, repo, path string, limit int) ([]RepoCommit, error)
	// GetContentsAt is GetContents as of a commit sha (empty ref = HEAD).
	GetContentsAt(ctx context.Context, repo, path, ref string) (content string, sha string, err error)
}

// boxRepos is the allowlist of box repos the Console may edit, mapping the
// public repo name to gating logic. Both live under the box's fixed Gitea
// admin owner.
var boxRepoAllowlist = map[string]struct{}{
	"rill":            {},
	"transformations": {},
}

// boxRepoMirrorAllowlist is the (wider) allowlist of box repos a customer may
// push-mirror to their own git hosting. The `pipelines` repo is
// platform-rendered — not Console-editable — but mirroring it is exactly the
// offsite-copy/no-lock-in story.
var boxRepoMirrorAllowlist = map[string]struct{}{
	"rill":            {},
	"transformations": {},
	"pipelines":       {},
}

// PushMirror is one configured Gitea push mirror (a repo replicated to the
// customer's own GitHub/GitLab). Times come through as Gitea's RFC 3339
// strings — displayed, never computed on.
type PushMirror struct {
	RemoteName   string
	RemoteURL    string
	LastUpdate   string
	LastError    string
	SyncOnCommit bool
}

// pushMirrorInterval is the periodic re-push cadence. Mirrors also push on
// every commit (sync_on_commit), so this is only the self-heal backstop.
const pushMirrorInterval = "8h0m0s"

// RepoMirrorClient is the push-mirror surface of the per-box Gitea API.
// Implementations (gitea.Client) are constructed per call with the tenant's
// deposited credential, same as RepoFileClient.
type RepoMirrorClient interface {
	ListPushMirrors(ctx context.Context, repo string) ([]PushMirror, error)
	// AddPushMirror stores the remote (credential included) in the box's
	// Gitea and starts mirroring. The credential never persists centrally.
	AddPushMirror(ctx context.Context, repo, remoteURL, username, password, interval string) error
	DeletePushMirror(ctx context.Context, repo, remoteName string) error
	SyncPushMirrors(ctx context.Context, repo string) error
}

// boxRepoBlockedFiles are platform-managed files the Console must never edit:
// they are overlaid from ConfigMaps / seed jobs, and hand edits would be
// silently overwritten (or worse, break the overlay). rill.yaml is NOT here:
// it moved to customer ownership (seeded into the repo, then tracked there),
// so a customer can run the project outside the box.
var boxRepoBlockedFiles = map[string]struct{}{
	"duckdb.yaml": {},
	".env":        {},
}

// BoxRepoService lets the Console read and edit customer-authored files in
// the box's Gitea repos, tenant-scoped and gated on the VM substrate. It also
// manages the repos' push mirrors to customer-owned remotes.
type BoxRepoService struct {
	Workspaces  Resolver
	Credentials BoxGitCredentialStore
	// Users resolves the acting user for commit attribution (git author).
	// Optional: nil keeps the plain platform attribution.
	Users UserReader
	// NewClient builds a Gitea client for a box. Injectable for tests; the
	// default (set in main.go) constructs gitea.Client.
	NewClient func(baseURL, username, token string) RepoFileClient
	// NewMirrorClient builds the push-mirror client for a box (in production
	// the same gitea.Client type behind a second interface).
	NewMirrorClient func(baseURL, username, token string) RepoMirrorClient
	Logger          *slog.Logger
}

// ListFiles returns the customer-authored file tree of repo, hiding dotfiles
// and platform-managed files.
func (s *BoxRepoService) ListFiles(ctx context.Context, callerID core.UserID, repo string) ([]RepoFileEntry, error) {
	client, err := s.clientFor(ctx, callerID, repo)
	if err != nil {
		return nil, err
	}
	entries, err := client.ListTree(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("list %s tree: %w", repo, err)
	}
	visible := entries[:0]
	for _, e := range entries {
		if isHiddenRepoPath(e.Path) {
			continue
		}
		visible = append(visible, e)
	}
	return visible, nil
}

// GetFile returns one file's content and blob sha.
func (s *BoxRepoService) GetFile(ctx context.Context, callerID core.UserID, repo, filePath string) (content, sha string, err error) {
	client, err := s.clientFor(ctx, callerID, repo)
	if err != nil {
		return "", "", err
	}
	if err := validateRepoPath(filePath); err != nil {
		return "", "", err
	}
	return client.GetContents(ctx, repo, filePath)
}

// PutFile creates or updates one file as a commit on the default branch.
func (s *BoxRepoService) PutFile(ctx context.Context, callerID core.UserID, repo, filePath, content, sha, message string) (string, error) {
	client, err := s.clientFor(ctx, callerID, repo)
	if err != nil {
		return "", err
	}
	if err := validateRepoPath(filePath); err != nil {
		return "", err
	}
	if message == "" {
		message = fmt.Sprintf("Update %s via FairTier Console", filePath)
	}
	return client.PutContents(ctx, repo, filePath, content, sha, message, resolveCommitAuthor(ctx, s.Users, callerID))
}

// DeleteFile removes one file as a commit on the default branch, guarded by
// the current blob sha. Used by the demo teardown to prune seed-committed
// files; the Console editor itself never deletes. A stale sha yields
// ErrRepoFileChanged.
func (s *BoxRepoService) DeleteFile(ctx context.Context, callerID core.UserID, repo, filePath, sha, message string) error {
	client, err := s.clientFor(ctx, callerID, repo)
	if err != nil {
		return err
	}
	if err := validateRepoPath(filePath); err != nil {
		return err
	}
	if message == "" {
		message = fmt.Sprintf("Delete %s via FairTier Console", filePath)
	}
	return client.DeleteContents(ctx, repo, filePath, sha, message, resolveCommitAuthor(ctx, s.Users, callerID))
}

// fileHistoryLimit caps a version-history listing; the Console shows one
// screenful, not an archive browser.
const fileHistoryLimit = 20

// ListFileHistory returns the newest-first commit history of one file (the
// Console's "version history"; git-centric gaps #2). Follows the current
// path only — a renamed file's history starts at the rename.
func (s *BoxRepoService) ListFileHistory(ctx context.Context, callerID core.UserID, repo, filePath string) ([]RepoCommit, error) {
	client, err := s.clientFor(ctx, callerID, repo)
	if err != nil {
		return nil, err
	}
	if err := validateRepoPath(filePath); err != nil {
		return nil, err
	}
	commits, err := client.ListCommits(ctx, repo, filePath, fileHistoryLimit)
	if err != nil {
		return nil, fmt.Errorf("list %s history: %w", filePath, err)
	}
	return commits, nil
}

// GetFileAtRef returns one file's content as of a commit sha, for the
// Console's version preview/restore. A restore is a plain PutFile of this
// content guarded by the CURRENT blob sha — a forward commit, never a git
// revert, so the conflict policy is untouched.
func (s *BoxRepoService) GetFileAtRef(ctx context.Context, callerID core.UserID, repo, filePath, ref string) (string, error) {
	client, err := s.clientFor(ctx, callerID, repo)
	if err != nil {
		return "", err
	}
	if err := validateRepoPath(filePath); err != nil {
		return "", err
	}
	if !isCommitSHA(ref) {
		return "", &ErrInvalidSourceConfig{Field: "ref", Msg: "ref must be a commit sha"}
	}
	content, _, err := client.GetContentsAt(ctx, repo, filePath, ref)
	if err != nil {
		return "", fmt.Errorf("get %s at %s: %w", filePath, ref, err)
	}
	return content, nil
}

// isCommitSHA accepts only (abbreviated) hex commit hashes — refs come from
// ListFileHistory, never free-form branch names.
func isCommitSHA(ref string) bool {
	if len(ref) < 7 || len(ref) > 64 {
		return false
	}
	for _, r := range ref {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// clientFor runs the shared gates (tenant, substrate, app enablement,
// deposited credential) and returns a Gitea client for the caller's box.
func (s *BoxRepoService) clientFor(ctx context.Context, callerID core.UserID, repo string) (RepoFileClient, error) {
	if _, ok := boxRepoAllowlist[repo]; !ok {
		return nil, &ErrInvalidSourceConfig{Field: "repo", Msg: fmt.Sprintf("repo must be one of: rill, transformations (got %q)", repo)}
	}
	baseURL, cred, err := s.boxGitea(ctx, callerID, repo)
	if err != nil {
		return nil, err
	}
	return s.NewClient(baseURL, cred.Username, cred.Token), nil
}

// mirrorClientFor is clientFor's twin for the push-mirror surface, with its
// own (wider) repo allowlist.
func (s *BoxRepoService) mirrorClientFor(ctx context.Context, callerID core.UserID, repo string) (RepoMirrorClient, error) {
	if _, ok := boxRepoMirrorAllowlist[repo]; !ok {
		return nil, &ErrInvalidSourceConfig{Field: "repo", Msg: fmt.Sprintf("repo must be one of: rill, transformations, pipelines (got %q)", repo)}
	}
	baseURL, cred, err := s.boxGitea(ctx, callerID, repo)
	if err != nil {
		return nil, err
	}
	return s.NewMirrorClient(baseURL, cred.Username, cred.Token), nil
}

// boxGitea runs the tenant/substrate/app/credential gates shared by the file
// and mirror surfaces and returns the box's Gitea base URL plus the
// deposited credential.
func (s *BoxRepoService) boxGitea(ctx context.Context, callerID core.UserID, repo string) (string, *BoxGitCredential, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return "", nil, fmt.Errorf("get customer: %w", err)
	}
	if !ws.OnVM {
		return "", nil, ErrBoxRepoUnavailable
	}
	if repo == "rill" && !ws.RillEnabled {
		return "", nil, ErrBoxRepoUnavailable
	}
	domainName := strings.TrimPrefix(ws.CustomerDomain, "*.")
	if domainName == "" {
		return "", nil, ErrBoxRepoUnavailable
	}

	cred, err := s.Credentials.GetBoxGitCredential(ctx, ws.Slug)
	if err != nil {
		return "", nil, err
	}

	return "https://git." + domainName, cred, nil
}

// ErrBoxRepoUnavailable means the box repo editor does not apply to this
// customer (not a VM box, app disabled, or not yet provisioned).
var ErrBoxRepoUnavailable = errors.New("the box repo editor is not available for this workspace")

// ErrBoxUnreachable means the box's Gitea could not be reached (dial refused,
// timeout, reset, EOF) — a transient outage distinct from the permanent
// ErrBoxRepoUnavailable ("editor doesn't apply here"). Read paths that hit the
// live box (e.g. pipeline version history) return it so the API answers a
// retryable CodeUnavailable instead of an opaque internal error.
var ErrBoxUnreachable = errors.New("the box is temporarily unreachable")

// SetPushMirror configures (or replaces) the repo's push mirror to the
// customer's own remote. Any previously configured mirror is removed first —
// the Console model is one mirror per repo. The remote credential travels
// straight through to the box's Gitea and is never stored or logged here.
func (s *BoxRepoService) SetPushMirror(ctx context.Context, callerID core.UserID, repo, remoteURL, username, password string) error {
	if err := validateMirrorRemote(remoteURL); err != nil {
		return err
	}
	if username == "" {
		return &ErrInvalidSourceConfig{Field: "remote_username", Msg: "remote username is required (GitHub: your account name; GitLab: \"oauth2\")"}
	}
	if password == "" {
		return &ErrInvalidSourceConfig{Field: "remote_password", Msg: "remote token is required"}
	}

	client, err := s.mirrorClientFor(ctx, callerID, repo)
	if err != nil {
		return err
	}

	existing, err := client.ListPushMirrors(ctx, repo)
	if err != nil {
		return fmt.Errorf("list push mirrors: %w", err)
	}
	for _, m := range existing {
		if err := client.DeletePushMirror(ctx, repo, m.RemoteName); err != nil {
			return fmt.Errorf("remove previous mirror: %w", err)
		}
	}

	if err := client.AddPushMirror(ctx, repo, remoteURL, username, password, pushMirrorInterval); err != nil {
		return fmt.Errorf("add push mirror: %w", err)
	}
	return nil
}

// GetPushMirror returns the repo's mirror status; ok=false when none is
// configured.
func (s *BoxRepoService) GetPushMirror(ctx context.Context, callerID core.UserID, repo string) (*PushMirror, bool, error) {
	client, err := s.mirrorClientFor(ctx, callerID, repo)
	if err != nil {
		return nil, false, err
	}
	mirrors, err := client.ListPushMirrors(ctx, repo)
	if err != nil {
		return nil, false, fmt.Errorf("list push mirrors: %w", err)
	}
	if len(mirrors) == 0 {
		return nil, false, nil
	}
	m := mirrors[0]
	m.RemoteURL = stripUserinfo(m.RemoteURL)
	return &m, true, nil
}

// DeletePushMirror removes the repo's push mirror(s) and the remote
// credential Gitea stored with them.
func (s *BoxRepoService) DeletePushMirror(ctx context.Context, callerID core.UserID, repo string) error {
	client, err := s.mirrorClientFor(ctx, callerID, repo)
	if err != nil {
		return err
	}
	mirrors, err := client.ListPushMirrors(ctx, repo)
	if err != nil {
		return fmt.Errorf("list push mirrors: %w", err)
	}
	for _, m := range mirrors {
		if err := client.DeletePushMirror(ctx, repo, m.RemoteName); err != nil {
			return fmt.Errorf("delete push mirror: %w", err)
		}
	}
	return nil
}

// SyncPushMirror triggers an immediate push to the repo's mirror remote.
func (s *BoxRepoService) SyncPushMirror(ctx context.Context, callerID core.UserID, repo string) error {
	client, err := s.mirrorClientFor(ctx, callerID, repo)
	if err != nil {
		return err
	}
	if err := client.SyncPushMirrors(ctx, repo); err != nil {
		return fmt.Errorf("sync push mirrors: %w", err)
	}
	return nil
}

// validateMirrorRemote requires a clean https remote with no embedded
// credentials (the token is passed separately and stays out of URLs/logs).
func validateMirrorRemote(remoteURL string) error {
	u, err := url.Parse(remoteURL)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return &ErrInvalidSourceConfig{Field: "remote_url", Msg: "remote URL must be an https:// clone URL"}
	}
	if u.User != nil {
		return &ErrInvalidSourceConfig{Field: "remote_url", Msg: "remote URL must not embed credentials; pass the token separately"}
	}
	return nil
}

// stripUserinfo removes any credential part from a URL before display.
func stripUserinfo(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	u.User = nil
	return u.String()
}

// validateRepoPath rejects non-clean or platform-managed paths.
func validateRepoPath(p string) error {
	if p == "" {
		return &ErrInvalidSourceConfig{Field: "path", Msg: "path is required"}
	}
	clean := path.Clean(p)
	if clean != p || strings.HasPrefix(clean, "/") || strings.HasPrefix(clean, "..") {
		return &ErrInvalidSourceConfig{Field: "path", Msg: fmt.Sprintf("path %q is not a clean relative path", p)}
	}
	if isHiddenRepoPath(clean) {
		return &ErrInvalidSourceConfig{Field: "path", Msg: fmt.Sprintf("path %q is platform-managed and cannot be edited", p)}
	}
	return nil
}

// isHiddenRepoPath reports whether a path is hidden from the editor: dotfiles
// anywhere in the path, or platform-managed files at the repo root.
func isHiddenRepoPath(p string) bool {
	if _, blocked := boxRepoBlockedFiles[p]; blocked {
		return true
	}
	for seg := range strings.SplitSeq(p, "/") {
		if strings.HasPrefix(seg, ".") {
			return true
		}
	}
	return false
}

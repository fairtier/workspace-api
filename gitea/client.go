// Package gitea is a minimal client for the Gitea REST API surface the
// Console's box-repo editor needs: list a repo tree, read a file, commit a
// file change. It talks to the per-box Gitea over its public ingress
// (git.customer-<slug>.<baseDomain>) using the box-deposited write-scoped
// token — see workspace.BoxRepoService.
package gitea

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"

	"github.com/fairtier/workspace-api/telemetry"
	"github.com/fairtier/workspace-api/workspace"
)

// Owner is the fixed Gitea admin user owning all platform-seeded repos on a
// box.
const Owner = "fairtier-admin"

// Client calls one box's Gitea REST API.
type Client struct {
	// BaseURL is the Gitea root, e.g. https://git.customer-acme.fairtier.com.
	BaseURL string
	// Username/Token authenticate via basic auth (a write:repository token).
	Username string
	Token    string
	// HTTPClient overrides the default client (tests).
	HTTPClient *http.Client
}

// NewClient constructs a client for one box; satisfies the
// workspace.BoxRepoService.NewClient factory shape.
func NewClient(baseURL, username, token string) workspace.RepoFileClient {
	return &Client{BaseURL: baseURL, Username: username, Token: token}
}

// NewMirrorClient is NewClient behind the push-mirror interface; satisfies
// the workspace.BoxRepoService.NewMirrorClient factory shape.
func NewMirrorClient(baseURL, username, token string) workspace.RepoMirrorClient {
	return &Client{BaseURL: baseURL, Username: username, Token: token}
}

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return tracedClient
}

// tracedClient is the default client, built once rather than per call so the
// instrumented transport — and the connection pool under it — is shared. Every
// mirror converge and repo-editor read shows up as a client span through it,
// which is what attributes a slow save to the box's Gitea.
var tracedClient = telemetry.InstrumentHTTPClient(&http.Client{Timeout: 30 * time.Second})

// maxAttempts bounds retries of an idempotent request; retryBackoff is the
// pause between them. A per-box Gitea can drop ingress for a sub-second-to-
// seconds window (the box's own OOM cascade under a heavy demo run), and a
// read-only history call landing in that window should return data on the
// next attempt rather than surfacing a hard 500 to Console.
const (
	maxAttempts  = 3
	retryBackoff = 300 * time.Millisecond
)

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	var err error
	for attempt := 1; ; attempt++ {
		err = c.doOnce(ctx, method, path, body, out)
		// Only idempotent GETs are safe to replay, and only a transport-level
		// blip (dial/connection refused, reset, EOF) is worth retrying — a
		// non-2xx status is a real answer from a reachable box.
		if err == nil || attempt >= maxAttempts || !isRetryable(method, err) {
			return classifyTransport(method, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryBackoff):
		}
	}
}

func (c *Client) doOnce(ctx context.Context, method, path string, body any, out any) error {
	req, err := c.buildRequest(ctx, method, path, body)
	if err != nil {
		return err
	}

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return fmt.Errorf("gitea %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if err := checkResponseStatus(resp, method, path); err != nil {
		return err
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("decode gitea response: %w", err)
		}
	}
	return nil
}

// isRetryable reports whether a failed request is safe to replay: only GETs
// (side-effect-free) and only when the failure is a transport error from
// http.Client.Do — not a checkResponseStatus/decode error, which means the
// box already answered.
func isRetryable(method string, err error) bool {
	// Only side-effect-free GETs are safe to replay.
	return method == http.MethodGet && isTransportErr(err)
}

// isTransportErr reports whether err is a transport-level failure from
// http.Client.Do (dial refused/reset, timeout, EOF) — i.e. the box was
// unreachable — rather than a checkResponseStatus/decode error, which means
// the box already answered. ErrRepoFileChanged is one such mapped status and
// is explicitly excluded.
func isTransportErr(err error) bool {
	if err == nil || errors.Is(err, workspace.ErrRepoFileChanged) {
		return false
	}
	if _, ok := errors.AsType[net.Error](err); ok {
		return true
	}
	// Non-timeout dial failures (connection refused/reset) and EOF surface as
	// *net.OpError / syscall errno, not net.Error timeouts.
	if _, ok := errors.AsType[*net.OpError](err); ok {
		return true
	}
	return errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET)
}

// classifyTransport marks a transport-level failure (box unreachable: dial
// refused, timeout, reset, EOF) with workspace.ErrBoxUnreachable so callers can
// surface a retryable CodeUnavailable instead of an opaque internal error.
//
// Only idempotent GETs are marked: a write (POST/PUT/DELETE) whose response was
// lost to a transport error may already have taken effect on the box, so
// presenting it as retryable Unavailable would invite a client retry that
// double-applies it. Non-GET transport failures, and any non-2xx status (a real
// answer from a reachable box), are left unwrapped.
func classifyTransport(method string, err error) error {
	if method == http.MethodGet && isTransportErr(err) {
		return fmt.Errorf("%w: %w", workspace.ErrBoxUnreachable, err)
	}
	return err
}

// buildRequest marshals body (when non-nil) and constructs the authenticated
// Gitea request.
func (c *Client) buildRequest(ctx context.Context, method, path string, body any) (*http.Request, error) {
	var reqBody io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request: %w", err)
		}
		reqBody = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, reqBody)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.SetBasicAuth(c.Username, c.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

// checkResponseStatus maps a non-2xx Gitea response to an error, translating
// stale-sha conflicts to workspace.ErrRepoFileChanged.
func checkResponseStatus(resp *http.Response, method, path string) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	// The contents API reports a stale-sha update as 409 (and some
	// versions as 422); both mean "changed under you".
	if resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusUnprocessableEntity {
		return workspace.ErrRepoFileChanged
	}
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("gitea %s %s: status %d: %s", method, path, resp.StatusCode, bytes.TrimSpace(snippet))
}

type treeResponse struct {
	Tree []struct {
		Path string `json:"path"`
		Type string `json:"type"`
		SHA  string `json:"sha"`
	} `json:"tree"`
	Truncated bool `json:"truncated"`
}

// ListTree returns all blobs in the repo's default branch (HEAD).
func (c *Client) ListTree(ctx context.Context, repo string) ([]workspace.RepoFileEntry, error) {
	var tr treeResponse
	path := fmt.Sprintf("/api/v1/repos/%s/%s/git/trees/HEAD?recursive=true", Owner, url.PathEscape(repo))
	if err := c.do(ctx, http.MethodGet, path, nil, &tr); err != nil {
		return nil, err
	}
	entries := make([]workspace.RepoFileEntry, 0, len(tr.Tree))
	for _, e := range tr.Tree {
		if e.Type != "blob" {
			continue
		}
		entries = append(entries, workspace.RepoFileEntry{Path: e.Path, SHA: e.SHA})
	}
	return entries, nil
}

type contentsResponse struct {
	Content  string `json:"content"`
	Encoding string `json:"encoding"`
	SHA      string `json:"sha"`
}

// GetContents returns one file's decoded content and blob sha (at HEAD).
func (c *Client) GetContents(ctx context.Context, repo, filePath string) (string, string, error) {
	return c.GetContentsAt(ctx, repo, filePath, "")
}

// GetContentsAt is GetContents as of a ref (commit sha); empty ref = HEAD.
func (c *Client) GetContentsAt(ctx context.Context, repo, filePath, ref string) (string, string, error) {
	var cr contentsResponse
	path := fmt.Sprintf("/api/v1/repos/%s/%s/contents/%s", Owner, url.PathEscape(repo), escapePath(filePath))
	if ref != "" {
		path += "?ref=" + url.QueryEscape(ref)
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &cr); err != nil {
		return "", "", err
	}
	if cr.Encoding != "base64" {
		return cr.Content, cr.SHA, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(cr.Content)
	if err != nil {
		return "", "", fmt.Errorf("decode file content: %w", err)
	}
	return string(decoded), cr.SHA, nil
}

// commitIdentity is the Gitea contents-API author/committer shape.
type commitIdentity struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// platformCommitter accompanies every author override. Sending only an
// author makes Gitea mirror it into the committer as well (verified on
// 1.26 — it does NOT fall back to the token owner), which would lose the
// authored-by-user / committed-by-platform split. With no author, both
// fields stay absent and the commit is attributed to the token owner.
var platformCommitter = &commitIdentity{Name: "FairTier Console", Email: "noreply@fairtier.com"}

func toCommitIdentity(a *workspace.CommitAuthor) (author, committer *commitIdentity) {
	if a == nil {
		return nil, nil
	}
	return &commitIdentity{Name: a.Name, Email: a.Email}, platformCommitter
}

type putContentsRequest struct {
	Content   string          `json:"content"`
	Message   string          `json:"message"`
	SHA       string          `json:"sha,omitempty"`
	Author    *commitIdentity `json:"author,omitempty"`
	Committer *commitIdentity `json:"committer,omitempty"`
}

type putContentsResponse struct {
	Content struct {
		SHA string `json:"sha"`
	} `json:"content"`
}

// PutContents creates (sha == "") or updates (sha == current blob sha) a file
// as a plain commit on the default branch — never a force push, so the box
// sidecar's fast-forward pull always applies it.
func (c *Client) PutContents(ctx context.Context, repo, filePath, content, sha, message string, author *workspace.CommitAuthor) (string, error) {
	method := http.MethodPost // create
	if sha != "" {
		method = http.MethodPut // update, guarded by the blob sha
	}
	body := putContentsRequest{
		Content: base64.StdEncoding.EncodeToString([]byte(content)),
		Message: message,
		SHA:     sha,
	}
	body.Author, body.Committer = toCommitIdentity(author)
	var pr putContentsResponse
	path := fmt.Sprintf("/api/v1/repos/%s/%s/contents/%s", Owner, url.PathEscape(repo), escapePath(filePath))
	if err := c.do(ctx, method, path, body, &pr); err != nil {
		return "", err
	}
	return pr.Content.SHA, nil
}

type deleteContentsRequest struct {
	Message   string          `json:"message"`
	SHA       string          `json:"sha"`
	Author    *commitIdentity `json:"author,omitempty"`
	Committer *commitIdentity `json:"committer,omitempty"`
}

// DeleteContents deletes a file as a plain commit on the default branch,
// guarded by the current blob sha. A stale sha yields ErrRepoFileChanged.
func (c *Client) DeleteContents(ctx context.Context, repo, filePath, sha, message string, author *workspace.CommitAuthor) error {
	body := deleteContentsRequest{Message: message, SHA: sha}
	body.Author, body.Committer = toCommitIdentity(author)
	path := fmt.Sprintf("/api/v1/repos/%s/%s/contents/%s", Owner, url.PathEscape(repo), escapePath(filePath))
	return c.do(ctx, http.MethodDelete, path, body, nil)
}

// commitListItem is the slice element of the Gitea commits API, reduced to
// what the Console's version history shows.
type commitListItem struct {
	SHA    string `json:"sha"`
	Commit struct {
		Message string `json:"message"`
		Author  struct {
			Name  string `json:"name"`
			Email string `json:"email"`
			Date  string `json:"date"`
		} `json:"author"`
	} `json:"commit"`
}

// ListCommits returns the newest-first commits touching filePath (the whole
// repo when empty), at most limit entries. History follows the current path
// only (no rename tracking, plain `git log -- <path>` semantics).
func (c *Client) ListCommits(ctx context.Context, repo, filePath string, limit int) ([]workspace.RepoCommit, error) {
	q := url.Values{}
	q.Set("limit", fmt.Sprint(limit))
	// Skip diff stats, signature verification, and file lists — metadata only.
	q.Set("stat", "false")
	q.Set("verification", "false")
	q.Set("files", "false")
	if filePath != "" {
		q.Set("path", filePath)
	}
	var items []commitListItem
	path := fmt.Sprintf("/api/v1/repos/%s/%s/commits?%s", Owner, url.PathEscape(repo), q.Encode())
	if err := c.do(ctx, http.MethodGet, path, nil, &items); err != nil {
		// Gitea answers 409 for a commit listing on an EMPTY repo, which the
		// shared status mapping reads as a stale-sha conflict — here it just
		// means "no history yet".
		if errors.Is(err, workspace.ErrRepoFileChanged) {
			return nil, nil
		}
		return nil, err
	}
	commits := make([]workspace.RepoCommit, 0, len(items))
	for _, it := range items {
		commits = append(commits, workspace.RepoCommit{
			SHA:         it.SHA,
			Message:     it.Commit.Message,
			AuthorName:  it.Commit.Author.Name,
			AuthorEmail: it.Commit.Author.Email,
			Date:        it.Commit.Author.Date,
		})
	}
	return commits, nil
}

type pushMirrorResponse struct {
	RemoteName    string `json:"remote_name"`
	RemoteAddress string `json:"remote_address"`
	LastUpdate    string `json:"last_update"`
	LastError     string `json:"last_error"`
	SyncOnCommit  bool   `json:"sync_on_commit"`
}

func toDomainPushMirror(m pushMirrorResponse) workspace.PushMirror {
	return workspace.PushMirror{
		RemoteName:   m.RemoteName,
		RemoteURL:    m.RemoteAddress,
		LastUpdate:   m.LastUpdate,
		LastError:    m.LastError,
		SyncOnCommit: m.SyncOnCommit,
	}
}

// ListPushMirrors returns the repo's configured push mirrors.
func (c *Client) ListPushMirrors(ctx context.Context, repo string) ([]workspace.PushMirror, error) {
	var mirrors []pushMirrorResponse
	path := fmt.Sprintf("/api/v1/repos/%s/%s/push_mirrors", Owner, url.PathEscape(repo))
	if err := c.do(ctx, http.MethodGet, path, nil, &mirrors); err != nil {
		return nil, err
	}
	out := make([]workspace.PushMirror, 0, len(mirrors))
	for _, m := range mirrors {
		out = append(out, toDomainPushMirror(m))
	}
	return out, nil
}

type addPushMirrorRequest struct {
	RemoteAddress  string `json:"remote_address"`
	RemoteUsername string `json:"remote_username"`
	RemotePassword string `json:"remote_password"`
	Interval       string `json:"interval"`
	SyncOnCommit   bool   `json:"sync_on_commit"`
}

// AddPushMirror configures a push mirror on the repo. The remote credential
// is stored by the box's Gitea (not centrally); the mirror pushes on every
// commit plus the given interval.
func (c *Client) AddPushMirror(ctx context.Context, repo, remoteURL, username, password, interval string) error {
	body := addPushMirrorRequest{
		RemoteAddress:  remoteURL,
		RemoteUsername: username,
		RemotePassword: password,
		Interval:       interval,
		SyncOnCommit:   true,
	}
	path := fmt.Sprintf("/api/v1/repos/%s/%s/push_mirrors", Owner, url.PathEscape(repo))
	return c.do(ctx, http.MethodPost, path, body, nil)
}

// DeletePushMirror removes one push mirror by its Gitea-assigned remote name.
func (c *Client) DeletePushMirror(ctx context.Context, repo, remoteName string) error {
	path := fmt.Sprintf("/api/v1/repos/%s/%s/push_mirrors/%s", Owner, url.PathEscape(repo), url.PathEscape(remoteName))
	return c.do(ctx, http.MethodDelete, path, nil, nil)
}

// SyncPushMirrors triggers an immediate push of all the repo's mirrors.
func (c *Client) SyncPushMirrors(ctx context.Context, repo string) error {
	path := fmt.Sprintf("/api/v1/repos/%s/%s/push_mirrors-sync", Owner, url.PathEscape(repo))
	return c.do(ctx, http.MethodPost, path, nil, nil)
}

// escapePath escapes each path segment while keeping the slashes.
func escapePath(p string) string {
	segs := strings.Split(p, "/")
	for i, seg := range segs {
		segs[i] = url.PathEscape(seg)
	}
	return strings.Join(segs, "/")
}

package workspace

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/fairtier/workspace-api/core"
)

// Workspace access tokens (PATs): long-lived, revocable bearer credentials a
// person mints for a non-interactive client of their own — today an MCP client
// such as a coding agent. A token acts as the person who minted it, within its
// scopes. It is accepted only by the /mcp endpoint; the ConnectRPC services
// keep authenticating interactive sign-in tokens alone.

// Access token scopes. Write implies read: a token that may trigger a
// pipeline but not look at it would be useless to the agent holding it.
const (
	ScopeWorkspaceRead  = "workspace:read"
	ScopeWorkspaceWrite = "workspace:write"
)

// AccessTokenPrefix marks a workspace access token's plaintext. It makes a
// leaked token recognizable to secret scanners, and lets a verifier route a
// bearer value without trying to parse it as a JWT first.
const AccessTokenPrefix = "ftw_"

const (
	accessTokenSecretBytes = 32
	// displayPrefixLen is how much of the plaintext the list view shows:
	// "ftw_" plus 8 hex characters (32 bits) — enough to tell tokens apart,
	// nowhere near enough to guess the rest.
	displayPrefixLen = len(AccessTokenPrefix) + 8

	DefaultAccessTokenLifetime = 90 * 24 * time.Hour
	MaxAccessTokenLifetime     = 365 * 24 * time.Hour
	maxAccessTokenNameLen      = 100

	// lastUsedThrottle bounds how often a token's last_used_at is written:
	// an agent can make dozens of calls a minute, and the column only has
	// to answer "is this token still in use".
	lastUsedThrottle = time.Minute
)

var (
	// ErrAccessTokenNotFound means no token with that id belongs to the caller.
	ErrAccessTokenNotFound = errors.New("access token not found")
	// ErrInvalidAccessToken is the single answer to every failed bearer
	// verification — unknown, malformed, revoked, expired or foreign — so a
	// caller probing tokens learns nothing about which it hit.
	ErrInvalidAccessToken = errors.New("invalid access token")
)

// AccessToken is a stored token's metadata. The secret itself is never held
// here: only its SHA-256, which the store indexes.
type AccessToken struct {
	ID           string
	CustomerSlug string
	UserSub      core.UserID
	Name         string
	Prefix       string
	Hash         []byte
	Scopes       []string
	CreatedAt    time.Time
	ExpiresAt    time.Time
	LastUsedAt   *time.Time
	RevokedAt    *time.Time
}

// HasScope reports whether the token grants scope, write implying read.
func (t *AccessToken) HasScope(scope string) bool {
	return HasAccessScope(t.Scopes, scope)
}

// HasAccessScope reports whether scopes grants scope, write implying read.
func HasAccessScope(scopes []string, scope string) bool {
	if slices.Contains(scopes, scope) {
		return true
	}
	return scope == ScopeWorkspaceRead && slices.Contains(scopes, ScopeWorkspaceWrite)
}

// AccessTokenStore persists access tokens.
type AccessTokenStore interface {
	// CreateAccessToken inserts t, setting ID and CreatedAt.
	CreateAccessToken(ctx context.Context, t *AccessToken) error
	// ListAccessTokens returns one person's tokens, newest first.
	ListAccessTokens(ctx context.Context, customerSlug string, userSub core.UserID) ([]AccessToken, error)
	// GetAccessTokenByHash returns the token with that secret hash, or
	// ErrAccessTokenNotFound. It does not filter on revocation or expiry;
	// the service decides.
	GetAccessTokenByHash(ctx context.Context, hash []byte) (*AccessToken, error)
	// RevokeAccessToken sets revoked_at on one of the person's tokens (a
	// no-op when already revoked), or returns ErrAccessTokenNotFound.
	RevokeAccessToken(ctx context.Context, customerSlug string, userSub core.UserID, id string, at time.Time) error
	// TouchAccessToken records a use.
	TouchAccessToken(ctx context.Context, id string, at time.Time) error
}

// AccessTokenService mints, lists, revokes and verifies access tokens.
type AccessTokenService struct {
	Workspaces Resolver
	Tokens     AccessTokenStore
	Logger     *slog.Logger
	// Now and Random are seams for tests; nil uses time.Now and crypto/rand.
	Now    func() time.Time
	Random func([]byte) error

	mu       sync.Mutex
	lastSeen map[string]time.Time // token id -> last recorded use
}

// CreateAccessTokenInput is what a person asks for when minting a token.
type CreateAccessTokenInput struct {
	Name   string
	Scopes []string
	// Lifetime is the requested validity; zero means the default.
	Lifetime time.Duration
}

// CreateAccessToken mints a token for the caller and returns it with its
// plaintext secret. The secret is never stored or logged; this is the only
// place it exists.
func (s *AccessTokenService) CreateAccessToken(ctx context.Context, callerID core.UserID, in CreateAccessTokenInput) (*AccessToken, string, error) {
	name, scopes, lifetime, err := validateAccessTokenInput(in)
	if err != nil {
		return nil, "", err
	}
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, "", err
	}
	secret, err := s.newSecret()
	if err != nil {
		return nil, "", fmt.Errorf("generate access token: %w", err)
	}
	t := &AccessToken{
		CustomerSlug: ws.Slug,
		UserSub:      callerID,
		Name:         name,
		Prefix:       secret[:displayPrefixLen],
		Hash:         HashAccessToken(secret),
		Scopes:       scopes,
		ExpiresAt:    s.now().Add(lifetime).UTC(),
	}
	if err := s.Tokens.CreateAccessToken(ctx, t); err != nil {
		return nil, "", err
	}
	return t, secret, nil
}

// ListAccessTokens returns the caller's own tokens.
func (s *AccessTokenService) ListAccessTokens(ctx context.Context, callerID core.UserID) ([]AccessToken, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, err
	}
	return s.Tokens.ListAccessTokens(ctx, ws.Slug, callerID)
}

// RevokeAccessToken revokes one of the caller's own tokens.
func (s *AccessTokenService) RevokeAccessToken(ctx context.Context, callerID core.UserID, id string) error {
	if id == "" {
		return &ErrInvalidSourceConfig{Field: "id", Msg: "id is required"}
	}
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return err
	}
	return s.Tokens.RevokeAccessToken(ctx, ws.Slug, callerID, id, s.now().UTC())
}

// AuthenticateAccessToken verifies a plaintext bearer secret and returns the
// token it names. Every failure is ErrInvalidAccessToken except a store
// outage, which is returned as is so it surfaces as a server error rather
// than as a credential the caller should throw away.
//
// The token must still belong to the workspace this deployment serves for its
// owner: the customer_slug column is the tenant boundary on every other row,
// and a token is no exception.
func (s *AccessTokenService) AuthenticateAccessToken(ctx context.Context, secret string) (*AccessToken, error) {
	if !looksLikeAccessToken(secret) {
		return nil, ErrInvalidAccessToken
	}
	t, err := s.Tokens.GetAccessTokenByHash(ctx, HashAccessToken(secret))
	if errors.Is(err, ErrAccessTokenNotFound) {
		return nil, ErrInvalidAccessToken
	}
	if err != nil {
		return nil, err
	}
	if err := s.checkUsable(ctx, t); err != nil {
		return nil, err
	}
	s.recordUse(t.ID)
	return t, nil
}

func (s *AccessTokenService) checkUsable(ctx context.Context, t *AccessToken) error {
	if t.RevokedAt != nil || !s.now().Before(t.ExpiresAt) {
		return ErrInvalidAccessToken
	}
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, t.UserSub)
	if err != nil || ws.Slug != t.CustomerSlug {
		return ErrInvalidAccessToken
	}
	return nil
}

// recordUse writes last_used_at at most once per lastUsedThrottle per token,
// off the request path: a slow or failing write must never fail or slow the
// call it is bookkeeping for.
func (s *AccessTokenService) recordUse(id string) {
	now := s.now()
	s.mu.Lock()
	if s.lastSeen == nil {
		s.lastSeen = map[string]time.Time{}
	}
	if last, ok := s.lastSeen[id]; ok && now.Sub(last) < lastUsedThrottle {
		s.mu.Unlock()
		return
	}
	s.lastSeen[id] = now
	s.mu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Tokens.TouchAccessToken(ctx, id, now.UTC()); err != nil && s.Logger != nil {
			s.Logger.Warn("record access token use", "token_id", id, "err", err)
		}
	}()
}

func (s *AccessTokenService) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *AccessTokenService) newSecret() (string, error) {
	buf := make([]byte, accessTokenSecretBytes)
	read := s.Random
	if read == nil {
		read = func(b []byte) error {
			_, err := rand.Read(b)
			return err
		}
	}
	if err := read(buf); err != nil {
		return "", err
	}
	return AccessTokenPrefix + hex.EncodeToString(buf), nil
}

// HashAccessToken is the stored form of a secret.
func HashAccessToken(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

// LooksLikeAccessToken reports whether a bearer value has the access token
// shape. It routes a bearer value; it authenticates nothing.
func LooksLikeAccessToken(bearer string) bool {
	return strings.HasPrefix(bearer, AccessTokenPrefix)
}

func looksLikeAccessToken(secret string) bool {
	body, ok := strings.CutPrefix(secret, AccessTokenPrefix)
	if !ok || len(body) != 2*accessTokenSecretBytes {
		return false
	}
	_, err := hex.DecodeString(body)
	return err == nil
}

// validateAccessTokenInput normalizes a create request: trimmed name, a
// deduplicated scope set in canonical order, and a bounded lifetime.
func validateAccessTokenInput(in CreateAccessTokenInput) (string, []string, time.Duration, error) {
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return "", nil, 0, &ErrInvalidSourceConfig{Field: "name", Msg: "name is required"}
	}
	if len(name) > maxAccessTokenNameLen {
		return "", nil, 0, &ErrInvalidSourceConfig{Field: "name", Msg: fmt.Sprintf("name must be at most %d characters", maxAccessTokenNameLen)}
	}
	scopes, err := normalizeScopes(in.Scopes)
	if err != nil {
		return "", nil, 0, err
	}
	lifetime := in.Lifetime
	switch {
	case lifetime == 0:
		lifetime = DefaultAccessTokenLifetime
	case lifetime < 0 || lifetime > MaxAccessTokenLifetime:
		return "", nil, 0, &ErrInvalidSourceConfig{Field: "expires_in_days", Msg: "expiry must be between 1 and 365 days"}
	}
	return name, scopes, lifetime, nil
}

func normalizeScopes(in []string) ([]string, error) {
	if len(in) == 0 {
		return nil, &ErrInvalidSourceConfig{Field: "scopes", Msg: "at least one scope is required (workspace:read, workspace:write)"}
	}
	write := false
	for _, s := range in {
		switch s {
		case ScopeWorkspaceRead:
		case ScopeWorkspaceWrite:
			write = true
		default:
			return nil, &ErrInvalidSourceConfig{Field: "scopes", Msg: fmt.Sprintf("unknown scope %q (want workspace:read or workspace:write)", s)}
		}
	}
	// Write implies read, and storing the implication explicitly keeps every
	// reader of the column honest without knowing the rule.
	if write {
		return []string{ScopeWorkspaceRead, ScopeWorkspaceWrite}, nil
	}
	return []string{ScopeWorkspaceRead}, nil
}

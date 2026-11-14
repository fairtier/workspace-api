package workspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fairtier/workspace-api/core"
)

const (
	patAlice core.UserID = "customer-acme/alice"
	patBob   core.UserID = "customer-acme/bob"
)

type patClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *patClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *patClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newPATService(t *testing.T) (*AccessTokenService, *MemoryAccessTokenStore, *patClock) {
	t.Helper()
	store := &MemoryAccessTokenStore{}
	clock := &patClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	svc := &AccessTokenService{
		Workspaces: &StaticResolver{Workspace: Workspace{Slug: "acme", CasdoorOrg: "customer-acme"}},
		Tokens:     store,
		Now:        clock.Now,
	}
	return svc, store, clock
}

func mustCreatePAT(t *testing.T, svc *AccessTokenService, user core.UserID, scopes ...string) (*AccessToken, string) {
	t.Helper()
	tok, secret, err := svc.CreateAccessToken(context.Background(), user, CreateAccessTokenInput{Name: "laptop", Scopes: scopes})
	if err != nil {
		t.Fatalf("CreateAccessToken: %v", err)
	}
	return tok, secret
}

func TestCreateAccessToken_StoresOnlyTheHash(t *testing.T) {
	svc, store, clock := newPATService(t)
	tok, secret := mustCreatePAT(t, svc, patAlice, ScopeWorkspaceRead)

	if !strings.HasPrefix(secret, "ftw_") || len(secret) != len("ftw_")+64 {
		t.Fatalf("secret %q: want ftw_ + 64 hex chars", secret)
	}
	sum := sha256.Sum256([]byte(secret))
	if !bytes.Equal(tok.Hash, sum[:]) {
		t.Error("stored hash is not the SHA-256 of the secret")
	}
	if tok.Prefix != secret[:12] {
		t.Errorf("prefix = %q, want %q", tok.Prefix, secret[:12])
	}
	if tok.CustomerSlug != "acme" || tok.UserSub != patAlice {
		t.Errorf("owner = %s/%s, want acme/%s", tok.CustomerSlug, tok.UserSub, patAlice)
	}
	if want := clock.Now().Add(90 * 24 * time.Hour); !tok.ExpiresAt.Equal(want) {
		t.Errorf("expires_at = %v, want the 90-day default %v", tok.ExpiresAt, want)
	}
	stored, err := store.GetAccessTokenByHash(context.Background(), sum[:])
	if err != nil {
		t.Fatalf("lookup by hash: %v", err)
	}
	// Nothing in the stored row may carry the plaintext.
	if strings.Contains(stored.Name+stored.Prefix+string(stored.Hash), secret) {
		t.Error("the plaintext secret was stored")
	}

	_, second := mustCreatePAT(t, svc, patAlice, ScopeWorkspaceRead)
	if second == secret {
		t.Error("two tokens share a secret")
	}
}

func TestCreateAccessToken_Validation(t *testing.T) {
	svc, _, _ := newPATService(t)
	tests := []struct {
		name string
		in   CreateAccessTokenInput
	}{
		{"no name", CreateAccessTokenInput{Scopes: []string{ScopeWorkspaceRead}}},
		{"blank name", CreateAccessTokenInput{Name: "   ", Scopes: []string{ScopeWorkspaceRead}}},
		{"long name", CreateAccessTokenInput{Name: strings.Repeat("x", 101), Scopes: []string{ScopeWorkspaceRead}}},
		{"no scopes", CreateAccessTokenInput{Name: "n"}},
		{"unknown scope", CreateAccessTokenInput{Name: "n", Scopes: []string{"admin"}}},
		{"over a year", CreateAccessTokenInput{Name: "n", Scopes: []string{ScopeWorkspaceRead}, Lifetime: 366 * 24 * time.Hour}},
		{"negative", CreateAccessTokenInput{Name: "n", Scopes: []string{ScopeWorkspaceRead}, Lifetime: -time.Hour}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := svc.CreateAccessToken(context.Background(), patAlice, tc.in)
			var invalid *ErrInvalidSourceConfig
			if !errors.As(err, &invalid) {
				t.Fatalf("err = %v, want a validation error", err)
			}
		})
	}
}

func TestCreateAccessToken_ScopesAndLifetime(t *testing.T) {
	svc, _, clock := newPATService(t)
	tok, _, err := svc.CreateAccessToken(context.Background(), patAlice, CreateAccessTokenInput{
		Name: "ci", Scopes: []string{ScopeWorkspaceWrite, ScopeWorkspaceWrite}, Lifetime: 365 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{ScopeWorkspaceRead, ScopeWorkspaceWrite}; !slices.Equal(tok.Scopes, want) {
		t.Errorf("scopes = %v, want write to imply read: %v", tok.Scopes, want)
	}
	if want := clock.Now().Add(365 * 24 * time.Hour); !tok.ExpiresAt.Equal(want) {
		t.Errorf("expires_at = %v, want %v", tok.ExpiresAt, want)
	}
}

func TestCreateAccessToken_RejectsServiceAccounts(t *testing.T) {
	svc, _, _ := newPATService(t)
	_, _, err := svc.CreateAccessToken(context.Background(), "admin/dlt-worker", CreateAccessTokenInput{Name: "n", Scopes: []string{ScopeWorkspaceRead}})
	if err == nil {
		t.Fatal("a service account minted an access token")
	}
}

func TestAuthenticateAccessToken(t *testing.T) {
	svc, store, clock := newPATService(t)
	ctx := context.Background()
	tok, secret := mustCreatePAT(t, svc, patAlice, ScopeWorkspaceRead)

	got, err := svc.AuthenticateAccessToken(ctx, secret)
	if err != nil {
		t.Fatalf("valid token: %v", err)
	}
	if got.ID != tok.ID || got.UserSub != patAlice || !got.HasScope(ScopeWorkspaceRead) || got.HasScope(ScopeWorkspaceWrite) {
		t.Errorf("authenticated %+v", got)
	}

	invalid := map[string]string{
		"empty":            "",
		"garbage":          "not-a-token",
		"jwt-shaped":       "eyJhbGciOiJFZERTQSJ9.e30.sig",
		"prefix only":      "ftw_",
		"wrong length":     secret[:len(secret)-1],
		"not hex":          "ftw_" + strings.Repeat("z", 64),
		"unknown":          "ftw_" + strings.Repeat("0", 64),
		"other case":       strings.ToUpper(secret),
		"trailing garbage": secret + "x",
	}
	for name, s := range invalid {
		if _, err := svc.AuthenticateAccessToken(ctx, s); !errors.Is(err, ErrInvalidAccessToken) {
			t.Errorf("%s: err = %v, want ErrInvalidAccessToken", name, err)
		}
	}

	// A token minted on another tenant's row must not authenticate here.
	foreign, foreignSecret := mustCreatePAT(t, svc, patBob, ScopeWorkspaceRead)
	store.mu.Lock()
	for i := range store.tokens {
		if store.tokens[i].ID == foreign.ID {
			store.tokens[i].CustomerSlug = "other"
		}
	}
	store.mu.Unlock()
	if _, err := svc.AuthenticateAccessToken(ctx, foreignSecret); !errors.Is(err, ErrInvalidAccessToken) {
		t.Errorf("foreign-tenant token: err = %v, want ErrInvalidAccessToken", err)
	}

	clock.advance(90*24*time.Hour - time.Second)
	if _, err := svc.AuthenticateAccessToken(ctx, secret); err != nil {
		t.Errorf("a second before expiry: %v", err)
	}
	clock.advance(time.Second)
	if _, err := svc.AuthenticateAccessToken(ctx, secret); !errors.Is(err, ErrInvalidAccessToken) {
		t.Errorf("at expiry: err = %v, want ErrInvalidAccessToken", err)
	}
}

func TestRevokeAccessToken(t *testing.T) {
	svc, _, _ := newPATService(t)
	ctx := context.Background()
	tok, secret := mustCreatePAT(t, svc, patAlice, ScopeWorkspaceRead)

	if err := svc.RevokeAccessToken(ctx, patBob, tok.ID); !errors.Is(err, ErrAccessTokenNotFound) {
		t.Errorf("revoking someone else's token: err = %v, want ErrAccessTokenNotFound", err)
	}
	if _, err := svc.AuthenticateAccessToken(ctx, secret); err != nil {
		t.Fatalf("token stopped working after a foreign revoke attempt: %v", err)
	}
	if err := svc.RevokeAccessToken(ctx, patAlice, tok.ID); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, err := svc.AuthenticateAccessToken(ctx, secret); !errors.Is(err, ErrInvalidAccessToken) {
		t.Errorf("revoked token: err = %v, want ErrInvalidAccessToken", err)
	}
	if err := svc.RevokeAccessToken(ctx, patAlice, tok.ID); err != nil {
		t.Errorf("second revoke should succeed: %v", err)
	}
	if err := svc.RevokeAccessToken(ctx, patAlice, ""); err == nil {
		t.Error("empty id accepted")
	}
}

func TestListAccessTokens_OwnTokensOnly(t *testing.T) {
	svc, _, _ := newPATService(t)
	ctx := context.Background()
	first, _ := mustCreatePAT(t, svc, patAlice, ScopeWorkspaceRead)
	second, _ := mustCreatePAT(t, svc, patAlice, ScopeWorkspaceWrite)
	mustCreatePAT(t, svc, patBob, ScopeWorkspaceRead)

	got, err := svc.ListAccessTokens(ctx, patAlice)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != second.ID || got[1].ID != first.ID {
		t.Fatalf("list = %+v, want alice's two tokens newest first", got)
	}
}

func TestAuthenticateAccessToken_RecordsUseThrottled(t *testing.T) {
	svc, store, clock := newPATService(t)
	ctx := context.Background()
	tok, secret := mustCreatePAT(t, svc, patAlice, ScopeWorkspaceRead)

	lastUsed := func() *time.Time {
		store.mu.Lock()
		defer store.mu.Unlock()
		for _, t := range store.tokens {
			if t.ID == tok.ID {
				return t.LastUsedAt
			}
		}
		return nil
	}
	waitFor := func(want time.Time) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if lu := lastUsed(); lu != nil && lu.Equal(want) {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("last_used_at = %v, want %v", lastUsed(), want)
	}

	first := clock.Now()
	if _, err := svc.AuthenticateAccessToken(ctx, secret); err != nil {
		t.Fatal(err)
	}
	waitFor(first)

	clock.advance(10 * time.Second)
	if _, err := svc.AuthenticateAccessToken(ctx, secret); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if lu := lastUsed(); !lu.Equal(first) {
		t.Errorf("last_used_at rewritten within the throttle window: %v", lu)
	}

	clock.advance(time.Minute)
	if _, err := svc.AuthenticateAccessToken(ctx, secret); err != nil {
		t.Fatal(err)
	}
	waitFor(clock.Now())
}

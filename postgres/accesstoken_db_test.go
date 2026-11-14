package postgres

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

// The access token store against a real, migrated database (TEST_DATABASE_URL,
// skipped when unset — see testDB). What a fake cannot show: the scopes
// round-trip through text[], the hash is the unique lookup key, and revoke and
// touch are scoped the way the service relies on.
func TestAccessTokenStore(t *testing.T) {
	db := testDB(t)
	if err := Migrate(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM access_tokens WHERE customer_slug = 'pat-test'`); err != nil {
		t.Fatal(err)
	}
	repo := &Repository{DB: db}
	ctx := context.Background()
	const alice core.UserID = "customer-pat-test/alice"

	secret := "ftw_" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	tok := &workspace.AccessToken{
		CustomerSlug: "pat-test",
		UserSub:      alice,
		Name:         "laptop",
		Prefix:       secret[:12],
		Hash:         workspace.HashAccessToken(secret),
		Scopes:       []string{workspace.ScopeWorkspaceRead, workspace.ScopeWorkspaceWrite},
		ExpiresAt:    time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond),
	}
	if err := repo.CreateAccessToken(ctx, tok); err != nil {
		t.Fatalf("create: %v", err)
	}
	if tok.ID == "" || tok.CreatedAt.IsZero() {
		t.Fatalf("create did not set id/created_at: %+v", tok)
	}

	dup := *tok
	if err := repo.CreateAccessToken(ctx, &dup); err == nil {
		t.Error("a second row with the same hash was accepted")
	}

	got, err := repo.GetAccessTokenByHash(ctx, workspace.HashAccessToken(secret))
	if err != nil {
		t.Fatalf("get by hash: %v", err)
	}
	if got.ID != tok.ID || got.UserSub != alice || !slices.Equal(got.Scopes, tok.Scopes) || !got.ExpiresAt.Equal(tok.ExpiresAt) {
		t.Errorf("round trip: got %+v, want %+v", got, tok)
	}
	if got.LastUsedAt != nil || got.RevokedAt != nil {
		t.Errorf("fresh token has last_used/revoked set: %+v", got)
	}
	if _, err := repo.GetAccessTokenByHash(ctx, workspace.HashAccessToken(secret+"x")); !errors.Is(err, workspace.ErrAccessTokenNotFound) {
		t.Errorf("unknown hash: %v, want ErrAccessTokenNotFound", err)
	}

	used := time.Now().UTC().Truncate(time.Microsecond)
	if err := repo.TouchAccessToken(ctx, tok.ID, used); err != nil {
		t.Fatalf("touch: %v", err)
	}
	// An older, late-arriving touch must not move last_used_at backwards.
	if err := repo.TouchAccessToken(ctx, tok.ID, used.Add(-time.Minute)); err != nil {
		t.Fatalf("touch: %v", err)
	}

	list, err := repo.ListAccessTokens(ctx, "pat-test", alice)
	if err != nil || len(list) != 1 {
		t.Fatalf("list: %v, %v", list, err)
	}
	if list[0].LastUsedAt == nil || !list[0].LastUsedAt.Equal(used) {
		t.Errorf("last_used_at = %v, want %v", list[0].LastUsedAt, used)
	}
	if other, _ := repo.ListAccessTokens(ctx, "pat-test", "customer-pat-test/bob"); len(other) != 0 {
		t.Errorf("bob sees alice's tokens: %v", other)
	}

	if err := repo.RevokeAccessToken(ctx, "pat-test", "customer-pat-test/bob", tok.ID, used); !errors.Is(err, workspace.ErrAccessTokenNotFound) {
		t.Errorf("foreign revoke: %v, want ErrAccessTokenNotFound", err)
	}
	if err := repo.RevokeAccessToken(ctx, "pat-test", alice, "not-a-uuid", used); !errors.Is(err, workspace.ErrAccessTokenNotFound) {
		t.Errorf("malformed id: %v, want ErrAccessTokenNotFound", err)
	}
	if err := repo.RevokeAccessToken(ctx, "pat-test", alice, tok.ID, used); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if err := repo.RevokeAccessToken(ctx, "pat-test", alice, tok.ID, used.Add(time.Hour)); err != nil {
		t.Fatalf("second revoke: %v", err)
	}
	got, err = repo.GetAccessTokenByHash(ctx, workspace.HashAccessToken(secret))
	if err != nil || got.RevokedAt == nil || !got.RevokedAt.Equal(used) {
		t.Errorf("revoked_at = %v (%v), want the first revocation %v", got.RevokedAt, err, used)
	}
}

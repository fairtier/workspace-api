package server

import (
	"errors"
	"testing"

	"github.com/fairtier/workspace-api/core"
)

// TestTokenUserReader covers the contract the box relies on: the profile is
// read from the caller's own verified token, and only for that caller.
func TestTokenUserReader(t *testing.T) {
	profile := core.TokenProfile{Subject: "u-1", Name: "rich", DisplayName: "Rich", Email: "rich@example.com"}

	t.Run("returns the caller's own profile", func(t *testing.T) {
		ctx := core.ContextWithTokenProfile(t.Context(), profile)
		got, err := (TokenUserReader{}).GetCommitUser(ctx, "u-1")
		if err != nil {
			t.Fatalf("GetCommitUser() error = %v", err)
		}
		if got.Name != "rich" || got.DisplayName != "Rich" || got.Email != "rich@example.com" {
			t.Errorf("GetCommitUser() = %+v", got)
		}
	})

	t.Run("refuses a caller the context does not belong to", func(t *testing.T) {
		ctx := core.ContextWithTokenProfile(t.Context(), profile)
		if _, err := (TokenUserReader{}).GetCommitUser(ctx, "someone-else"); !errors.Is(err, core.ErrUserNotFound) {
			t.Errorf("GetCommitUser() error = %v, want ErrUserNotFound", err)
		}
	})

	t.Run("unauthenticated context falls back to platform attribution", func(t *testing.T) {
		if _, err := (TokenUserReader{}).GetCommitUser(t.Context(), "u-1"); !errors.Is(err, core.ErrUserNotFound) {
			t.Errorf("GetCommitUser() error = %v, want ErrUserNotFound", err)
		}
	})
}

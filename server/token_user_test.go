package server

import (
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/fairtier/workspace-api/core"
)

// TestTokenProfileClaimFormats pins the mapping the whole box-side attribution
// path rests on. The two Casdoor token formats disagree about "name" and the
// standard one is the counter-intuitive direction, so reading it wrong yields
// commits authored by a workspace's admin login instead of a person's name —
// wrong but plausible, i.e. the kind of bug nobody reports.
func TestTokenProfileClaimFormats(t *testing.T) {
	tests := []struct {
		name   string
		claims jwt.MapClaims
		want   TokenProfile
	}{
		{
			// tokenFormat "JWT-Standard" (Casdoor UserStandard): the username
			// is preferred_username and "name" is the DISPLAY name.
			name: "standard format",
			claims: jwt.MapClaims{
				"sub":                "u-1",
				"preferred_username": "rich",
				"name":               "Tomáš Procházka",
				"email":              "rich@example.com",
			},
			want: TokenProfile{Subject: "u-1", Name: "rich", DisplayName: "Tomáš Procházka", Email: "rich@example.com"},
		},
		{
			// tokenFormat "JWT" embeds the user object: "name" is the username
			// and the display name is its own claim.
			name: "full user-object format",
			claims: jwt.MapClaims{
				"sub":         "u-2",
				"name":        "rich",
				"displayName": "Tomáš Procházka",
				"email":       "rich@example.com",
			},
			want: TokenProfile{Subject: "u-2", Name: "rich", DisplayName: "Tomáš Procházka", Email: "rich@example.com"},
		},
		{
			// Every standard-format field is omitempty, so a user with no
			// display name simply has no "name" claim.
			name:   "standard format without a display name",
			claims: jwt.MapClaims{"sub": "u-3", "preferred_username": "rich", "email": "rich@example.com"},
			want:   TokenProfile{Subject: "u-3", Name: "rich", Email: "rich@example.com"},
		},
		{
			name:   "no profile claims at all",
			claims: jwt.MapClaims{"sub": "u-4"},
			want:   TokenProfile{Subject: "u-4"},
		},
		{
			// A claim of the wrong JSON type must not panic the auth path.
			name:   "non-string claims are ignored",
			claims: jwt.MapClaims{"sub": "u-5", "email": 42, "name": []string{"x"}},
			want:   TokenProfile{Subject: "u-5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tokenProfile(tt.want.Subject, tt.claims); got != tt.want {
				t.Errorf("tokenProfile() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestTokenUserReader covers the contract the box relies on: the profile is
// read from the caller's own verified token, and only for that caller.
func TestTokenUserReader(t *testing.T) {
	profile := TokenProfile{Subject: "u-1", Name: "rich", DisplayName: "Rich", Email: "rich@example.com"}

	t.Run("returns the caller's own profile", func(t *testing.T) {
		ctx := ContextWithTokenProfile(t.Context(), profile)
		got, err := (TokenUserReader{}).GetCommitUser(ctx, "u-1")
		if err != nil {
			t.Fatalf("GetCommitUser() error = %v", err)
		}
		if got.Name != "rich" || got.DisplayName != "Rich" || got.Email != "rich@example.com" {
			t.Errorf("GetCommitUser() = %+v", got)
		}
	})

	t.Run("refuses a caller the context does not belong to", func(t *testing.T) {
		ctx := ContextWithTokenProfile(t.Context(), profile)
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

// TestAuthInterceptorSetsTokenProfile proves the claims survive the actual
// interceptor, not just the parser: this is the wiring commit attribution
// depends on, and a context value is exactly the kind of thing a refactor
// drops silently.
func TestAuthInterceptorSetsTokenProfile(t *testing.T) {
	jwks, sign := testJWKS(t)
	auth := UserAuth{JWKS: jwks, Issuer: "https://auth.example.com"}
	token := sign(jwt.MapClaims{
		"sub":                "u-1",
		"iss":                "https://auth.example.com",
		"exp":                time.Now().Add(time.Hour).Unix(),
		"preferred_username": "rich",
		"name":               "Rich",
		"email":              "rich@example.com",
	})

	ctx, err := auth.authenticateBearer(t.Context(), "Bearer "+token)
	if err != nil {
		t.Fatalf("authenticateBearer() error = %v", err)
	}
	if got := UserIDFromContext(ctx); got != core.UserID("u-1") {
		t.Errorf("UserIDFromContext() = %q, want u-1", got)
	}
	got, ok := TokenProfileFromContext(ctx)
	if !ok {
		t.Fatal("TokenProfileFromContext() not set")
	}
	want := TokenProfile{Subject: "u-1", Name: "rich", DisplayName: "Rich", Email: "rich@example.com"}
	if got != want {
		t.Errorf("TokenProfileFromContext() = %+v, want %+v", got, want)
	}
}

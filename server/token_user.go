package server

import (
	"context"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

// TokenUserReader answers workspace.UserReader from the profile claims of the
// caller's own verified token, for a deployment that has no directory to look
// the caller up in.
//
// That is the box: central resolves a commit author out of its users table,
// but the box's database holds no user rows — identity lives in the box's
// Casdoor, and the token the caller just presented was minted by it. Querying
// Casdoor back would mean giving the box API admin credentials for its own
// identity provider and paying a network round trip per save, to learn what
// the request already carried.
//
// It is deliberately usable for nothing but commit attribution. The claims are
// unverified beyond the issuer's signature, and workspace.UserReader exists
// solely to name a git author — a use where a wrong answer costs a misspelt
// commit and no access decision turns on it.
type TokenUserReader struct{}

// GetCommitUser implements workspace.UserReader.
//
// The context's profile must belong to the caller being asked about. It always
// does today — both are set from one token by one interceptor — but this reads
// an ambient value on behalf of a named subject, and the day some path resolves
// an author for anyone other than the caller, silently attributing the commit
// to whoever happened to make the request is the wrong failure. Returning
// ErrUserNotFound falls back to platform attribution, which is what this whole
// path degrades to anyway.
func (TokenUserReader) GetCommitUser(ctx context.Context, callerID core.UserID) (*workspace.UserInfo, error) {
	profile, ok := core.TokenProfileFromContext(ctx)
	if !ok || profile.Subject != string(callerID) {
		return nil, core.ErrUserNotFound
	}
	return &workspace.UserInfo{
		Name:        profile.Name,
		DisplayName: profile.DisplayName,
		Email:       profile.Email,
	}, nil
}

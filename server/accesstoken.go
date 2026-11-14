package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"connectrpc.com/connect"

	"github.com/fairtier/workspace-api/core"
	accesstokenv1 "github.com/fairtier/workspace-api/proto/access_token/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// AccessTokenServer implements access_token.v1.AccessTokenService: the
// sign-in-token-only surface through which a person mints, lists and revokes
// their workspace access tokens.
//
// A PAT must never reach these handlers. Today it cannot — the auth
// interceptor on the public mux verifies JWTs only, and the /mcp endpoint that
// does accept PATs exposes no token tools — but "a leaked token cannot mint
// its own successor" is the property this whole surface rests on, so each
// handler also refuses a PAT-shaped bearer itself rather than trusting the
// mux wiring to stay that way.
type AccessTokenServer struct {
	Service *workspace.AccessTokenService
}

// caller returns the authenticated interactive user, refusing PAT bearers.
func (s *AccessTokenServer) caller(ctx context.Context, h interface{ Header() http.Header }) (core.UserID, error) {
	bearer, _ := strings.CutPrefix(h.Header().Get("Authorization"), "Bearer ")
	if workspace.LooksLikeAccessToken(bearer) {
		return "", connect.NewError(connect.CodePermissionDenied, errors.New("access tokens cannot manage access tokens; sign in interactively"))
	}
	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return "", connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}
	return callerID, nil
}

func (s *AccessTokenServer) CreateAccessToken(ctx context.Context, req *connect.Request[accesstokenv1.CreateAccessTokenRequest]) (*connect.Response[accesstokenv1.CreateAccessTokenResponse], error) {
	callerID, err := s.caller(ctx, req)
	if err != nil {
		return nil, err
	}
	days := req.Msg.ExpiresInDays
	if days < 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("expires_in_days must not be negative"))
	}
	t, secret, err := s.Service.CreateAccessToken(ctx, callerID, workspace.CreateAccessTokenInput{
		Name:     req.Msg.Name,
		Scopes:   req.Msg.Scopes,
		Lifetime: time.Duration(days) * 24 * time.Hour,
	})
	if err != nil {
		return nil, domainError(err)
	}
	return connect.NewResponse(&accesstokenv1.CreateAccessTokenResponse{
		Token:  accessTokenToPB(t),
		Secret: secret,
	}), nil
}

func (s *AccessTokenServer) ListAccessTokens(ctx context.Context, req *connect.Request[accesstokenv1.ListAccessTokensRequest]) (*connect.Response[accesstokenv1.ListAccessTokensResponse], error) {
	callerID, err := s.caller(ctx, req)
	if err != nil {
		return nil, err
	}
	tokens, err := s.Service.ListAccessTokens(ctx, callerID)
	if err != nil {
		return nil, domainError(err)
	}
	out := make([]*accesstokenv1.AccessToken, 0, len(tokens))
	for i := range tokens {
		out = append(out, accessTokenToPB(&tokens[i]))
	}
	return connect.NewResponse(&accesstokenv1.ListAccessTokensResponse{Tokens: out}), nil
}

func (s *AccessTokenServer) RevokeAccessToken(ctx context.Context, req *connect.Request[accesstokenv1.RevokeAccessTokenRequest]) (*connect.Response[accesstokenv1.RevokeAccessTokenResponse], error) {
	callerID, err := s.caller(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := s.Service.RevokeAccessToken(ctx, callerID, req.Msg.Id); err != nil {
		return nil, domainError(err)
	}
	return connect.NewResponse(&accesstokenv1.RevokeAccessTokenResponse{}), nil
}

// accessTokenToPB projects a token's metadata. There is deliberately no field
// to put the hash in.
func accessTokenToPB(t *workspace.AccessToken) *accesstokenv1.AccessToken {
	pb := &accesstokenv1.AccessToken{
		Id:        t.ID,
		Name:      t.Name,
		Prefix:    t.Prefix,
		Scopes:    t.Scopes,
		CreatedAt: t.CreatedAt.UTC().Format(time.RFC3339),
		ExpiresAt: t.ExpiresAt.UTC().Format(time.RFC3339),
	}
	if t.LastUsedAt != nil {
		pb.LastUsedAt = t.LastUsedAt.UTC().Format(time.RFC3339)
	}
	if t.RevokedAt != nil {
		pb.RevokedAt = t.RevokedAt.UTC().Format(time.RFC3339)
	}
	return pb
}

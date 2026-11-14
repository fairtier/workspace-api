package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/fairtier/workspace-api/core"
	accesstokenv1 "github.com/fairtier/workspace-api/proto/access_token/v1"
	"github.com/fairtier/workspace-api/proto/access_token/v1/accesstokenv1connect"
	"github.com/fairtier/workspace-api/workspace"
)

const patTestIssuer = "https://auth.example.test"

// accessTokenTestServer mounts AccessTokenService behind the same JWT auth
// interceptor the public mux uses.
func accessTokenTestServer(t *testing.T) (accesstokenv1connect.AccessTokenServiceClient, func(sub string) string) {
	t.Helper()
	jwks, sign := testJWKS(t)
	svc := &workspace.AccessTokenService{
		Workspaces: &workspace.StaticResolver{Workspace: workspace.Workspace{Slug: "acme", CasdoorOrg: "customer-acme"}},
		Tokens:     &workspace.MemoryAccessTokenStore{},
	}
	mux := http.NewServeMux()
	mux.Handle(accesstokenv1connect.NewAccessTokenServiceHandler(&AccessTokenServer{Service: svc},
		connect.WithInterceptors(core.NewAuthInterceptor(core.UserAuth{JWKS: jwks, Issuer: patTestIssuer}))))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	signIn := func(sub string) string {
		return sign(jwt.MapClaims{"sub": sub, "iss": patTestIssuer, "exp": time.Now().Add(time.Hour).Unix()})
	}
	return accesstokenv1connect.NewAccessTokenServiceClient(srv.Client(), srv.URL), signIn
}

func withBearer[T any](msg *T, token string) *connect.Request[T] {
	req := connect.NewRequest(msg)
	req.Header().Set("Authorization", "Bearer "+token)
	return req
}

func TestAccessTokenService_Lifecycle(t *testing.T) {
	client, signIn := accessTokenTestServer(t)
	ctx := context.Background()
	jwt := signIn("customer-acme/alice")

	created, err := client.CreateAccessToken(ctx, withBearer(&accesstokenv1.CreateAccessTokenRequest{
		Name: "laptop", Scopes: []string{"workspace:read"}, ExpiresInDays: 30,
	}, jwt))
	if err != nil {
		t.Fatalf("CreateAccessToken: %v", err)
	}
	secret := created.Msg.Secret
	if !strings.HasPrefix(secret, "ftw_") {
		t.Fatalf("secret = %q, want an ftw_ token", secret)
	}
	if got := created.Msg.Token.Prefix; !strings.HasPrefix(secret, got) || len(got) >= len(secret) {
		t.Errorf("prefix %q is not a strict prefix of the secret", got)
	}

	list, err := client.ListAccessTokens(ctx, withBearer(&accesstokenv1.ListAccessTokensRequest{}, jwt))
	if err != nil {
		t.Fatalf("ListAccessTokens: %v", err)
	}
	if len(list.Msg.Tokens) != 1 || list.Msg.Tokens[0].Id != created.Msg.Token.Id {
		t.Fatalf("list = %v", list.Msg.Tokens)
	}
	wire, err := protojson.Marshal(list.Msg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(wire), secret[len(created.Msg.Token.Prefix):]) {
		t.Fatal("ListAccessTokens leaked the secret")
	}

	// Another person sees none of it and cannot revoke it.
	bob := signIn("customer-acme/bob")
	other, err := client.ListAccessTokens(ctx, withBearer(&accesstokenv1.ListAccessTokensRequest{}, bob))
	if err != nil || len(other.Msg.Tokens) != 0 {
		t.Fatalf("bob's list = %v, %v; want empty", other, err)
	}
	_, err = client.RevokeAccessToken(ctx, withBearer(&accesstokenv1.RevokeAccessTokenRequest{Id: created.Msg.Token.Id}, bob))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("bob revoking alice's token: %v, want NotFound", err)
	}

	if _, err := client.RevokeAccessToken(ctx, withBearer(&accesstokenv1.RevokeAccessTokenRequest{Id: created.Msg.Token.Id}, jwt)); err != nil {
		t.Fatalf("RevokeAccessToken: %v", err)
	}
	list, err = client.ListAccessTokens(ctx, withBearer(&accesstokenv1.ListAccessTokensRequest{}, jwt))
	if err != nil || list.Msg.Tokens[0].RevokedAt == "" {
		t.Fatalf("revoked token not marked revoked: %v, %v", list, err)
	}
}

func TestAccessTokenService_Validation(t *testing.T) {
	client, signIn := accessTokenTestServer(t)
	jwt := signIn("customer-acme/alice")
	for name, req := range map[string]*accesstokenv1.CreateAccessTokenRequest{
		"no scopes":      {Name: "x"},
		"bad scope":      {Name: "x", Scopes: []string{"root"}},
		"no name":        {Scopes: []string{"workspace:read"}},
		"too long":       {Name: "x", Scopes: []string{"workspace:read"}, ExpiresInDays: 366},
		"negative":       {Name: "x", Scopes: []string{"workspace:read"}, ExpiresInDays: -1},
		"unknown scopes": {Name: "x", Scopes: []string{"workspace:read", "workspace:admin"}},
	} {
		_, err := client.CreateAccessToken(context.Background(), withBearer(req, jwt))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s: err = %v, want InvalidArgument", name, err)
		}
	}
}

// The property the whole token surface rests on: a leaked PAT cannot mint a
// successor, list its siblings or revoke anything.
func TestAccessTokenService_RefusesAccessTokens(t *testing.T) {
	client, signIn := accessTokenTestServer(t)
	ctx := context.Background()
	created, err := client.CreateAccessToken(ctx, withBearer(&accesstokenv1.CreateAccessTokenRequest{
		Name: "agent", Scopes: []string{"workspace:write"},
	}, signIn("customer-acme/alice")))
	if err != nil {
		t.Fatal(err)
	}
	pat := created.Msg.Secret

	_, err = client.CreateAccessToken(ctx, withBearer(&accesstokenv1.CreateAccessTokenRequest{Name: "next", Scopes: []string{"workspace:write"}}, pat))
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("create with a PAT: %v, want Unauthenticated", err)
	}
	_, err = client.ListAccessTokens(ctx, withBearer(&accesstokenv1.ListAccessTokensRequest{}, pat))
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("list with a PAT: %v, want Unauthenticated", err)
	}
	_, err = client.RevokeAccessToken(ctx, withBearer(&accesstokenv1.RevokeAccessTokenRequest{Id: created.Msg.Token.Id}, pat))
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("revoke with a PAT: %v, want Unauthenticated", err)
	}
}

// Defence in depth: even if a future mux let a PAT-authenticated context
// reach the handler, the handler itself refuses a PAT-shaped bearer.
func TestAccessTokenServer_HandlerRefusesPATBearer(t *testing.T) {
	s := &AccessTokenServer{Service: &workspace.AccessTokenService{
		Workspaces: &workspace.StaticResolver{Workspace: workspace.Workspace{Slug: "acme"}},
		Tokens:     &workspace.MemoryAccessTokenStore{},
	}}
	ctx := core.ContextWithUserID(context.Background(), "alice")
	req := withBearer(&accesstokenv1.CreateAccessTokenRequest{Name: "x", Scopes: []string{"workspace:read"}}, "ftw_"+strings.Repeat("a", 64))
	_, err := s.CreateAccessToken(ctx, req)
	var cerr *connect.Error
	if !errors.As(err, &cerr) || cerr.Code() != connect.CodePermissionDenied {
		t.Fatalf("err = %v, want PermissionDenied", err)
	}
}

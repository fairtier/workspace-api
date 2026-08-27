package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/oauthgoogle"
	oauthclientv1 "github.com/fairtier/workspace-api/proto/oauthclient/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// OAuthClientServer implements the ConnectRPC OAuthClientService handler: the
// Console's Integrations page, where a workspace connects its OWN Google OAuth
// application for the Sheets sign-in flow.
//
// The tenant is resolved from the caller's JWT on every call, so a user can only
// ever read or write their own workspace's app.
type OAuthClientServer struct {
	Workspaces workspace.Resolver
	Clients    workspace.OAuthClientStore
	// OAuth supplies the deployment-wide half of the configuration: the
	// redirect URI the customer must register, and whether this deployment can
	// run a consent round-trip at all. Nil on a box, which stores the pair (its
	// mirror needs it) but cannot serve the popup.
	OAuth *oauthgoogle.Client
	// Mirror, when set, re-renders the tenant's pipelines repo after the pair
	// changes. The .age render injects the pair into every OAuth google_sheets
	// credential, so without this converge a rotated or newly entered secret
	// only reaches a pipeline's file on that pipeline's next save — until then
	// its runs keep failing on the stale render with nothing pointing at the
	// fix.
	Mirror workspace.PipelineMirrorer
	Logger *slog.Logger
}

// convergePipelines best-effort re-renders the tenant's pipeline .age files
// after the stored pair changed; a failure is logged, never surfaced — the
// pair save itself succeeded, and the next pipeline save retries the render.
func (s *OAuthClientServer) convergePipelines(ctx context.Context, slug string) {
	if s.Mirror == nil {
		return
	}
	mctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := s.Mirror.SyncCustomer(mctx, slug, nil); err != nil && s.Logger != nil {
		s.Logger.WarnContext(ctx, "post-pair-change pipeline mirror sync", "slug", slug, "err", err)
	}
}

func (s *OAuthClientServer) GetOAuthClient(ctx context.Context, req *connect.Request[oauthclientv1.GetOAuthClientRequest]) (*connect.Response[oauthclientv1.GetOAuthClientResponse], error) {
	slug, provider, err := s.resolve(ctx, req.Msg.Provider)
	if err != nil {
		return nil, err
	}
	out, err := s.state(ctx, slug, provider)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(out), nil
}

func (s *OAuthClientServer) SetOAuthClient(ctx context.Context, req *connect.Request[oauthclientv1.SetOAuthClientRequest]) (*connect.Response[oauthclientv1.SetOAuthClientResponse], error) {
	slug, provider, err := s.resolve(ctx, req.Msg.Provider)
	if err != nil {
		return nil, err
	}

	cc := &workspace.OAuthClient{
		CustomerSlug: slug,
		Provider:     provider,
		ClientID:     req.Msg.ClientId,
		ClientSecret: req.Msg.ClientSecret,
		UpdatedBy:    string(core.UserIDFromContext(ctx)),
	}
	if err := workspace.ValidateOAuthClient(cc); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := s.Clients.UpsertOAuthClient(ctx, cc); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if s.Logger != nil {
		// The client id is not a secret; the secret is never logged.
		s.Logger.InfoContext(ctx, "oauth client connected",
			"slug", slug, "provider", provider, "client_id", cc.ClientID)
	}
	s.convergePipelines(ctx, slug)

	out, err := s.state(ctx, slug, provider)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&oauthclientv1.SetOAuthClientResponse{Client: out}), nil
}

func (s *OAuthClientServer) DeleteOAuthClient(ctx context.Context, req *connect.Request[oauthclientv1.DeleteOAuthClientRequest]) (*connect.Response[oauthclientv1.DeleteOAuthClientResponse], error) {
	slug, provider, err := s.resolve(ctx, req.Msg.Provider)
	if err != nil {
		return nil, err
	}
	if err := s.Clients.DeleteOAuthClient(ctx, slug, provider); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if s.Logger != nil {
		s.Logger.InfoContext(ctx, "oauth client disconnected", "slug", slug, "provider", provider)
	}
	// Re-render so the files drop the deleted pair rather than shipping a
	// secret the customer just asked us to forget.
	s.convergePipelines(ctx, slug)
	return connect.NewResponse(&oauthclientv1.DeleteOAuthClientResponse{}), nil
}

// resolve binds the caller to their workspace and normalises the provider key.
func (s *OAuthClientServer) resolve(ctx context.Context, provider string) (slug, normalized string, err error) {
	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return "", "", connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return "", "", connect.NewError(connect.CodePermissionDenied, errors.New("no workspace for this user"))
	}
	probe := &workspace.OAuthClient{Provider: provider, ClientID: "x", ClientSecret: "x"}
	if err := workspace.ValidateOAuthClient(probe); err != nil {
		return "", "", connect.NewError(connect.CodeInvalidArgument, err)
	}
	return ws.Slug, probe.Provider, nil
}

// state builds the response shape shared by Get and Set. The client secret is
// deliberately absent from it: there is no code path that reads a stored secret
// back out to a browser.
func (s *OAuthClientServer) state(ctx context.Context, slug, provider string) (*oauthclientv1.GetOAuthClientResponse, error) {
	out := &oauthclientv1.GetOAuthClientResponse{
		// Every scope this server can ask for, not the subset one pipeline
		// needs: Google refuses a consent requesting a scope the customer's
		// own app does not declare, so the setup page has to list them all.
		RequiredScopes: oauthgoogle.AllScopes(),
		FlowAvailable:  s.OAuth != nil,
	}
	if s.OAuth != nil {
		out.RedirectUri = s.OAuth.RedirectURL()
	}

	cc, err := s.Clients.GetOAuthClient(ctx, slug, provider)
	if errors.Is(err, workspace.ErrOAuthClientNotFound) {
		return out, nil
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	out.Configured = true
	out.ClientId = cc.ClientID
	out.UpdatedBy = cc.UpdatedBy
	if !cc.UpdatedAt.IsZero() {
		out.UpdatedAt = cc.UpdatedAt.UTC().Format(time.RFC3339)
	}
	return out, nil
}

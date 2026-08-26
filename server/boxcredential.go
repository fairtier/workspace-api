package server

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"connectrpc.com/connect"

	"github.com/fairtier/workspace-api/core"
	boxcredentialv1 "github.com/fairtier/workspace-api/proto/boxcredential/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// BoxCredentialServer implements the ConnectRPC BoxCredentialService handler:
// the internal-mux endpoint where a box deposits the OAuth client it minted
// for itself, and reads back the secrets only central can mint — the
// "credential inversion": the box pushes credentials up, central never holds
// box admin credentials.
//
// The three repo-writing deposits (git token, snapshot bearer, age public
// key) were retired with split Phase 3E; see the proto for why.
type BoxCredentialServer struct {
	Federation workspace.BoxFederationClientStore
	// Secrets serves FetchBoxSecrets, the one direction that runs
	// central→box. Optional: a deployment without it answers Unimplemented
	// rather than an empty map, so a box can tell "central has no secrets
	// for me" from "central cannot serve them yet".
	Secrets workspace.BoxSecretStore
	Logger  *slog.Logger
}

// DepositFederationClient upserts the OAuth client the calling box minted for
// itself. Only a box-issued service token is accepted: the slug is bound by
// issuer trust, so a box can only ever deposit its own. A central identity
// (or an unauthenticated log-mode call) is rejected — unlike the dlt-worker
// RPCs there is no legacy shared-substrate caller to grandfather in.
//
// A re-deposit with a different secret is a rotation the customer initiated;
// central converges both ends on the next reconcile.
func (s *BoxCredentialServer) DepositFederationClient(ctx context.Context, req *connect.Request[boxcredentialv1.DepositFederationClientRequest]) (*connect.Response[boxcredentialv1.DepositFederationClientResponse], error) {
	if s.Federation == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("this deployment does not accept credential deposits"))
	}
	caller := core.InternalCallerFromContext(ctx)
	if caller.Slug == "" {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("credential deposit requires a box service token"))
	}

	clientID := strings.TrimSpace(req.Msg.ClientId)
	clientSecret := strings.TrimSpace(req.Msg.ClientSecret)
	if clientID == "" || clientSecret == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("client_id and client_secret are required"))
	}

	err := s.Federation.UpsertBoxFederationClient(ctx, &workspace.BoxFederationClient{
		CustomerSlug: caller.Slug,
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Note:         req.Msg.Note,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if s.Logger != nil {
		// client id only — the secret never reaches a log line.
		s.Logger.Info("box federation client deposited", "slug", caller.Slug, "client_id", clientID, "note", req.Msg.Note)
	}
	return connect.NewResponse(&boxcredentialv1.DepositFederationClientResponse{}), nil
}

// FetchBoxSecrets returns the centrally-minted secrets held for the calling
// box. Same trust rules as the deposit — box-issued service token only, slug
// bound by issuer trust — which is what makes this safe to expose at all: the
// caller cannot name a tenant, so it cannot read another box's secrets.
//
// Missing keys are omitted rather than errored. The caller is a sync loop, and
// the common transient state is "central has not minted it yet".
func (s *BoxCredentialServer) FetchBoxSecrets(ctx context.Context, req *connect.Request[boxcredentialv1.FetchBoxSecretsRequest]) (*connect.Response[boxcredentialv1.FetchBoxSecretsResponse], error) {
	caller := core.InternalCallerFromContext(ctx)
	if caller.Slug == "" {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("secret fetch requires a box service token"))
	}
	if s.Secrets == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("this deployment does not serve box secrets"))
	}

	keys := make([]string, 0, len(req.Msg.Keys))
	for _, k := range req.Msg.Keys {
		if k = strings.TrimSpace(k); k != "" {
			keys = append(keys, k)
		}
	}

	secrets, err := s.Secrets.GetBoxSecrets(ctx, caller.Slug, keys)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if s.Logger != nil {
		// Key names and a count only. The values are the payload; none of
		// them belongs in a log line, and neither does a per-key hit/miss
		// list that would leak which secrets a tenant has.
		s.Logger.Info("box secrets fetched", "slug", caller.Slug, "requested", len(keys), "returned", len(secrets))
	}
	return connect.NewResponse(&boxcredentialv1.FetchBoxSecretsResponse{Secrets: secrets}), nil
}

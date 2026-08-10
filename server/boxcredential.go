package server

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"connectrpc.com/connect"
	"filippo.io/age"

	boxcredentialv1 "github.com/fairtier/workspace-api/proto/boxcredential/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// BoxCredentialServer implements the ConnectRPC BoxCredentialService handler:
// the internal-mux endpoint where a box deposits its write-scoped Gitea
// token, its snapshot-sidecar bearer, and its age public key — the
// "credential inversion": the box pushes credentials up, central never holds
// box admin credentials.
type BoxCredentialServer struct {
	Store      workspace.BoxGitCredentialStore
	Snapshots  workspace.BoxSnapshotCredentialStore
	AgeKeys    workspace.BoxAgeKeyStore
	Federation workspace.BoxFederationClientStore
	// Mirror, when set, re-renders the depositing tenant's pipelines repo
	// after an age-key deposit so existing pipelines get their
	// .credentials.age files immediately (the data migration for free).
	// Best-effort, same contract as PipelineService.mirrorPipelines.
	Mirror workspace.PipelineMirrorer
	Logger *slog.Logger
}

// DepositGitToken upserts the calling box's editor git credential. Only a
// box-issued service token is accepted — the slug is bound by issuer trust,
// so a box can only ever deposit for its own tenant. A central identity (or
// an unauthenticated log-mode call) is rejected: unlike the dlt-worker RPCs
// there is no legacy shared-substrate caller to grandfather in.
func (s *BoxCredentialServer) DepositGitToken(ctx context.Context, req *connect.Request[boxcredentialv1.DepositGitTokenRequest]) (*connect.Response[boxcredentialv1.DepositGitTokenResponse], error) {
	caller := InternalCallerFromContext(ctx)
	if caller.Slug == "" {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("credential deposit requires a box service token"))
	}

	username := strings.TrimSpace(req.Msg.Username)
	token := strings.TrimSpace(req.Msg.Token)
	if username == "" || token == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("username and token are required"))
	}

	err := s.Store.UpsertBoxGitCredential(ctx, &workspace.BoxGitCredential{
		CustomerSlug: caller.Slug,
		Username:     username,
		Token:        token,
		Note:         req.Msg.Note,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if s.Logger != nil {
		s.Logger.Info("box git credential deposited", "slug", caller.Slug, "username", username, "note", req.Msg.Note)
	}
	return connect.NewResponse(&boxcredentialv1.DepositGitTokenResponse{}), nil
}

// DepositSnapshotToken upserts the calling box's snapshot-sidecar bearer.
// Same trust rules as DepositGitToken: box-issued service token only, slug
// bound by issuer trust.
func (s *BoxCredentialServer) DepositSnapshotToken(ctx context.Context, req *connect.Request[boxcredentialv1.DepositSnapshotTokenRequest]) (*connect.Response[boxcredentialv1.DepositSnapshotTokenResponse], error) {
	caller := InternalCallerFromContext(ctx)
	if caller.Slug == "" {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("credential deposit requires a box service token"))
	}

	token := strings.TrimSpace(req.Msg.Token)
	if token == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("token is required"))
	}

	err := s.Snapshots.UpsertBoxSnapshotCredential(ctx, &workspace.BoxSnapshotCredential{
		CustomerSlug: caller.Slug,
		Token:        token,
		Note:         req.Msg.Note,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if s.Logger != nil {
		s.Logger.Info("box snapshot credential deposited", "slug", caller.Slug, "note", req.Msg.Note)
	}
	return connect.NewResponse(&boxcredentialv1.DepositSnapshotTokenResponse{}), nil
}

// DepositAgePublicKey upserts the calling box's age public key
// (pipelines-as-files Phase 3) and best-effort re-renders its pipelines
// repo so every existing pipeline gets its .credentials.age file. Same
// trust rules as the other deposits: box-issued service token only, slug
// bound by issuer trust.
func (s *BoxCredentialServer) DepositAgePublicKey(ctx context.Context, req *connect.Request[boxcredentialv1.DepositAgePublicKeyRequest]) (*connect.Response[boxcredentialv1.DepositAgePublicKeyResponse], error) {
	caller := InternalCallerFromContext(ctx)
	if caller.Slug == "" {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("credential deposit requires a box service token"))
	}

	publicKey := strings.TrimSpace(req.Msg.PublicKey)
	if _, err := age.ParseX25519Recipient(publicKey); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("public_key is not a valid age X25519 recipient"))
	}

	err := s.AgeKeys.UpsertBoxAgeKey(ctx, &workspace.BoxAgeKey{
		CustomerSlug: caller.Slug,
		PublicKey:    publicKey,
		Note:         req.Msg.Note,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if s.Logger != nil {
		s.Logger.Info("box age public key deposited", "slug", caller.Slug, "note", req.Msg.Note)
	}

	// Detached + capped like PipelineService.mirrorPipelines: a slow or
	// unreachable box Gitea must neither hang nor fail the deposit.
	if s.Mirror != nil {
		mctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		// nil author: a key deposit is platform-initiated, no acting user.
		if err := s.Mirror.SyncCustomer(mctx, caller.Slug, nil); err != nil && s.Logger != nil {
			s.Logger.WarnContext(ctx, "post-deposit pipeline mirror sync", "slug", caller.Slug, "err", err)
		}
	}
	return connect.NewResponse(&boxcredentialv1.DepositAgePublicKeyResponse{}), nil
}

// DepositFederationClient upserts the OAuth client the calling box minted for
// itself. Same trust rules as the other deposits: box-issued service token
// only, slug bound by issuer trust, so a box can only ever deposit its own.
//
// A re-deposit with a different secret is a rotation the customer initiated;
// central converges both ends on the next reconcile.
func (s *BoxCredentialServer) DepositFederationClient(ctx context.Context, req *connect.Request[boxcredentialv1.DepositFederationClientRequest]) (*connect.Response[boxcredentialv1.DepositFederationClientResponse], error) {
	caller := InternalCallerFromContext(ctx)
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

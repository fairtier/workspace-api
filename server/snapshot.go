package server

import (
	"context"
	"errors"
	"strings"

	"connectrpc.com/connect"

	snapshotv1 "github.com/fairtier/workspace-api/proto/snapshot/v1"
	"github.com/fairtier/workspace-api/proto/snapshot/v1/snapshotv1connect"
	"github.com/fairtier/workspace-api/workspace"
)

// SnapshotServer proxies snapshot requests to the target app's snapshot
// sidecar via ConnectRPC. The "app" field in the request selects which
// sidecar to target. Shared substrate: cluster-internal Service URL, no
// auth. Dedicated-VM substrate: the box's published rill-snapshot endpoint,
// authenticated with the box-deposited bearer (BoxSnapshotCredentialStore).
type SnapshotServer struct {
	Workspaces workspace.Resolver
	Snapshots  workspace.BoxSnapshotCredentialStore
	HTTPClient connect.HTTPClient
}

func (s *SnapshotServer) sidecarClient(baseURL string) snapshotv1connect.SnapshotServiceClient {
	return snapshotv1connect.NewSnapshotServiceClient(s.HTTPClient, baseURL)
}

// sidecarTarget resolves the snapshot sidecar base URL — and, on the
// dedicated-VM substrate, the bearer token to present — for the target app.
func (s *SnapshotServer) sidecarTarget(ctx context.Context, ws *workspace.Workspace, app string) (baseURL, bearer string, err error) {
	if ws.OnVM {
		if app != "rill" {
			return "", "", connect.NewError(connect.CodeInvalidArgument, errors.New("only \"rill\" snapshots are available on the dedicated-VM substrate"))
		}
		if !ws.RillEnabled {
			return "", "", connect.NewError(connect.CodeFailedPrecondition, errors.New("rill is not enabled for this customer"))
		}
		if ws.CustomerDomain == "" {
			return "", "", connect.NewError(connect.CodeFailedPrecondition, errors.New("customer not yet provisioned"))
		}
		if s.Snapshots == nil {
			// No credential source at all: this deployment cannot reach any
			// box's snapshot sidecar. That is central after split Phase 3E —
			// the box's own workspace plane serves its snapshots.
			return "", "", connect.NewError(connect.CodeUnimplemented, errors.New("snapshots for a dedicated-VM box are served by the box's own workspace API"))
		}
		cred, err := s.Snapshots.GetBoxSnapshotCredential(ctx, ws.Slug)
		if errors.Is(err, workspace.ErrBoxCredentialNotFound) {
			return "", "", connect.NewError(connect.CodeFailedPrecondition, errors.New("the box has not deposited its snapshot token yet — retry after its next sync"))
		}
		if err != nil {
			return "", "", connect.NewError(connect.CodeInternal, err)
		}
		domainName := strings.TrimPrefix(ws.CustomerDomain, "*.")
		return "https://rill-snapshot." + domainName, cred.Token, nil
	}

	if ws.Namespace == "" {
		return "", "", connect.NewError(connect.CodeFailedPrecondition, errors.New("customer not yet provisioned"))
	}
	url, err := sidecarURL(ws, app)
	return url, "", err
}

// sidecarURL resolves the cluster-internal sidecar URL (shared substrate).
func sidecarURL(ws *workspace.Workspace, app string) (string, error) {
	switch app {
	case "rill":
		if !ws.RillEnabled {
			return "", connect.NewError(connect.CodeFailedPrecondition, errors.New("rill is not enabled for this customer"))
		}
		return ws.RillSnapshotURL(), nil
	case "cube":
		if !ws.CubeEnabled {
			return "", connect.NewError(connect.CodeFailedPrecondition, errors.New("cube is not enabled for this customer"))
		}
		return ws.CubeSnapshotURL(), nil
	default:
		return "", connect.NewError(connect.CodeInvalidArgument, errors.New("app must be \"rill\" or \"cube\""))
	}
}

func (s *SnapshotServer) TriggerSnapshot(ctx context.Context, req *connect.Request[snapshotv1.TriggerSnapshotRequest]) (*connect.Response[snapshotv1.TriggerSnapshotResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, domainError(err)
	}

	url, bearer, err := s.sidecarTarget(ctx, ws, req.Msg.App)
	if err != nil {
		return nil, err
	}

	out := connect.NewRequest(&snapshotv1.TriggerSnapshotRequest{})
	if bearer != "" {
		out.Header().Set("Authorization", "Bearer "+bearer)
	}
	return s.sidecarClient(url).TriggerSnapshot(ctx, out)
}

func (s *SnapshotServer) ListSnapshots(ctx context.Context, req *connect.Request[snapshotv1.ListSnapshotsRequest]) (*connect.Response[snapshotv1.ListSnapshotsResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, domainError(err)
	}

	url, bearer, err := s.sidecarTarget(ctx, ws, req.Msg.App)
	if err != nil {
		return nil, err
	}

	out := connect.NewRequest(&snapshotv1.ListSnapshotsRequest{})
	if bearer != "" {
		out.Header().Set("Authorization", "Bearer "+bearer)
	}
	return s.sidecarClient(url).ListSnapshots(ctx, out)
}

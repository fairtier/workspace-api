package server

import (
	"context"
	"errors"
	"strings"

	"connectrpc.com/connect"
	"github.com/fairtier/workspace-api/core"
)

// workerAuth gates the worker-facing RPCs that PipelineService and
// TransformationService expose alongside their user-facing ones.
//
// Both services are mounted twice: on the public mux, where callers carry an
// end-user JWT, and on the internal mux, where the dlt-worker carries a
// tenant-bound service token. The worker RPCs take the tenant as a request
// field (customer_slug) rather than deriving it from the caller, so on the
// public mux — which never sets an internal caller — they would serve
// whatever slug was asked for, handing one tenant's object-storage keys and
// dbt git credentials to any other tenant's Console user.
//
// The zero value therefore denies them. Only NewInternalPipelineServer /
// NewInternalTransformationServer turn them on, and RegisterWorkspaceInternal
// refuses to mount a handler that has not been built that way — so "which mux
// am I on" is a wiring fact the type carries, not a check each handler has to
// remember.
type workerAuth struct {
	// internal marks the handler instance mounted on the internal mux.
	internal bool
}

// newWorkerAuth builds the gate for an internal-mux handler.
func newWorkerAuth() workerAuth {
	return workerAuth{internal: true}
}

// callerSlug returns the tenant slug bound to the caller's service token.
// Box callers carry the slug from their verified issuer host; central
// (shared-substrate) callers encode it in the app name "dlt-worker-<slug>",
// and any other central service identity is rejected — only the dlt-worker
// calls these RPCs.
//
// It fails closed: a non-empty slug always comes back, so a missing or
// tenant-less caller is a denial, never a fallback to the request's own
// customer_slug field.
func (w workerAuth) callerSlug(ctx context.Context) (string, error) {
	if !w.internal {
		return "", connect.NewError(connect.CodePermissionDenied,
			errors.New("worker RPC is served on the internal API only"))
	}

	caller := core.InternalCallerFromContext(ctx)
	if caller.Slug != "" {
		return caller.Slug, nil
	}
	if caller.App == "" {
		return "", connect.NewError(connect.CodeUnauthenticated,
			errors.New("worker RPC requires a tenant-bound service token"))
	}
	slug, ok := strings.CutPrefix(caller.App, "dlt-worker-")
	if !ok || slug == "" {
		return "", connect.NewError(connect.CodePermissionDenied, errors.New("token is not a dlt-worker identity"))
	}
	return slug, nil
}

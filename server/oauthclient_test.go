package server

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"

	oauthclientv1 "github.com/fairtier/workspace-api/proto/oauthclient/v1"
	"github.com/fairtier/workspace-api/workspace"
)

type stubPipelineMirror struct {
	synced []string
	err    error
}

func (m *stubPipelineMirror) SyncCustomer(_ context.Context, slug string, _ *workspace.CommitAuthor) error {
	m.synced = append(m.synced, slug)
	return m.err
}

func testOAuthClientServer(mirror workspace.PipelineMirrorer) (*OAuthClientServer, *stubOAuthClientStore) {
	clients := testCustomerClients()
	return &OAuthClientServer{
		Workspaces: &workspace.StaticResolver{Workspace: workspace.Workspace{Slug: "acme"}},
		Clients:    clients,
		Mirror:     mirror,
	}, clients
}

// Saving or deleting the pair must re-render the pipelines repo: the .age
// files carry the injected pair, so without a converge a rotated secret only
// reaches each pipeline on that pipeline's own next save.
func TestSetOAuthClientConvergesPipelines(t *testing.T) {
	mirror := &stubPipelineMirror{}
	s, _ := testOAuthClientServer(mirror)
	ctx := ContextWithUserID(context.Background(), "u1")

	_, err := s.SetOAuthClient(ctx, connect.NewRequest(&oauthclientv1.SetOAuthClientRequest{
		ClientId: "cid", ClientSecret: "csecret",
	}))
	if err != nil {
		t.Fatalf("SetOAuthClient: %v", err)
	}
	if len(mirror.synced) != 1 || mirror.synced[0] != "acme" {
		t.Fatalf("synced = %v, want one converge for acme", mirror.synced)
	}
}

func TestDeleteOAuthClientConvergesPipelines(t *testing.T) {
	mirror := &stubPipelineMirror{}
	s, _ := testOAuthClientServer(mirror)
	ctx := ContextWithUserID(context.Background(), "u1")

	if _, err := s.DeleteOAuthClient(ctx, connect.NewRequest(&oauthclientv1.DeleteOAuthClientRequest{})); err != nil {
		t.Fatalf("DeleteOAuthClient: %v", err)
	}
	if len(mirror.synced) != 1 || mirror.synced[0] != "acme" {
		t.Fatalf("synced = %v, want one converge for acme", mirror.synced)
	}
}

// A failed converge is logged, never surfaced: the pair itself was stored,
// and the next pipeline save retries the render.
func TestSetOAuthClientConvergeFailureDoesNotFailSave(t *testing.T) {
	mirror := &stubPipelineMirror{err: errors.New("gitea down")}
	s, _ := testOAuthClientServer(mirror)
	ctx := ContextWithUserID(context.Background(), "u1")

	if _, err := s.SetOAuthClient(ctx, connect.NewRequest(&oauthclientv1.SetOAuthClientRequest{
		ClientId: "cid", ClientSecret: "csecret",
	})); err != nil {
		t.Fatalf("SetOAuthClient with failing mirror: %v", err)
	}
}

// A failed store write must not trigger a converge — there is nothing new to
// render, and the old files are still correct.
func TestSetOAuthClientStoreErrorSkipsConverge(t *testing.T) {
	mirror := &stubPipelineMirror{}
	s, clients := testOAuthClientServer(mirror)
	clients.err = errors.New("db down")
	ctx := ContextWithUserID(context.Background(), "u1")

	if _, err := s.SetOAuthClient(ctx, connect.NewRequest(&oauthclientv1.SetOAuthClientRequest{
		ClientId: "cid", ClientSecret: "csecret",
	})); err == nil {
		t.Fatal("SetOAuthClient with failing store: want error")
	}
	if len(mirror.synced) != 0 {
		t.Fatalf("synced = %v, want no converge after a failed upsert", mirror.synced)
	}
}

// Nil mirror (converge not wired) must keep working — central before the
// go.mod bump, and any deployment that renders elsewhere.
func TestSetOAuthClientNilMirror(t *testing.T) {
	s, _ := testOAuthClientServer(nil)
	ctx := ContextWithUserID(context.Background(), "u1")

	if _, err := s.SetOAuthClient(ctx, connect.NewRequest(&oauthclientv1.SetOAuthClientRequest{
		ClientId: "cid", ClientSecret: "csecret",
	})); err != nil {
		t.Fatalf("SetOAuthClient with nil mirror: %v", err)
	}
}

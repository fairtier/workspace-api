package workspace

import (
	"context"
	"errors"
	"testing"

	"github.com/fairtier/workspace-api/core"
)

func TestStaticResolver(t *testing.T) {
	r := &StaticResolver{Workspace: Workspace{Slug: "acme", OnVM: true, CustomerDomain: "customer-acme.example.com", CasdoorOrg: "customer-acme"}}

	ws, err := r.GetWorkspace(context.Background(), "acme")
	if err != nil {
		t.Fatalf("GetWorkspace: %v", err)
	}
	if ws.Slug != "acme" || !ws.OnVM {
		t.Fatalf("unexpected workspace: %+v", ws)
	}
	// The returned workspace is a copy — callers must not be able to mutate
	// the resolver's state.
	ws.Slug = "mutated"
	if r.Workspace.Slug != "acme" {
		t.Fatal("GetWorkspace leaked a pointer to the resolver's workspace")
	}

	if _, err := r.GetWorkspace(context.Background(), "other"); !errors.Is(err, ErrWorkspaceNotFound) {
		t.Fatalf("foreign slug: want ErrWorkspaceNotFound, got %v", err)
	}

	byUser, err := r.GetWorkspaceByUser(context.Background(), "9f0b2c1e-6a4d-4c8f-8d2a-0e1b3c4d5e6f")
	if err != nil || byUser.Slug != "acme" {
		t.Fatalf("GetWorkspaceByUser: %v %+v", err, byUser)
	}

	// The box Casdoor signs machine identities too — Lakekeeper service
	// accounts (handed to end users as read-only warehouse credentials) and
	// the dlt-worker pair. None of them may act as a workspace member, and an
	// owner-qualified subject from a foreign org is not this workspace's user.
	for _, sub := range []string{"admin/lk-acme-reader", "admin/dlt-worker", "customer-other/alice", ""} {
		if _, err := r.GetWorkspaceByUser(context.Background(), core.UserID(sub)); !errors.Is(err, ErrWorkspaceNotFound) {
			t.Fatalf("GetWorkspaceByUser(%q): want ErrWorkspaceNotFound, got %v", sub, err)
		}
	}

	// An owner-qualified subject from the workspace's own org stays accepted.
	if own, err := r.GetWorkspaceByUser(context.Background(), "customer-acme/alice"); err != nil || own.Slug != "acme" {
		t.Fatalf("GetWorkspaceByUser(customer-acme/alice): %v %+v", err, own)
	}

	slugs, err := r.ListVMWorkspaceSlugs(context.Background())
	if err != nil || len(slugs) != 1 || slugs[0] != "acme" {
		t.Fatalf("ListVMWorkspaceSlugs: %v %v", err, slugs)
	}
}

func TestStaticBoxCredentials(t *testing.T) {
	s := &StaticBoxCredentials{
		Slug:        "acme",
		GitUsername: "fairtier-admin",
		GitToken:    "tok",
		// SnapshotToken deliberately empty: reads must degrade to
		// ErrBoxCredentialNotFound, the same signal as "not deposited yet".
		AgePublicKey: "age1example",
	}
	ctx := context.Background()

	git, err := s.GetBoxGitCredential(ctx, "acme")
	if err != nil || git.Username != "fairtier-admin" || git.Token != "tok" {
		t.Fatalf("git credential: %v %+v", err, git)
	}
	if _, err := s.GetBoxGitCredential(ctx, "other"); !errors.Is(err, ErrWorkspaceNotFound) {
		t.Fatalf("foreign slug: want ErrWorkspaceNotFound, got %v", err)
	}
	if _, err := s.GetBoxSnapshotCredential(ctx, "acme"); !errors.Is(err, ErrBoxCredentialNotFound) {
		t.Fatalf("empty snapshot token: want ErrBoxCredentialNotFound, got %v", err)
	}
	key, err := s.GetBoxAgeKey(ctx, "acme")
	if err != nil || key.PublicKey != "age1example" {
		t.Fatalf("age key: %v %+v", err, key)
	}

	if err := s.UpsertBoxGitCredential(ctx, &BoxGitCredential{}); err == nil {
		t.Fatal("Upsert on a static store must fail")
	}
}

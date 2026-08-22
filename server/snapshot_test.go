package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	"github.com/fairtier/workspace-api/workspace"
)

// TestSnapshotServer_NoCredentialStoreIsUnimplemented pins the deployment
// shape central has after split Phase 3E: the snapshot-bearer deposit is
// retired, so nothing wires a store and the box's own workspace plane serves
// its snapshots. The VM branch must answer before dereferencing the store.
func TestSnapshotServer_NoCredentialStoreIsUnimplemented(t *testing.T) {
	s := &SnapshotServer{} // Snapshots nil
	ws := &workspace.Workspace{
		Slug:           "acme",
		OnVM:           true,
		RillEnabled:    true,
		CustomerDomain: "customer-acme.fairtier.com",
	}

	_, _, err := s.sidecarTarget(context.Background(), ws, "rill")
	if connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Fatalf("code = %v, want Unimplemented (got err %v)", connect.CodeOf(err), err)
	}
}

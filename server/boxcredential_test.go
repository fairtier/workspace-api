package server

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"

	boxcredentialv1 "github.com/fairtier/workspace-api/proto/boxcredential/v1"
	"github.com/fairtier/workspace-api/workspace"
)

type fakeFederationStore struct {
	got *workspace.BoxFederationClient
	err error
}

func (f *fakeFederationStore) UpsertBoxFederationClient(_ context.Context, c *workspace.BoxFederationClient) error {
	f.got = c
	return f.err
}

func (f *fakeFederationStore) GetBoxFederationClient(context.Context, string) (*workspace.BoxFederationClient, error) {
	if f.got == nil {
		return nil, workspace.ErrBoxCredentialNotFound
	}
	return f.got, nil
}

func depositFederation(id, secret string) *connect.Request[boxcredentialv1.DepositFederationClientRequest] {
	return connect.NewRequest(&boxcredentialv1.DepositFederationClientRequest{
		ClientId:     id,
		ClientSecret: secret,
		Note:         "casdoor-seed",
	})
}

func TestDepositFederationClient_StoresForTheCallingBox(t *testing.T) {
	store := &fakeFederationStore{}
	srv := &BoxCredentialServer{Federation: store}

	if _, err := srv.DepositFederationClient(withBoxCaller("dlt-worker", "acme"), depositFederation("cid", "csecret")); err != nil {
		t.Fatalf("DepositFederationClient: %v", err)
	}

	if store.got == nil {
		t.Fatal("nothing stored")
	}
	// The slug comes from the verified issuer, never from the request: a box
	// can only ever deposit its own.
	if store.got.CustomerSlug != "acme" {
		t.Errorf("slug = %q, want acme", store.got.CustomerSlug)
	}
	if store.got.ClientID != "cid" || store.got.ClientSecret != "csecret" {
		t.Errorf("stored pair = %q/%q, want cid/csecret", store.got.ClientID, store.got.ClientSecret)
	}
}

func TestDepositFederationClient_RejectsACentralToken(t *testing.T) {
	store := &fakeFederationStore{}
	srv := &BoxCredentialServer{Federation: store}

	// A central service token carries no slug — there is no tenant to bind it
	// to, so the deposit must not be attributed to a guess.
	_, err := srv.DepositFederationClient(withInternalCaller("platform-api"), depositFederation("cid", "csecret"))
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("code = %v, want PermissionDenied", connect.CodeOf(err))
	}
	if store.got != nil {
		t.Error("a rejected deposit still wrote to the store")
	}
}

func TestDepositFederationClient_RequiresBothHalves(t *testing.T) {
	srv := &BoxCredentialServer{Federation: &fakeFederationStore{}}

	for name, req := range map[string]*connect.Request[boxcredentialv1.DepositFederationClientRequest]{
		"no id":     depositFederation("", "csecret"),
		"no secret": depositFederation("cid", ""),
		"blank id":  depositFederation("   ", "csecret"),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := srv.DepositFederationClient(withBoxCaller("dlt-worker", "acme"), req)
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("code = %v, want InvalidArgument", connect.CodeOf(err))
			}
		})
	}
}

func TestDepositFederationClient_StoreFailureIsInternal(t *testing.T) {
	srv := &BoxCredentialServer{Federation: &fakeFederationStore{err: errors.New("boom")}}

	_, err := srv.DepositFederationClient(withBoxCaller("dlt-worker", "acme"), depositFederation("cid", "csecret"))
	if connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("code = %v, want Internal", connect.CodeOf(err))
	}
}

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

type fakeSecretStore struct {
	// bySlug is the whole store, so a test can prove one tenant's fetch
	// cannot reach another's rows.
	bySlug  map[string]map[string]string
	gotSlug string
	gotKeys []string
	err     error
}

func (f *fakeSecretStore) UpsertBoxSecret(_ context.Context, s *workspace.BoxSecret) error {
	if f.bySlug == nil {
		f.bySlug = map[string]map[string]string{}
	}
	if f.bySlug[s.CustomerSlug] == nil {
		f.bySlug[s.CustomerSlug] = map[string]string{}
	}
	f.bySlug[s.CustomerSlug][s.Key] = s.Value
	return nil
}

func (f *fakeSecretStore) GetBoxSecrets(_ context.Context, slug string, keys []string) (map[string]string, error) {
	f.gotSlug, f.gotKeys = slug, keys
	if f.err != nil {
		return nil, f.err
	}
	held := f.bySlug[slug]
	if len(keys) == 0 {
		return held, nil
	}
	out := map[string]string{}
	for _, k := range keys {
		if v, ok := held[k]; ok {
			out[k] = v
		}
	}
	return out, nil
}

func fetchSecrets(keys ...string) *connect.Request[boxcredentialv1.FetchBoxSecretsRequest] {
	return connect.NewRequest(&boxcredentialv1.FetchBoxSecretsRequest{Keys: keys})
}

func TestFetchBoxSecrets_ServesOnlyTheCallersOwnTenant(t *testing.T) {
	store := &fakeSecretStore{bySlug: map[string]map[string]string{
		"acme":  {"betterstack_source_token": "acme-token"},
		"other": {"betterstack_source_token": "other-token"},
	}}
	srv := &BoxCredentialServer{Secrets: store}

	resp, err := srv.FetchBoxSecrets(withBoxCaller("dlt-worker", "acme"), fetchSecrets("betterstack_source_token"))
	if err != nil {
		t.Fatalf("FetchBoxSecrets: %v", err)
	}

	// The slug reaching the store comes from the verified issuer. Nothing in
	// the request can name a tenant, which is the only reason serving secrets
	// outward is safe at all.
	if store.gotSlug != "acme" {
		t.Errorf("store queried for %q, want acme", store.gotSlug)
	}
	if got := resp.Msg.Secrets["betterstack_source_token"]; got != "acme-token" {
		t.Errorf("token = %q, want acme-token", got)
	}
}

func TestFetchBoxSecrets_OmitsMissingKeysInsteadOfFailing(t *testing.T) {
	store := &fakeSecretStore{bySlug: map[string]map[string]string{
		"acme": {"present": "value"},
	}}
	srv := &BoxCredentialServer{Secrets: store}

	// The sync Job runs on a loop and central mints asynchronously, so
	// "not yet" must not take down delivery of everything else.
	resp, err := srv.FetchBoxSecrets(withBoxCaller("dlt-worker", "acme"), fetchSecrets("present", "not_minted_yet"))
	if err != nil {
		t.Fatalf("FetchBoxSecrets: %v", err)
	}
	if len(resp.Msg.Secrets) != 1 || resp.Msg.Secrets["present"] != "value" {
		t.Errorf("secrets = %v, want only present=value", resp.Msg.Secrets)
	}
}

func TestFetchBoxSecrets_DropsBlankKeys(t *testing.T) {
	store := &fakeSecretStore{}
	srv := &BoxCredentialServer{Secrets: store}

	// A blank key must not reach the store as an empty-string lookup, and
	// must not turn a keyed request into the "everything" request.
	if _, err := srv.FetchBoxSecrets(withBoxCaller("dlt-worker", "acme"), fetchSecrets("real", "  ")); err != nil {
		t.Fatalf("FetchBoxSecrets: %v", err)
	}
	if len(store.gotKeys) != 1 || store.gotKeys[0] != "real" {
		t.Errorf("keys = %v, want [real]", store.gotKeys)
	}
}

func TestFetchBoxSecrets_RejectsACentralToken(t *testing.T) {
	store := &fakeSecretStore{bySlug: map[string]map[string]string{"acme": {"k": "v"}}}
	srv := &BoxCredentialServer{Secrets: store}

	// No slug means no tenant to bind to. On a read that is sharper than on a
	// deposit: guessing here would hand one tenant's secret to another caller.
	_, err := srv.FetchBoxSecrets(withInternalCaller("platform-api"), fetchSecrets("k"))
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("code = %v, want PermissionDenied", connect.CodeOf(err))
	}
	if store.gotSlug != "" {
		t.Error("a rejected fetch still queried the store")
	}
}

func TestFetchBoxSecrets_UnwiredStoreIsUnimplemented(t *testing.T) {
	srv := &BoxCredentialServer{}

	// Not an empty map: a box must be able to tell "central holds nothing for
	// me" from "this central build cannot serve secrets", or a rollback would
	// look like a legitimate instruction to wipe its Secret.
	_, err := srv.FetchBoxSecrets(withBoxCaller("dlt-worker", "acme"), fetchSecrets("k"))
	if connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Fatalf("code = %v, want Unimplemented", connect.CodeOf(err))
	}
}

func TestFetchBoxSecrets_StoreFailureIsInternal(t *testing.T) {
	srv := &BoxCredentialServer{Secrets: &fakeSecretStore{err: errors.New("boom")}}

	_, err := srv.FetchBoxSecrets(withBoxCaller("dlt-worker", "acme"), fetchSecrets("k"))
	if connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("code = %v, want Internal", connect.CodeOf(err))
	}
}

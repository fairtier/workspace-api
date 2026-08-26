package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type fakeConnectionStore struct {
	conns map[string]*Connection
}

func (f *fakeConnectionStore) CreateConnection(_ context.Context, c *Connection) error {
	if f.conns == nil {
		f.conns = map[string]*Connection{}
	}
	f.conns[c.ID] = c
	return nil
}

func (f *fakeConnectionStore) ReauthorizeConnection(_ context.Context, slug, id string, creds json.RawMessage) error {
	c, ok := f.conns[id]
	if !ok || c.CustomerSlug != slug {
		return ErrConnectionNotFound
	}
	c.Credentials = creds
	c.Status = "active"
	return nil
}

func (f *fakeConnectionStore) GetConnection(_ context.Context, slug, id string) (*Connection, error) {
	c, ok := f.conns[id]
	if !ok || c.CustomerSlug != slug {
		return nil, ErrConnectionNotFound
	}
	return c, nil
}

func (f *fakeConnectionStore) ListConnections(_ context.Context, slug string) ([]Connection, error) {
	var out []Connection
	for _, c := range f.conns {
		if c.CustomerSlug == slug {
			out = append(out, *c)
		}
	}
	return out, nil
}

func (f *fakeConnectionStore) DeleteConnection(_ context.Context, slug, id string) error {
	if c, ok := f.conns[id]; ok && c.CustomerSlug == slug {
		delete(f.conns, id)
	}
	return nil
}

type fakeGrantStore struct{ grant *GoogleOAuthGrant }

func (f *fakeGrantStore) CreateGoogleOAuthGrant(context.Context, *GoogleOAuthGrant) error { return nil }

func (f *fakeGrantStore) ConsumeGoogleOAuthGrant(_ context.Context, grantID, slug string) (*GoogleOAuthGrant, error) {
	if f.grant == nil || f.grant.GrantID != grantID || f.grant.CustomerSlug != slug {
		return nil, ErrOAuthGrantNotFound
	}
	g := f.grant
	f.grant = nil // one-time use
	return g, nil
}

func (f *fakeGrantStore) DeleteExpiredGoogleOAuthGrants(context.Context) (int64, error) {
	return 0, nil
}

type fakeCredentialReader map[PipelineID]json.RawMessage

func (f fakeCredentialReader) ListPipelineCredentialsByCustomer(context.Context, string) (map[PipelineID]json.RawMessage, error) {
	return f, nil
}

type fakeOAuthClients struct{ client *OAuthClient }

func (f *fakeOAuthClients) UpsertOAuthClient(context.Context, *OAuthClient) error { return nil }
func (f *fakeOAuthClients) GetOAuthClient(context.Context, string, string) (*OAuthClient, error) {
	if f.client == nil {
		return nil, ErrOAuthClientNotFound
	}
	return f.client, nil
}
func (f *fakeOAuthClients) DeleteOAuthClient(context.Context, string, string) error { return nil }

func googleConn(id, slug, refreshToken, email, clientID string) *Connection {
	creds, _ := json.Marshal(googleConnectionCredentials{
		RefreshToken: refreshToken, Email: email, ClientID: clientID,
	})
	return &Connection{
		ID: id, CustomerSlug: slug, Type: ConnectionTypeGoogle,
		Name: email, Status: "active", Credentials: creds,
	}
}

func TestCreateGoogleConnectionConsumesGrant(t *testing.T) {
	store := &fakeConnectionStore{}
	grants := &fakeGrantStore{grant: &GoogleOAuthGrant{
		GrantID: "g1", CustomerSlug: "acme",
		RefreshToken: "rt-1", Email: "alice@corp.com", ClientID: "client-1",
	}}
	svc := &ConnectionService{Connections: store, GoogleOAuth: grants}

	c, err := svc.CreateGoogleConnection(context.Background(), "acme", "g1", "")
	if err != nil {
		t.Fatalf("CreateGoogleConnection: %v", err)
	}
	if c.Name != "alice@corp.com" {
		t.Fatalf("name should default to the granting email, got %q", c.Name)
	}
	if c.GoogleEmail() != "alice@corp.com" {
		t.Fatalf("GoogleEmail: got %q", c.GoogleEmail())
	}
	gc, err := c.googleCredentials()
	if err != nil {
		t.Fatalf("googleCredentials: %v", err)
	}
	if gc.RefreshToken != "rt-1" || gc.ClientID != "client-1" {
		t.Fatalf("credentials not carried over: %+v", gc)
	}

	// One-time use: the same grant cannot mint a second connection.
	if _, err := svc.CreateGoogleConnection(context.Background(), "acme", "g1", ""); !errors.Is(err, ErrOAuthGrantNotFound) {
		t.Fatalf("expected ErrOAuthGrantNotFound on reuse, got %v", err)
	}
}

// Reconnecting an account that is already connected must re-authorize the
// EXISTING connection, keeping its id — every pipeline references it by id, so
// minting a second connection would leave all of them on the dead token, and
// refusing (the old behaviour) made the documented fix for a stale grant the
// one operation the API could not perform.
func TestCreateGoogleConnectionReauthorizesSameAccount(t *testing.T) {
	ctx := context.Background()
	store := &fakeConnectionStore{}
	_ = store.CreateConnection(ctx, googleConn("c1", "acme", "rt-old", "alice@corp.com", "old-client"))

	grants := &fakeGrantStore{grant: &GoogleOAuthGrant{
		GrantID: "g2", CustomerSlug: "acme",
		RefreshToken: "rt-new", Email: "alice@corp.com", ClientID: "new-client",
	}}
	svc := &ConnectionService{Connections: store, GoogleOAuth: grants}

	c, err := svc.CreateGoogleConnection(ctx, "acme", "g2", "")
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if c.ID != "c1" {
		t.Fatalf("reconnect must keep the connection id, got %q", c.ID)
	}
	if len(store.conns) != 1 {
		t.Fatalf("reconnect must not fork a second connection, got %d", len(store.conns))
	}
	gc, err := store.conns["c1"].googleCredentials()
	if err != nil {
		t.Fatalf("googleCredentials: %v", err)
	}
	if gc.RefreshToken != "rt-new" || gc.ClientID != "new-client" {
		t.Fatalf("stored credentials not replaced: %+v", gc)
	}
}

// A DIFFERENT account signing in is a new connection, never a rebind of the
// existing one — pipelines referencing it would silently start reading another
// person's sheets.
func TestCreateGoogleConnectionDistinctAccountIsSeparate(t *testing.T) {
	ctx := context.Background()
	store := &fakeConnectionStore{}
	_ = store.CreateConnection(ctx, googleConn("c1", "acme", "rt-1", "alice@corp.com", "client-1"))

	grants := &fakeGrantStore{grant: &GoogleOAuthGrant{
		GrantID: "g2", CustomerSlug: "acme",
		RefreshToken: "rt-2", Email: "bob@corp.com", ClientID: "client-1",
	}}
	svc := &ConnectionService{Connections: store, GoogleOAuth: grants}

	c, err := svc.CreateGoogleConnection(ctx, "acme", "g2", "")
	if err != nil {
		t.Fatalf("connect second account: %v", err)
	}
	if c.ID == "c1" {
		t.Fatal("a different Google account must not rebind an existing connection")
	}
	if len(store.conns) != 2 {
		t.Fatalf("expected two connections, got %d", len(store.conns))
	}
}

// The connection read happens BEFORE the grant is redeemed: a grant is
// one-time and unspendable twice, so a failure that could have been detected
// first must not cost the customer another trip through Google's consent
// screen.
func TestCreateGoogleConnectionKeepsGrantWhenStoreFails(t *testing.T) {
	grants := &fakeGrantStore{grant: &GoogleOAuthGrant{
		GrantID: "g1", CustomerSlug: "acme",
		RefreshToken: "rt-1", Email: "alice@corp.com", ClientID: "client-1",
	}}
	svc := &ConnectionService{Connections: &failingListStore{}, GoogleOAuth: grants}

	if _, err := svc.CreateGoogleConnection(context.Background(), "acme", "g1", ""); err == nil {
		t.Fatal("expected the list failure to surface")
	}
	if grants.grant == nil {
		t.Fatal("the grant was consumed despite the save never being attempted")
	}
}

// failingListStore fails the pre-redemption read and nothing else.
type failingListStore struct{ fakeConnectionStore }

func (*failingListStore) ListConnections(context.Context, string) ([]Connection, error) {
	return nil, errors.New("postgres is down")
}

func TestDeleteConnectionRefusesWhileReferenced(t *testing.T) {
	store := &fakeConnectionStore{}
	conn := googleConn("c1", "acme", "rt-1", "alice@corp.com", "client-1")
	_ = store.CreateConnection(context.Background(), conn)

	referencing, _ := json.Marshal(googleSheetsCreds{OAuth: &googleOAuthCredential{ConnectionID: "c1"}})
	svc := &ConnectionService{
		Connections:         store,
		PipelineCredentials: fakeCredentialReader{"p1": referencing},
	}
	if err := svc.DeleteConnection(context.Background(), "acme", "c1"); !errors.Is(err, ErrConnectionInUse) {
		t.Fatalf("expected ErrConnectionInUse, got %v", err)
	}

	// Unreferenced → deleted.
	svc.PipelineCredentials = fakeCredentialReader{}
	if err := svc.DeleteConnection(context.Background(), "acme", "c1"); err != nil {
		t.Fatalf("DeleteConnection: %v", err)
	}
	if _, err := store.GetConnection(context.Background(), "acme", "c1"); !errors.Is(err, ErrConnectionNotFound) {
		t.Fatal("connection should be gone")
	}
}

// TestResolverConnectionParity is the contract the dlt-worker depends on: a
// pipeline referencing a connection must serve/render EXACTLY the bytes an
// embedded credential does, so the worker never learns which storage form a
// pipeline uses and needs no new reader branch.
//
// Table-driven over every Google-backed source type. Parity is required
// WITHIN a type, not across types: google_sheets serves the oauth member the
// dlt source reads, duckdb/gdrive serves the flattened DuckDB secret the
// extension reads, and those are different shapes by design.
func TestResolverConnectionParity(t *testing.T) {
	for _, tc := range []struct {
		name        string
		sourceType  string
		embedded    func() json.RawMessage
		referencing func() json.RawMessage
		wantSecret  string
	}{
		{
			name:       "google_sheets",
			sourceType: "google_sheets",
			embedded: func() json.RawMessage {
				raw, _ := storedGoogleOAuthCreds("google_sheets", nil, "rt-1", "alice@corp.com", "client-1")
				return raw
			},
			referencing: func() json.RawMessage {
				raw, _ := json.Marshal(googleSheetsCreds{OAuth: &googleOAuthCredential{ConnectionID: "c1"}})
				return raw
			},
			wantSecret: `"client_secret":"sec-1"`,
		},
		{
			name:       "duckdb gdrive",
			sourceType: "duckdb",
			embedded: func() json.RawMessage {
				raw, _ := storedGoogleOAuthCreds("duckdb", nil, "rt-1", "alice@corp.com", "client-1")
				return raw
			},
			referencing: func() json.RawMessage {
				raw, _ := json.Marshal(duckdbCreds{OAuth: &googleOAuthCredential{ConnectionID: "c1"}})
				return raw
			},
			// The gdrive extension authenticates through a DuckDB secret, so
			// the served form is the flattened one, not the oauth member.
			wantSecret: `"CLIENT_SECRET":"sec-1"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := &fakeConnectionStore{}
			_ = store.CreateConnection(ctx, googleConn("c1", "acme", "rt-1", "alice@corp.com", "client-1"))
			clients := &fakeOAuthClients{client: &OAuthClient{ClientID: "client-1", ClientSecret: "sec-1"}}

			inject := func(raw json.RawMessage) json.RawMessage {
				r := newOAuthClientResolver(clients, store, "acme")
				p := &Pipeline{SourceType: tc.sourceType, SourceCredentials: raw}
				out, ok := r.inject(ctx, p)
				if !ok {
					t.Fatalf("inject returned false for %s", raw)
				}
				return out
			}

			got, want := inject(tc.referencing()), inject(tc.embedded())
			if string(got) != string(want) {
				t.Fatalf("connection-referencing render differs from embedded:\n got: %s\nwant: %s", got, want)
			}
			if !strings.Contains(string(got), tc.wantSecret) {
				t.Fatalf("client pair not injected: %s", got)
			}
			if strings.Contains(string(got), "connection_id") {
				t.Fatalf("connection_id must not leak into the worker-facing shape: %s", got)
			}
		})
	}
}

// TestResolverPreservesSiblingCredentials guards the merge in
// resolveConnectionCredential. A duckdb credential carries attach_params and
// its own secret keys beside the oauth member; rebuilding the envelope from
// the connection alone would serve the pipeline with the rest of its
// credentials silently stripped, and the run would fail somewhere unrelated.
func TestResolverPreservesSiblingCredentials(t *testing.T) {
	ctx := context.Background()
	store := &fakeConnectionStore{}
	_ = store.CreateConnection(ctx, googleConn("c1", "acme", "rt-1", "alice@corp.com", "client-1"))
	clients := &fakeOAuthClients{client: &OAuthClient{ClientID: "client-1", ClientSecret: "sec-1"}}

	referencing, _ := json.Marshal(duckdbCreds{
		AttachParams: map[string]string{"host": "db.example.com"},
		Secret:       map[string]string{"EXTRA": "keep-me"},
		OAuth:        &googleOAuthCredential{ConnectionID: "c1"},
	})
	r := newOAuthClientResolver(clients, store, "acme")
	p := &Pipeline{SourceType: "duckdb", SourceCredentials: referencing}
	out, ok := r.inject(ctx, p)
	if !ok {
		t.Fatal("expected the resolved credential to be returned")
	}
	for _, want := range []string{
		`"host":"db.example.com"`,
		`"EXTRA":"keep-me"`,
		`"REFRESH_TOKEN":"rt-1"`,
		`"CLIENT_SECRET":"sec-1"`,
		`"PROVIDER":"config"`,
	} {
		if !strings.Contains(string(out), want) {
			t.Errorf("served credential lost %s: %s", want, out)
		}
	}
	if strings.Contains(string(out), `"oauth"`) {
		t.Errorf("the oauth member must not reach the worker: %s", out)
	}
}

// TestResolverConnectionWithoutClientPair: a resolved connection still renders
// its refresh token when the customer has no OAuth app connected — failing the
// run on a missing client_id (the honest signal), not a missing refresh_token.
func TestResolverConnectionWithoutClientPair(t *testing.T) {
	ctx := context.Background()
	store := &fakeConnectionStore{}
	_ = store.CreateConnection(ctx, googleConn("c1", "acme", "rt-1", "alice@corp.com", "client-1"))

	referencing, _ := json.Marshal(googleSheetsCreds{OAuth: &googleOAuthCredential{ConnectionID: "c1"}})
	r := newOAuthClientResolver(&fakeOAuthClients{}, store, "acme")
	p := &Pipeline{SourceType: "google_sheets", SourceCredentials: referencing}
	out, ok := r.inject(ctx, p)
	if !ok {
		t.Fatal("expected the resolved credential to be returned")
	}
	if !strings.Contains(string(out), `"refresh_token":"rt-1"`) {
		t.Fatalf("refresh token not resolved: %s", out)
	}
	if strings.Contains(string(out), "client_secret") {
		t.Fatalf("no client pair should be present: %s", out)
	}
}

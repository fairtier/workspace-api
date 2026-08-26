package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// newConnectionID mints a connection id.
func newConnectionID() string { return uuid.NewString() }

// ConnectionTypeGoogle is the only connection type today: a delegated-user
// Google grant (Sheets scope), created from the same "Sign in with Google"
// flow that pipelines use. The store shape is generic so a second type
// (a database, an HTTP endpoint) needs no migration.
const ConnectionTypeGoogle = "google"

// Connection is a workspace-level authorization to an external system —
// the customer connects Google ONCE, and everything that needs that access
// consumes the connection: DLT pipelines reference it by id instead of
// embedding a refresh token. Deleting the connection kills every consumer,
// which is what a customer expects "disconnect Google" to mean.
//
// (The box query engine was a second consumer once — short-lived access
// tokens minted into DuckFlight's reconcile SQL, the query-time-federation
// PoC — retired 2026-08-27: nobody queried sheets live, pipelines are the
// product. See docs/plans/query-time-federation.md in the platform repo.)
//
// Credentials is plaintext in the domain; the postgres layer encrypts it at
// rest (same Encryptor as pipeline credentials). For Google it is a
// googleConnectionCredentials JSON — the client pair itself stays in
// customer_oauth_clients, exactly as for per-pipeline grants.
type Connection struct {
	ID           string
	CustomerSlug string
	Type         string
	Name         string
	Status       string
	Config       json.RawMessage
	Credentials  json.RawMessage
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// googleConnectionCredentials is the credentials JSON for a "google"
// connection. Mirrors the stored shape of a per-pipeline OAuth credential
// (googleOAuthCredential) minus the pipeline-specific wrapper.
type googleConnectionCredentials struct {
	RefreshToken string `json:"refresh_token"`
	Email        string `json:"email,omitempty"`
	// ClientID records which of the customer's OAuth apps minted the token;
	// a refresh token is only refreshable by the client it was issued to.
	ClientID string `json:"client_id,omitempty"`
}

// ConnectionStore persists workspace connections. All reads and writes are
// slug-scoped: a caller can never address another workspace's connection.
type ConnectionStore interface {
	// CreateConnection inserts a new connection (credentials encrypted at
	// rest). ErrConnectionAlreadyExists when (customer, type, name) is taken.
	CreateConnection(ctx context.Context, c *Connection) error

	// ReauthorizeConnection replaces an existing connection's credentials in
	// place and marks it active, keeping its id so every consumer follows the
	// new authorization. ErrConnectionNotFound when id does not address a
	// connection in this workspace.
	ReauthorizeConnection(ctx context.Context, customerSlug, id string, credentials json.RawMessage) error

	// GetConnection returns the connection, or ErrConnectionNotFound when id
	// does not exist or belongs to another workspace.
	GetConnection(ctx context.Context, customerSlug, id string) (*Connection, error)

	// ListConnections returns the workspace's connections, newest first.
	ListConnections(ctx context.Context, customerSlug string) ([]Connection, error)

	// DeleteConnection removes the connection. Deleting one that does not
	// exist is not an error.
	DeleteConnection(ctx context.Context, customerSlug, id string) error
}

// Connection error sentinels. Beside the others in errors.go in spirit; kept
// here so the whole Connection contract reads in one file.
var (
	// ErrConnectionNotFound means the id does not address a connection in the
	// caller's workspace.
	ErrConnectionNotFound = errors.New("connection not found")

	// ErrConnectionAlreadyExists rejects a duplicate (type, name) per
	// workspace. Reserved for a genuine clash — an explicit name already taken
	// by a DIFFERENT account. Re-authorizing an account that is already
	// connected is a reconnect, not a duplicate, and updates it in place.
	ErrConnectionAlreadyExists = errors.New("a connection with this name already exists")

	// ErrConnectionInUse refuses deleting a connection that pipelines still
	// reference. The Console names the fix: detach or delete those pipelines
	// first.
	ErrConnectionInUse = errors.New("connection is referenced by one or more pipelines")

	// ErrUnsupportedConnectionType guards the type column: the store shape is
	// generic, the flows behind it are not.
	ErrUnsupportedConnectionType = errors.New("unsupported connection type")

	// ErrConnectionsNotConfigured means this deployment has no connection
	// support wired (no store); the Console hides the surface.
	ErrConnectionsNotConfigured = errors.New("connections are not enabled on this server")
)

// ConnectionService owns the connection lifecycle: creating one from a
// redeemed Google grant, listing, and delete-with-in-use-guard.
type ConnectionService struct {
	Connections ConnectionStore
	// GoogleOAuth redeems the one-time grant produced by the consent popup —
	// the same store PipelineService uses. Nil disables Google connections.
	GoogleOAuth GoogleOAuthGrantStore
	// PipelineCredentials, when set, backs the delete guard: a connection
	// referenced by a pipeline credential refuses deletion. Same store the
	// mirror renders from, so the guard sees exactly what the renderer would.
	PipelineCredentials PipelineCredentialReader
}

// CreateGoogleConnection redeems a "Sign in with Google" grant into a
// workspace connection. One-time use, tenant-checked — identical trust rules
// to the per-pipeline grant swap. Name defaults to the granting email.
//
// Re-authorizing an account that is already connected updates that connection
// in place and returns it, id unchanged. Reconnecting is the fix we tell
// customers to apply whenever a token goes stale — an expired grant, or an
// OAuth app swap, which invalidates every token the previous app minted — so
// an insert-only create makes the documented remedy the one operation that
// cannot succeed: the name defaults to the email, the email is the same
// account, and the duplicate is rejected. Worse, the rejection is unfixable
// from the outside, because the existing connection cannot be deleted while
// pipelines reference it and pipelines cannot be repointed at a connection
// that does not exist yet.
//
// Identity is the granting account, NOT the display name. Matching on name
// would let an explicit name rebind an existing connection to a different
// Google account behind the pipelines already referencing it, and would fork
// a second connection for the same account after a rename.
func (s *ConnectionService) CreateGoogleConnection(ctx context.Context, customerSlug, grantID, name string) (*Connection, error) {
	if s.Connections == nil {
		return nil, ErrConnectionsNotConfigured
	}
	if s.GoogleOAuth == nil {
		return nil, ErrConnectionsNotConfigured
	}
	grantID = strings.TrimSpace(grantID)
	if grantID == "" {
		return nil, errors.New("grant_id is required")
	}

	// Read the workspace's connections BEFORE redeeming, so the one dependency
	// that can realistically fail here fails while the grant is still spendable.
	// A grant is consumed atomically and cannot be handed back: every error
	// raised after this point costs the customer another trip through the
	// Google consent screen, so as little as possible is left after it.
	existing, err := s.Connections.ListConnections(ctx, customerSlug)
	if err != nil {
		return nil, fmt.Errorf("list connections: %w", err)
	}

	grant, creds, err := s.redeemGoogleGrant(ctx, customerSlug, grantID)
	if err != nil {
		return nil, err
	}

	// Reconnect: same account, same connection. The display name is left
	// alone — the customer re-authorized an account, they did not rename it.
	if prior := findGoogleConnectionByEmail(existing, grant.Email); prior != nil {
		if err := s.Connections.ReauthorizeConnection(ctx, customerSlug, prior.ID, creds); err != nil {
			return nil, err
		}
		prior.Status = "active"
		prior.Credentials = creds
		return prior, nil
	}

	c := &Connection{
		ID:           newConnectionID(),
		CustomerSlug: customerSlug,
		Type:         ConnectionTypeGoogle,
		Name:         googleConnectionName(name, grant.Email),
		Status:       "active",
		Config:       json.RawMessage(`{}`),
		Credentials:  creds,
	}
	if err := s.Connections.CreateConnection(ctx, c); err != nil {
		return nil, err
	}
	return c, nil
}

// redeemGoogleGrant consumes the one-time grant and packs it into the stored
// credential form. Split out of CreateGoogleConnection so the create/reconnect
// decision reads at one level.
func (s *ConnectionService) redeemGoogleGrant(ctx context.Context, customerSlug, grantID string) (*GoogleOAuthGrant, json.RawMessage, error) {
	grant, err := s.GoogleOAuth.ConsumeGoogleOAuthGrant(ctx, grantID, customerSlug)
	if err != nil {
		if errors.Is(err, ErrOAuthGrantNotFound) {
			return nil, nil, ErrOAuthGrantNotFound
		}
		return nil, nil, fmt.Errorf("consume oauth grant: %w", err)
	}

	creds, err := json.Marshal(googleConnectionCredentials{
		RefreshToken: grant.RefreshToken,
		Email:        grant.Email,
		ClientID:     grant.ClientID,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("build connection credentials: %w", err)
	}
	return grant, creds, nil
}

// googleConnectionName picks a fresh connection's display name: the caller's
// explicit name, else the granting email, else a literal fallback for the
// rare consent that yields no identity.
func googleConnectionName(name, email string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		name = email
	}
	if name == "" {
		name = "Google"
	}
	return name
}

// findGoogleConnectionByEmail returns the workspace's existing connection for
// a Google account, or nil. An empty email matches nothing: Google normally
// returns one, but a consent that did not yield an identity must fall through
// to a fresh connection rather than collide with an arbitrary earlier one.
func findGoogleConnectionByEmail(conns []Connection, email string) *Connection {
	email = strings.TrimSpace(email)
	if email == "" {
		return nil
	}
	for i := range conns {
		c := &conns[i]
		if c.Type == ConnectionTypeGoogle && strings.EqualFold(c.GoogleEmail(), email) {
			return c
		}
	}
	return nil
}

// ListConnections returns the workspace's connections.
func (s *ConnectionService) ListConnections(ctx context.Context, customerSlug string) ([]Connection, error) {
	if s.Connections == nil {
		return nil, ErrConnectionsNotConfigured
	}
	return s.Connections.ListConnections(ctx, customerSlug)
}

// DeleteConnection removes a connection unless a pipeline credential still
// references it. The guard is best-effort by construction (a pipeline saved
// in the same instant can slip past), but it turns the common case — "delete
// while three pipelines use it" — into a named error instead of three broken
// pipelines.
func (s *ConnectionService) DeleteConnection(ctx context.Context, customerSlug, id string) error {
	if s.Connections == nil {
		return ErrConnectionsNotConfigured
	}
	if s.PipelineCredentials != nil {
		creds, err := s.PipelineCredentials.ListPipelineCredentialsByCustomer(ctx, customerSlug)
		if err != nil {
			return fmt.Errorf("list pipeline credentials: %w", err)
		}
		for _, raw := range creds {
			if credentialReferencesConnection(raw, id) {
				return ErrConnectionInUse
			}
		}
	}
	return s.Connections.DeleteConnection(ctx, customerSlug, id)
}

// GoogleEmail reads the granting email out of a google connection's
// credentials, for display. Empty when absent or not a google connection.
func (c *Connection) GoogleEmail() string {
	if c.Type != ConnectionTypeGoogle || isEmptyJSON(c.Credentials) {
		return ""
	}
	var gc googleConnectionCredentials
	if err := json.Unmarshal(c.Credentials, &gc); err != nil {
		return ""
	}
	return gc.Email
}

// googleCredentials parses a google connection's credential JSON.
func (c *Connection) googleCredentials() (*googleConnectionCredentials, error) {
	if c.Type != ConnectionTypeGoogle {
		return nil, ErrUnsupportedConnectionType
	}
	var gc googleConnectionCredentials
	if err := json.Unmarshal(c.Credentials, &gc); err != nil {
		return nil, fmt.Errorf("parse google connection credentials: %w", err)
	}
	if gc.RefreshToken == "" {
		return nil, errors.New("google connection has no refresh token")
	}
	return &gc, nil
}

// credentialReferencesConnection reports whether a stored pipeline credential
// JSON references the given connection id. Type-agnostic on purpose: it
// checks the oauth.connection_id shape wherever it appears, which is what
// keeps the delete guard correct for every Google-backed source type — the
// google_sheets envelope and the duckdb/gdrive one both name the member
// "oauth", so neither needs its own branch here.
func credentialReferencesConnection(raw json.RawMessage, connectionID string) bool {
	if isEmptyJSON(raw) {
		return false
	}
	var env struct {
		OAuth *googleOAuthCredential `json:"oauth"`
	}
	if err := json.Unmarshal(raw, &env); err != nil || env.OAuth == nil {
		return false
	}
	return env.OAuth.ConnectionID == connectionID
}

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
// embedding a refresh token, and the box query engine receives short-lived
// access tokens minted from it (query-time federation). Deleting the
// connection kills every consumer, which is what a customer expects
// "disconnect Google" to mean.
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
// (googleSheetsOAuth) minus the pipeline-specific wrapper.
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

	// GetConnection returns the connection, or ErrConnectionNotFound when id
	// does not exist or belongs to another workspace.
	GetConnection(ctx context.Context, customerSlug, id string) (*Connection, error)

	// ListConnections returns the workspace's connections, newest first.
	ListConnections(ctx context.Context, customerSlug string) ([]Connection, error)

	// DeleteConnection removes the connection. Deleting one that does not
	// exist is not an error.
	DeleteConnection(ctx context.Context, customerSlug, id string) error
}

// GoogleTokenMinter mints a short-lived Google access token from a stored
// refresh token under the customer's own OAuth client. Port interface —
// oauthgoogle provides the implementation, wired in cmd (workspace may not
// import oauthgoogle).
type GoogleTokenMinter interface {
	AccessToken(ctx context.Context, refreshToken, clientID, clientSecret string) (token string, ttl time.Duration, err error)
}

// Connection error sentinels. Beside the others in errors.go in spirit; kept
// here so the whole Connection contract reads in one file.
var (
	// ErrConnectionNotFound means the id does not address a connection in the
	// caller's workspace.
	ErrConnectionNotFound = errors.New("connection not found")

	// ErrConnectionAlreadyExists rejects a duplicate (type, name) per workspace.
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
	grant, err := s.GoogleOAuth.ConsumeGoogleOAuthGrant(ctx, grantID, customerSlug)
	if err != nil {
		if errors.Is(err, ErrOAuthGrantNotFound) {
			return nil, ErrOAuthGrantNotFound
		}
		return nil, fmt.Errorf("consume oauth grant: %w", err)
	}

	creds, err := json.Marshal(googleConnectionCredentials{
		RefreshToken: grant.RefreshToken,
		Email:        grant.Email,
		ClientID:     grant.ClientID,
	})
	if err != nil {
		return nil, fmt.Errorf("build connection credentials: %w", err)
	}

	name = strings.TrimSpace(name)
	if name == "" {
		name = grant.Email
	}
	if name == "" {
		name = "Google"
	}

	c := &Connection{
		ID:           newConnectionID(),
		CustomerSlug: customerSlug,
		Type:         ConnectionTypeGoogle,
		Name:         name,
		Status:       "active",
		Config:       json.RawMessage(`{}`),
		Credentials:  creds,
	}
	if err := s.Connections.CreateConnection(ctx, c); err != nil {
		return nil, err
	}
	return c, nil
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
// checks the oauth.connection_id shape wherever it appears.
func credentialReferencesConnection(raw json.RawMessage, connectionID string) bool {
	if isEmptyJSON(raw) {
		return false
	}
	var creds googleSheetsCreds
	if err := json.Unmarshal(raw, &creds); err != nil || creds.OAuth == nil {
		return false
	}
	return creds.OAuth.ConnectionID == connectionID
}

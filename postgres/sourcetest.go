package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/fairtier/workspace-api/workspace"
)

// Source tests are short-lived rows carrying a credential nobody else owns —
// the same shape as google_oauth_grants, and swept the same way. Nothing here
// hands a credential back to a user-facing caller: only ClaimPendingSourceTests
// decrypts, and it is reachable from the internal mux alone.

func (r *Repository) CreateSourceTest(ctx context.Context, t *workspace.SourceTest) error {
	encCreds, err := r.encryptCredentials(t.SourceCredentials)
	if err != nil {
		return fmt.Errorf("postgres: encrypt source test credentials: %w", err)
	}
	config := t.SourceConfig
	if len(config) == 0 {
		config = json.RawMessage(`{}`)
	}
	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO source_tests (id, customer_slug, source_type, source_config, source_credentials, status, created_at, expires_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		t.ID, t.CustomerSlug, t.SourceType, []byte(config), encCreds, t.Status, t.CreatedAt, t.ExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("postgres: create source test: %w", err)
	}
	return nil
}

func (r *Repository) GetSourceTest(ctx context.Context, id, customerSlug string) (*workspace.SourceTest, error) {
	var (
		t           workspace.SourceTest
		details     []byte
		completedAt sql.NullTime
	)
	err := r.DB.QueryRowContext(ctx,
		`SELECT id, customer_slug, source_type, status, message, details, created_at, completed_at, expires_at
		   FROM source_tests
		  WHERE id = $1 AND customer_slug = $2 AND expires_at > now()`,
		id, customerSlug,
	).Scan(&t.ID, &t.CustomerSlug, &t.SourceType, &t.Status, &t.Message, &details, &t.CreatedAt, &completedAt, &t.ExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, workspace.ErrSourceTestNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: get source test: %w", err)
	}
	if completedAt.Valid {
		ts := completedAt.Time
		t.CompletedAt = &ts
	}
	if err := json.Unmarshal(details, &t.Details); err != nil {
		return nil, fmt.Errorf("postgres: decode source test details: %w", err)
	}
	return &t, nil
}

// ClaimPendingSourceTests flips pending rows to running and returns them in one
// statement. Two worker polls overlapping (a slow probe, a restarted pod) would
// otherwise both see the same pending row and probe it twice — with a database
// password, twice is how an account gets locked out.
func (r *Repository) ClaimPendingSourceTests(ctx context.Context, customerSlug string) ([]workspace.SourceTest, error) {
	rows, err := r.DB.QueryContext(ctx,
		`UPDATE source_tests
		    SET status = $1
		  WHERE id IN (
		        SELECT id FROM source_tests
		         WHERE customer_slug = $2 AND status = $3 AND expires_at > now()
		         ORDER BY created_at
		         FOR UPDATE SKIP LOCKED
		  )
		 RETURNING id, customer_slug, source_type, source_config, source_credentials, created_at, expires_at`,
		workspace.SourceTestRunning, customerSlug, workspace.SourceTestPending,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: claim source tests: %w", err)
	}
	defer rows.Close()

	var out []workspace.SourceTest
	for rows.Next() {
		var (
			t          workspace.SourceTest
			config     []byte
			storedCred string
		)
		if err := rows.Scan(&t.ID, &t.CustomerSlug, &t.SourceType, &config, &storedCred, &t.CreatedAt, &t.ExpiresAt); err != nil {
			return nil, fmt.Errorf("postgres: scan source test: %w", err)
		}
		creds, err := r.decryptCredentials(storedCred)
		if err != nil {
			return nil, fmt.Errorf("postgres: decrypt source test credentials: %w", err)
		}
		t.SourceConfig = json.RawMessage(config)
		t.SourceCredentials = creds
		t.Status = workspace.SourceTestRunning
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: claim source tests: %w", err)
	}
	return out, nil
}

// CompleteSourceTest records the outcome and drops the credentials with it:
// once the probe has run they have no further use, and a completed row is
// readable by the Console.
func (r *Repository) CompleteSourceTest(ctx context.Context, id, customerSlug, status, message string, details []string) error {
	if details == nil {
		details = []string{}
	}
	encoded, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("postgres: encode source test details: %w", err)
	}
	res, err := r.DB.ExecContext(ctx,
		`UPDATE source_tests
		    SET status = $1, message = $2, details = $3, completed_at = $4, source_credentials = ''
		  WHERE id = $5 AND customer_slug = $6`,
		status, message, encoded, time.Now(), id, customerSlug,
	)
	if err != nil {
		return fmt.Errorf("postgres: complete source test: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return workspace.ErrSourceTestNotFound
	}
	return nil
}

func (r *Repository) DeleteExpiredSourceTests(ctx context.Context) (int64, error) {
	res, err := r.DB.ExecContext(ctx, `DELETE FROM source_tests WHERE expires_at <= now()`)
	if err != nil {
		return 0, fmt.Errorf("postgres: sweep expired source tests: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

package postgres

import (
	"database/sql"
	"embed"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	pgxv5 "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/fairtier/workspace-api/crypto"
)

// The migration set covers only the workspace plane's own tables, and expects
// a database dedicated to it. Migrate writes golang-migrate's schema_migrations
// bookkeeping table, so pointing it at a database that carries an unrelated
// migration history will not work.
//
//go:embed migrations/*.sql
var migrations embed.FS

func Migrate(db *sql.DB) error {
	source, err := iofs.New(migrations, "migrations")
	if err != nil {
		return fmt.Errorf("postgres: open migration source: %w", err)
	}

	driver, err := pgxv5.WithInstance(db, &pgxv5.Config{})
	if err != nil {
		return fmt.Errorf("postgres: open migration driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", source, "pgx5", driver)
	if err != nil {
		return fmt.Errorf("postgres: create migrator: %w", err)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("postgres: run migrations: %w", err)
	}

	return nil
}

// MigrateEncryptCredentials encrypts any plaintext source_credentials rows.
// Idempotent: only processes rows without the "enc:" prefix.
func MigrateEncryptCredentials(db *sql.DB, enc crypto.Encryptor) error {
	if enc == nil {
		return nil
	}

	rows, err := db.Query(`SELECT id, source_credentials FROM pipelines WHERE source_credentials NOT LIKE 'enc:%'`)
	if err != nil {
		return fmt.Errorf("postgres: query plaintext credentials: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		var creds string
		if err := rows.Scan(&id, &creds); err != nil {
			return fmt.Errorf("postgres: scan credential row: %w", err)
		}

		encrypted, err := enc.Encrypt([]byte(creds))
		if err != nil {
			return fmt.Errorf("postgres: encrypt credentials for pipeline %s: %w", id, err)
		}

		if _, err := db.Exec(`UPDATE pipelines SET source_credentials = $1 WHERE id = $2`, encrypted, id); err != nil {
			return fmt.Errorf("postgres: update encrypted credentials for pipeline %s: %w", id, err)
		}
	}

	return rows.Err()
}

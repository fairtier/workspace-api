// Package postgres implements the workspace plane's store ports
// (workspace/ports.go and the per-service store interfaces) against
// PostgreSQL. On a box this is the box-local Postgres; on the central
// deployment (SaaS transition) it runs against central Postgres over the
// same schema — the control plane constructs its own repository type for
// its own tables.
package postgres

import (
	"database/sql"

	"github.com/fairtier/workspace-api/crypto"
)

// Repository implements the workspace plane's repository interfaces backed
// by PostgreSQL.
type Repository struct {
	DB        *sql.DB
	Encryptor crypto.Encryptor // nil = no encryption (backward compat, local dev)
}

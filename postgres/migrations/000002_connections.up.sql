-- Workspace-level Connections: one authorization to an external system (a
-- Google account, later a database), consumed by more than one feature —
-- DLT pipelines reference it instead of embedding a refresh token, and the
-- box query engine receives short-lived tokens minted from it
-- (query-time federation). See docs/plans/query-time-federation.md in the
-- platform monorepo.

CREATE TABLE connections (
    id            TEXT PRIMARY KEY,
    customer_slug TEXT NOT NULL,
    -- Connection type key: 'google' today (Sheets OAuth grant).
    type          TEXT NOT NULL,
    -- Display name, unique per (workspace, type) so a picker row is unambiguous.
    name          TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'active',
    -- Non-secret, type-specific settings.
    config        JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Encrypted at rest (crypto.Encryptor envelope), hence TEXT not JSONB.
    -- For 'google': {"refresh_token","email","client_id"} — the client pair
    -- itself stays in customer_oauth_clients.
    credentials   TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (customer_slug, type, name)
);

CREATE INDEX connections_customer_idx ON connections (customer_slug);

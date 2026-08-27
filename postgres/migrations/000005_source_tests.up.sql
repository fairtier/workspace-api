-- "Test connection": a queued probe of a source, run by the box's own worker.
--
-- The probe cannot run here. It has to run where extraction runs — the
-- dlt-worker's subprocess isolation, with its drivers, its baked DuckDB
-- extensions and the box's network path — so a test is a row the worker claims
-- on its next poll rather than a call. Until this table existed, the discovery
-- mechanism for a wrong password was the first scheduled run, hours later.
--
-- Every row is short-lived by construction (expires_at, swept like
-- google_oauth_grants). It carries the credentials being tested, which is why
-- the column is encrypted and why the sweep is not optional: this is the one
-- place a credential lives that no pipeline owns.
CREATE TABLE source_tests (
    id            TEXT PRIMARY KEY,
    customer_slug TEXT NOT NULL,
    source_type   TEXT NOT NULL,
    -- Non-secret, as in pipelines.
    source_config      JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Encrypted at rest (crypto.Encryptor envelope), hence TEXT not JSONB.
    source_credentials TEXT  NOT NULL DEFAULT '',
    -- 'pending' | 'running' | 'success' | 'failed'.
    status        TEXT NOT NULL DEFAULT 'pending',
    message       TEXT NOT NULL DEFAULT '',
    -- Per-table/per-step lines, in probe order.
    details       JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ,
    expires_at    TIMESTAMPTZ NOT NULL
);

-- The worker's claim query: pending rows of one customer that have not expired.
CREATE INDEX source_tests_pending_idx ON source_tests (customer_slug, status, expires_at);

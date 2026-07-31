-- The workspace schema: everything the workspace plane stores in its own
-- Postgres database.
--
-- Rows are keyed by customer_slug even though a box serves exactly one
-- workspace. The column is the tenant boundary in every query, so a shared
-- deployment and a single-tenant box run the identical code path.

-- ---------------------------------------------------------------------------
-- Pipelines: declarative dlt (data load tool) ingestion definitions.
-- ---------------------------------------------------------------------------

-- source_credentials is encrypted at rest by the application layer
-- (crypto.Encryptor writes an "enc:" prefix); it is TEXT rather than JSONB so
-- the ciphertext is stored verbatim instead of double-marshalled.
--
-- credentials_external marks a pipeline whose credentials file was edited
-- out-of-band in the box's git repository. The renderer cannot decrypt such a
-- file (the age private key lives only on the box), so it stops re-rendering
-- it and lets the repository's copy stand until the next edit through the API
-- reclaims ownership.
CREATE TABLE pipelines (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_slug        TEXT NOT NULL,
    name                 TEXT NOT NULL,
    source_type          TEXT NOT NULL,
    source_config        JSONB NOT NULL DEFAULT '{}',
    source_credentials   TEXT NOT NULL DEFAULT '{}',
    dataset_name         TEXT NOT NULL,
    schedule             TEXT NOT NULL DEFAULT '',
    write_disposition    TEXT NOT NULL DEFAULT 'append',
    merge_strategy       TEXT NOT NULL DEFAULT '',
    credentials_external BOOLEAN NOT NULL DEFAULT FALSE,
    enabled              BOOLEAN NOT NULL DEFAULT TRUE,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (customer_slug, name)
);

CREATE INDEX idx_pipelines_customer_slug ON pipelines (customer_slug);

CREATE TABLE pipeline_runs (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id   UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    status        TEXT NOT NULL,
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    rows_loaded   BIGINT NOT NULL DEFAULT 0,
    error_message TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- History view: newest-first per pipeline.
CREATE INDEX idx_pipeline_runs_pipeline_id
    ON pipeline_runs (pipeline_id, created_at DESC);

-- Claim hot path: the worker polls for the oldest pending run, so keep the
-- partial index small and ordered the way the poll reads it.
CREATE INDEX idx_pipeline_runs_pending
    ON pipeline_runs (pipeline_id, created_at ASC)
    WHERE status = 'pending';

-- ---------------------------------------------------------------------------
-- Transformations: git-backed dbt projects run against the warehouse.
-- ---------------------------------------------------------------------------

-- An empty repo_url means the box-hosted git repository (its URL and token
-- live on the box only). git_credentials is encrypted at rest exactly like
-- pipelines.source_credentials.
CREATE TABLE transformations (
    id                        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_slug             TEXT NOT NULL,
    name                      TEXT NOT NULL,
    repo_url                  TEXT NOT NULL DEFAULT '',
    repo_ref                  TEXT NOT NULL DEFAULT 'main',
    git_credentials           TEXT NOT NULL DEFAULT '{}',
    schedule                  TEXT NOT NULL DEFAULT '',
    -- Run after this pipeline succeeds; the FK keeps it pointing at a real
    -- pipeline and clears it when that pipeline is deleted.
    trigger_after_pipeline_id UUID REFERENCES pipelines(id) ON DELETE SET NULL,
    dbt_selector              TEXT NOT NULL DEFAULT '',
    enabled                   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (customer_slug, name)
);

CREATE INDEX idx_transformations_customer_slug
    ON transformations (customer_slug);

-- Every run is pinned to the commit SHA the ref resolved to, so each output
-- table's state is attributable to an exact (data, code) pair.
CREATE TABLE transformation_runs (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transformation_id UUID NOT NULL REFERENCES transformations(id) ON DELETE CASCADE,
    status            TEXT NOT NULL,
    started_at        TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,
    commit_sha        TEXT NOT NULL DEFAULT '',
    models_total      INTEGER NOT NULL DEFAULT 0,
    models_failed     INTEGER NOT NULL DEFAULT 0,
    tests_total       INTEGER NOT NULL DEFAULT 0,
    tests_failed      INTEGER NOT NULL DEFAULT 0,
    -- Per-node results: [{"name", "resource_type", "status",
    -- "execution_time", "message"}, ...]
    model_results     JSONB NOT NULL DEFAULT '[]',
    error_message     TEXT NOT NULL DEFAULT '',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_transformation_runs_transformation_id
    ON transformation_runs (transformation_id, created_at DESC);
CREATE INDEX idx_transformation_runs_pending
    ON transformation_runs (transformation_id) WHERE status = 'pending';

-- ---------------------------------------------------------------------------
-- Render bookkeeping for the git-backed definition files.
-- ---------------------------------------------------------------------------
-- Pipelines and transformations are also rendered to YAML in the box's git
-- repositories, which are the source of truth. These tables are a pure cache
-- of what the renderer last wrote: losing a row costs one redundant commit or
-- one missed drift signal, never correctness.

-- age ciphertext is non-deterministic, so the renderer cannot compare file
-- contents to decide "unchanged". It compares a fingerprint (an HMAC over
-- recipient + plaintext) and the git blob sha instead.
CREATE TABLE pipeline_credential_renders (
    pipeline_id UUID PRIMARY KEY REFERENCES pipelines(id) ON DELETE CASCADE,
    fingerprint TEXT NOT NULL,
    blob_sha    TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Drift detection: a tree blob sha differing from the recorded one is a commit
-- the renderer did not make — an out-of-band edit, which is adopted or
-- overwritten with a notification instead of silently clobbered.
--
-- refused_blob_sha records a blob the adopt pass could not take (unparseable,
-- foreign id, or a source-type change); it suppresses repeat notifications for
-- the same refused commit, while any new commit is evaluated afresh.
CREATE TABLE pipeline_definition_renders (
    pipeline_id      UUID PRIMARY KEY REFERENCES pipelines(id) ON DELETE CASCADE,
    path             TEXT NOT NULL,
    blob_sha         TEXT NOT NULL,
    refused_blob_sha TEXT NOT NULL DEFAULT '',
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE transformation_definition_renders (
    transformation_id UUID PRIMARY KEY REFERENCES transformations(id) ON DELETE CASCADE,
    path              TEXT NOT NULL,
    blob_sha          TEXT NOT NULL,
    refused_blob_sha  TEXT NOT NULL DEFAULT '',
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Notifications: the in-app feed behind the console's bell.
-- ---------------------------------------------------------------------------

CREATE TABLE notifications (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_slug TEXT NOT NULL,
    -- 'pipeline_run' | 'provisioning' | 'snapshot' | 'info'
    type          TEXT NOT NULL,
    title         TEXT NOT NULL,
    body          TEXT NOT NULL DEFAULT '',
    -- Optional console route name to deep-link to (e.g. 'pipelines').
    link          TEXT NOT NULL DEFAULT '',
    read          BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- List view: newest-first per tenant.
CREATE INDEX idx_notifications_customer_created
    ON notifications (customer_slug, created_at DESC);

-- Unread-count hot path: keep the partial index small.
CREATE INDEX idx_notifications_unread
    ON notifications (customer_slug)
    WHERE NOT read;

-- ---------------------------------------------------------------------------
-- Google OAuth grants: short-lived hand-off for "Sign in with Google".
-- ---------------------------------------------------------------------------

-- The OAuth callback exchanges the auth code for a refresh token and stores it
-- here keyed by a random grant_id. The browser only ever holds the grant_id and
-- redeems it when the pipeline is created, at which point the row is deleted —
-- one-time use. refresh_token is encrypted at rest by the application, and rows
-- expire and are swept periodically.
CREATE TABLE google_oauth_grants (
    grant_id      UUID PRIMARY KEY,
    customer_slug TEXT NOT NULL,
    user_sub      TEXT NOT NULL,
    refresh_token TEXT NOT NULL,
    email         TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ NOT NULL
);

CREATE INDEX google_oauth_grants_expires_at_idx
    ON google_oauth_grants (expires_at);

-- ---------------------------------------------------------------------------
-- Demo seeds: bookkeeping for the loadable starter project.
-- ---------------------------------------------------------------------------

-- One row per workspace that has the demo loaded; teardown deletes exactly what
-- is recorded here. Deliberately not FK-referenced to pipelines: teardown
-- deletes the pipelines first and must keep this row until every artifact is
-- pruned, so a failed removal can be retried.
CREATE TABLE demo_seeds (
    customer_slug     TEXT PRIMARY KEY,
    tier              TEXT NOT NULL,
    trips_pipeline_id TEXT NOT NULL,
    zones_pipeline_id TEXT NOT NULL,
    transformation_id TEXT NOT NULL,
    -- [{"repo","path"}, ...] recorded so teardown prunes only seeded files.
    repo_files        JSONB NOT NULL DEFAULT '[]',
    -- 'loading' | 'ready'. Loading runs in the background: the row is written
    -- up front and flipped when the seed completes.
    status            TEXT NOT NULL DEFAULT 'ready',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

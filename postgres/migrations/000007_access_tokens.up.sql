-- Workspace access tokens: long-lived, revocable bearer credentials a person
-- mints for a non-interactive client of their own (an MCP client today).
--
-- Only the SHA-256 of the secret is stored. The secret is 32 random bytes, so
-- a fast unsalted hash is the right tool: there is nothing to brute-force, and
-- the hash doubles as the unique lookup key, so verification is one indexed
-- read rather than a scan with a slow KDF per row.
--
-- user_sub is the Casdoor subject of the person who minted the token; the
-- token acts as them, within scopes. Rows are never deleted by the API:
-- revoked_at records a revocation, so the list can show what was revoked and
-- when.
CREATE TABLE access_tokens (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_slug TEXT NOT NULL,
    user_sub      TEXT NOT NULL,
    name          TEXT NOT NULL,
    -- First characters of the secret, for display only ("ftw_1a2b3c4d").
    token_prefix  TEXT NOT NULL,
    token_hash    BYTEA NOT NULL UNIQUE,
    scopes        TEXT[] NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ NOT NULL,
    last_used_at  TIMESTAMPTZ,
    revoked_at    TIMESTAMPTZ
);

-- The list view: one person's tokens, newest first.
CREATE INDEX access_tokens_owner_idx ON access_tokens (customer_slug, user_sub, created_at DESC);

-- Catch-up for the 000001 squash edit that shipped with 0.10.0 ("the Google
-- OAuth app is the customer's, not ours", 624c271). Editing an already-applied
-- migration is invisible to golang-migrate: a box whose database ran the
-- ORIGINAL 000001 sits at version 1 and never re-applies it, so the table and
-- column appended there simply do not exist on any box provisioned before the
-- edit — GetOAuthClient then 500s with "relation customer_oauth_clients does
-- not exist". This file replays exactly that delta, idempotently: on a box
-- that ran the edited squash (or central, which has its own lineage) every
-- statement is a no-op.
--
-- The rule this codifies: NEVER edit the 000001 squash again — additions go in
-- a new numbered file, which is the only thing existing databases ever see.

-- Verbatim from 000001 (see the comment block there for why the app is the
-- customer's own).
CREATE TABLE IF NOT EXISTS customer_oauth_clients (
    customer_slug TEXT NOT NULL,
    provider      TEXT NOT NULL,
    client_id     TEXT NOT NULL,
    client_secret TEXT NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Casdoor sub of whoever last saved it, for the Console's "changed by" line.
    updated_by    TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (customer_slug, provider)
);

-- The same edit recorded WHICH client minted each grant (a refresh token is
-- only refreshable by the client it was issued to).
ALTER TABLE google_oauth_grants
    ADD COLUMN IF NOT EXISTS client_id TEXT NOT NULL DEFAULT '';

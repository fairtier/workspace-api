-- Record what a Google consent actually granted.
--
-- The consent screen lets the user untick a scope, so "what we asked for" is
-- not "what we got" — and a duckdb/gdrive pipeline reading a Drive file needs
-- drive.file specifically. Without this column the only place that difference
-- shows up is a 403 inside a scheduled run on the box, hours later.
--
-- Space-separated, which is the OAuth wire encoding Google itself returns.
-- Empty means "not recorded" (every grant minted before this column existed),
-- and the domain reads that as unknown rather than as "nothing granted": see
-- workspace.Connection.HasGoogleScope.
ALTER TABLE google_oauth_grants
    ADD COLUMN IF NOT EXISTS scopes TEXT NOT NULL DEFAULT '';

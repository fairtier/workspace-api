-- Workflows as files: the workflow mirror's bookkeeping for one rendered
-- workflows/<slug>.yaml in the box's transformations repo — the path and the
-- Gitea blob sha of its last write (or of the last repo commit it adopted).
-- Same shape and cache semantics as transformation_definition_renders: a lost
-- row costs one missed drift signal, never correctness. refused_blob_sha is
-- the last repo blob the adopt pass refused, so a refusal is reported once
-- per commit.
CREATE TABLE workflow_definition_renders (
    workflow_id      UUID PRIMARY KEY REFERENCES workflows(id) ON DELETE CASCADE,
    path             TEXT NOT NULL,
    blob_sha         TEXT NOT NULL,
    refused_blob_sha TEXT NOT NULL DEFAULT '',
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

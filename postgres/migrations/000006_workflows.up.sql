-- Workflows: a named, ordered, linear list of steps, each running an existing
-- pipeline or transformation. One optional cron schedule per workflow; the
-- box's dlt-worker runs the steps in order and stops at the first failure.
--
-- Unlike pipelines and transformations, workflows are not (yet) rendered to
-- files in the box's git repositories: these rows are the only copy.
CREATE TABLE workflows (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_slug TEXT NOT NULL,
    name          TEXT NOT NULL,
    description   TEXT NOT NULL DEFAULT '',
    -- Cron expression, or '' for manual runs only.
    schedule      TEXT NOT NULL DEFAULT '',
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (customer_slug, name)
);

CREATE INDEX idx_workflows_customer_slug ON workflows (customer_slug);

-- A step references exactly one pipeline or transformation, per kind.
--
-- Deleting the referenced pipeline/transformation removes the step (CASCADE)
-- rather than being refused: deletions also arrive from paths nobody is
-- watching — an out-of-band file removal adopted from the repository, the
-- demo teardown — and a RESTRICT there would wedge them. It is the same
-- choice transformations.trigger_after_pipeline_id makes (SET NULL). Run
-- history is unaffected: workflow_step_runs keeps its own snapshot.
CREATE TABLE workflow_steps (
    workflow_id       UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    position          INTEGER NOT NULL CHECK (position >= 0),
    kind              TEXT NOT NULL CHECK (kind IN ('pipeline', 'transformation')),
    pipeline_id       UUID REFERENCES pipelines(id) ON DELETE CASCADE,
    transformation_id UUID REFERENCES transformations(id) ON DELETE CASCADE,
    PRIMARY KEY (workflow_id, position),
    CONSTRAINT workflow_steps_one_ref CHECK (
        (kind = 'pipeline' AND pipeline_id IS NOT NULL AND transformation_id IS NULL) OR
        (kind = 'transformation' AND transformation_id IS NOT NULL AND pipeline_id IS NULL)
    ),
    -- A pipeline/transformation appears at most once per workflow (NULLs are
    -- distinct, so each constraint only bites on its own kind).
    UNIQUE (workflow_id, pipeline_id),
    UNIQUE (workflow_id, transformation_id)
);

-- The CASCADE paths look steps up by their reference.
CREATE INDEX idx_workflow_steps_pipeline_id ON workflow_steps (pipeline_id) WHERE pipeline_id IS NOT NULL;
CREATE INDEX idx_workflow_steps_transformation_id ON workflow_steps (transformation_id) WHERE transformation_id IS NOT NULL;

CREATE TABLE workflow_runs (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id   UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    -- 'pending' | 'running' | 'success' | 'failed' (the same vocabulary as
    -- pipeline_runs and transformation_runs).
    status        TEXT NOT NULL CHECK (status IN ('pending', 'running', 'success', 'failed')),
    trigger       TEXT NOT NULL DEFAULT 'schedule' CHECK (trigger IN ('manual', 'schedule')),
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    error_message TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- History view and pagination: newest-first per workflow.
CREATE INDEX idx_workflow_runs_workflow_id
    ON workflow_runs (workflow_id, created_at DESC, id DESC);
CREATE INDEX idx_workflow_runs_pending
    ON workflow_runs (workflow_id, created_at ASC) WHERE status = 'pending';

-- One row per step of a run, a snapshot of what the step was when it ran:
-- kind/ref_id/name are copied, not referenced, so history outlives edits and
-- deletions. pipeline_run_id / transformation_run_id are deliberately not
-- foreign keys: the worker writes those run rows itself (local-first) or
-- reports them separately, in an order this table cannot rely on.
CREATE TABLE workflow_step_runs (
    workflow_run_id       UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    position              INTEGER NOT NULL CHECK (position >= 0),
    kind                  TEXT NOT NULL CHECK (kind IN ('pipeline', 'transformation')),
    ref_id                UUID NOT NULL,
    name                  TEXT NOT NULL DEFAULT '',
    status                TEXT NOT NULL CHECK (status IN ('pending', 'running', 'success', 'failed', 'skipped')),
    pipeline_run_id       UUID,
    transformation_run_id UUID,
    started_at            TIMESTAMPTZ,
    completed_at          TIMESTAMPTZ,
    error_message         TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (workflow_run_id, position)
);

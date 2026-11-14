# Workflows

A **workflow** is the simplest orchestration that is still useful: a named,
ordered, **linear** list of steps, each running an existing pipeline or
transformation, with one optional cron schedule. Steps run one after the
other; the first failure stops the run and every later step is recorded as
skipped. There is deliberately no DAG, no parallelism and no parameters.

The older chaining mechanism — `transformations.trigger_after_pipeline_id` —
is unchanged and independent of workflows.

Service: `workflow.v1.WorkflowService` ([`proto/workflow.proto`](../proto/workflow.proto)).

## Model

| Table | Holds |
| --- | --- |
| `workflows` | `customer_slug`, `name` (unique per workspace), `description`, `schedule` (`''` = manual only), `enabled`. |
| `workflow_steps` | `(workflow_id, position)`, `kind` (`pipeline` \| `transformation`), and exactly one of `pipeline_id` / `transformation_id` (FKs). A target appears at most once per workflow. |
| `workflow_runs` | `status` (`pending` \| `running` \| `success` \| `failed`), `trigger` (`manual` \| `schedule`), timestamps, `error_message`. |
| `workflow_step_runs` | `(workflow_run_id, position)`, a snapshot of `kind` / `ref_id` / `name`, `status` (`pending` \| `running` \| `success` \| `failed` \| `skipped`), the `pipeline_run_id` or `transformation_run_id` the step produced, timestamps, `error_message`. |

Run statuses use the same words as pipeline and transformation runs
(`success`, not `succeeded`), so one status vocabulary serves every run in the
Console.

Rules enforced on create/update: name required; 1–20 steps; each step's kind
is `pipeline` or `transformation` and its `ref_id` a UUID of that kind that
exists **in the same workspace** (a missing and a foreign target are
indistinguishable); no target twice; the schedule is empty, an `@macro`
(`@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly`/`@annually`,
`@midnight`) or a five-field cron expression of values, ranges, lists, steps
and month/day names. That is a subset of what the worker's `croniter` accepts
— pipelines and transformations get no server-side cron check at all.

**Deleting a referenced pipeline or transformation removes the step** (FK
`ON DELETE CASCADE`) rather than being refused. Deletions also arrive from
unattended paths — a definition file removed from the repository and adopted,
the demo teardown — where a refusal would wedge the sweep. It matches what
`trigger_after_pipeline_id` does (`SET NULL`). Run history is unaffected:
step runs keep their own snapshot. A workflow left with no steps is not served
to the worker, and `TriggerWorkflow` refuses it (`FailedPrecondition`).

Authorization mirrors `TransformationService` method for method: user RPCs
need an authenticated caller and act only on their workspace (another
workspace's workflow is `NotFound`); worker RPCs are served on the internal
port only, with a tenant-bound service token.

## Worker contract

Every poll tick, alongside `GetPipelineConfigs` / `GetTransformationConfigs`:

1. `GetWorkflowConfigs{customer_slug}`. Each item carries the steps (position,
   kind, `ref_id`, current name, `ref_enabled`), `schedule`, `enabled`,
   `trigger_now` + `pending_run_id`, and `last_run_at` / `last_run_status`.
2. **Is it due?** `trigger_now` → due, even when the workflow is disabled (the
   pipeline rule: disabling stops the schedule, not a run a person asked
   for). Otherwise, not `enabled` or empty `schedule` → not due. Otherwise
   evaluate cron with `last_run_at` as the baseline — it is the start of the
   last run that left `pending`, whatever its outcome, so a failing workflow
   waits for its next slot instead of retrying every tick. Never run → due.
3. **Pick the run id**: `pending_run_id` when set, else a fresh UUID. Use the
   same id for every report of this run.
4. **Report start**: `ReportWorkflowRun{run_id, workflow_id, status:
   "running", trigger: "manual"|"schedule", started_at, step_runs: [every
   step as "pending"]}`. Reporting the pending id with any status other than
   `pending` is what clears `trigger_now` — the server needs no other
   acknowledgement.
5. **For each step in position order**:
   - Look the target up by `ref_id` in the configs the worker already has (or
     its definition checkout). If it is missing or `ref_enabled` is false,
     the step fails without running ("pipeline is disabled" / "not found").
   - Report the step `running` (optional, but it makes the Console live).
   - Run it exactly as a standalone run of that pipeline/transformation —
     same isolation, same local-first recording, same `ReportPipelineRun` /
     `ReportTransformationRun`. The step's run id is the run id the worker
     records that run under; send it as `pipeline_run_id` /
     `transformation_run_id`. These are plain columns, not foreign keys, so
     the order in which the run row and the step report land does not matter.
   - Report the workflow run again with the step's terminal status.
6. **Finish**: on the first failed step, mark every later step `skipped` and
   report the run `failed` with `completed_at` and an `error_message`;
   otherwise report it `success`.

`ReportWorkflowRun` is an idempotent upsert: the run by `run_id`, step runs by
`(run_id, position)`. Repeating a report changes nothing; a later report
overwrites status, `completed_at` and `error_message`, keeps the stored
`started_at` / `trigger` / step run ids when the report leaves them empty, and
a run id that already belongs to another workflow is refused (`NotFound`).
Send the full step list every time; positions not sent are left as they were.
The in-app "workflow failed" notification fires once, on the transition into
`failed`.

## PoC shortcuts and known gaps

- **Not mirrored to git.** Pipelines and transformations live as files in the
  box's repositories and the database is a cache over them; workflows are
  rows only. A database rebuilt from the repositories loses them, and they do
  not travel with an exported repository.
- **Linear only.** No branching, fan-out, conditional steps, retries per step,
  or parameters.
- **Double runs.** A pipeline or transformation used as a step keeps its own
  `schedule` (and a transformation its `trigger_after_pipeline_id`), so it
  can run both on its own and as part of the workflow. The intended setup is
  to clear the step targets' own schedules (leave them enabled — a disabled
  target fails its step). The worker should additionally not start a step
  whose target is already running in the same tick; the server does not
  arbitrate.
- **No stuck-run sweep.** A worker that dies mid-workflow leaves the run
  `running` until it is reported again. The worker's start-up stale-run
  finalisation should cover `workflow_runs` and `workflow_step_runs` the way
  it covers the other run tables; the central pipeline sweep does not.
- **No metrics** beyond the RPC-level ones; workflow runs are not counted in
  the `workspace.` metric family yet.
- The step **name** in the definition is resolved live from the target; the
  step run keeps the name the worker reported.

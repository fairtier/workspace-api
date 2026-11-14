# Workflows

A **workflow** is the simplest orchestration that is still useful: a named,
ordered, **linear** list of steps, each running an existing pipeline or
transformation, with one optional cron schedule. Steps run one after the
other; the first failure stops the run and every later step is recorded as
skipped. There is deliberately no DAG, no parallelism and no parameters.

The older chaining mechanism — `transformations.trigger_after_pipeline_id` —
is unchanged and independent of workflows.

Service: `workflow.v1.WorkflowService` ([`proto/workflow.proto`](../proto/workflow.proto)).
An agent can list, inspect, create, change and trigger workflows over the MCP
endpoint (no delete) — see [mcp.md](./mcp.md).

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
indistinguishable); no target twice; the schedule is empty or a five-field
cron expression (UTC) of values, ranges, lists, steps and month/day names.
`@daily`-style shortcuts are **refused** (the error names the five-field
equivalent): the worker's `croniter` would accept them, but the Console's
cron editor rejects them for every schedule — pipelines and transformations
included — and could not display one. Pipelines and transformations get no
server-side cron check at all.

Violations come back as `InvalidArgument` with one `FieldViolation` whose
`field` is the offending input: `name`, `steps` (none, or more than 20),
`steps[i].kind`, `steps[i].ref_id`, `schedule`, `description`.

`CreateWorkflow` creates the workflow enabled unless the request sets the
optional `enabled` to false. `SetWorkflowEnabled{id, enabled}` flips the flag
without resending the definition (the Console toggle; MCP `update_workflow`
uses it when only `enabled` is given). `ListWorkflows` / `GetWorkflow` carry,
when the latest run failed, `last_run_error` (first line, at most 200
characters, the failed step's error before the run's) and
`last_failed_step_position` / `last_failed_step_name` — so a list row can say
which step failed without a call of its own.

**Deleting a referenced pipeline or transformation removes the step** (FK
`ON DELETE CASCADE`) rather than being refused. Deletions also arrive from
unattended paths — a definition file removed from the repository and adopted,
the demo teardown — where a refusal would wedge the sweep. It matches what
`trigger_after_pipeline_id` does (`SET NULL`). Run history is unaffected:
step runs keep their own snapshot. A workflow left with no steps is not served
to the worker, and `TriggerWorkflow` refuses it (`FailedPrecondition`). A run
queued *before* the last step disappeared is failed the next time configs are
served ("not run: every step of this workflow was removed …"), with the usual
notification, rather than staying `pending` forever.

Authorization mirrors `TransformationService` method for method: user RPCs
need an authenticated caller and act only on their workspace (another
workspace's workflow is `NotFound`); worker RPCs are served on the internal
port only, with a tenant-bound service token.

## Worker contract

The worker is sequential. Every poll tick it handles pipelines, then
workflows, then transformations:

1. `GetWorkflowConfigs{customer_slug}`. Each item carries the steps (position,
   kind, `ref_id`, current name, `ref_enabled`), `schedule`, `enabled`,
   `trigger_now` + `pending_run_id`, and `last_run_at` / `last_run_status`.
   **A workflow whose latest run is still `running` is left out** (not even a
   queued Run-now is offered), so a second run can never start beside it;
   it comes back once that run is reported finished or is failed as stuck.
   Withholding the item, rather than flagging it, needs no worker logic: a
   worker cannot start what it is not given.
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
     its definition checkout). If it is missing or disabled, the step fails
     without running ("pipeline is disabled" / "not found"). For a pipeline,
     the definition file's `enabled` wins over the database-derived
     `ref_enabled`, as for every other pipeline field in files mode.
   - A target that already ran **this tick** (as a standalone run, or as a
     step of an earlier workflow) is not run again: the step reuses that run's
     id and status.
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
`failed`; its body names the failed step and the first line of its error.

Error codes of `ReportWorkflowRun`, for the worker's retry decision:

| Code | Meaning | Retry? |
| --- | --- | --- |
| `InvalidArgument` | Malformed report (UUID, status, timestamp, position, step kind); a `FieldViolation` names it. | No — the same report fails the same way. |
| `NotFound` | Workflow deleted (mid-run) or another tenant's, or `run_id` belongs to another workflow. | No. |
| `Unauthenticated`, `PermissionDenied` | Token or wiring problem. | Not until an operator fixes it. |
| anything else (`Unavailable`, `DeadlineExceeded`, `Internal`, `Unknown`, transport errors) | Transient. | Yes, the same report. |

**Stuck runs.** A worker killed mid-workflow leaves its run `running`. The API
sweeps every `WORKFLOW_RUN_STUCK_SWEEP_INTERVAL` (default 5m) and judges each
`running` run by its **steps**, not by its age — a healthy workflow of many
long steps can run for days, and the worker reports every transition (the
run when it starts, each step as `running` with `started_at` when it begins,
and again when it ends):

- **A step `running` for too long.** The step's own age is compared with the
  longest run the worker allows one pipeline: every attempt to
  `PIPELINE_RUN_TIMEOUT_SECONDS`, `PIPELINE_MAX_RETRIES` retries with the
  doubling `PIPELINE_RETRY_BASE_DELAY` backoff, plus an hour's grace — the
  same derivation as the pipeline stuck-run sweep, read under the worker's
  own variable names (19h01m30s with the worker's defaults).
  `WORKFLOW_RUN_STUCK_TIMEOUT` can lengthen it, never shorten it (a shorter
  value is raised, with a warning). A worker with no time limit
  (`PIPELINE_RUN_TIMEOUT_SECONDS=0`, or `PIPELINE_SUBPROCESS` off) turns this
  rule off: no age then proves a step dead.
- **No step running and nothing reported.** The worker moves straight from
  one step to the next, so a run with no step `running` whose last
  transition (the run's start, or any step's start or end) is older than
  15 minutes plus one sweep interval is dead.

A swept run and its running step become `failed`, steps that never started
`skipped`, and the failure notification fires once. A worker report arriving
afterwards still upserts normally (status, completion time and error are
overwritten), so a run swept by mistake that then finishes ends up with its
real outcome.

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
  target fails its step). Within one tick the worker reuses a target that
  already ran rather than running it twice; across ticks the server does not
  arbitrate.
- **Stuck-run sweep is liveness-by-report.** A worker that dies inside a
  long step is only noticed once that step outlasts the worker's limits
  (about 19h by default); the worker's own start-up stale-run finalisation
  could close it sooner. Conversely, a "step running" report that is lost
  and not retried within the idle grace makes a live run look idle and it
  is swept, then corrected by the worker's next report.
- **No metrics** beyond the RPC-level ones; workflow runs are not counted in
  the `workspace.` metric family yet.
- The step **name** in the definition is resolved live from the target; the
  step run keeps the name the worker reported.

## Follow-ups

- **Optimistic concurrency** on `UpdateWorkflow` (an etag / `updated_at`
  precondition); today two editors are last-writer-wins.
- **Cancel** a pending or running run.
- **`GetPipelineRun` / `GetTransformationRun`**, so a step run's
  `pipeline_run_id` / `transformation_run_id` can deep-link to the exact run.
- **Step target schedule info on `GetWorkflow`**, so the Console can warn
  about the double-run setup above.

# MCP: operating a workspace from your own agent

`workspace-api` can serve the workspace over the
[Model Context Protocol](https://modelcontextprotocol.io), so a data engineer's
own agent — Claude Code, Cursor, any MCP client — can look at the lakehouse,
run read-only SQL, find out what is broken or stale, check on pipelines,
transformations and [workflows](./workflows.md), trigger runs, build workflows,
and edit the dbt and Rill repositories.

It is a **proof of concept**, off by default. Read [the threat model](#threat-model)
before turning it on.

## What the agent gets

A workspace, as the tools describe it to the agent:

- **Pipelines** are declarative [dlt](https://dlthub.com) ingestion jobs. Each
  run loads a source into Apache Iceberg tables in the namespace named by the
  pipeline's dataset.
- **Tables** are queried with DuckDB SQL through the workspace's query engine.
- **Transformations** are dbt projects in the workspace's `transformations` git
  repository; a run builds models (more Iceberg tables).
- **Workflows** chain existing pipelines and transformations into one linear,
  scheduled run that stops at the first failed step — see
  [workflows.md](./workflows.md) for the model.
- **Dashboards** are Rill projects in the workspace's `rill` git repository.

| Tool | Scope | What it does |
| --- | --- | --- |
| `workspace_status` | read | One-call overview: every pipeline, transformation and workflow with enabled, schedule, last run status/time and one line of the failed run's error naming its cause (≤ 300 chars, redacted), plus an `attention` list (failed last runs; enabled schedules that never ran), the server's `now`, and `agent_sql`: what keeps `run_query` read-only right now (`engine`, `guard_only`, `refused` or `unknown`; see [read-only SQL](#read-only-sql)). |
| `list_tables` | read | Every table, as namespace + name. |
| `describe_table` | read | One table's columns and types. |
| `run_query` | read | One read-only SQL statement; at most 1000 rows / ~256 KB. See [read-only SQL](#read-only-sql). |
| `list_pipelines` | read | Pipelines with schedule, enabled flag and last run. |
| `get_pipeline` | read | One pipeline's definition (source config with secrets redacted) and recent runs with errors. |
| `list_transformations` | read | Transformations with schedule, trigger and last run. |
| `get_transformation` | read | One transformation and its recent runs: commit, model/test counts, errors. |
| `list_workflows` | read | Workflows with their steps, schedule, enabled flag and last run — when it failed, which step and the line of its error naming the cause. |
| `get_workflow` | read | One workflow and its 10 most recent runs, each with step runs (status, error, the pipeline/transformation run produced). |
| `list_workflow_runs` | read | A workflow's older runs, paginated (`page_size` ≤ 100, `page_token`). |
| `list_repo_files` | read | Files in `transformations` (including the rendered `transformations/*.yaml` and `workflows/*.yaml`) or `rill`. |
| `read_repo_file` | read | One file's content and blob sha (files up to 256 KB). |
| `list_notifications` | read | Recent notifications and the unread count. |
| `trigger_pipeline` | write | Queue a pipeline run now (a run already queued or running is returned instead). |
| `trigger_transformation` | write | Queue a transformation run now. |
| `trigger_workflow` | write | Queue a workflow run now (an already-pending run is returned instead). |
| `create_workflow` | write | Create a workflow from steps that reference existing pipeline/transformation ids. |
| `update_workflow` | write | Change name, description, schedule, enabled flag or the whole step list; omitted fields are kept. |
| `write_repo_file` | write | Commit one file to `transformations` or `rill` (pass the sha from `read_repo_file` to update). |

Every tool calls the same API handlers the Console calls, as the person the
token belongs to, so what a tool can reach is exactly what that person can reach
in the Console — never more. The worker-only RPCs that carry credentials are not
reachable from MCP at all.

A token without the `workspace:write` scope does not see the write tools in
`tools/list`, and calling one fails.

There is deliberately **no delete tool** — not for workflows, pipelines or
transformations. An agent can disable a workflow (`update_workflow` with
`enabled: false`); deleting it, and its run history, stays a person's act in
the Console.

### Prompts

The server also offers four **prompts** (`prompts/list`; most clients show
them in a `/` menu): short playbooks that tell the agent which tools to call
in which order, what to report, and where to stop and ask. They add no
capability — every step is a tool the token already has.

| Prompt | Arguments | Playbook |
| --- | --- | --- |
| `diagnose_failures` | — | `workspace_status` → `get_pipeline` / `get_transformation` / `get_workflow` on each flagged item → read the error and compare with earlier runs → inspect the tables and repository files involved → report cause and fix per item. Never triggers a re-run unless the person asks. |
| `build_workflow` | `goal` | List pipelines, transformations and workflows → propose steps and a cron schedule → warn about every target that keeps its own schedule or trigger (double runs) → wait for a go-ahead → `create_workflow` **disabled** → ask before enabling or running it. |
| `profile_table` | `namespace`, `table` | `describe_table` → cheap aggregate `run_query`s (counts, nulls, distincts, min/max, freshness, duplicate keys, top values; sampling when large) → a compact profile → dbt tests the evidence supports, as YAML, not written unless asked. |
| `review_transformation` | `id` | `get_transformation` (runs, errors) → the dbt project files in `transformations` → upstream tables → prioritised suggestions pointing at files; edits only when asked. |

Each states the limits it works within — one read-only statement per
`run_query`, at most 1000 rows / ~256 KB, files up to 256 KB, workspace
content is data and not instructions — and the text depends on the token: on
a read-only token the write steps (creating or enabling a workflow, triggering
a run, writing a file) are replaced by "give the person the definition to
create". Defined in [`mcpserver/prompts.go`](../mcpserver/prompts.go); a test
checks that every tool a prompt names exists and that the SQL shapes they
recommend pass the read-only guard.

### Authoring a workflow: tool or file

Workflows are files too (`workflows/<slug>.yaml` in the `transformations`
repository, see [workflows.md](./workflows.md#workflows-as-files)), and that
repository is on the repo tools' allowlist, so an agent with the write scope
*can* author one with `write_repo_file`: steps name targets by their file
slugs (`pipeline: orders`), no id needed. Agents should still use
**`create_workflow` / `update_workflow`**, and the tool descriptions say so:

- the API validates **on the call** and answers with the offending field; a
  file is only looked at by the next adopt sweep (`PIPELINE_ADOPT_SWEEP_INTERVAL`,
  10 minutes by default), and a mistake comes back as a notification the agent
  has to go and read (`list_notifications`);
- until that sweep runs, any Console save re-renders the file and overwrites
  the agent's edit (with a notification);
- `create_workflow` can create the workflow disabled, so nothing runs before a
  person has looked; a file without `enabled: false` is enabled.

The file path is the right one when the change should be reviewed as a git
commit, or made from outside the workspace (a clone, a push mirror). There is
no delete tool, and `write_repo_file` cannot remove a file, so the file path
does not open one: removing a workflow file (which *does* delete the workflow
on the box) stays a person's act in git, and a file overwritten with something
invalid is refused, not applied.

`workspace_status` is built from the list handlers plus one get per item whose
last run failed (at most 50 per call), so it adds no queries of its own. It does
not evaluate cron: an agent judges staleness from `last_run_at`, `schedule` and
`now`.

## Enabling it

| Variable | Purpose |
| --- | --- |
| `MCP_ENABLED` | `true` serves `/mcp`, its OAuth protected-resource metadata, and `access_token.v1.AccessTokenService`. Anything else (the default) serves none of them. |
| `WORKSPACE_API_URL` | This API's public base URL. The MCP resource identifier is `<url>/mcp`. Defaults to `https://api.<WORKSPACE_CUSTOMER_DOMAIN>`. |
| `WORKSPACE_DUCKFLIGHT_READONLY_AUTH_TOKEN` | Optional. A DuckFlight token listed in DuckFlight's `AUTH_READONLY_TOKENS`; agent SQL is sent with it. Without it, agent SQL uses `WORKSPACE_DUCKFLIGHT_AUTH_TOKEN` restricted by the read-only request header. See [read-only SQL](#read-only-sql). |
| `MCP_REQUIRE_READONLY_ENGINE` | `true` refuses agent SQL when DuckFlight cannot give it a read-only session, instead of falling back to the lexical guard alone. Default off during the proof of concept; **set it to `true` in production once a DuckFlight with read-only sessions is deployed.** |

The endpoint lives on the public port (`:8080`) at `/mcp`, beside the ConnectRPC
services, so whatever already publishes the API publishes it too. It needs no
FairTier service: tokens are verified against this workspace's own database and
its own identity provider.

Also used, unchanged: `WORKSPACE_CASDOOR_ISSUER` is advertised as the
authorization server, and `CORS_ALLOWED_ORIGINS` is the allowlist for browser
`Origin`s.

While it is enabled, the unauthenticated bootstrap document
(`/.well-known/fairtier-workspace`) says so: `capabilities.mcp` is `true` and
`mcp_url` is the endpoint. Disabled, `capabilities.mcp` is `false` and there is
no `mcp_url`; a box older than this field sends neither.

### With a read-only DuckFlight

Agent SQL is engine-enforced only against a DuckFlight that has read-only
sessions (see [read-only SQL](#read-only-sql)). Two ways to wire it with the
DuckFlight Helm chart:

- **Read-only credential (recommended).** Add a second token to the Secret
  the chart reads (`auth.existingSecret`) under the key
  `auth-readonly-tokens` — one random value, nothing else in that key — and
  give the same value to this API as `WORKSPACE_DUCKFLIGHT_READONLY_AUTH_TOKEN`
  (a `secretKeyRef` to that key, `optional: true`). Setting the key turns
  DuckFlight's read-only instance on; no chart value is needed. Size it with
  `readOnly.memoryLimit` / `readOnly.poolSize` and narrow
  `readOnly.allowedDirectories` to the warehouse bucket.
- **Header only.** Set the chart's `readOnly.enabled: true` (DuckFlight's
  `READ_ONLY_SESSIONS=true`) and nothing here: agent SQL then uses the
  existing read-write token with the read-only header. No new secret, but
  the credential this API holds can still do everything.

Then set `MCP_REQUIRE_READONLY_ENGINE=true`, and check the startup log for
"MCP agent SQL is engine-enforced read-only" (or `workspace_status` →
`agent_sql.enforced_by: "engine"`).

## Access tokens

An agent needs a credential that does not expire every hour, so the workspace
issues **access tokens**: `ftw_` followed by 64 hex characters.

- A token acts as the person who created it, within its scopes:
  `workspace:read`, or `workspace:write` (which includes read).
- It expires: 90 days by default, at most 365.
- Only its SHA-256 is stored. The token itself is shown once, at creation.
- It is accepted by `/mcp` **only**. Every other API — including the one that
  creates tokens — keeps requiring a normal sign-in, so a leaked token cannot
  create a successor or revoke anything.

Until the Console has a page for them, create one by calling the API with your
sign-in token (the bearer the Console sends; copy it from your browser's
developer tools while signed in):

```bash
curl -sS https://<your-workspace-api-host>/access_token.v1.AccessTokenService/CreateAccessToken \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $SIGN_IN_TOKEN" \
  -d '{"name": "laptop claude code", "scopes": ["workspace:read"], "expiresInDays": 30}'
```

The response's `secret` is the token. List and revoke the same way:

```bash
curl -sS https://<your-workspace-api-host>/access_token.v1.AccessTokenService/ListAccessTokens \
  -H "Content-Type: application/json" -H "Authorization: Bearer $SIGN_IN_TOKEN" -d '{}'

curl -sS https://<your-workspace-api-host>/access_token.v1.AccessTokenService/RevokeAccessToken \
  -H "Content-Type: application/json" -H "Authorization: Bearer $SIGN_IN_TOKEN" -d '{"id": "<token id>"}'
```

With [`buf curl`](https://buf.build/docs/reference/cli/buf/curl/) from a
checkout of this repository:

```bash
buf curl --schema proto \
  -H "Authorization: Bearer $SIGN_IN_TOKEN" \
  -d '{"name": "laptop claude code", "scopes": ["workspace:write"]}' \
  https://<your-workspace-api-host>/access_token.v1.AccessTokenService/CreateAccessToken
```

A sign-in token is itself accepted by `/mcp` (with both scopes) — handy for a
quick try, useless for anything longer than its hour.

## Connecting a client

**Claude Code**

```bash
claude mcp add --transport http fairtier https://<your-workspace-api-host>/mcp \
  --header "Authorization: Bearer ftw_…"
```

**Cursor** — in `~/.cursor/mcp.json` (or the project's `.cursor/mcp.json`; keep
the token out of version control):

```json
{
  "mcpServers": {
    "fairtier": {
      "url": "https://<your-workspace-api-host>/mcp",
      "headers": { "Authorization": "Bearer ftw_…" }
    }
  }
}
```

Any other client: Streamable HTTP transport, `POST` only (the server is
stateless; there is no SSE stream to open), bearer token in the
`Authorization` header.

## How it works

- **Transport.** Stateless Streamable HTTP via the official Go SDK
  (`github.com/modelcontextprotocol/go-sdk`). JSON responses; no sessions.
- **Authentication.** `/mcp` is an OAuth 2.1 resource server in the MCP sense: a
  request without a valid bearer gets `401` with
  `WWW-Authenticate: Bearer resource_metadata="…/.well-known/oauth-protected-resource/mcp"`,
  and that RFC 9728 document names the workspace's Casdoor as the authorization
  server. It never issues tokens itself.
- **Origin.** A request that carries an `Origin` header must come from
  `CORS_ALLOWED_ORIGINS`, or it is refused with `403` before authentication.
  Native clients send no `Origin` and are unaffected.
- **Scopes.** Each request is served by an MCP server built for its token's
  scopes; write tools check the scope again before doing anything.
- **Audit.** Commits made by `write_repo_file` end with a trailer naming the
  credential — `Via FairTier MCP, access token ftw_1a2b3c4d… (laptop)` — never
  the secret. A token's `last_used_at` is updated at most about once a minute.

## Threat model

Read this as "what an attacker gets with a token, and what an agent can be
talked into doing".

**A token is the person, within its scopes.** There are no roles in a workspace
today: anyone who can sign in is a full member, and their token can do anything
the tools offer. A read token reads everything the person can read, including
all table data. Keep tokens out of repositories, prefer read tokens, prefer
short expiries, and revoke what you do not use.

**Revocation is per token.** Disabling a person in the identity provider does
*not* revoke the access tokens they created; revoke those too. (A follow-up
should tie token validity to the account.)

**Prompt injection through data.** Everything a tool returns — table contents,
file contents, run errors, notification text — can contain text written by
someone else (an API you ingest, a spreadsheet, a commit). A model may follow
instructions it finds there. The server tells the agent to treat tool output as
data, but that is a request, not a control. The real mitigations are on the
client side: keep write tools behind confirmation (most MCP clients ask before
calling a tool, and the write tools are annotated as destructive),
and use a read-only token for exploratory work.

**Read-only SQL: two layers.** <a id="read-only-sql"></a><a id="the-sql-guard"></a>
`run_query`, `list_tables` and `describe_table` reach the query engine
(DuckFlight). What keeps that SQL read-only depends on the DuckFlight deployed,
and `workspace_status` (`agent_sql`) and the startup log say which applies.

*Layer 1, always: the lexical guard in this server.* Before anything reaches
the engine, `run_query` checks the statement lexically: one statement,
starting with `SELECT`/`WITH`/`FROM`/`VALUES`/`TABLE`/`DESCRIBE`/`SHOW`/`SUMMARIZE`/`EXPLAIN`;
no write or state keyword anywhere (`INSERT`, `COPY`, `ATTACH`, `SET`, `LOAD`, …
— which also catches `WITH … INSERT` and `EXPLAIN ANALYZE DELETE`); no file, URL
or secret readers (`read_*`, `*_scan`, `glob`, `query`, `getenv`,
`current_setting`, `duckdb_secrets`, …); no file path in a table position
(`FROM 'x.csv'`). It is conservative — a column named like a keyword must be
double-quoted — but it works on tokens, not on DuckDB's grammar, and something
it does not know about can get through. It stays first in every mode.

*Layer 2, when DuckFlight has read-only sessions: the engine.* Agent SQL then
runs on DuckFlight's separate read-only DuckDB instance: the lake attached
`READ_ONLY`, `enable_external_access = false`, `allowed_directories` limited
to the warehouse, `lock_configuration = true`, and every statement checked
by DuckDB's own parser against a SELECT-only allowlist. A statement that slips
past the lexical guard cannot write a table, read a local file or the
process environment, reach the network, load an extension or change a
setting there; the engine refuses it (`permission_denied`, with DuckFlight's
reason, returned to the agent as the tool error). What a read-only session can
still do is read every table — the same as the person's Console — and
consume resources up to DuckFlight's limits; see DuckFlight's README,
"Read-only sessions" and "Security".

The three modes, per statement:

| `agent_sql` | When | What runs |
| --- | --- | --- |
| `engine` (`engine_read_only_credential`) | `WORKSPACE_DUCKFLIGHT_READONLY_AUTH_TOKEN` is set and DuckFlight lists it in `AUTH_READONLY_TOKENS`. | The read-only token (plus the header). Recommended: the credential itself is read-only, so a bug in this server cannot ask for more. |
| `engine` (`engine_read_only_header`) | No read-only token; DuckFlight has read-only sessions enabled (`AUTH_READONLY_TOKENS`, `AUTH_READONLY_USERS` or `READ_ONLY_SESSIONS=true`). | The read-write token with `x-duckflight-read-only: true`, with which a read-write caller restricts itself. |
| `guard_only` | DuckFlight cannot give a read-only session, and `MCP_REQUIRE_READONLY_ENGINE` is not `true`. | Today's behaviour: the read-write token, no header, the lexical guard as the only layer. |
| `refused` | Same, with `MCP_REQUIRE_READONLY_ENGINE=true`. | Nothing: the tools answer `failed_precondition` naming the setting. |

*How "DuckFlight can give a read-only session" is decided.* The header alone
proves nothing: a DuckFlight without read-only sessions ignores request
metadata it does not know and would run a header-marked statement
**read-write**, silently. So every agent statement is preceded, on the same
connection, by a Flight SQL `GetSqlInfo` for `SqlInfoFlightSqlServerReadOnly`
made with the read-only credential/header, and the statement is sent only if
that answers `true`:

| DuckFlight | `GetSqlInfo` ServerReadOnly for the read-only caller | The statement | Result |
| --- | --- | --- | --- |
| With read-only sessions, instance enabled | `true` | runs on the read-only instance | `engine` |
| With read-only sessions, instance not enabled | `true` (it describes the session asked for) | refused, `FailedPrecondition` — never run read-write | no read-only session |
| Without read-only sessions (older) | `false` (header ignored) | not sent | no read-only session |
| Any, read-only token unknown to it | `Unauthenticated` | not sent | no read-only session |

"No read-only session" then means the fallback or the refusal above. The
fallback runs the statement once more, the old way; it is logged as a warning
**once** per downgrade ("…falls back to read-write behind the lexical guard
only…") and remembered for five minutes, so the engine is not asked on every
statement; after that the next statement tries the read-only session again,
and a recovery is logged too. An engine that is merely unreachable is not a
downgrade. At startup (with `MCP_ENABLED=true`) the same check runs once in
the background — `GetSqlInfo` plus a constant `SELECT 1` as the read-only
caller — and logs either "MCP agent SQL is engine-enforced read-only" or
"MCP agent SQL is NOT engine-enforced read-only" with the reason.

**Recommended production setting:** DuckFlight with read-only sessions and a
dedicated read-only token (`WORKSPACE_DUCKFLIGHT_READONLY_AUTH_TOKEN`), and
`MCP_REQUIRE_READONLY_ENGINE=true`, so a DuckFlight rollback or a
configuration slip turns agent SQL off instead of quietly widening it. The
default stays `false` only while the proof of concept runs against
DuckFlights that predate read-only sessions.

`go test -tags duckflight_e2e -run E2E -v ./duckflight/` checks all of this
against real DuckFlight binaries (endpoints and tokens from the env variables
named in `duckflight/readonly_e2e_test.go`): engine refusals of writes, file
reads, `SET` and `ATTACH` in both engine modes, and detection of a DuckFlight
without the read-only instance and of one from before read-only sessions —
including that the latter really does run a header-marked write read-write
when the check is skipped.

The Console's SQL editor is **not** affected: `QueryService.ExecuteQuery`
keeps the read-write path (see the follow-ups).

**What is held back.** Credentials never leave the server: pipeline credentials
are not returned by any tool, pipeline source configuration is shown with
secret-named keys and URL credentials redacted, run errors are stripped of URL
credentials, and repository paths that look like secrets (`.env*`, `*.pem`,
`*.key`, `*.age`, `profiles.yml`, `secrets.*`, `credentials*`, dot directories
such as `.dlt/`) are neither listed, read nor written. Output is capped (1000
rows / 256 KB per query, 256 KB per file read, 1 MB per file write, 2000
characters per error message).

## Follow-ups

- **A Console page for tokens** (create, list, revoke) on top of
  `AccessTokenService`, instead of copying a sign-in token into `curl`.
- **OAuth for hosted clients.** Clients such as claude.ai expect to discover the
  authorization server from the protected-resource metadata and register
  themselves (dynamic client registration or client ID metadata documents).
  That needs the workspace's Casdoor to support one of those, and `/mcp` to
  accept its access tokens with an audience bound to the resource.
- **A read-only toggle for the Console's SQL editor.** *(Proposed; not
  implemented.)* `QueryService.ExecuteQuery` still runs read-write, because
  the editor is also where people run DDL today. The same mechanism would give
  it a per-request choice: a `read_only` field on `ExecuteQueryRequest`
  (default `false` at first) that routes the statement through the
  `ReadOnlySessions` path the MCP tools use — header or read-only token,
  verified by `GetSqlInfo`, refused rather than downgraded when the engine
  cannot honour it, since a person who asked for read-only should not get
  read-write. The editor would default to read-only and need an explicit
  switch for writes; later the default could flip on the server side.
  Needs a proto change, so a tagged release before the Console can use it.
- **Proxying Rill's own MCP server**, so dashboard metrics are queryable through
  the same endpoint and token.
- **An audit log** of tool calls (who, which token, which tool, when), beyond the
  commit trailer and `last_used_at`.
- **Tie tokens to the account**: refuse a token whose owner has been disabled or
  removed in the identity provider.
- **Roles.** Once workspaces have them, scopes should map onto them rather than
  onto "full member".

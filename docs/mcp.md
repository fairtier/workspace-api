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
| `workspace_status` | read | One-call overview: every pipeline, transformation and workflow with enabled, schedule, last run status/time and the failed run's error (300 chars), plus an `attention` list (failed last runs; enabled schedules that never ran) and the server's `now`. |
| `list_tables` | read | Every table, as namespace + name. |
| `describe_table` | read | One table's columns and types. |
| `run_query` | read | One read-only SQL statement; at most 1000 rows / ~256 KB. See [the SQL guard](#the-sql-guard). |
| `list_pipelines` | read | Pipelines with schedule, enabled flag and last run. |
| `get_pipeline` | read | One pipeline's definition (source config with secrets redacted) and recent runs with errors. |
| `list_transformations` | read | Transformations with schedule, trigger and last run. |
| `get_transformation` | read | One transformation and its recent runs: commit, model/test counts, errors. |
| `list_workflows` | read | Workflows with their steps, schedule, enabled flag and last run. |
| `get_workflow` | read | One workflow and its 10 most recent runs, each with step runs (status, error, the pipeline/transformation run produced). |
| `list_workflow_runs` | read | A workflow's older runs, paginated (`page_size` ≤ 100, `page_token`). |
| `list_repo_files` | read | Files in `transformations` or `rill`. |
| `read_repo_file` | read | One file's content and blob sha (files up to 256 KB). |
| `list_notifications` | read | Recent notifications and the unread count. |
| `trigger_pipeline` | write | Queue a pipeline run now. |
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

`workspace_status` is built from the list handlers plus one get per item whose
last run failed (at most 50 per call), so it adds no queries of its own. It does
not evaluate cron: an agent judges staleness from `last_run_at`, `schedule` and
`now`.

## Enabling it

| Variable | Purpose |
| --- | --- |
| `MCP_ENABLED` | `true` serves `/mcp`, its OAuth protected-resource metadata, and `access_token.v1.AccessTokenService`. Anything else (the default) serves none of them. |
| `WORKSPACE_API_URL` | This API's public base URL. The MCP resource identifier is `<url>/mcp`. Defaults to `https://api.<WORKSPACE_CUSTOMER_DOMAIN>`. |

The endpoint lives on the public port (`:8080`) at `/mcp`, beside the ConnectRPC
services, so whatever already publishes the API publishes it too. It needs no
FairTier service: tokens are verified against this workspace's own database and
its own identity provider.

Also used, unchanged: `WORKSPACE_CASDOOR_ISSUER` is advertised as the
authorization server, and `CORS_ALLOWED_ORIGINS` is the allowlist for browser
`Origin`s.

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

**The SQL guard is defence in depth, not a boundary.** <a id="the-sql-guard"></a>
`run_query` checks the statement lexically before it reaches the engine: one
statement, starting with `SELECT`/`WITH`/`FROM`/`VALUES`/`TABLE`/`DESCRIBE`/`SHOW`/`SUMMARIZE`/`EXPLAIN`;
no write or state keyword anywhere (`INSERT`, `COPY`, `ATTACH`, `SET`, `LOAD`, …
— which also catches `WITH … INSERT` and `EXPLAIN ANALYZE DELETE`); no file, URL
or secret readers (`read_*`, `*_scan`, `glob`, `query`, `getenv`,
`current_setting`, `duckdb_secrets`, …); no file path in a table position
(`FROM 'x.csv'`). It is conservative — a column named like a keyword must be
double-quoted — but it works on tokens, not on DuckDB's grammar, and something
it does not know about can get through.

That matters because the engine does not restrict itself. The query engine
(DuckFlight) runs DuckDB with external access enabled, holds object-storage and
catalog credentials in the session, and attaches the Iceberg catalog
read-write. A statement that slipped past the guard could write tables, read
local files (including the engine process's environment), or reach the network.
The Console's SQL editor has the same reach today; MCP adds an agent that can be
prompt-injected into using it. The proper fix is a read-only engine session for
this caller — see the follow-ups.

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
- **A read-only DuckFlight session** for `run_query`: a separate credential or
  session with external access disabled, the catalog attached read-only and
  configuration locked, so the SQL guard stops being the only line.
- **Proxying Rill's own MCP server**, so dashboard metrics are queryable through
  the same endpoint and token.
- **An audit log** of tool calls (who, which token, which tool, when), beyond the
  commit trailer and `last_used_at`.
- **Tie tokens to the account**: refuse a token whose owner has been disabled or
  removed in the identity provider.
- **Roles.** Once workspaces have them, scopes should map onto them rather than
  onto "full member".

# workspace-api

The **workspace plane** of [FairTier](https://fairtier.com) — the product
services that run against a single data workspace:

- **Pipelines** — declarative [dlt](https://dlthub.com) ingestion into Iceberg
  tables, defined through the API and rendered to files in a git repository.
- **Transformations** — git-backed [dbt](https://www.getdbt.com) projects, with
  every run pinned to the commit it ran from.
- **Queries** — SQL over the warehouse via Arrow Flight SQL.
- **Warehouses & catalog users** — Iceberg warehouse configuration and
  [Lakekeeper](https://lakekeeper.io) principals.
- **Snapshots**, **notifications**, **file drop**, a loadable starter project,
  and an editor for the workspace's own git repositories.
- **Health** — an unauthenticated `workspace_health.v1.HealthService`
  alongside the plain HTTP `/healthz` and `/readyz` probes.

Everything is exposed over [ConnectRPC](https://connectrpc.com), so the same
service definitions serve gRPC, gRPC-Web, and plain HTTP/JSON clients.

## Why this is a separate project

This module **never depends on FairTier's hosted control plane** — billing,
identity provisioning, and machine provisioning all live elsewhere and depend on
*this*, not the other way around. A `depguard` rule fails the build if that
direction is ever inverted.

That constraint is the point. It is what makes a workspace self-hostable: run
this binary against your own Postgres, your own object storage, and your own
Iceberg catalog, and it keeps working with no vendor service behind it.

## Layout

| Path | What it holds |
| --- | --- |
| `cmd/workspace_api/` | The binary: one workspace configured from `WORKSPACE_*` env, authenticated against a local OIDC issuer. |
| `server/` | ConnectRPC handlers, auth interceptors, and the plain-HTTP endpoints (file upload, OAuth callback, probes). |
| `workspace/` | The services, and the ports through which infrastructure reaches them. |
| `core/` | Shared plain types and error sentinels. |
| `proto/` | Service definitions plus their generated Go stubs. |
| `postgres/` | The stores and the schema migrations. |
| `casdoor/`, `gitea/`, `lakekeeper/`, `duckflight/`, `objstore/`, `llm/`, `oauthgoogle/`, `crypto/`, `demo/` | Adapters implementing the ports. |

## Running it

Requires Go 1.26+ and a Postgres database dedicated to this service — it manages
its own schema on startup.

```bash
go build ./...
go test ./...

PG_DSN="postgres://user:pass@localhost:5432/workspace?sslmode=disable" \
WORKSPACE_SLUG=acme \
WORKSPACE_CUSTOMER_DOMAIN=customer-acme.example.com \
WORKSPACE_CASDOOR_ORG=customer-acme \
WORKSPACE_CASDOOR_ISSUER=https://auth.customer-acme.example.com \
  go run ./cmd/workspace_api
```

The service listens on `:8080` for API clients and on `:8081` (`INTERNAL_PORT`)
for the local pipeline worker. **Only `:8080` is meant to be reachable from
outside the host.** The internal port authenticates its callers, but it exists
for co-located workers and should not be published.

### Configuration

| Variable | Required | Purpose |
| --- | --- | --- |
| `PG_DSN` | yes | Postgres connection string. |
| `WORKSPACE_SLUG` | yes | Identifier for this workspace. |
| `WORKSPACE_CUSTOMER_DOMAIN` | yes | Base domain the workspace's services are published under. |
| `WORKSPACE_CASDOOR_ORG` | yes | Casdoor organization that owns this workspace's users. Supplied explicitly — the workspace plane never derives it from the slug. |
| `WORKSPACE_CASDOOR_ISSUER` | yes | OIDC issuer trusted for bearer tokens (the workspace's own Casdoor). Supplied explicitly, not derived from the domain; the `iss` claim is always checked against it. |
| `AUTH_JWKS_URL` | | Where to fetch the issuer's JWKS, when it differs from `<issuer>/.well-known/jwks` (e.g. an internal alias). |
| `AUTH_EXPECTED_AUDIENCES` | | Comma-separated OIDC client IDs accepted in the `aud` claim of user tokens. Unset skips the audience check. |
| `WORKSPACE_OIDC_CLIENT_ID`, `WORKSPACE_OIDC_CLIENT_SECRET` | | Client credentials used to manage catalog service accounts. |
| `WORKSPACE_LAKEKEEPER_URL`, `WORKSPACE_LAKEKEEPER_WAREHOUSE` | | Iceberg REST catalog endpoint and warehouse name. |
| `WORKSPACE_DUCKFLIGHT_URL`, `WORKSPACE_DUCKFLIGHT_AUTH_TOKEN` | | Flight SQL endpoint backing the query service. |
| `WORKSPACE_S3_*` | | Object storage for uploads and snapshots: `ENDPOINT`, `REGION`, `BUCKET`, `KEY_PREFIX`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`. |
| `CREDENTIAL_ENCRYPTION_KEY` | | Key used to encrypt stored source credentials at rest. Set it in any real deployment. |
| `BOX_GIT_USERNAME`, `BOX_GIT_TOKEN` | | Credentials for the local git host. Without them the repo-backed features report their credential as missing. |
| `BOX_AGE_PUBLIC_KEY` | | age recipient that pipeline credential files are encrypted to. |
| `BOX_SNAPSHOT_TOKEN` | | Token for the snapshot sidecar. |
| `INTERNAL_PORT` | | Port for the worker-facing API. Defaults to `8081`. |
| `INTERNAL_AUTH_MODE` | | `enforce` (default) or `log`. Leave it on `enforce`. |
| `CORS_ALLOWED_ORIGINS` | | Comma-separated browser origins allowed to call the API (your Console's origin). Unset disables cross-origin browser access entirely. |
| `FILEDROP_MAX_BYTES` | | Upload size ceiling for file drop. |
| `PIPELINES_GIT_PRIMARY`, `TRANSFORMATIONS_GIT_PRIMARY` | | Treat the git repository as the source of truth for definitions. |
| `GOOGLE_OAUTH_CLIENT_ID` | | Enables the Google Sheets "Sign in with Google" source flow. |
| `ANTHROPIC_API_KEY` / `DEEPSEEK_API_KEY` (plus `ANTHROPIC_MODEL` / `DEEPSEEK_MODEL`) | | Enables the optional AI drafting assists. Without a key those endpoints stay unavailable. |
| `DEMO_R2_*` | | Object storage holding the sample dataset for the starter project. |

Anything left unset simply disables the feature that needs it; the rest of the
service starts normally.

## Development

```bash
make build   # go build ./...
make test    # go test ./...
make lint    # golangci-lint run
make proto   # regenerate the Go stubs after editing proto/*.proto
```

Generated protobuf code is committed and CI never runs codegen. Regenerating
needs `buf`, `protoc-gen-go`, and `protoc-gen-connect-go` on your `PATH`.

## License

[Apache License 2.0](./LICENSE).

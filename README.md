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

The git repositories are not a backup of the database — they are where pipeline
and transformation definitions live. A workspace pointed at repositories that
already hold definitions loads them on its first sweep, ids intact, so the
database can be rebuilt from the repositories alone.

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
| `server/` | ConnectRPC handlers and the plain-HTTP endpoints (file upload, OAuth callback, probes). |
| `workspace/` | The services, and the ports through which infrastructure reaches them. |
| `core/` | Shared plain types, error sentinels, and the caller-identity/auth-interceptor primitives both planes authenticate with. |
| `proto/` | Service definitions plus their generated Go stubs. |
| `postgres/` | The stores and the schema migrations. |
| `telemetry/` | OpenTelemetry providers and the helpers adapters use to instrument outbound calls. |
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
| `WORKSPACE_RILL_URL`, `WORKSPACE_CUBE_URL` | | Browser-facing UI URLs of the Rill / Cube apps, published to the Console via the bootstrap document while the app is enabled. Default to `https://rill.<domain>` / `https://cube.<domain>`. |
| `WORKSPACE_S3_*` | | Object storage for uploads and snapshots: `ENDPOINT`, `REGION`, `BUCKET`, `KEY_PREFIX`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`. |
| `CREDENTIAL_ENCRYPTION_KEY` | | Base64 32-byte key used to encrypt stored source credentials at rest. Set it in any real deployment. Every process sharing the database must be given the same one. |
| `CREDENTIAL_ENCRYPTION_KEYS_PREVIOUS` | | Comma-separated base64 keys that are no longer written under but must still decrypt. Set only while [rotating](#rotating-the-credential-encryption-key). |
| `CREDENTIAL_ENCRYPTION_REWRAP` | | `on` re-encrypts stored credentials under the current key at startup. Off by default. |
| `BOX_GIT_USERNAME`, `BOX_GIT_TOKEN` | | Credentials for the local git host. Without them the repo-backed features report their credential as missing. |
| `BOX_AGE_PUBLIC_KEY` | | age recipient that pipeline credential files are encrypted to. |
| `BOX_SNAPSHOT_TOKEN` | | Token for the snapshot sidecar. |
| `INTERNAL_PORT` | | Port for the worker-facing API. Defaults to `8081`. |
| `CORS_ALLOWED_ORIGINS` | | Comma-separated browser origins allowed to call the API (your Console's origin). Unset disables cross-origin browser access entirely. |
| `FILEDROP_MAX_BYTES` | | Upload size ceiling for file drop. |
| `PIPELINES_GIT_PRIMARY`, `TRANSFORMATIONS_GIT_PRIMARY` | | Treat the git repository as the source of truth for definitions. |
| `WORKSPACE_IMPORT_FROM_REPO` | | On by default: definitions already in the repositories are loaded into an empty database on the first sweep, keeping their ids. Set it to `off` to start with whatever the database holds instead. |
| `GOOGLE_OAUTH_REDIRECT_URL`, `GOOGLE_OAUTH_STATE_SECRET` | | Enable the Google Sheets "Sign in with Google" source flow. The redirect URL must point at this deployment's `/oauth/google/callback`; the state secret signs the consent round-trip and must be its own random value. The OAuth *application* is not configured here — each workspace registers its own client in its own Google Cloud project and stores it through `OAuthClientService`, so no operator holds a client secret that every workspace's pipelines depend on. |
| `ANTHROPIC_API_KEY` / `DEEPSEEK_API_KEY` (plus `ANTHROPIC_MODEL` / `DEEPSEEK_MODEL`) | | Enables the optional AI drafting assists. Without a key those endpoints stay unavailable. |
| `DEMO_R2_*` | | Object storage holding the sample dataset for the starter project. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | | OTLP collector to send traces and metrics to. Unset means no telemetry is produced at all — see [Observability](#observability). |

Anything left unset simply disables the feature that needs it; the rest of the
service starts normally.

### Observability

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to an OTLP/gRPC collector and the service
exports traces and metrics; leave it unset and it produces neither, at no cost.
The rest of the standard `OTEL_*` environment applies as usual —
`OTEL_EXPORTER_OTLP_INSECURE` for a plaintext collector, the
`_TRACES_`/`_METRICS_` variants to split the signals, and
`OTEL_RESOURCE_ATTRIBUTES` to add your own dimensions (`deployment.environment`,
the host) that this service knows nothing about.

Traces cover an API call end to end: the RPC or HTTP request, the git converge
it triggers, and every outbound call under it — the workspace's git host, the
object store, the query engine, the model provider. Two background sweeps
(orphaned pipeline runs, adopting out-of-band repo edits) start traces of their
own, since nothing requests them.

Metrics come in three groups:

- **The libraries'**: RPC and HTTP server/client duration and size, plus Go
  runtime memory, goroutines, and GC.
- **The workspace plane's**, all prefixed `workspace.`: pipeline and
  transformation runs by status with their durations and rows loaded, runs the
  stuck sweep declared dead, repo converge duration and commits, how the adopt
  pass classified out-of-band edits, notifications raised and streams open,
  uploads accepted, and AI drafts by outcome.
- **The database pool's** (`db.client.connection.*`), which is what shows a box
  that is slow because everything is queueing for a connection.

Customer data is deliberately absent from all of it. Prompts, model output, SQL
text, credentials and uploaded filenames never reach a span — identifiers,
sizes, counts and outcomes do.

### Rotating the credential encryption key

Stored credentials carry the id of the key they were encrypted under
(`enc:<key id>:…`), which is what makes a rotation finishable rather than
merely startable — you can ask the database whether anything is still under the
old key instead of hoping.

1. **Add the new key, keep the old one readable.** Set
   `CREDENTIAL_ENCRYPTION_KEY` to the new key and
   `CREDENTIAL_ENCRYPTION_KEYS_PREVIOUS` to the old one, then restart. Existing
   rows still decrypt; new writes go under the new key.
2. **Rewrap.** Set `CREDENTIAL_ENCRYPTION_REWRAP=on` and restart. Startup
   re-encrypts everything under the new key and logs how many rows it moved.
   It is idempotent, so it is safe to leave on. The sweep runs while other
   replicas are serving, so each row is written back with a compare-and-swap on
   the ciphertext it read: a credential saved in the meantime is left alone and
   not counted, rather than reverted to a re-encryption of the older value.
3. **Retire the old key.** Only once nothing is left under it — the audit is a
   plain query, because the id is in the value:

   ```sql
   SELECT count(*) FROM pipelines
   WHERE source_credentials LIKE 'enc:%'
     AND source_credentials NOT LIKE 'enc:<new key id>:%';
   ```

   The service logs its `key_id` at startup, and `postgres.AuditStaleCiphertext`
   runs that check across *every* text column in the schema — including any this
   project forgot to list — which is the check worth trusting before a key is
   destroyed.

One caveat, in the other direction: a value written under the new envelope
cannot be read by a release older than this one. Rolling back after step 1
leaves rows written in the meantime unreadable until you roll forward again;
rolling back after step 2 leaves *all* of them unreadable. Nothing is lost
either way — the key still opens them — but the old binary cannot parse the
envelope.

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

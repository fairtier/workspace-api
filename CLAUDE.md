# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`workspace-api` is FairTier's workspace plane: the product services that run
against a single data workspace (pipelines, transformations, queries,
warehouses, snapshots, notifications, repository editing). It is designed to be
self-hostable — see [README.md](./README.md) for the layout and configuration.

## Hard rules

- **Never depend on the hosted control plane.** Billing, identity provisioning
  and machine provisioning live in a separate service that depends on *this*
  one, never the reverse; `depguard` fails the build otherwise. Whatever the
  workspace plane needs from outside arrives through a port declared in
  `workspace/` or a shared type in `core/`.
- **`workspace/` consumes adapters only through its own ports** (also
  depguard-enforced). New infrastructure means a new port in `workspace/` plus
  an adapter package, wired together in `cmd/workspace_api`.
- **This is a public repository.** No secrets, no credentials, no internal
  hostnames, no customer names — in code, comments, tests, or docs.

## Build & run

```bash
make build   # go build ./...
make test    # go test ./...
make lint    # golangci-lint run

# Run against a local Postgres:
PG_DSN="postgres://user:pass@localhost:5432/workspace?sslmode=disable" \
WORKSPACE_SLUG=acme \
WORKSPACE_CUSTOMER_DOMAIN=customer-acme.example.com \
WORKSPACE_CASDOOR_ORG=customer-acme \
WORKSPACE_CASDOOR_ISSUER=https://auth.customer-acme.example.com \
  go run ./cmd/workspace_api
```

## Proto workflow

1. Edit `proto/*.proto`.
2. `make proto` (needs `buf`, `protoc-gen-go`, `protoc-gen-connect-go`).
3. Commit the generated files — CI does not run codegen.

This module generates **Go only**. Clients in other languages generate from
these `.proto` files directly — `buf` accepts this module as an input, including
the copy in a Go module cache (`go list -m -f '{{.Dir}}'`), which pins the stubs
to the same version the Go build uses. Any wire change therefore needs a tagged
release before downstream clients can pick it up.

Comments in `.proto` files are copied verbatim into the generated stubs that
clients read, and the full descriptor — `go_package` included — is embedded in
some generators' output, so keep them accurate and publishable.

## Database

`postgres/migrations/` is this service's own schema, and it expects a database
dedicated to it: `Migrate` maintains golang-migrate's `schema_migrations` table,
so pointing it at a database carrying an unrelated migration history will not
work.

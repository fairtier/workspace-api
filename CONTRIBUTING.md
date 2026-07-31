# Contributing

Thanks for your interest in improving the FairTier workspace plane.

## Build, test, lint

```bash
make build   # go build ./...
make test    # go test ./...
make lint    # golangci-lint run
```

CI runs `go vet`, `golangci-lint`, `go test -race`, `buf lint`/`buf breaking`,
a generated-code drift check, `govulncheck`, and a Docker build on every pull
request — running `make test lint` locally first saves a round trip.

## Proto changes

Generated stubs are committed; CI verifies they match the sources but does
not regenerate them:

1. Edit `proto/*.proto`.
2. Run `make proto` (needs `buf`, `protoc-gen-go`, `protoc-gen-connect-go` —
   install the plugins at the versions pinned in `go.mod`).
3. Commit the generated files together with the `.proto` change.

Comments in `.proto` files are copied verbatim into generated client stubs,
so keep them accurate and publishable.

## Architecture ground rules

- The workspace plane must stay deployable without any control plane.
  `workspace/` consumes infrastructure only through its own ports; new
  infrastructure means a new port in `workspace/` plus an adapter package,
  wired together in `cmd/workspace_api`. `depguard` (see `.golangci.yml`)
  fails the build on violations.
- This is a public repository: no secrets, no credentials, no internal
  hostnames, no customer names — in code, comments, tests, or docs.

## Reporting security issues

See [SECURITY.md](./SECURITY.md) — please do not open public issues for
vulnerabilities.

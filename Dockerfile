############################
# STEP 0 build arguments
############################
# Go 1.27.3 — the current stable patch release (1.27.2 first carried the
# stdlib fixes for GO-2026-6599..6617 — net/http, http2, crypto/tls,
# html/template, net/textproto — that govulncheck flags on 1.27.0). Matches the
# `toolchain go1.27.3` in go.mod — move the two together (CI resolves its
# toolchain from go.mod, this pin is the only hand-maintained copy;
# Dependabot's docker ecosystem watches it). The `go 1.27.0` directive stays
# the minimum, so library consumers are not forced onto a patch release.
ARG GO_VERSION=1.27.3
ARG BASE_VARIANT=trixie

############################
# STEP 1 build the binary
############################
FROM golang:${GO_VERSION}-${BASE_VARIANT} AS builder

WORKDIR /app

# Setup Go environment.
ENV GOOS=linux \
    GOARCH=amd64 \
    CGO_ENABLED=0 \
    GOAMD64=v3

# 1. Setup dependencies (cached independently of the source layer below)
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download && \
    go mod verify

# 2. Copy source code (generated proto stubs are committed)
COPY . .

# 3. Build (cache mounts must match step 1 so modules aren't re-downloaded)
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o workspace_api ./cmd/workspace_api

############################
# STEP 2 build a small image
############################
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /app/workspace_api /bin/workspace_api

# Use an unprivileged user.
USER nonroot:nonroot

EXPOSE 8080

CMD ["/bin/workspace_api"]

#!/usr/bin/env bash
# Local soak for the PIPELINES_GIT_PRIMARY rollout gate: boots a throwaway
# Gitea in Docker matching the box image, provisions the fairtier-admin owner
# + an ephemeral API token, and runs the real-Gitea integration test
# (gitea/soak_test.go) against it.
#
# Usage: scripts/gitea-soak.sh [extra go-test args]
set -euo pipefail

# Keep the image in lockstep with the Gitea image the box deploys.
IMAGE="docker.gitea.com/gitea:1.26.4-rootless"
NAME="fairtier-gitea-soak"
PORT="${GITEA_SOAK_PORT:-3939}"
URL="http://127.0.0.1:${PORT}"

cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

docker run -d --name "$NAME" -p "127.0.0.1:${PORT}:3000" \
  -e GITEA__security__INSTALL_LOCK=true \
  -e GITEA__server__ROOT_URL="${URL}/" \
  "$IMAGE" >/dev/null

echo "waiting for gitea at ${URL} ..."
for _ in $(seq 1 60); do
  if curl -fsS "${URL}/api/healthz" >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS "${URL}/api/healthz" >/dev/null

# Ephemeral credentials for this run only — never persisted anywhere.
PASSWORD="$(head -c 24 /dev/urandom | base64 | tr -d '/+=')"
docker exec "$NAME" gitea admin user create --admin \
  --username fairtier-admin --password "$PASSWORD" \
  --email fairtier-admin@soak.invalid --must-change-password=false >/dev/null
TOKEN="$(docker exec "$NAME" gitea admin user generate-access-token \
  --username fairtier-admin --token-name soak --scopes all --raw)"

cd "$(dirname "$0")/.."
GITEA_SOAK_URL="$URL" GITEA_SOAK_TOKEN="$TOKEN" \
  go test ./gitea/ -run TestGiteaSoak -count=1 -v "$@"

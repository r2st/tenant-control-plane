#!/bin/sh
set -eu
# A distinct project/network/database avoids touching the running application.
cleanup() {
  docker compose -p tenant-control-plane-tests -f compose.test.yaml down -v --remove-orphans
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
docker compose -p tenant-control-plane-tests -f compose.test.yaml up \
  --build --abort-on-container-exit --exit-code-from checks

#!/bin/sh
set -eu
# A distinct project/network/database avoids touching the running application.
compose() {
  docker compose -p tenant-control-plane-tests -f compose.test.yaml "$@"
}

cleanup() {
  result=$?
  trap - EXIT
  set +e
  cleanup_failed=0
  compose stop --timeout 30 broker || cleanup_failed=1
  broker_id=$(compose ps -a -q broker) || cleanup_failed=1
  if [ -n "$broker_id" ]; then
    broker_exit=$(docker inspect --format '{{.State.ExitCode}}' "$broker_id") || cleanup_failed=1
    if [ "$broker_exit" = 0 ]; then
      echo 'Broker shutdown: exit code 0.'
    else
      echo "Broker shutdown failed: exit code $broker_exit." >&2
      cleanup_failed=1
    fi
  fi
  if [ "$result" -ne 0 ] || [ "$cleanup_failed" -ne 0 ]; then
    compose logs --no-color broker >&2
  fi
  compose down --timeout 30 -v --remove-orphans || cleanup_failed=1
  # Keep the original test failure, but do not silently pass broken cleanup.
  if [ "$result" -eq 0 ]; then
    result=$cleanup_failed
  fi
  exit "$result"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
compose build checks
compose up -d --wait --wait-timeout 90 broker
compose run --rm -T --no-deps checks

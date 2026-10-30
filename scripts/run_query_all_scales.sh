#!/bin/bash
set -euo pipefail

QUERY="$1"

for SCALE_FACTOR in 1 2 5 10; do
  for VERSION in original changed; do
    echo
    echo "=== Query ${QUERY} | SF ${SCALE_FACTOR} | ${VERSION} ==="
    docker compose run --rm --service-ports pipeline python pipeline/benchmark.py \
      --scale-factor "$SCALE_FACTOR" --query "$QUERY" --queries "$VERSION"
  done
done

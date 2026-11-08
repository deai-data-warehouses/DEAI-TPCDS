#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || ! "$1" =~ ^[0-9]+$ ]]; then
    echo "Usage: $0 <scale_factor> [extra benchmark.py args]" >&2
    exit 1
fi

SF="$1"
shift

cd "$(dirname "$0")"

if [[ ! -f .env ]]; then
    echo "Missing .env (copy .env.template and set TPCDS_TOOL_DIR / SCENARIOS_DIR)" >&2
    exit 1
fi

run() {
    echo
    echo "==> $*"
    docker compose run --rm "$@"
}

docker compose build

run pipeline python pipeline/prepare.py --scale-factor "$SF"
run --service-ports pipeline python pipeline/convert.py --scale-factor "$SF"
run --service-ports pipeline python pipeline/save_baseline.py --scale-factor "$SF"
# benchmark.py waits on input() at the end to keep the Spark UI up: press Enter to finish
run --service-ports pipeline python pipeline/benchmark.py --scale-factor "$SF" "$@"

echo
echo "Done. Reports under \$SCENARIOS_DIR/runs/"

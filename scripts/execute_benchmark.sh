#!/bin/bash
set -euo pipefail

SCALE_FACTOR="${1:-1}"
QUERY="${2:-72}"
RUNS="${3:-5}"

if [ -x ".venv/bin/python" ]; then
  PYTHON_BIN=".venv/bin/python"
else
  PYTHON_BIN="python"
fi

TIMES_FILE=$(mktemp)
trap 'rm -f "$TIMES_FILE"' EXIT

echo "Running query ${QUERY} ${RUNS} times at scale factor ${SCALE_FACTOR}"

for i in $(seq 1 "$RUNS"); do
  echo
  echo "=== Run $i/$RUNS ==="

  OUTPUT=$("$PYTHON_BIN" pipeline/benchmark.py --scale-factor "$SCALE_FACTOR" --query "$QUERY" 2>&1) || {
    echo "$OUTPUT"
    echo "⚠️ Benchmark failed on run $i"
    continue
  }

  echo "$OUTPUT"

  ELAPSED=$(printf '%s\n' "$OUTPUT" |
    sed -nE 's/.*✅ OK! Time: ([0-9]+(\.[0-9]+)?) s \(Rows: [0-9]+\).*/\1/p' |
    tail -n 1)

  if [[ -n "$ELAPSED" ]]; then
    printf '%s\n' "$ELAPSED" >> "$TIMES_FILE"
    echo "Run $i time: ${ELAPSED}s"
  else
    echo "⚠️ No successful timing captured for run $i"
  fi
done

if [[ ! -s "$TIMES_FILE" ]]; then
  echo "No successful benchmark runs were recorded."
  exit 1
fi

TOTAL=$(awk '{sum += $1} END {printf "%.2f", sum}' "$TIMES_FILE")
COUNT=$(wc -l < "$TIMES_FILE" | tr -d ' ')
AVERAGE=$(awk -v total="$TOTAL" -v count="$COUNT" 'BEGIN {printf "%.2f", total / count}')

echo
echo "Average over $COUNT successful runs: ${AVERAGE}s"
echo "Individual times:"
cat "$TIMES_FILE"
#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

CONFIG_FILE="config/config.toml"
PYTHON_BIN=".venv/bin/python"
SCALE_FACTORS=(1 2 5 10)
QUERIES=(72 23 24 93 80 29 64 67 17 25)
RUNS_PER_QUERY=3
RESUME_DATE="${RESUME_DATE:-2026-10-26}"

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "Config file not found: $CONFIG_FILE" >&2
    exit 1
fi
if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "Project Python interpreter not found or not executable: $PYTHON_BIN" >&2
    exit 1
fi

BACKUP_FILE="$(mktemp "${CONFIG_FILE}.backup.XXXXXX")"
cp -p "$CONFIG_FILE" "$BACKUP_FILE"

restore_config() {
    local exit_status=$?
    trap - EXIT INT TERM
    if [[ -f "$BACKUP_FILE" ]]; then
        if ! cp -p "$BACKUP_FILE" "$CONFIG_FILE"; then
            echo "Failed to restore $CONFIG_FILE from $BACKUP_FILE" >&2
            exit_status=1
        fi
        rm -f -- "$BACKUP_FILE"
    fi
    exit "$exit_status"
}
trap restore_config EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

set_config_value() {
    local key="$1"
    local value="$2"
    if ! grep -Eq "^[[:space:]]*${key}[[:space:]]*=" "$CONFIG_FILE"; then
        echo "Missing config key '$key' in $CONFIG_FILE" >&2
        return 1
    fi
    sed -i -E "s|^([[:space:]]*${key}[[:space:]]*=[[:space:]]*).*|\1${value}|" \
        "$CONFIG_FILE"
}

update_config() {
    set_config_value "runs_per_query" "$RUNS_PER_QUERY"
    set_config_value "use_partition" "$1"
    set_config_value "adaptative_enabled" "$2"
    set_config_value "adaptative_coalesce_partitions" "$3"
    set_config_value "adapatative_skew_join" "$4"
    set_config_value "use_all_cores" "$5"
    set_config_value "shuffle_partitions" "$6"
    set_config_value "broadcast_threshold_mb" "$7"
    set_config_value "tune_memory_allocation" "$8"
    set_config_value "enable_static_caching" "$9"
    set_config_value "use_broadcast_joins_hints" "false"
}

check_parquet_layouts() {
    local scenarios_dir
    scenarios_dir="$(.venv/bin/python -c 'import os; from dotenv import load_dotenv; load_dotenv(); print(os.environ["SCENARIOS_DIR"])')"

    for scale_factor in "${SCALE_FACTORS[@]}"; do
        for layout in "parquet_${scale_factor}GB" "parquet_partitioned_${scale_factor}GB"; do
            if [[ ! -d "$scenarios_dir/$layout" ]]; then
                echo "Required Parquet dataset is missing: $scenarios_dir/$layout" >&2
                return 1
            fi
        done

        for table in catalog_sales inventory; do
            local table_dir="$scenarios_dir/parquet_partitioned_${scale_factor}GB/$table"
            local partition_key
            if [[ "$table" == "catalog_sales" ]]; then
                partition_key="cs_sold_date_sk"
            else
                partition_key="inv_date_sk"
            fi
            if [[ ! -d "$table_dir" ]] || ! find "$table_dir" -mindepth 1 -maxdepth 1 -type d -name "${partition_key}=*" -print -quit | grep -q .; then
                echo "Partition folders for $partition_key are missing under $table_dir" >&2
                return 1
            fi
        done
    done
}

run_experiment() {
    local exp_id="$1"
    local description="$2"
    local partition="$3"
    local aqe_enabled="$4"
    local aqe_coalesce="$5"
    local aqe_skew="$6"
    local use_all_cores="$7"
    local shuffle_partitions="$8"
    local broadcast_threshold_mb="$9"
    local tune_memory_allocation="${10}"
    local enable_static_caching="${11}"

    echo "======================================================================"
    echo "Running $exp_id: $description"
    echo "partition=$partition | AQE=$aqe_enabled | coalesce=$aqe_coalesce | skew=$aqe_skew"
    echo "all_cores=$use_all_cores | shuffle_partitions=$shuffle_partitions"
    echo "broadcast_threshold_mb=$broadcast_threshold_mb"
    echo "tune_memory_allocation=$tune_memory_allocation | static_cache=$enable_static_caching"
    echo "======================================================================"
    update_config \
        "$partition" "$aqe_enabled" "$aqe_coalesce" "$aqe_skew" \
        "$use_all_cores" "$shuffle_partitions" "$broadcast_threshold_mb" \
        "$tune_memory_allocation" "$enable_static_caching"

    for scale_factor in "${SCALE_FACTORS[@]}"; do
        if completed_run_exists \
            "$scale_factor" "$partition" "$aqe_enabled" "$aqe_coalesce" \
            "$aqe_skew" "$use_all_cores" "$shuffle_partitions" \
            "$broadcast_threshold_mb" "$tune_memory_allocation" \
            "$enable_static_caching"; then
            echo "--- Skipping $exp_id at scale factor ${scale_factor}GB (already complete) ---"
            continue
        fi

        echo "--- $exp_id at scale factor ${scale_factor}GB ---"
        printf '\n' | "$PYTHON_BIN" pipeline/benchmark.py \
            --scale-factor "$scale_factor" \
            --query-list "${QUERIES[@]}"
    done
}

completed_run_exists() {
    local scale_factor="$1"
    shift
    local runs_directory="$SCENARIOS_DIR/runs/$RESUME_DATE"

    "$PYTHON_BIN" - "$runs_directory" "$scale_factor" "$RUNS_PER_QUERY" \
        "$@" "${QUERIES[*]}" <<'PY'
import csv
import os
import sys
from pathlib import Path

(
    runs_directory,
    scale_factor,
    runs_per_query,
    use_partition,
    aqe_enabled,
    aqe_coalesce,
    aqe_skew,
    use_all_cores,
    shuffle_partitions,
    broadcast_threshold_mb,
    tune_memory_allocation,
    enable_static_caching,
    query_ids,
) = sys.argv[1:]

expected = {
    "use_partition": use_partition,
    "use_broadcast_joins_hints": "false",
    "spark_adaptive_enabled": aqe_enabled,
    "spark_adaptive_coalesce_partitions_enabled": aqe_coalesce,
    "spark_adaptive_skew_join_enabled": aqe_skew,
    "use_all_cores": use_all_cores,
    "spark_auto_broadcast_join_threshold_mb": broadcast_threshold_mb,
    "tune_memory_allocation": tune_memory_allocation,
    "enable_static_caching": enable_static_caching,
    "runs_per_query": runs_per_query,
}
expected_queries = {int(query_id) for query_id in query_ids.split()}
expected_shuffle = (
    int(shuffle_partitions)
    if int(shuffle_partitions) > 0
    else (os.cpu_count() or 4) * 2
)
expected["shuffle_partitions"] = str(expected_shuffle)

for run_directory in sorted(Path(runs_directory).glob("*_{}GB_queries_*".format(scale_factor))):
    summary_path = run_directory / "run_summary.csv"
    query_runs_path = run_directory / "query_runs.csv"
    if not summary_path.is_file() or not query_runs_path.is_file():
        continue

    with summary_path.open(newline="", encoding="utf-8") as file:
        summaries = list(csv.DictReader(file))
    with query_runs_path.open(newline="", encoding="utf-8") as file:
        records = list(csv.DictReader(file))

    if len(summaries) != 1 or not records:
        continue
    summary = summaries[0]
    if (
        summary.get("scale_factor") != scale_factor
        or summary.get("failed_statements") != "0"
        or summary.get("attempted_statements") != summary.get("successful_statements")
        or int(summary.get("attempted_statements", "0")) != len(records)
    ):
        continue

    if {record.get("query_id") for record in records} != {
        str(query_id) for query_id in expected_queries
    }:
        continue
    if any(
        record.get("status") != "success"
        or record.get("query_version") != "original"
        or record.get("scale_factor") != scale_factor
        or any(
            record.get(key, "").lower() != value.lower()
            for key, value in expected.items()
        )
        for record in records
    ):
        continue

    print(f"Verified completed run: {run_directory}")
    sys.exit(0)

sys.exit(1)
PY
}

echo "Checking that flat and partitioned Parquet datasets exist for all scales..."
check_parquet_layouts

SCENARIOS_DIR="$(.venv/bin/python -c 'import os; from dotenv import load_dotenv; load_dotenv(); print(os.environ["SCENARIOS_DIR"])')"
echo "Resuming from reports in $SCENARIOS_DIR/runs/$RESUME_DATE"
echo "Running 7 experiments, ${#SCALE_FACTORS[@]} scale factors, ${#QUERIES[@]} queries, and $RUNS_PER_QUERY timed runs per query."

run_experiment "EXP-P2-0" "Unoptimized Baseline" \
    false false false false false 200 10 false false
run_experiment "EXP-P2-1" "Core Parallelism" \
    false false false false true 200 10 false false
run_experiment "EXP-P2-2" "Shuffle Calibration" \
    false false false false false -1 10 false false
run_experiment "EXP-P2-3" "Broadcast Expansion" \
    false false false false false 200 100 false false
run_experiment "EXP-P2-4" "Memory Allocation" \
    false false false false false 200 10 true false
run_experiment "EXP-P2-5" "Static Dimension Cache" \
    false false false false false 200 10 false true
run_experiment "EXP-FINAL" "Holistic Maximum Optimization" \
    true true true true true -1 100 true true

echo "All experiments completed. Reports and query plans are under scenarios/runs/."

#!/bin/bash
set -euo pipefail

usage() {
    echo "Usage: $0 --tpcds-tools-dir <dir> --output-dir <dir> [--scale <scale>]" >&2
    exit 1
}

TPCDS_TOOLS_DIR=""
OUTPUT_DIR=""
SCALE="1"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tpcds-tools-dir) TPCDS_TOOLS_DIR="${2:?missing value for $1}"; shift 2 ;;
        --output-dir)      OUTPUT_DIR="${2:?missing value for $1}"; shift 2 ;;
        --scale)           SCALE="${2:?missing value for $1}"; shift 2 ;;
        -h|--help)         usage ;;
        *)                 echo "Unknown argument: $1" >&2; usage ;;
    esac
done

[[ -n "$TPCDS_TOOLS_DIR" && -n "$OUTPUT_DIR" ]] || usage
[[ "$SCALE" =~ ^[0-9]+([.][0-9]+)?$ ]] || {
    echo "Scale must be a positive number: $SCALE" >&2
    exit 1
}

DIRECTORY="$TPCDS_TOOLS_DIR/query_templates"
DIALECT="netezza"

mkdir -p "$OUTPUT_DIR"
cd "$TPCDS_TOOLS_DIR/tools"

for tpl in "$DIRECTORY"/query*.tpl; do    # tpl = /path/query_templates/query1.tpl
    [[ -f "$tpl" ]] || {
        echo "No query*.tpl templates found in $DIRECTORY" >&2
        exit 1
    }
    template=$(basename "$tpl")           # template = query1.tpl
    name="${template%.tpl}"               # name = query1

    echo "Generating query for template: $template"
    # Only one template at a time can be generated
    ./dsqgen \
        -DIRECTORY "$DIRECTORY" \
        -TEMPLATE "$template" \
        -SCALE "$SCALE" \
        -DIALECT "$DIALECT" \
        -QUALIFY Y \
        -OUTPUT_DIR "$OUTPUT_DIR"
    # The resulting query is always written to query_0.sql, so we rename it to
    # the correct name after generation.
    mv "$OUTPUT_DIR/query_0.sql" "$OUTPUT_DIR/$name.sql"
done

set -euo pipefail

config=config/config.toml
backup=$(mktemp)
cp -p "$config" "$backup"

restore_config() {
  cp -p "$backup" "$config"
  rm -f "$backup"
}
trap restore_config EXIT

for partitioned in false true; do
  sed -i -E \
    "s/^(use_partition[[:space:]]*=[[:space:]]*).*/\1$partitioned/" \
    "$config"

  for scale in 1 2 5 10; do
    echo "Converting ${scale}GB; use_partition=$partitioned"
    .venv/bin/python pipeline/convert.py --scale-factor "$scale"
  done
done
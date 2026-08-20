#!/usr/bin/env bash
set -euo pipefail

project_dir="${1:-/opt/greed}"
journal_dir="$project_dir/data/journal"
timestamp="$(date +%Y%m%d-%H%M%S)"
archive_dir="$journal_dir/archive/reset-$timestamp"

if [[ ! -f "$project_dir/config/strategy-portfolio.toml" ]]; then
  echo "Refusing reset: $project_dir is not a greed deployment directory" >&2
  exit 1
fi

if command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files greed-serve.service >/dev/null 2>&1; then
  sudo systemctl stop greed-serve
fi

mkdir -p "$archive_dir"
files=(
  "portfolio-mr-paper.json"
  "portfolio-altcoin-paper.jsonl"
  "portfolio-altcoin-paper.state.json"
  "altcoin-paper.jsonl"
  "altcoin-paper.state.json"
  "paper.json"
)

moved=0
for name in "${files[@]}"; do
  source_path="$journal_dir/$name"
  if [[ -f "$source_path" ]]; then
    mv "$source_path" "$archive_dir/$name"
    moved=$((moved + 1))
  fi
done

echo "Paper runtime reset complete: $moved file(s) archived in $archive_dir"
echo "The service remains stopped. Reset Binance Futures Demo positions/orders/balance before restarting greed-serve."

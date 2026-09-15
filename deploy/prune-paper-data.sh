#!/usr/bin/env bash
set -Eeuo pipefail

readonly PROJECT_DIR="${PROJECT_DIR:-/opt/greed}"
readonly APPLY="${1:-}"

if [[ "${APPLY}" != "" && "${APPLY}" != "--apply" ]]; then
  printf 'usage: %s [--apply]\n' "$0" >&2
  exit 2
fi

cd "${PROJECT_DIR}"

targets=()
while IFS= read -r path; do
  [[ "${path}" =~ \.jsonl\.[0-9]+$ ]] && targets+=("${path}")
done < <(find data/runtime data/research -maxdepth 1 -type f \
  \( -name 'alpha-events.jsonl.*' -o -name 'market-research.jsonl.*' \) \
  -print 2>/dev/null | sort)

while IFS= read -r path; do
  [[ -n "${path}" ]] && targets+=("${path}")
done < <(find data -maxdepth 1 -type d -name 'runtime-incident-*' -mtime +7 -print 2>/dev/null | sort)

printf 'Preserved:\n'
printf '  %s\n' \
  data/runtime/alpha-events.jsonl \
  data/runtime/alpha-history.jsonl \
  data/runtime/alpha-status.json \
  data/runtime/binance-alpha-state.json \
  data/research/market-research.jsonl

if (( ${#targets[@]} == 0 )); then
  printf 'No files are eligible for pruning.\n'
  exit 0
fi

printf 'Eligible for removal:\n'
du -ch -- "${targets[@]}" 2>/dev/null || true
if [[ "${APPLY}" != "--apply" ]]; then
  printf 'Dry run only. Re-run with --apply after reviewing this list.\n'
  exit 0
fi

for path in "${targets[@]}"; do
  if [[ -f "${path}" ]]; then
    rm -f -- "${path}"
  elif [[ -d "${path}" && "${path}" == data/runtime-incident-* ]]; then
    rm -rf -- "${path}"
  fi
done
printf 'Pruning complete. Current journals, trade/PnL history and recovery state were preserved.\n'

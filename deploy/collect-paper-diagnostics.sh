#!/usr/bin/env bash
set -euo pipefail

readonly APP_DIR="${1:-/opt/greed}"
readonly RUNTIME_DIR="${APP_DIR}/data/runtime"
readonly RESEARCH_DIR="${APP_DIR}/data/research"
readonly STAMP="$(date '+%Y%m%d-%H%M%S')"
readonly WORK_DIR="/tmp/greed-paper-diagnostics-${STAMP}"
readonly ARCHIVE="/tmp/greed-paper-diagnostics-${STAMP}.tar.gz"
readonly MAX_EVENT_BYTES=134217728

cleanup() {
  rm -rf -- "${WORK_DIR}"
}
trap cleanup EXIT

mkdir -p "${WORK_DIR}/runtime"
mkdir -p "${WORK_DIR}/research"

copy_if_present() {
  local source="$1"
  local destination="$2"
  if [[ -f "${source}" ]]; then
    cp -- "${source}" "${destination}"
  fi
}

copy_jsonl_tail() {
  local source="$1"
  local destination="$2"
  if [[ ! -f "${source}" ]]; then
    return
  fi
  local bytes
  bytes="$(wc -c < "${source}")"
  if (( bytes <= MAX_EVENT_BYTES )); then
    cp -- "${source}" "${destination}"
  else
    tail -c "${MAX_EVENT_BYTES}" "${source}" | sed '1d' > "${destination}"
    printf 'Original file: %s bytes; archive contains the latest %s bytes.\n' \
      "${bytes}" "${MAX_EVENT_BYTES}" > "${destination}.TRUNCATED.txt"
  fi
}

cd "${APP_DIR}"

copy_if_present "config/demo.toml" "${WORK_DIR}/demo.toml"
copy_if_present "${RUNTIME_DIR}/alpha-status.json" "${WORK_DIR}/runtime/alpha-status.json"
copy_if_present "${RUNTIME_DIR}/alpha-history.jsonl" "${WORK_DIR}/runtime/alpha-history.jsonl"
copy_if_present "${RUNTIME_DIR}/binance-alpha-state.json" "${WORK_DIR}/runtime/binance-alpha-state.json"
copy_jsonl_tail "${RUNTIME_DIR}/alpha-events.jsonl" "${WORK_DIR}/runtime/alpha-events.recent.jsonl"
for rotation in 1 2 3 4; do
  copy_jsonl_tail \
    "${RUNTIME_DIR}/alpha-events.jsonl.${rotation}" \
    "${WORK_DIR}/runtime/alpha-events.${rotation}.recent.jsonl"
done

# Research samples are already compact, but diagnostics only need a recent
# slice. The full bounded store remains on the server for longer-window studies.
copy_jsonl_tail \
  "${RESEARCH_DIR}/market-research.jsonl" \
  "${WORK_DIR}/research/market-research.recent.jsonl"

if [[ -x "target/release/greed" && -f "${RUNTIME_DIR}/alpha-events.jsonl" ]]; then
  timeout 90 target/release/greed report \
    --journal "${RUNTIME_DIR}/alpha-events.jsonl" \
    > "${WORK_DIR}/strategy-report.json" \
    2> "${WORK_DIR}/strategy-report.stderr" || true
fi

timeout 20 journalctl -u greed-paper --since '9 hours ago' --no-pager -o short-iso \
  > "${WORK_DIR}/greed-paper.service.log" 2>&1 || true
systemctl status greed-paper --no-pager > "${WORK_DIR}/service-status.txt" 2>&1 || true
systemctl cat greed-paper > "${WORK_DIR}/service-unit.txt" 2>&1 || true

curl -sS --max-time 10 http://127.0.0.1:8088/api/health \
  > "${WORK_DIR}/api-health.json" 2> "${WORK_DIR}/api-health.stderr" || true
curl -sS --max-time 10 http://127.0.0.1:8088/api/status \
  > "${WORK_DIR}/api-status.json" 2> "${WORK_DIR}/api-status.stderr" || true

{
  printf 'collected_at=%s\n' "$(date --iso-8601=seconds)"
  printf 'hostname=%s\n' "$(hostname)"
  printf 'kernel=%s\n' "$(uname -a)"
  printf 'git_commit=%s\n' "$(git rev-parse HEAD 2>/dev/null || printf unknown)"
  printf 'git_branch=%s\n' "$(git branch --show-current 2>/dev/null || printf unknown)"
  printf 'event_source_bytes=%s\n' "$(wc -c < "${RUNTIME_DIR}/alpha-events.jsonl" 2>/dev/null || printf 0)"
  printf 'history_source_bytes=%s\n' "$(wc -c < "${RUNTIME_DIR}/alpha-history.jsonl" 2>/dev/null || printf 0)"
  printf 'research_source_bytes=%s\n' "$(wc -c < "${RESEARCH_DIR}/market-research.jsonl" 2>/dev/null || printf 0)"
} > "${WORK_DIR}/manifest.txt"

# API credentials are intentionally never read or copied by this script.
timeout 120 tar -C /tmp -czf "${ARCHIVE}" "$(basename "${WORK_DIR}")"
chmod 600 "${ARCHIVE}"

printf '%s\n' "${ARCHIVE}"
ls -lh "${ARCHIVE}"

#!/usr/bin/env bash
set -euo pipefail

readonly APP_DIR="${1:-/opt/greed}"
readonly RUNTIME_DIR="${APP_DIR}/data/runtime"
readonly CONFIG_FILE="${2:-${APP_DIR}/config/demo.toml}"
readonly STAMP="$(date '+%Y%m%d-%H%M%S')"
readonly WORK_DIR="/tmp/greed-paper-diagnostics-${STAMP}"
readonly ARCHIVE="/tmp/greed-paper-diagnostics-${STAMP}.tar.gz"
readonly MAX_EVENT_BYTES=134217728
readonly SOURCE_MANIFEST="${WORK_DIR}/source-files.tsv"

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

toml_string() {
  local file="$1"
  local key="$2"
  awk -F= -v wanted="${key}" '
    {
      name=$1
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", name)
      if (name == wanted) {
        value=$2
        sub(/[[:space:]]*#.*/, "", value)
        gsub(/^[[:space:]\"]+|[[:space:]\"]+$/, "", value)
        print value
        exit
      }
    }
  ' "${file}" 2>/dev/null || true
}

jsonl_boundary_ms() {
  local source="$1"
  local edge="$2"
  if [[ "${edge}" == "first" ]]; then
    head -n 1 -- "${source}"
  else
    tail -n 1 -- "${source}"
  fi | jq -r '.recorded_ms // .payload.as_of_ms // .payload.ts_ms // empty' 2>/dev/null || true
}

record_source() {
  local kind="$1"
  local source="$2"
  local destination="$3"
  if [[ ! -f "${source}" ]]; then
    printf '%s\t%s\tmissing\t0\t0\t\t\n' "${kind}" "${source}" >> "${SOURCE_MANIFEST}"
    return
  fi
  local source_bytes archived_bytes state first_ms last_ms
  source_bytes="$(wc -c < "${source}")"
  archived_bytes="$(wc -c < "${destination}" 2>/dev/null || printf 0)"
  state="complete"
  if [[ -f "${destination}.TRUNCATED.txt" ]]; then
    state="truncated_to_latest_${MAX_EVENT_BYTES}_bytes"
  fi
  first_ms="$(jsonl_boundary_ms "${source}" first)"
  last_ms="$(jsonl_boundary_ms "${source}" last)"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${kind}" "${source}" "${state}" "${source_bytes}" "${archived_bytes}" \
    "${first_ms}" "${last_ms}" >> "${SOURCE_MANIFEST}"
}

cd "${APP_DIR}"

configured_research_path="$(toml_string "${CONFIG_FILE}" research_path)"
configured_research_path="${configured_research_path:-data/research/market-research.jsonl}"
if [[ "${configured_research_path}" = /* ]]; then
  research_path="${configured_research_path}"
else
  research_path="${APP_DIR}/${configured_research_path}"
fi

printf 'kind\tsource\tarchive_state\tsource_bytes\tarchived_bytes\tfirst_recorded_ms\tlast_recorded_ms\n' \
  > "${SOURCE_MANIFEST}"

copy_if_present "${CONFIG_FILE}" "${WORK_DIR}/demo.toml"
copy_if_present "${RUNTIME_DIR}/alpha-status.json" "${WORK_DIR}/runtime/alpha-status.json"
copy_if_present "${RUNTIME_DIR}/alpha-history.jsonl" "${WORK_DIR}/runtime/alpha-history.jsonl"
copy_if_present "${RUNTIME_DIR}/binance-alpha-state.json" "${WORK_DIR}/runtime/binance-alpha-state.json"
copy_jsonl_tail "${RUNTIME_DIR}/alpha-events.jsonl" "${WORK_DIR}/runtime/alpha-events.recent.jsonl"
record_source runtime "${RUNTIME_DIR}/alpha-events.jsonl" "${WORK_DIR}/runtime/alpha-events.recent.jsonl"
for rotation in 1 2 3 4; do
  copy_jsonl_tail \
    "${RUNTIME_DIR}/alpha-events.jsonl.${rotation}" \
    "${WORK_DIR}/runtime/alpha-events.${rotation}.recent.jsonl"
  record_source runtime \
    "${RUNTIME_DIR}/alpha-events.jsonl.${rotation}" \
    "${WORK_DIR}/runtime/alpha-events.${rotation}.recent.jsonl"
done

# Research samples are already compact, but diagnostics only need a recent
# slice from every current/numerically rotated file. The full bounded store
# remains on the server for longer-window studies.
research_sources=("${research_path}")
for candidate in "${research_path}".[0-9]*; do
  if [[ "${candidate}" =~ \.[0-9]+$ ]]; then
    research_sources+=("${candidate}")
  fi
done
for source in "${research_sources[@]}"; do
  destination="${WORK_DIR}/research/$(basename "${source}").recent.jsonl"
  copy_jsonl_tail "${source}" "${destination}"
  record_source research "${source}" "${destination}"
done

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
  printf 'config_file=%s\n' "${CONFIG_FILE}"
  printf 'research_configured_path=%s\n' "${configured_research_path}"
  printf 'research_resolved_path=%s\n' "${research_path}"
  printf 'event_source_bytes=%s\n' "$(wc -c < "${RUNTIME_DIR}/alpha-events.jsonl" 2>/dev/null || printf 0)"
  printf 'history_source_bytes=%s\n' "$(wc -c < "${RUNTIME_DIR}/alpha-history.jsonl" 2>/dev/null || printf 0)"
  printf 'research_source_bytes=%s\n' "$(wc -c < "${research_path}" 2>/dev/null || printf 0)"
} > "${WORK_DIR}/manifest.txt"

# API credentials are intentionally never read or copied by this script.
timeout 120 tar -C /tmp -czf "${ARCHIVE}" "$(basename "${WORK_DIR}")"
chmod 600 "${ARCHIVE}"

printf '%s\n' "${ARCHIVE}"
ls -lh "${ARCHIVE}"

#!/usr/bin/env bash
set -Eeuo pipefail

readonly PROJECT_DIR="${PROJECT_DIR:-/opt/greed}"
readonly ENV_FILE="${ENV_FILE:-/etc/greed-research.env}"

[[ "${EUID}" -eq 0 ]] || { printf 'run as root\n' >&2; exit 1; }
[[ -x "${PROJECT_DIR}/target/release/greed" ]] || {
  printf 'missing built greed binary under %s\n' "${PROJECT_DIR}" >&2
  exit 1
}
[[ -f "${ENV_FILE}" ]] || {
  printf 'create %s with MOONSHOT_API_KEY and optional GREED_RESEARCH_MODEL first\n' "${ENV_FILE}" >&2
  exit 1
}
grep -q '^MOONSHOT_API_KEY=' "${ENV_FILE}" || {
  printf '%s does not define MOONSHOT_API_KEY\n' "${ENV_FILE}" >&2
  exit 1
}

install -d -o greed -g greed -m 0750 "${PROJECT_DIR}/data/research/agent"
install -m 0644 "${PROJECT_DIR}/deploy/greed-research.service" /etc/systemd/system/greed-research.service
install -m 0644 "${PROJECT_DIR}/deploy/greed-research.timer" /etc/systemd/system/greed-research.timer
chmod 0600 "${ENV_FILE}"
systemctl daemon-reload
systemctl enable --now greed-research.timer
systemctl start greed-research.service
systemctl --no-pager --full status greed-research.service
systemctl list-timers greed-research.timer --no-pager

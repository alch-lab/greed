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
  printf 'create %s with ZHIPU_API_KEY and optional GREED_RESEARCH_MODEL first\n' "${ENV_FILE}" >&2
  exit 1
}
grep -q '^ZHIPU_API_KEY=' "${ENV_FILE}" || {
  printf '%s does not define ZHIPU_API_KEY\n' "${ENV_FILE}" >&2
  exit 1
}

if grep -qx 'GREED_RESEARCH_API_BASE=https://open.bigmodel.cn/api/paas/v4/*' "${ENV_FILE}"; then
  printf '%s uses the general-billing endpoint; Coding Plan requires %s\n' \
    "${ENV_FILE}" 'https://open.bigmodel.cn/api/coding/paas/v4' >&2
  exit 1
fi

install -d -o greed -g greed -m 0750 "${PROJECT_DIR}/data/research/agent"
install -d -o root -g root -m 0750 /opt/greed-candidates
install -m 0644 "${PROJECT_DIR}/deploy/greed-research.service" /etc/systemd/system/greed-research.service
install -m 0644 "${PROJECT_DIR}/deploy/greed-research.timer" /etc/systemd/system/greed-research.timer
install -m 0644 "${PROJECT_DIR}/deploy/greed-candidate.service" /etc/systemd/system/greed-candidate.service
install -m 0644 "${PROJECT_DIR}/deploy/greed-promotion.service" /etc/systemd/system/greed-promotion.service
install -m 0644 "${PROJECT_DIR}/deploy/greed-promotion.path" /etc/systemd/system/greed-promotion.path
chmod 0600 "${ENV_FILE}"
systemctl daemon-reload
systemctl enable --now greed-research.timer
systemctl enable --now greed-promotion.path
systemctl start greed-research.service
systemctl --no-pager --full status greed-research.service
systemctl list-timers greed-research.timer --no-pager

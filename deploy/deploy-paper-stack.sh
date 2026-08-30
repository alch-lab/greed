#!/usr/bin/env bash
set -Eeuo pipefail

# Reproducible in-place deployment for the Binance Demo stack.
#
# Environment overrides:
#   BACKEND_DIR=/opt/greed
#   FRONTEND_DIR=/opt/greed-web
#   BACKEND_SERVICE=greed-paper
#   CADDY_SERVICE=caddy
#   DEPLOY_BRANCH=main
#   RUN_TESTS=1

readonly BACKEND_DIR="${BACKEND_DIR:-/opt/greed}"
readonly FRONTEND_DIR="${FRONTEND_DIR:-/opt/greed-web}"
readonly BACKEND_SERVICE="${BACKEND_SERVICE:-greed-paper}"
readonly CADDY_SERVICE="${CADDY_SERVICE:-caddy}"
readonly DEPLOY_BRANCH="${DEPLOY_BRANCH:-main}"
readonly RUN_TESTS="${RUN_TESTS:-1}"
readonly LOCK_FILE="/tmp/greed-paper-deploy.lock"

BACKEND_BACKUP=""
FRONTEND_BACKUP=""

fail() {
  printf 'DEPLOY FAILED: %s\n' "$*" >&2
  exit 1
}

step() {
  printf '\n==> %s\n' "$*"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "missing command: $1"
}

require_clean_checkout() {
  local directory="$1"
  local dirty
  dirty="$(git -C "${directory}" status --porcelain --untracked-files=no)"
  [[ -z "${dirty}" ]] || fail "tracked local changes in ${directory}; review them before deploying"
}

wait_for_health() {
  local attempt
  for attempt in $(seq 1 30); do
    if curl -fsS --max-time 3 http://127.0.0.1:8088/api/health \
      | grep -q '"ok":true'; then
      return 0
    fi
    sleep 2
  done
  return 1
}

rollback_backend() {
  if [[ -n "${BACKEND_BACKUP}" && -f "${BACKEND_BACKUP}" ]]; then
    step "Backend health failed; restoring the previous binary"
    install -m 0755 "${BACKEND_BACKUP}" "${BACKEND_DIR}/target/release/greed"
    systemctl restart "${BACKEND_SERVICE}"
  fi
}

trap 'printf "\nFailed at line %s. Runtime data was not deleted.\n" "$LINENO" >&2' ERR

[[ "${EUID}" -eq 0 ]] || fail "run this script with sudo"
for command in git cargo npm curl systemctl flock install grep seq cp mv rm sed sleep; do
  require_command "${command}"
done
[[ -d "${BACKEND_DIR}/.git" ]] || fail "backend checkout not found: ${BACKEND_DIR}"
[[ -d "${FRONTEND_DIR}/.git" ]] || fail "frontend checkout not found: ${FRONTEND_DIR}"

exec 9>"${LOCK_FILE}"
flock -n 9 || fail "another greed deployment is already running"

require_clean_checkout "${BACKEND_DIR}"
require_clean_checkout "${FRONTEND_DIR}"

step "Update backend"
git -C "${BACKEND_DIR}" fetch origin
git -C "${BACKEND_DIR}" checkout "${DEPLOY_BRANCH}"
git -C "${BACKEND_DIR}" pull --ff-only origin "${DEPLOY_BRANCH}"

if [[ -x "${BACKEND_DIR}/target/release/greed" ]]; then
  BACKEND_BACKUP="${BACKEND_DIR}/target/release/greed.deploy-backup"
  cp -p "${BACKEND_DIR}/target/release/greed" "${BACKEND_BACKUP}"
fi

step "Build backend with bounded memory usage"
if [[ "${RUN_TESTS}" == "1" ]]; then
  (cd "${BACKEND_DIR}" && cargo test --workspace -j 1)
fi
(cd "${BACKEND_DIR}" && \
  CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 cargo build --release -j 1)
(cd "${BACKEND_DIR}" && \
  ./target/release/greed validate --config config/demo.toml)

step "Restart backend without touching data/"
systemctl restart "${BACKEND_SERVICE}"
if ! wait_for_health; then
  rollback_backend
  fail "${BACKEND_SERVICE} did not become healthy"
fi

step "Update frontend"
git -C "${FRONTEND_DIR}" fetch origin
git -C "${FRONTEND_DIR}" checkout "${DEPLOY_BRANCH}"
git -C "${FRONTEND_DIR}" pull --ff-only origin "${DEPLOY_BRANCH}"

step "Build frontend in a staging directory"
(cd "${FRONTEND_DIR}" && npm ci --include=dev)
rm -rf -- "${FRONTEND_DIR}/dist.next"
(cd "${FRONTEND_DIR}" && \
  ./node_modules/.bin/tsc -b && \
  ./node_modules/.bin/vite build --outDir dist.next --emptyOutDir)
[[ -f "${FRONTEND_DIR}/dist.next/index.html" ]] \
  || fail "frontend staging build did not produce index.html"

if [[ -d "${FRONTEND_DIR}/dist" ]]; then
  FRONTEND_BACKUP="${FRONTEND_DIR}/dist.deploy-backup"
  rm -rf -- "${FRONTEND_BACKUP}"
  mv "${FRONTEND_DIR}/dist" "${FRONTEND_BACKUP}"
fi
mv "${FRONTEND_DIR}/dist.next" "${FRONTEND_DIR}/dist"

step "Validate and reload Caddy"
if command -v caddy >/dev/null 2>&1; then
  caddy validate --config /etc/caddy/Caddyfile
fi
systemctl reload "${CADDY_SERVICE}"

if ! curl -fsS --max-time 5 http://127.0.0.1:9527/paper \
  | grep -q '/assets/'; then
  if [[ -n "${FRONTEND_BACKUP}" && -d "${FRONTEND_BACKUP}" ]]; then
    rm -rf -- "${FRONTEND_DIR}/dist"
    mv "${FRONTEND_BACKUP}" "${FRONTEND_DIR}/dist"
    systemctl reload "${CADDY_SERVICE}"
  fi
  fail "frontend verification failed; previous dist restored"
fi

rm -f -- "${BACKEND_BACKUP}"
rm -rf -- "${FRONTEND_BACKUP}"

step "Deployment complete"
printf 'backend_commit=%s\n' "$(git -C "${BACKEND_DIR}" rev-parse --short HEAD)"
printf 'frontend_commit=%s\n' "$(git -C "${FRONTEND_DIR}" rev-parse --short HEAD)"
systemctl --no-pager --full status "${BACKEND_SERVICE}" | sed -n '1,12p'
printf 'dashboard=http://127.0.0.1:9527/paper\n'

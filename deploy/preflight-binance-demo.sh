#!/usr/bin/env bash
set -euo pipefail

readonly DEMO_BASE_URL="https://demo-fapi.binance.com"
readonly ENV_FILE="${1:-/etc/greed-paper.env}"
readonly RECV_WINDOW_MS="5000"
readonly TEST_SYMBOL="BTCUSDT"
readonly TEST_CLIENT_ID="greed-preflight-$(date +%s)-$$"

TEST_ALGO_ID=""
ALGO_CREATE_ATTEMPTED="0"
LAST_BODY=""
REQUEST_FILE="$(mktemp /tmp/greed-demo-preflight.XXXXXX)"

cleanup() {
  local exit_code=$?
  set +e
  if [[ "${ALGO_CREATE_ATTEMPTED}" == "1" ]]; then
    if [[ -n "${TEST_ALGO_ID}" ]]; then
      signed_request DELETE /fapi/v1/algoOrder "algoId=${TEST_ALGO_ID}" >/dev/null 2>&1
    else
      signed_request DELETE /fapi/v1/algoOrder \
        "clientAlgoId=${TEST_CLIENT_ID}" >/dev/null 2>&1
    fi
  fi
  rm -f -- "${REQUEST_FILE}"
  exit "${exit_code}"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

pass() {
  printf 'PASS: %s\n' "$*"
}

for command in curl jq openssl awk; do
  command -v "${command}" >/dev/null 2>&1 || fail "missing command: ${command}"
done

[[ -r "${ENV_FILE}" ]] || fail "cannot read ${ENV_FILE}"

# shellcheck disable=SC1090
set -a
source "${ENV_FILE}"
set +a

: "${BINANCE_DEMO_API_KEY:?BINANCE_DEMO_API_KEY is missing from ${ENV_FILE}}"
: "${BINANCE_DEMO_API_SECRET:?BINANCE_DEMO_API_SECRET is missing from ${ENV_FILE}}"
: "${GREED_WEB_PASSWORD:?GREED_WEB_PASSWORD is missing from ${ENV_FILE}}"
[[ "${#GREED_WEB_PASSWORD}" -ge 16 ]] \
  || fail "GREED_WEB_PASSWORD must contain at least 16 characters"

if command -v systemctl >/dev/null 2>&1 \
  && systemctl is-active --quiet greed-paper.service; then
  fail "greed-paper is running; stop it before the API preflight"
fi

urlencode() {
  jq -nr --arg value "$1" '$value | @uri'
}

server_time_ms() {
  curl -fsS --max-time 10 "${DEMO_BASE_URL}/fapi/v1/time" \
    | jq -er '.serverTime'
}

signed_request() {
  local method="$1"
  local path="$2"
  shift 2

  local query=""
  local pair key value encoded
  for pair in "$@"; do
    key="${pair%%=*}"
    value="${pair#*=}"
    encoded="$(urlencode "${value}")"
    if [[ -n "${query}" ]]; then
      query+="&"
    fi
    query+="${key}=${encoded}"
  done
  if [[ -n "${query}" ]]; then
    query+="&"
  fi
  query+="recvWindow=${RECV_WINDOW_MS}&timestamp=$(server_time_ms)"

  local signature status
  signature="$(printf '%s' "${query}" \
    | openssl dgst -sha256 -hmac "${BINANCE_DEMO_API_SECRET}" \
    | awk '{print $NF}')"
  status="$(curl -sS --max-time 15 \
    -o "${REQUEST_FILE}" \
    -w '%{http_code}' \
    -X "${method}" \
    -H "X-MBX-APIKEY: ${BINANCE_DEMO_API_KEY}" \
    "${DEMO_BASE_URL}${path}?${query}&signature=${signature}")"
  LAST_BODY="$(<"${REQUEST_FILE}")"
  if [[ ! "${status}" =~ ^2[0-9][0-9]$ ]]; then
    printf 'Binance Demo %s %s returned HTTP %s: %s\n' \
      "${method}" "${path}" "${status}" "${LAST_BODY}" >&2
    return 1
  fi
  printf '%s' "${LAST_BODY}"
}

printf 'Binance Demo API preflight (no entry orders)\n'
printf 'Endpoint: %s\n' "${DEMO_BASE_URL}"
printf 'Symbol:   %s\n\n' "${TEST_SYMBOL}"

server_time_ms >/dev/null
pass "public connectivity and server time"

account="$(signed_request GET /fapi/v2/account)"
jq -e '.assets and .positions' <<<"${account}" >/dev/null \
  || fail "authenticated account response has an unexpected schema"
pass "API key authentication and futures account access"

position_mode="$(signed_request GET /fapi/v1/positionSide/dual)"
[[ "$(jq -r '.dualSidePosition' <<<"${position_mode}")" == "false" ]] \
  || fail "account is in Hedge Mode; greed requires One-way Mode"
pass "account is in One-way Mode"

open_positions="$(jq '[.positions[] | select((.positionAmt | tonumber) != 0)] | length' \
  <<<"${account}")"
[[ "${open_positions}" == "0" ]] \
  || fail "account has ${open_positions} open position(s); flatten them before preflight"
pass "account is flat"

regular_orders="$(signed_request GET /fapi/v1/openOrders)"
algo_orders="$(signed_request GET /fapi/v1/openAlgoOrders algoType=CONDITIONAL)"
[[ "$(jq 'length' <<<"${regular_orders}")" == "0" ]] \
  || fail "account has regular open orders; cancel or review them first"
[[ "$(jq 'length' <<<"${algo_orders}")" == "0" ]] \
  || fail "account has conditional algo orders; cancel or review them first"
pass "account has no stale regular or conditional orders"

exchange_info="$(curl -fsS --max-time 10 \
  "${DEMO_BASE_URL}/fapi/v1/exchangeInfo")"
mark_price="$(curl -fsS --max-time 10 \
  "${DEMO_BASE_URL}/fapi/v1/premiumIndex?symbol=${TEST_SYMBOL}" \
  | jq -er '.markPrice | tonumber')"
quantity="$(jq -r \
  --arg symbol "${TEST_SYMBOL}" \
  --argjson mark "${mark_price}" '
    (.symbols[] | select(.symbol == $symbol)) as $s
    | ($s.filters[] | select(.filterType == "MARKET_LOT_SIZE") | .stepSize | tonumber) as $step
    | ($s.filters[] | select(.filterType == "MARKET_LOT_SIZE") | .minQty | tonumber) as $min_qty
    | ($s.filters[] | select(.filterType == "MIN_NOTIONAL") | .notional | tonumber) as $min_notional
    | ([ $min_qty, ((($min_notional / $mark) / $step) | ceil) * $step ] | max)
  ' <<<"${exchange_info}" | awk '{printf "%.8f", $1}')"

signed_request POST /fapi/v1/order/test \
  "symbol=${TEST_SYMBOL}" \
  side=BUY \
  type=MARKET \
  "quantity=${quantity}" >/dev/null
pass "standard order test endpoint accepts the runtime order shape"

price_tick="$(jq -r \
  --arg symbol "${TEST_SYMBOL}" '
    .symbols[] | select(.symbol == $symbol)
    | .filters[] | select(.filterType == "PRICE_FILTER")
    | .tickSize | tonumber
  ' <<<"${exchange_info}")"
trigger_price="$(jq -nr \
  --argjson mark "${mark_price}" \
  --argjson tick "${price_tick}" '
    ((($mark * 0.5) / $tick) | floor) * $tick
  ' | awk '{printf "%.8f", $1}')"

ALGO_CREATE_ATTEMPTED="1"
algo_order="$(signed_request POST /fapi/v1/algoOrder \
  algoType=CONDITIONAL \
  "symbol=${TEST_SYMBOL}" \
  side=SELL \
  type=STOP_MARKET \
  "triggerPrice=${trigger_price}" \
  closePosition=true \
  workingType=MARK_PRICE \
  priceProtect=true \
  "clientAlgoId=${TEST_CLIENT_ID}")"
TEST_ALGO_ID="$(jq -er '.algoId' <<<"${algo_order}")"
[[ "$(jq -r '.algoStatus' <<<"${algo_order}")" == "NEW" ]] \
  || fail "conditional order was created with an unexpected status"
pass "conditional protection order creation via /fapi/v1/algoOrder"

queried="$(signed_request GET /fapi/v1/algoOrder \
  "clientAlgoId=${TEST_CLIENT_ID}")"
[[ "$(jq -r '.algoId' <<<"${queried}")" == "${TEST_ALGO_ID}" ]] \
  || fail "conditional order reconciliation returned a different algoId"
pass "conditional order deterministic reconciliation"

signed_request DELETE /fapi/v1/algoOrder "algoId=${TEST_ALGO_ID}" >/dev/null
TEST_ALGO_ID=""
ALGO_CREATE_ATTEMPTED="0"
pass "conditional order cancellation"

remaining="$(signed_request GET /fapi/v1/openAlgoOrders \
  algoType=CONDITIONAL \
  "symbol=${TEST_SYMBOL}")"
[[ "$(jq --arg client_id "${TEST_CLIENT_ID}" \
  '[.[] | select(.clientAlgoId == $client_id)] | length' <<<"${remaining}")" == "0" ]] \
  || fail "test conditional order remains open after cancellation"
pass "no test order was left behind"

printf '\nALL CHECKS PASSED — Binance Demo execution API is compatible.\n'

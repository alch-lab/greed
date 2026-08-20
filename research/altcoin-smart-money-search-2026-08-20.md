# Altcoin smart-money and early-entry search — 2026-08-20

## Target

- $1,000 paper sleeve;
- $1,500 nominal per entry;
- 5–10 entries per day;
- 4% Beijing-day entry lock;
- positive train, validation, untouched holdout and recent results after fees
  and adverse slippage.

## Binance derivatives smart money

Used Binance's public five-minute futures metrics archives: open interest,
top-trader account ratio, top-trader position ratio, global account ratio and
taker long/short volume ratio. The universe is rebuilt causally from the five
highest-volume non-major contracts using only the preceding UTC day.

Tested top-position changes, top-versus-global divergence, top-account versus
global divergence, and taker-confirmed divergence. Entries occur at the next
15-minute open. The portfolio uses 1.5x equity per entry, two slots, 0.75%–2%
stops and one-, two- or four-hour exits.

The closest high-frequency candidate used top/global divergence, a 0.75% stop
and one-hour exit. Base assumptions produced +2.9% train, +11.5% validation,
+65.0% holdout and +3.9% recent at 6.8–9.0 trades/day. Doubling adverse
slippage to 10bp per side changed train to -44.1% and validation to -10.5%.
The apparent edge is smaller than realistic execution uncertainty and is
rejected.

## Cross-asset momentum propagation

Built a point-in-time top-20 liquid universe. At every completed 15-minute bar,
estimated each contract's beta and correlation from the preceding 24 hours.
When at least 75% of the market moved together by at least 0.5%, tested buying
or selling a correlated contract that lagged its beta-implied move by at least
0.5%.

The only development-positive candidate returned +2.3% train and +4.8%
validation at 6.8/3.1 trades per day. It lost 27.8% in untouched holdout and
2.9% recent; at 10bp slippage all four periods were negative. Single-token
altcoin impulses do not propagate reliably enough to trade laggards.

## Copy trading and Web3 signals

- Binance's supported Copy Trading API exposes the user's lead-trader status
  and symbol whitelist, not a stable public point-in-time history of every
  leader's entries and exits.
- Public leaderboards condition selection on surviving, currently visible
  traders. Ranking by displayed ROI, win rate or PnL cannot be reconstructed
  without deleted and failed leaders.
- Binance Web3 Smart Money signals are discrete wallet events on BSC and
  Solana. They are useful for current discovery but do not provide the required
  historical futures-mapped dataset for this test.

None is accepted as a futures execution signal.

## Decision

No public-data directional alpha tested here satisfies the target. Increasing
nominal size does not repair an edge that disappears after 10bp adverse
slippage. Do not add top-trader, copy-trader or lead-lag execution lanes to the
paper engine.

The remaining materially different research class is non-directional execution
alpha: maker grid/market making around inventory and queue imbalance. It needs
continuous L2 book updates, actual maker fill simulation, cancel latency and
adverse-selection mark-outs; OHLC and periodic snapshots cannot validate it.

## Reproduction

- `scripts/exp_altcoin_smart_money.py`
- `scripts/exp_altcoin_lead_lag.py`

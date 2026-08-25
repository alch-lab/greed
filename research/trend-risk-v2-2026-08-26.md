# Trend risk-management v2 walk-forward

## Decision

This version is suitable for another Binance Demo run. It is not approved for
real-money deployment. The funded core is `trend_continuation`; SFP remains a
half-risk Demo lane because its historical sample is too small. The ignition
lane is disabled because its recent and stress results are negative.

## Reproducible method

- Input: locally cached Binance Futures 1m archives for 46 USDT perpetuals,
  aggregated to 15m.
- Capital: 2,000 USDT; at most three positions; at most 4x gross notional.
- Standard cost: 8 bps per side. Stress cost: 15 bps per side.
- Selection: 384 parameter combinations evaluated on train and validation
  only. The 2026-08-15 through 2026-08-23 locked test was opened once after
  selection.
- Intrabar ambiguity is conservative: when stop and target are both touched,
  the stop wins.

Selected parameters:

- 4h move >= 6%, 4h path efficiency >= 45%, 12h and EMA structure aligned;
- EMA21 pullback followed by EMA8/prior-close reclaim and aligned taker flow;
- 1% stop, 2R first target, 40% partial exit;
- remaining 60% trails by 0.3% after the first target;
- after a losing trade, that symbol cannot re-enter for 180 minutes;
- 1% account risk per trend trade.

## Results

| Window | Return | Trades | Trades/day | PF | Max DD |
| --- | ---: | ---: | ---: | ---: | ---: |
| Train, 53 days | +6.08% | 43 | 0.81 | 1.17 | 10.84% |
| Validation, 22 days | +3.77% | 9 | 0.41 | 1.62 | 3.52% |
| Locked test, 8 days | +11.58% | 22 | 2.75 | 1.80 | 5.10% |
| Locked test, 15 bps/side | +7.53% | 22 | 2.75 | 1.46 | 6.48% |

Of the 28 configurations that passed train/validation gates, 96.4% were
profitable in the locked test. The median locked return was +10.52%; the range
was -1.38% to +13.93%.

## Limits

- The locked period is only eight days and is a strong-trend regime.
- The universe has survivorship bias and minute bars cannot reproduce queue
  position, WebSocket latency, exchange rejection, or stop slippage.
- Train drawdown exceeded 10% after a discrete exit, so real-money promotion is
  explicitly rejected.
- Frequency is regime-dependent and does not meet a stable 5–10 trades/day
  target. Adding the ignition lane increases activity but reduces expectancy.

## Demo acceptance gate

Run for at least seven days and require at least 15 completed exchange trades,
net PF >= 1.15, positive net PnL, max drawdown <= 10%, no missing protective
orders, no unexplained positions, and no persistent market-data gaps or HTTP
429 responses. Failing any gate means continue Demo or disable the lane; it
does not auto-promote to live.

Research scripts:

- `tools/unified_compound_research.py`
- `tools/trend_v2_walkforward.py`

Machine report: `data/alpha-backtest/trend-v2-walkforward.json`

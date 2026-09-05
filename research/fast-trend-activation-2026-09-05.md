# Fast trend activation — 2026-09-05

## Objective

Add an independently attributed paper lane that detects an altcoin trend before
the slower 4-hour continuation recipe, without turning every first volume spike
into a market-order chase.

## Rejected shortcuts

- Direct completed-5m ignition: 1,536 train/validation configurations produced
  no survivor at 12 bps round-trip cost.
- Market-synchronised 5m EMA reclaim: 4,608 configurations produced no
  train/validation survivor.
- Lowering the existing 4h trend threshold to 1%, 1.5%, or 2% produced no
  train/validation-viable regime/allocation variant. The apparent 7–9 trades a
  day in the later strong-trend window did not exist in the earlier windows.

These failures show that speed alone creates adverse selection. The deployed
paper candidate therefore keeps one short confirmation and moderate OI state.

## Frozen paper candidate

- Long-only non-major Binance USDT perpetuals.
- A completed 5m candle closes above the previous 60m range.
- 5m body at least 0.5%, volume at least 2.5x its rolling median, and buyer
  aggressor imbalance at least 15%.
- The prior 60m range is no wider than 1.05x its recent normal range; the move
  before the break is at most 1%; directional 4h return is at most 5%.
- Altcoin-market median 1h return is at least +0.2% and positive breadth is at
  least 50%.
- 15m OI change is between 0% and +1%, and 60m OI change is non-negative.
- Within three minutes, a completed 1m candle must shallow-retest and close
  back above the breakout with positive taker flow.
- Rest a post-only buy 4 bps below confirmation for at most two minutes; no
  taker fallback.
- Stop at max(1.25 ATR, 0.35%), full target at 2R, maximum hold 30 minutes.
- Paper risk is 0.5% of equity with a 1.0x-equity notional cap. The mature trend
  recipe has higher same-symbol priority, and both recipes share portfolio
  position, gross-exposure, daily-loss, cooldown, and rolling-PF gates.

## Walk-forward result

The archive contains 42 continuously available non-major contracts and about
97 days of minute bars. Entry and exit selection used only train and
validation. Audit and locked windows were read afterwards. Results below use a
more conservative 12 bps round-trip cost and the research sizing of 0.25% risk;
the runtime uses 0.5% risk but a tighter 1.0x notional cap.

| Fold | Trades | Trades/day | Win rate | PF | $1,000 PnL | Max DD |
|---|---:|---:|---:|---:|---:|---:|
| Train | 50 | 1.04 | 56.0% | 1.45 | +$26.24 | 1.09% |
| Validation | 6 | 0.32 | 66.7% | 2.95 | +$7.78 | 0.32% |
| Audit | 3 | 0.20 | 66.7% | 3.67 | +$5.42 | 0.34% |
| Locked | 8 | 0.51 | 50.0% | 1.99 | +$5.77 | 0.39% |

Across all folds this is 67 trades and +$45.21 at research sizing. It is an
earlier incremental lane, not evidence of daily profit or a high-frequency
system. The continuously available archive has survivorship bias and the later
folds have small samples, so this configuration is authorised for Binance Demo
only. Every candidate, blocker, plan, entry, fill, exit, OI state, market state,
and execution result remains journaled for the next out-of-sample decision.

Replaying the same frozen trades with the deployed 0.5% risk and 1.0x notional
cap produced +5.38%, +1.66%, +1.20%, and +1.29% in the four folds, or about
+9.81% compounded across the complete period. Maximum fold drawdown was 1.97%.
This sizing result is conditional on the historical fills and does not include
capital competition with the mature trend lane, so it is a paper expectation,
not a return promise.

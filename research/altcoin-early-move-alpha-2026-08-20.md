# Altcoin early-move alpha study — 2026-08-20

## Objective

Find a causal long/short entry that participates before an altcoin has already
completed most of its pump or dump. The target was 5–10 entries per day on a
$1,000 paper sleeve, after fees and adverse execution.

## Data and controls

- 329 Binance USDT perpetuals, 15-minute bars from 2026-05-15 to 2026-08-20.
- Train: 2026-05-22—07-01; validation: 07-01—08-01; untouched holdout:
  08-01—08-15; recent: 08-15—08-20.
- Signal on a closed bar, fill at the next open.
- 5bp taker fee and 5bp adverse slippage per side unless noted.
- Same-bar stop ambiguity resolves against the strategy.
- $1,000 compounding equity, 0.5x notional per position, two positions,
  four-hour symbol cooldown, ten entries per Beijing day, 4% daily entry lock.
- Contracts require at least 95 of the preceding 96 bars and $10m 24h volume.
  Stop exits use the next tradable open when the market gaps.

## Experiment 1: first expansion

Tested immediate continuation, next-bar continuation confirmation and rejected
expansion fade at three strictness levels and 27 exit configurations.

No candidate was positive in both train and validation. The closest strict
rejection model produced +4.59% train, -7.12% validation, -7.73% holdout and
-4.74% recent. Lowering the existing 1h/4h threshold therefore creates more
false starts rather than earlier positive-expectancy entries.

## Experiment 2: faster cross-sectional rotation

Tested 1h/2h/4h/8h/12h/24h formation against 30m/1h/2h/3h ranking intervals,
with protected exits. Results that ignored missing bars appeared extremely
profitable; realistic gap handling removed the apparent edge.

The best development candidate, 12h formation with hourly ranking, returned
+15.89% train and +51.78% validation but -2.25% holdout and -1.00% recent.
The 24h/hourly version returned +32.75% recent but lost 19.71% in the preceding
holdout. Shortening the production three-hour ranking is not robust.

## Experiment 3: spot, taker flow and OI

Downloaded 520 event-days; 500 strict next-bar confirmations had complete spot
15m and futures OI data. Tested spot direction/lead, spot and perpetual taker
delta, 15m OI expansion and perp-versus-spot premium.

Static continuation had no train/validation-positive configuration. Static
fade was positive in development but failed holdout and recent. A causal online
selector using the last 20 completed continuation/fade shadow outcomes was the
only four-period-positive candidate:

| period | return | max drawdown | trades/day | PF |
|---|---:|---:|---:|---:|
| train | +0.09% | 2.97% | 0.83 | 1.01 |
| validation | +1.69% | 5.24% | 1.61 | 1.10 |
| holdout | +0.67% | 3.47% | 4.14 | 1.04 |
| recent | +1.72% | 0.82% | 1.17 | 2.02 |

At 10bp adverse slippage per side it lost 0.81% in train and 1.93% in holdout;
at 15bp most periods were negative. This edge is too small for production and
cannot meet the rapid-capital-growth objective.

## Experiment 4: quarter-hour taker imbalance

Motivated by *The Quarter-Hour Effect* (Kim and Hansen, 2026), tested the
Binance kline taker-buy imbalance as a coarse proxy for the clock-opening flow
shock. Raw, price-aligned, two-bar persistent and absorption profiles used 4h,
8h and 12h horizons.

The best development candidate returned +130.53% train and +42.68% validation,
but -2.82% holdout. Other high-frequency variants lost about 5%–26% in holdout.
Whole-bar 15m taker flow does not reproduce the paper's seconds-around-the-mark
effect and must not be deployed as if it did.

## Experiment 5: exact first-minute clock flow

Downloaded one-minute futures bars for a causal daily universe: the five most
liquid non-major contracts were selected only from the preceding UTC day's
volume. Signals use the first completed minute at `:00`, `:15`, `:30` and
`:45`, enter at the next minute open, and never use the remainder of the
quarter-hour candle.

The train/validation-selected configuration requires at least $50,000 quote
volume in that minute and absolute taker imbalance of 35%. It follows the flow,
uses 0.5x sleeve equity, a 3% price stop and a 12-hour maximum hold.

| period | return | max drawdown | trades/day | PF |
|---|---:|---:|---:|---:|
| train | +97.62% | 26.90% | 5.15 | 1.31 |
| validation | +15.25% | 24.44% | 4.77 | 1.11 |
| holdout | +7.33% | 13.92% | 4.93 | 1.10 |
| recent | +13.86% | 9.14% | 4.50 | 1.52 |

With adverse slippage doubled from 5bp to 10bp per side, all four periods stay
positive: +65.58%, +2.79%, +2.95% and +12.80%. At 15bp per side the edge no
longer survives all periods. The base recent result is tail-dependent: the top
five winners account for 80.92% of gross profit, which is consistent with the
objective of catching rare expansions but makes drawdown and execution quality
critical.

A minute-of-quarter placebo found no general candle effect. Minute zero and
minute twelve were the only phases positive in all four periods at both 5bp
and 10bp slippage; combining them lost money in validation and holdout. Minute
twelve was discovered after inspecting the test and is therefore rejected.
Only the pre-specified clock-opening hypothesis remains eligible.

Sizing at 0.75x–1.0x increased development drawdown to 34%–44%. At 1.5x and
2.0x, train fell 64%/75%, recent fell 21%/24%, and drawdown reached 87%.
The 0.5x setting is retained: a stopped trade costs about 1.5% of sleeve
equity, while two simultaneous stops stay below the 4% daily entry lock before
slippage.

## Production decision

- Remove the failed 15m shock-reversal execution path.
- Prevent independent sleeves from reversing the same symbol for four hours.
- Keep historical shock labels readable for journal migration only.
- Do not lower the production breakout thresholds or shorten rotation merely
  to manufacture trades.
- Promote only the exact minute-zero flow model to a paper-execution candidate.
  It must keep the causal top-five universe, next-minute entry, actual spread
  gate and 0.5x sizing; it is not approved for real capital.
- Do not deploy the post-hoc minute-twelve variant or combine it with minute
  zero.

## Required execution-validation dataset

The one-minute archive is sufficient to justify a paper pilot, but not real
capital. Persist continuously for the liquid candidate universe:

- aggTrades in 1s/5s/10s/30s/60s buckets for spot and perpetual;
- book updates, cancellations, replenishment and queue imbalance near the best
  bid/ask, not only periodic depth snapshots;
- mark/index/spot basis and OI at one-minute or faster cadence;
- liquidation bursts and exact clock phase;
- actual spread, impact, fill latency and post-fill 5s/30s/1m/5m mark-outs.

The paper pilot must validate actual spread, fill and 5s–5m mark-outs around
the first 60 seconds of the clock shock. Until that evidence exists, claiming
reliable real-money access to the first leg would be overfitting.

## Reproduction

- `scripts/exp_altcoin_early_event.py`
- `scripts/exp_altcoin_rotation_horizon.py`
- `scripts/exp_altcoin_spot_oi_early.py`
- `scripts/exp_altcoin_quarter_hour_flow.py`
- `scripts/exp_altcoin_clock_open_1m.py`

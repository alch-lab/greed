# Altcoin shock-reversal alpha — 2026-08-19

## Objective

Find a causal, directly tradable altcoin sleeve that can produce roughly 5–10
paper entries per active day without relying on the existing 3-hour
cross-sectional rotation.

## Data and protocol

- Binance Futures Testnet public 15-minute klines, intersected with Binance
  spot symbols: 329 symbols.
- Data range: 2026-05-15 through 2026-08-19.
- Major symbols excluded exactly as in production.
- Signals use closed bars only and execute at the next 15-minute open.
- Portfolio starts at $1,000, compounds, holds at most two fast-alpha
  positions, permits at most ten entries per Beijing day, and stops adding
  entries after a 4% daily settled-equity loss. Production is stricter because
  its gate uses Binance mark-to-market equity.
- Every fill pays 5 bps taker fee per side. The production gate assumes an
  additional 10 bps adverse slippage per side (30 bps round trip total).
- Same-bar ambiguity is resolved against the strategy: an already active stop
  is evaluated before a new trailing level.

## Selected rule

1. The prior hour moved 4.5%–22% in one direction.
2. The current 15-minute bar sweeps the prior two-hour high/low and closes at
   least 0.6% in the opposite direction.
3. Current quote volume is at least 2.5x its prior seven-day 15-minute mean,
   directional close location is at least 68%, and candle range is at most
   15%.
4. Long and short signals have independent causal gates. The latest 30
   completed shadow outcomes must have positive cumulative return and profit
   factor at least 1.0 after modeled costs.
5. Strict bars (volume >=3.5x, close location >=75%, range <=12%) use 1.0x
   equity notional; other qualifying bars use 0.5x.
6. Exit with a 2% hard stop, activate a 1% trailing distance after +3%, or
   close after three hours. No partial take profit.

## Out-of-sample results at the production gate cost

| Segment | Return | Max drawdown | Trades | Trades/day | Profit factor |
|---|---:|---:|---:|---:|---:|
| Train, 2026-06-01–07-10 | +5.6% | 33.0% | 157 | 4.0 | 1.03 |
| Validation, 2026-07-10–08-01 | +8.5% | 17.0% | 63 | 2.9 | 1.13 |
| Holdout, 2026-08-01–08-15 | +9.4% | 21.4% | 64 | 4.6 | 1.13 |
| Recent, 2026-08-15–08-19 | +11.5% | 8.4% | 29 | 5.8 | 1.44 |
| Continuous, 2026-06-01–08-19 | +45.7% | 44.4% | 311 | 3.9 | 1.12 |

At the less conservative 5 bps slippage assumption, holdout returned +56.2%
and recent returned +18.7%. At 15 bps slippage per side the older segments
failed, so the live spread/depth/impact checks are part of the alpha, not an
optional execution optimization.

Recent Beijing-day entry counts under the 5 bps execution scenario were 7,
10, 10, and 2. The model should be described as averaging near the requested
frequency during active shock regimes, not as a promise of five entries every
calendar day.

## Deployment decision

Deploy to paper only. Keep the 10 bps spread, depth, impact, recent-trade and
price-freshness checks as hard gates. Persist every detected signal, gate
state, liquidity rejection, entry, protection update, exit, MFE and MAE so the
paper sample can test whether real execution costs remain below the failure
boundary.

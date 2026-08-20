# Cross-section execution review — 2026-08-19

## Question

Explain the repeated GPSUSDT rotations, add profit protection, verify whether
the strategy should be symmetric long/short, and define how it coexists with
the new 15-minute shock-reversal alpha.

## Replay contract

- Source: Binance Futures Testnet public 15-minute bars, 2026-08-10 through
  2026-08-17 (528 contracts).
- Universe proxy: 396 symbols also present in the locally cached Binance spot
  universe; the production major-coin exclusions and $10m 24-hour quote-volume
  floor are applied.
- Causal timing: ranks use only closed bars and enter at the next bar open.
- Costs: 5 bps taker fee plus 5 bps adverse slippage on every fill.
- Sizing: fixed 0.40x current equity, starting with $1,000, to isolate execution
  behavior from the rolling quality-size gate.
- The replay cannot reconstruct historical order-book spread/depth, funding,
  leverage availability, or the exact historical spot-listing set. It is an
  execution study, not a live-performance forecast.

## Findings

1. The production direction rule is not short-only. It goes long the strongest
   coin when the universe median 12-hour return is at least +1%; otherwise it
   shorts the weakest coin. A fully symmetric tail rule performed worse in this
   sample, so the validated short bias is retained.
2. Closing and reopening the same symbol in the same direction every three
   hours is unnecessary churn. A repeated winner/loser now carries the original
   position and its tightened protection. Three hours is a reranking boundary,
   not a mandatory exit.
3. Profit protection at +2% cut trends too early. A wider neighborhood search
   supports a +5% activation for this high-offense sleeve: realize 33%, then
   trail the remaining position 2% behind its favorable extreme.
4. Initial hard stop is reduced from 8% to 4%.
5. A completed stop or take-profit no longer leaves the cross-section slot idle
   until the next three-hour boundary. The engine reranks on each following
   closed 15-minute bar, excludes every symbol already exited in the current
   three-hour window, and requires at least 12% return excess over the universe
   median before entering a replacement. An unconditional replacement rule
   increased trades but reduced replay return, so the quality floor is retained.

## Selected replay

Parameters: validated direction rule, same-signal carry, 4% hard stop, +5%
partial activation, 33% partial size, 2% trailing distance, and post-exit 15m
reranking with a 12% replacement-excess floor.

| Metric | Result |
|---|---:|
| Starting equity | $1,000.00 |
| Ending equity | $1,370.47 |
| Net return after modeled costs | +37.05% |
| Entries / completed trades | 67 / 67 |
| Winning trades | 34 |
| Win rate | 50.75% |
| Modeled fees | $31.78 |
| Maximum realized-equity drawdown | 6.62% |

| UTC date | Entries | Net P&L |
|---|---:|---:|
| 2026-08-10 | 2 | +$17.63 |
| 2026-08-11 | 10 | -$4.31 |
| 2026-08-12 | 10 | +$1.65 |
| 2026-08-13 | 10 | +$166.34 |
| 2026-08-14 | 8 | +$10.45 |
| 2026-08-15 | 10 | +$208.39 |
| 2026-08-16 | 10 | +$19.38 |
| 2026-08-17 (partial) | 7 | -$49.06 |

The old mandatory three-hour exit returned +20.90% in the same replay but had
12.74% maximum realized-equity drawdown and no intra-hold profit protection.
The selected rule improves the replay result while removing the GPS-style
close/reopen churn. Performance is concentrated in two days, so it must still
be treated as high-risk and sample-dependent.

## Strategy coordination

- BTC MR has its own $1,000 accounting sleeve and BTC-only position.
- Shock reversal and cross-section share the altcoin sleeve, daily loss guard,
  daily entry count, and two regular position slots.
- Shock reversal has execution priority when both produce a fresh candidate,
  but an existing position is never forcibly replaced just because a higher
  priority signal arrives.
- Same-symbol concurrent positions are forbidden. A same-symbol opposite
  signal is logged and the existing position wins.
- The exhaustion-short probe keeps its independent slot and gross cap, but it
  is still forbidden from opening the same symbol as another altcoin strategy.
- Each alpha keeps its own sizing and exit parameters. They share risk capacity,
  not identical order size.

The reproducible replay is `scripts/exp_cross_section_carry_exit.py`.

# Cross-section multi-name experiment — 2026-08-21

## Question

Compare selecting 1, 2, 3, 5, 6 or 10 same-direction cross-sectional extremes under
the production 12h formation / 3h rebalance direction rule.

## Data and execution assumptions

- Binance Futures Testnet public 15m bars, 2026-05-15 through 2026-08-20.
- 329 contracts before the production exclusions and $10m trailing 24h volume filter.
- Closed-bar signal, next-open fill.
- 5bp taker fee plus 5bp adverse slippage on entry and exit.
- 4% initial stop; at +5%, realize 33%, then trail the remainder by 2%.
- Production rolling 30-basket quality gate: 0.05x total gross while closed,
  0.40x while open, and 0.50x for a strong extreme.
- Total basket gross is shared equally by the selected names. It is not
  multiplied by N.

The replay does not reproduce the final live spread/depth rejection or spot
co-list requirement. It therefore measures ranking alpha, not expected fills.
It also rotates at each 3h boundary rather than preserving unchanged legs, so
cost assumptions are conservative for carried names.

## Production-sizing results

| Names | Full return | Full max DD | August return | Recent 10d return | Recent DD | Entries / active day |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | +17.29% | 18.27% | +28.92% | +21.70% | 9.41% | 7.0 |
| 2 | -0.73% | 22.61% | +17.59% | +20.25% | 7.33% | 15.6 |
| 3 | -4.01% | 23.59% | +10.86% | +13.98% | 8.64% | 24.0 |
| 5 | +18.37% | 13.17% | +33.78% | +24.94% | 2.43% | 40.0 |
| 6 | +11.99% | 12.20% | +24.08% | +18.73% | 3.38% | 48.0 |
| 10 | -3.10% | 10.18% | +6.79% | +6.80% | 5.13% | 80.0 |

June / July returns were negative for both 1-name and 5-name variants:

- 1 name: -2.78% / -3.11%
- 5 names: -5.01% / -4.92%

With 15bp adverse slippage per side, full-period returns were -4.70% for one
name, +3.90% for five, -4.37% for six and -11.45% for ten. At 30bp per side,
all variants lost money.

## Risk-multiplying counterexample

Keeping 0.5x gross *per name* instead of per basket produced +215.84% in August
for five names, but lost 65.28% over the full window with a 92.05% max drawdown.
That is leverage/regime exposure, not a better ranking alpha, and must not be
used.

## Conclusion

Two and three names dilute the extreme without enough diversification. Five
names is the clear optimum in this tested grid: six already loses roughly a
third of the five-name return, while ten dilutes the signal to a small full-
period loss despite lower headline drawdown. Five is the only multi-name
variant worth a paper test. The improvement is not stable in June and July, so
this is a portfolio-construction improvement rather than a new alpha.

Production cannot be changed by setting `names = 5` alone. Today every selected
candidate requests the entire basket gross, so the first accepted order would
consume the cap and later names would be skipped. A real implementation must
allocate `basket_gross / names`, keep per-leg carry/replacement state, and log
both basket and leg attribution.

Reproducer: `scripts/exp_cross_section_multi_name.py`.

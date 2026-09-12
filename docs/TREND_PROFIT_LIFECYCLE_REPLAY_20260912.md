# Trend profit lifecycle replay — 2026-09-12

## Question

Would the current executable-profit lifecycle avoid reducing the manually
closed `1000PEPEUSDT` second leg from `$169.80` to the roughly `$7.56` static
profit floor used by the earlier conservative replay?

## Causal inputs

- Entry: short `4,561,923` at `0.0033234`, actual notional `$15,161.09`.
- Source: epoch-19 `exchange_entry` plus 1,135 execution-venue depth
  observations recorded before the operator close.
- Returns are execution-VWAP returns after the runtime's conservative 10 bps
  round-trip cost reserve. No later price is used to arm or ratchet a floor.
- Observed peak executable net return: `1.148721%`.
- The deployed lifecycle caps a restored second leg's trailing handoff at a
  1.0% gross move and trails the executable peak by 0.5%.

## Result

| Policy | Causal automatic result for 1000PEPE | Difference |
|---|---:|---:|
| Legacy static protected floor | `$7.56` | baseline |
| 1.0% handoff + 0.5% executable peak giveback | `$97.06` | `+$89.50` |
| Operator close (diagnostic only) | `$169.80` | not strategy PnL |

The new lifecycle first becomes executable at the observed short-lived peak,
then exits on the subsequent pullback at about `0.640206%` net. It therefore
does not claim the later operator timing, but it preserves materially more of
the move without hindsight.

## Whole-period comparison

The authoritative epoch-19 account series starts at `$10,000.00` and ends at
`$10,224.56`, with `$229.84` realized PnL and `$2,237.66` still open at the
archive cutoff. That observed result includes the operator's `$169.80`
1000PEPE close.

For an apples-to-apples exit-only replay, every other fill and the final open
position mark are frozen. Only that operator close is replaced:

| Epoch-19 account basis | Old automatic floor | New causal lifecycle | Difference |
|---|---:|---:|---:|
| End-equity PnL | `$62.33` | `$151.82` | `+$89.50` |
| Realized PnL | `$67.61` | `$157.10` | `+$89.50` |

The previously quoted `$377.09` is intentionally excluded. It came from a
conditional research replay that also synthesized other reviewed execution and
liquidation changes; it was not the PnL of the running old binary and therefore
is not a valid old-version baseline for this comparison.

Missed candidates and any future same-side re-entry after the new exit cannot
be reconstructed from this history, so neither is credited.

## Runtime change

An executable-profit exit no longer creates an immediate opposite position.
It can arm the existing same-side, reset-and-resume confirmation after a main
trend leg. A genuine opposite move must independently pass a normal funded
recipe. This separates profit harvesting from direction discovery and avoids
turning a routine pullback into a blind reversal chain.

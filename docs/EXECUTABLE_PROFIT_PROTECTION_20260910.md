# Executable-profit protection deployment

## Enabled changes

- `execution.executable_profit_guard = true` in the Demo recipe. Older configs
  without this key retain the old behavior.
- Every eligible remaining position is valued using `/fapi/v1/depth` on the
  **configured execution base URL**, not the signal provider's public book.
  Long exits walk bids; short exits walk asks; the full remaining quantity must
  fit. Invalid/crossed/unsorted, missing, future (>250ms), or stale (>1500ms)
  snapshots are unscorable. Parallel read-only requests have a 750ms timeout;
  account mutation remains serialized in the existing runtime owner.
- Estimated return subtracts a conservative 10bps reserve (4 entry, 4 exit,
  2 stress). Funding is not included. It is not an exact realized net PnL or a
  guaranteed fill; spread/depth impact already enters the VWAP and is not
  subtracted again. Existing gross activation/floor settings are translated
  to this same estimated-net basis, with a nonnegative floor.
- Once activated, giveback to the estimated-net floor requests a reduce-only
  close. The hosted stop remains the backstop. Mark-price extrema still support
  diagnostics/early-failure logic; profit ratchets use valid executable extrema.
- All full-close paths now retain the hosted stop until an account reread
  confirms flatness. Partial or ambiguous closes remain protected and are
  reconciled on subsequent syncs. This is not an atomic exchange bracket;
  network outages and price gaps can still cause losses.
- Profitable Trend exits through this path retain the existing second-leg
  eligibility checks. No new strategy slot or opposite-direction entry added.
- Managed liquidation exits are enabled for NEW positions: protection after
  25bps, floor 10bps gross, trail from 50bps with 30bps distance, maximum 15min.
  Old fixed-time positions keep their persisted settings.

## State and deployment

Epoch remains **19**, initial virtual equity remains **$10,000**. No entry
threshold, funded risk budget, or deployed minimum-liquidity-size setting is
changed by this deployment. Do not delete runtime state, history or research.

`ExecutionMeta.executable_profit` is optional with a serde default. Legacy
positions start without fabricated executable peaks. An existing non-fixed
position acquires a peak only from a fresh post-upgrade quote. Peaks/floors are
persisted before exit submission and survive restart; quantity changes reset
the remaining-position estimator without removing hosted protection.

UI exposes the latest estimated net value, peak, floor and its observation time;
old servers without the fields still render. Exit history labels the new reason.

The deploy script asserts `EXPECTED_EPOCH` (or the pre-pull epoch) before any
build/restart. A mismatch stops deployment rather than silently resetting
accounting. Callers pulling a new script first must capture/pass the epoch
before that pull.

## Verification and limits

Workspace Rust tests, fmt, Clippy warnings-as-errors, frontend TypeScript/Vite
build, and deployment shell syntax are checked. Regression cases cover long/
short VWAP, insufficient/stale/malformed quotes, restart, no historical peak,
quantity changes, and non-loosening trailing floor. No authenticated orders
were submitted during development; exchange outage/race behavior is not
claimed to have been integration-tested on a live account.

The previous +$21.86 conditional replay improvement applies to managed
liquidation exits, **not this newly combined executable-profit version**.
Historical Demo depth around exits is incomplete, so do not claim a full
historical profit improvement for this new mode. New observation and rejection
events explicitly record the estimator/source/cost reserve for validation.

API reference: [Binance order book](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Order-Book).

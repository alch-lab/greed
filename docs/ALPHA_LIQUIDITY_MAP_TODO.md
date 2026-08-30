# Altcoin liquidity-map alpha TODO

Status: data collection and observe-only map enabled; no order generation.

## Hypothesis

Recent pivot clusters, volume-heavy rejection zones and breakout bases identify where latent
liquidity is likely to sit. The level is context, not an entry. Live derivatives positioning and
price response must decide between rejection, absorption and genuine breakout continuation.

## State machine to test

1. `far`: price is more than 0.5 ATR from the nearest qualified level.
2. `approaching`: price is within 0.5 ATR; capture book, aggressor flow, OFI and liquidations.
3. `testing`: price trades through or rejects the level by at most 0.3 ATR.
4. One of:
   - `rejection`: flow is aggressive but price progress stalls, then OFI/return reverses;
   - `absorption`: liquidation and volume spike while price holds/reclaims the level;
   - `breakout`: spot/perpetual participation agrees and a retest holds outside the level;
   - `invalid`: price crosses more than 0.5 ATR without the required response.
5. If promoted to execution, use structural invalidation and ATR-normalized sizing; never place a
   blind limit order solely because a historical price was reached.

## Data now collected

- 1m: 360 bars (6 hours)
- 5m: 576 bars (2 days)
- 15m: 1,200 bars (12.5 days)
- 1h: 720 bars (30 days)
- 15-second book spread/depth/slippage, trade imbalance, OFI, price impact and liquidations
- 60-second liquidity maps with nearest support/resistance, touches, age, volume strength and ATR
- forward returns at 10s, 30s, 1m, 3m, 5m and 15m plus path MFE/MAE

## Remaining instrumentation

- Binance all-symbol open-interest changes at 1m/5m/15m
- funding, mark/index basis and next-funding countdown
- spot/perpetual lead-lag for symbols with a liquid Binance spot market
- optional cross-venue confirmation after the Binance-only hypothesis has evidence

These inputs belong in a non-blocking background collector. They must never delay account sync,
stop management or the five-second trading loop.

## Promotion criteria

Do not turn this observer into an order-producing lane until all conditions pass:

- at least seven complete UTC days and at least 200 level tests;
- strict chronological train/validation/locked test split;
- no use of a candle before its close and no universe survivorship leakage;
- maker/taker fees plus observed spread and slippage charged to every replay;
- positive locked-test expectancy and profit factor above 1.15;
- positive results on at least four separate days, not one outlier coin;
- stable results after removing the best symbol and the best trading day;
- rejection and breakout variants evaluated independently;
- account-level replay with existing lanes, position limits and drawdown guard.

## Analysis output

Report event count, fillable count, win rate, expectancy, profit factor, MFE/MAE, holding time,
cost drag and PnL by day/symbol/direction/level age. Also report every rejected hypothesis so the
same idea is not repeatedly rediscovered and overfit.

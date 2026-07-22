//! 性能回归：空策略回放 100 万笔合成逐笔成交（自包含，不依赖外部数据）。
//! 验收：空策略跑 1 个月数据 < 30s。此处 1M 笔（约 1.3 天真实密度）应 < 2s。
use tcore::types::{Exchange, Price, Qty, Symbol, Timestamp};
use tcore::Trade;

#[test]
fn replay_1m_synthetic_trades_under_2s() {
    let symbol = Symbol::new("BTCUSDT");
    let trades: Vec<Trade> = (0..1_000_000)
        .map(|i| Trade {
            ts: Timestamp::from_millis(1_704_067_200_000 + i * 80),
            exchange: Exchange::BinanceFutures,
            symbol: symbol.clone(),
            price: Price::from_f64(67000.0 + ((i / 500) % 100) as f64),
            qty: Qty::from_f64(0.01 + (i % 10) as f64 * 0.001),
            is_buyer_maker: i % 2 == 0,
        })
        .collect();

    let toml = "[strategy]\ntrigger = \"NoopTrigger\"\n";
    let strat = strategy::assemble_from_toml(toml, &strategy::builtin_registry()).unwrap();
    let mut eng = backtest::BacktestEngine::new(strat, symbol, backtest::BacktestConfig::default());
    let t0 = std::time::Instant::now();
    let res = eng.run(&trades);
    let el = t0.elapsed();
    println!(
        "回放 {} 笔耗时 {:?}（{:.0} 笔/秒）",
        trades.len(),
        el,
        trades.len() as f64 / el.as_secs_f64()
    );
    assert!(res.fills.is_empty());
    assert_eq!(res.final_equity, res.initial_cash);
    assert!(el.as_secs() < 2, "回放 1M 笔应在 2s 内，实际 {:?}", el);
}

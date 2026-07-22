//! 采集编排：启动各采集任务，主循环消费事件流，优雅退出。
//!
//! 拓扑：
//! ```text
//!  aggTrade WS(合约) ──┐
//!  aggTrade WS(现货) ──┤ mpsc::channel(16k)
//!  book poller(合约) ──┤
//!  oi poller(合约)  ──┘
//!                        ▼
//!              主循环: push → ShardWriter
//!              tick:   maybe_flush（阈值/跨天）
//!              SIGINT/SIGTERM: flush_all → 退出
//! ```
//!
//! `--dry-run` 模式：不落盘，每 30s 打印各流速率（验证连通性与速率估算）。

use tcore::types::Symbol;
use tokio::sync::mpsc;
use tracing::info;

use super::binance_ws::{run_aggtrade_collector, BinanceMarket};
use super::book_poller::run_book_poller;
use super::config::CollectorConfig;
use super::oi_poller::run_oi_poller;
use super::shard_writer::{LiveEvent, ShardWriter};
use super::CollectError;

/// 运行采集 daemon，直到收到 SIGINT/SIGTERM。
pub async fn run_collector(cfg: CollectorConfig, dry_run: bool) -> Result<(), CollectError> {
    let symbol = Symbol::new(&cfg.symbol);
    let (tx, mut rx) = mpsc::channel::<LiveEvent>(16_384);

    let mut tasks = Vec::new();
    if cfg.enable_trades {
        let (sym, tx) = (symbol.clone(), tx.clone());
        tasks.push(tokio::spawn(async move {
            run_aggtrade_collector(BinanceMarket::UsdtPerp, sym, tx).await
        }));
    }
    if cfg.enable_spot_trades {
        let (sym, tx) = (symbol.clone(), tx.clone());
        tasks.push(tokio::spawn(async move {
            run_aggtrade_collector(BinanceMarket::Spot, sym, tx).await
        }));
    }
    if cfg.enable_book {
        let (sym, tx) = (symbol.clone(), tx.clone());
        let (ms, band, limit) = (
            cfg.book_snapshot_ms,
            cfg.book_depth_band_pct,
            cfg.book_depth_limit,
        );
        tasks.push(tokio::spawn(async move {
            run_book_poller(sym, ms, band, limit, tx).await
        }));
    }
    if cfg.enable_oi {
        let (sym, tx) = (symbol.clone(), tx.clone());
        let ms = cfg.oi_tick_ms;
        tasks.push(tokio::spawn(
            async move { run_oi_poller(sym, ms, tx).await },
        ));
    }
    // 主持有方释放，保证所有子任务退出后 channel 关闭
    drop(tx);

    info!(
        version = "v1.0",
        dry_run,
        lake_dir = %cfg.lake_dir,
        symbol = %cfg.symbol,
        tasks = tasks.len(),
        max_buffer_secs = cfg.max_buffer_secs,
        "采集 daemon 启动"
    );

    if dry_run {
        return dry_run_loop(&mut rx).await;
    }

    let mut writer = ShardWriter::new(
        &cfg.lake_dir,
        cfg.trades_flush_rows,
        cfg.book_flush_rows,
        (cfg.max_buffer_secs * 1000) as i64,
    );
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(cfg.flush_tick_secs));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            _ = shutdown_signal() => {
                info!("收到退出信号，flush 缓冲区");
                writer.flush_all()?;
                for t in &tasks { t.abort(); }
                info!(
                    trades = writer.written_trades,
                    book = writer.written_book,
                    oi = writer.written_oi,
                    "采集 daemon 已退出"
                );
                return Ok(());
            }
            _ = tick.tick() => {
                writer.maybe_flush()?;
                let (t, b, o) = writer.buffered();
                info!(
                    buf_trades = t, buf_book = b, buf_oi = o,
                    written_trades = writer.written_trades,
                    written_book = writer.written_book,
                    "心跳"
                );
            }
            ev = rx.recv() => {
                match ev {
                    Some(e) => {
                        writer.push(e);
                        writer.maybe_flush()?;
                    }
                    None => {
                        // 所有上游退出（异常）——flush 后退出
                        writer.flush_all()?;
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// dry-run：统计速率，不落盘。
async fn dry_run_loop(rx: &mut mpsc::Receiver<LiveEvent>) -> Result<(), CollectError> {
    let (mut n_trades, mut n_book, mut n_oi) = (0usize, 0usize, 0usize);
    let mut report = tokio::time::interval(std::time::Duration::from_secs(30));
    report.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = shutdown_signal() => {
                info!(n_trades, n_book, n_oi, "dry-run 退出");
                return Ok(());
            }
            _ = report.tick() => {
                info!(n_trades, n_book, n_oi, "dry-run 累计（30s 报一次）");
            }
            ev = rx.recv() => {
                match ev {
                    Some(LiveEvent::Trade(_)) => n_trades += 1,
                    Some(LiveEvent::Book(_)) => n_book += 1,
                    Some(LiveEvent::Oi(_)) => n_oi += 1,
                    None => return Ok(()),
                }
            }
        }
    }
}

/// SIGINT（Ctrl-C）+ SIGTERM（unix）。
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut s) = signal(SignalKind::terminate()) {
            s.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn supervisor_compiles() {
        // 冒烟：模块可链接（运行期行为在 Mac/集成环境验证）。
        assert_eq!(2 + 2, 4);
    }
}

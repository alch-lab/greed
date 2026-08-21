use data::live::binlog::read_trade_log;
use std::path::Path;

fn pct(x: f64) -> String {
    format!("{:+.4}", x * 100.0)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("用法: {} <binlog文件>", args[0]);
        std::process::exit(1);
    }

    let trades = read_trade_log(Path::new(&args[1])).expect("读取失败");
    println!("总记录数: {}", trades.len());

    // 按秒聚合
    let mut sec_data: Vec<(i64, f64, f64, f64, f64)> = Vec::new();
    let mut cur_sec = 0i64;
    let mut px_first = 0.0_f64;
    let mut px_last = 0.0_f64;
    let mut buy_usd = 0.0_f64;
    let mut sell_usd = 0.0_f64;

    for t in &trades {
        let sec = t.ts_ms / 1000;
        let price = t.price_raw as f64 / 100_000_000.0;
        let qty = t.qty_raw as f64 / 100_000_000.0;
        let usd = price * qty;

        if sec != cur_sec && cur_sec != 0 {
            sec_data.push((cur_sec, px_first, px_last, buy_usd, sell_usd));
            px_first = price;
            buy_usd = 0.0;
            sell_usd = 0.0;
        }
        if cur_sec == 0 {
            px_first = price;
        }
        cur_sec = sec;
        px_last = price;
        if t.is_buyer_maker {
            sell_usd += usd;
        } else {
            buy_usd += usd;
        }
    }
    if cur_sec != 0 {
        sec_data.push((cur_sec, px_first, px_last, buy_usd, sell_usd));
    }

    println!("秒级聚合数: {}", sec_data.len());

    // === 1. Volume Imbalance 分析 ===
    println!("\n=== Volume Imbalance 分析 (60s窗口, 预测60s) ===");
    let window = 60usize;
    let mut wins_buy = 0u32;
    let mut wins_sell = 0u32;
    let mut total_buy = 0u32;
    let mut total_sell = 0u32;
    let mut buy_pnl = 0.0_f64;
    let mut sell_pnl = 0.0_f64;

    for i in window..sec_data.len().saturating_sub(60) {
        let (_, _, px_last_i, _, _) = sec_data[i];
        let delta = sec_data
            .iter()
            .take(i)
            .skip(i - window)
            .map(|row| row.3 - row.4)
            .sum::<f64>();
        let future_px = sec_data[i + 60].2;
        let ret = future_px / px_last_i - 1.0;
        let threshold = 1_000_000.0;

        if delta > threshold {
            total_buy += 1;
            if ret > 0.0 {
                wins_buy += 1;
            }
            buy_pnl += ret;
        } else if delta < -threshold {
            total_sell += 1;
            if ret < 0.0 {
                wins_sell += 1;
            }
            sell_pnl += -ret;
        }
    }

    if total_buy > 0 {
        let wr = wins_buy as f64 / total_buy as f64;
        println!(
            "买方信号 (delta > 1M):  {}笔, 胜率={:.1}%, 总收益={}%",
            total_buy,
            wr * 100.0,
            pct(buy_pnl)
        );
    }
    if total_sell > 0 {
        let wr = wins_sell as f64 / total_sell as f64;
        println!(
            "卖方信号 (delta < -1M): {}笔, 胜率={:.1}%, 总收益={}%",
            total_sell,
            wr * 100.0,
            pct(sell_pnl)
        );
    }

    // === 2. 大单冲击分析 ===
    println!("\n=== 大单冲击分析 (逐笔 >100K USD, 后30s) ===");
    let mut large_buy_wins = 0u32;
    let mut large_buy_total = 0u32;
    let mut large_buy_pnl = 0.0_f64;
    let mut large_sell_wins = 0u32;
    let mut large_sell_total = 0u32;
    let mut large_sell_pnl = 0.0_f64;

    for (i, t) in trades.iter().enumerate() {
        let price = t.price_raw as f64 / 100_000_000.0;
        let qty = t.qty_raw as f64 / 100_000_000.0;
        let usd = price * qty;
        if usd < 100_000.0 {
            continue;
        }

        let t_end = t.ts_ms + 30_000;
        let mut future_px = price;
        for j in (i + 1)..trades.len() {
            if trades[j].ts_ms > t_end {
                if j > i + 1 {
                    future_px = trades[j - 1].price_raw as f64 / 100_000_000.0;
                }
                break;
            }
            if j == trades.len() - 1 {
                future_px = trades[j].price_raw as f64 / 100_000_000.0;
            }
        }

        let ret = future_px / price - 1.0;
        if t.is_buyer_maker {
            large_sell_total += 1;
            if ret < 0.0 {
                large_sell_wins += 1;
            }
            large_sell_pnl += -ret;
        } else {
            large_buy_total += 1;
            if ret > 0.0 {
                large_buy_wins += 1;
            }
            large_buy_pnl += ret;
        }
    }

    if large_buy_total > 0 {
        let wr = large_buy_wins as f64 / large_buy_total as f64;
        println!(
            "大买单冲击 (>100K):   {}笔, 胜率={:.1}%, 总收益={}%",
            large_buy_total,
            wr * 100.0,
            pct(large_buy_pnl)
        );
    }
    if large_sell_total > 0 {
        let wr = large_sell_wins as f64 / large_sell_total as f64;
        println!(
            "大卖单冲击 (>100K):   {}笔, 胜率={:.1}%, 总收益={}%",
            large_sell_total,
            wr * 100.0,
            pct(large_sell_pnl)
        );
    }

    // === 3. 时段效应 ===
    println!("\n=== 时段效应 (按小时) ===");
    let mut hour_ret = vec![(0.0_f64, 0u32); 24];
    for i in 1..sec_data.len() {
        let sec = sec_data[i].0;
        let hour = ((sec % 86400) / 3600) as usize;
        let ret = sec_data[i].2 / sec_data[i - 1].2 - 1.0;
        hour_ret[hour].0 += ret;
        hour_ret[hour].1 += 1;
    }
    for (h, (sum, count)) in hour_ret.iter().copied().enumerate() {
        if count > 0 {
            let avg = sum / count as f64;
            let session = if h < 7 {
                "asia"
            } else if h < 13 {
                "europe"
            } else {
                "us"
            };
            println!(
                "  {:02}:00 ({:>6}): 秒均收益={} (n={})",
                h,
                session,
                pct(avg),
                count
            );
        }
    }

    // === 4. 波动率与动量 ===
    println!("\n=== 波动率与动量 (60s 窗口, 预测60s) ===");
    let mut low_vol_wins = 0u32;
    let mut low_vol_total = 0u32;
    let mut high_vol_wins = 0u32;
    let mut high_vol_total = 0u32;

    for i in 60..sec_data.len().saturating_sub(60) {
        let mut px_min = f64::MAX;
        let mut px_max = 0.0_f64;
        for row in sec_data.iter().take(i).skip(i - 60) {
            px_min = px_min.min(row.1);
            px_max = px_max.max(row.2);
        }
        let range = (px_max - px_min) / sec_data[i].2;
        let ret = (sec_data[i + 60].2 - sec_data[i].2).abs() / sec_data[i].2;

        if range < 0.0005 {
            low_vol_total += 1;
            if ret > 0.001 {
                low_vol_wins += 1;
            }
        }
        if range > 0.002 {
            high_vol_total += 1;
            if ret > 0.001 {
                high_vol_wins += 1;
            }
        }
    }

    if low_vol_total > 0 {
        println!(
            "  低波动期 (range<0.05%): {}次, 显著移动率={:.1}%",
            low_vol_total,
            low_vol_wins as f64 / low_vol_total as f64 * 100.0
        );
    }
    if high_vol_total > 0 {
        println!(
            "  高波动期 (range>0.20%): {}次, 显著移动率={:.1}%",
            high_vol_total,
            high_vol_wins as f64 / high_vol_total as f64 * 100.0
        );
    }
}

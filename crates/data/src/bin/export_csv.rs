use data::live::binlog::read_trade_log;
use std::path::Path;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("用法: {} <binlog文件> <输出csv>", args[0]);
        std::process::exit(1);
    }
    
    let trades = read_trade_log(Path::new(&args[1])).expect("读取失败");
    
    let mut w = csv::Writer::from_path(&args[2]).expect("创建CSV失败");
    w.write_record(&["ts_ms", "price", "qty", "is_buyer_maker", "taker_side"]).unwrap();
    
    let mut count = 0;
    for t in trades {
        let price = t.price_raw as f64 / 100_000_000.0;
        let qty = t.qty_raw as f64 / 100_000_000.0;
        let taker_side = if t.is_buyer_maker { "sell" } else { "buy" };
        w.write_record(&[
            t.ts_ms.to_string(),
            format!("{:.2}", price),
            format!("{:.8}", qty),
            t.is_buyer_maker.to_string(),
            taker_side.to_string(),
        ]).unwrap();
        count += 1;
    }
    w.flush().unwrap();
    
    println!("导出 {} 条记录到 {}", count, args[2]);
}

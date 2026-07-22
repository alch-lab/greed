//! 记账：权益 / 保证金 / 仓位；每笔成交后守恒对账。
//!
//! 记账模型（合约回测的简化）：
//! - `cash`：现金余额，只随**已实现盈亏**与**手续费**变动。
//! - 开仓建立 [`OpenPosition`]（均价、数量、方向），不搬动现金（仅扣手续费）。
//! - 平仓时结算已实现盈亏入现金。
//! - `equity = cash + 未实现盈亏`，每笔平仓后满足守恒：
//!   `cash == initial + Σ(realized) − Σ(fees)`（无持仓时 equity == cash）。
//!
//! 本策略同向最多一笔持仓 + 一组网格（PR-10 网格以多笔开仓近似），
//! 故采用**净持仓 + 加权均价**模型：同向加仓摊均价，反向成交减仓直至反向翻仓。

use tcore::types::{Price, Qty, Side, Symbol, Timestamp};

/// 一笔成交请求（apply_fill 的输入打包）
#[derive(Debug, Clone)]
pub struct FillRequest {
    pub ts: Timestamp,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub fee: f64,
    pub is_maker: bool,
    pub reason: String,
}

/// 一笔成交记录（含盈亏与费用），供绩效报告与对账
#[derive(Debug, Clone)]
pub struct Fill {
    pub ts: Timestamp,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub fee: f64,
    pub is_maker: bool,
    /// 本次成交导致的已实现盈亏（开仓为0）
    pub realized_pnl: f64,
    /// 成交后持仓方向（None = 无持仓）
    pub position_side_after: Option<Side>,
    /// 成交原因（开仓扳机 / 止损 / TP / 时间止损 / 反手）
    pub reason: String,
}

/// 内部持仓
#[derive(Debug, Clone, Copy)]
pub struct OpenPosition {
    pub side: Side,
    pub entry_price: Price,
    pub qty: Qty,
    pub entry_ts: Timestamp,
    /// 当前止损价（出场插件移动）
    pub stop_price: Option<Price>,
    /// 第一止盈参考位
    pub tp1_price: Option<Price>,
    pub breakeven_moved: bool,
    pub closed_frac: f64,
}

impl OpenPosition {
    /// 未实现盈亏（按给定现价）
    pub fn unrealized(&self, latest: Price) -> f64 {
        let diff = match self.side {
            Side::Buy => latest.to_f64() - self.entry_price.to_f64(),
            Side::Sell => self.entry_price.to_f64() - latest.to_f64(),
        };
        diff * self.qty.to_f64()
    }
}

/// 账户
#[derive(Debug)]
pub struct Account {
    initial_cash: f64,
    cash: f64,
    position: Option<OpenPosition>,
    fills: Vec<Fill>,
}

impl Account {
    pub fn new(initial_cash: f64) -> Self {
        Account {
            initial_cash,
            cash: initial_cash,
            position: None,
            fills: Vec::new(),
        }
    }
    pub fn cash(&self) -> f64 {
        self.cash
    }
    pub fn position(&self) -> Option<&OpenPosition> {
        self.position.as_ref()
    }
    pub fn position_view(&self, symbol: &Symbol) -> Option<tcore::Position> {
        self.position.as_ref().map(|p| tcore::Position {
            symbol: symbol.clone(),
            side: p.side,
            entry_price: p.entry_price,
            qty: p.qty,
            entry_ts: p.entry_ts,
            stop_price: p.stop_price.unwrap_or(p.entry_price),
            tp1_price: p.tp1_price,
            breakeven_moved: p.breakeven_moved,
            closed_frac: p.closed_frac,
        })
    }
    pub fn position_mut(&mut self) -> Option<&mut OpenPosition> {
        self.position.as_mut()
    }
    pub fn fills(&self) -> &[Fill] {
        &self.fills
    }

    /// 权益 = 现金 + 未实现盈亏（无持仓即现金）。
    pub fn equity(&self, latest: Price) -> f64 {
        self.cash + self.position.map(|p| p.unrealized(latest)).unwrap_or(0.0)
    }

    /// 累计已实现盈亏（含费用）。
    pub fn realized_pnl(&self) -> f64 {
        self.fills.iter().map(|f| f.realized_pnl - f.fee).sum()
    }

    /// 执行一笔成交，更新持仓与现金。
    ///
    /// - 同向加仓：加权均价。
    /// - 反向成交：先减现有仓位（结算已实现），超量部分反向开仓。
    pub fn apply_fill(&mut self, req: FillRequest) {
        let (ts, side, price, qty, fee, is_maker, reason) = (
            req.ts,
            req.side,
            req.price,
            req.qty,
            req.fee,
            req.is_maker,
            req.reason,
        );
        let mut realized = 0.0;
        let mut remaining = qty.to_f64();

        // 1) 先处理与现有持仓的反向部分（减仓/平仓）
        if let Some(pos) = self.position {
            if pos.side != side {
                let close_qty = remaining.min(pos.qty.to_f64());
                let diff = match pos.side {
                    Side::Buy => price.to_f64() - pos.entry_price.to_f64(),
                    Side::Sell => pos.entry_price.to_f64() - price.to_f64(),
                };
                realized += diff * close_qty;

                let rest = pos.qty.to_f64() - close_qty;
                remaining -= close_qty;

                if rest < 1e-9 {
                    self.position = None;
                } else {
                    self.position = Some(OpenPosition {
                        qty: Qty::from_f64(rest),
                        closed_frac: pos.closed_frac + close_qty / pos.qty.to_f64(),
                        ..pos
                    });
                }
            }
        }

        // 2) 剩余量同向加仓或新开仓
        if remaining > 1e-9 {
            match self.position {
                Some(pos) if pos.side == side => {
                    let new_qty = pos.qty.to_f64() + remaining;
                    let avg = (pos.entry_price.to_f64() * pos.qty.to_f64()
                        + price.to_f64() * remaining)
                        / new_qty;
                    self.position = Some(OpenPosition {
                        entry_price: Price::from_f64(avg),
                        qty: Qty::from_f64(new_qty),
                        ..pos
                    });
                }
                _ => {
                    self.position = Some(OpenPosition {
                        side,
                        entry_price: price,
                        qty: Qty::from_f64(remaining),
                        entry_ts: ts,
                        stop_price: None,
                        tp1_price: None,
                        breakeven_moved: false,
                        closed_frac: 0.0,
                    });
                }
            }
        }

        // 3) 现金：已实现盈亏入账，手续费扣出
        self.cash += realized - fee;
        let position_side_after = self.position.map(|p| p.side);
        self.fills.push(Fill {
            ts,
            side,
            price,
            qty,
            fee,
            is_maker,
            realized_pnl: realized,
            position_side_after,
            reason,
        });
    }

    /// 守恒对账：无持仓时 `cash == initial + realized_net`。
    /// 有持仓时 `equity(latest) == initial + realized_net + unrealized(latest)`。
    /// 返回最大偏差（应 ~0）。
    pub fn conservation_error(&self, latest: Price) -> f64 {
        let realized_net = self
            .fills
            .iter()
            .map(|f| f.realized_pnl - f.fee)
            .sum::<f64>();
        let expected = self.initial_cash
            + realized_net
            + self.position.map(|p| p.unrealized(latest)).unwrap_or(0.0);
        (self.equity(latest) - expected).abs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(ms: i64) -> Timestamp {
        Timestamp::from_millis(ms)
    }

    #[test]
    fn open_close_profit() {
        let mut acc = Account::new(10_000.0);
        // 67000 开多 0.1，手续费 10
        acc.apply_fill(FillRequest {
            ts: ts(1000),
            side: Side::Buy,
            price: Price::from_f64(67000.0),
            qty: Qty::from_f64(0.1),
            fee: 10.0,
            is_maker: false,
            reason: "open".into(),
        });
        assert_eq!(acc.cash(), 10_000.0 - 10.0);
        assert_eq!(acc.position().unwrap().side, Side::Buy);
        // 浮盈：现价 67300 → +30
        assert!((acc.equity(Price::from_f64(67300.0)) - (10_000.0 - 10.0 + 30.0)).abs() < 1e-6);
        // 67600 平掉，realized = 60，手续费 10
        acc.apply_fill(FillRequest {
            ts: ts(2000),
            side: Side::Sell,
            price: Price::from_f64(67600.0),
            qty: Qty::from_f64(0.1),
            fee: 10.0,
            is_maker: false,
            reason: "tp".into(),
        });
        assert!(acc.position().is_none());
        // cash = 10000 -10 +60 -10 = 10040
        assert!((acc.cash() - 10_040.0).abs() < 1e-6);
        assert!(acc.conservation_error(Price::from_f64(67600.0)) < 1e-9);
    }

    #[test]
    fn average_up_same_side() {
        let mut acc = Account::new(10_000.0);
        acc.apply_fill(FillRequest {
            ts: ts(1),
            side: Side::Buy,
            price: Price::from_f64(100.0),
            qty: Qty::from_f64(1.0),
            fee: 0.0,
            is_maker: false,
            reason: "a".into(),
        });
        acc.apply_fill(FillRequest {
            ts: ts(2),
            side: Side::Buy,
            price: Price::from_f64(110.0),
            qty: Qty::from_f64(1.0),
            fee: 0.0,
            is_maker: false,
            reason: "b".into(),
        });
        let p = acc.position().unwrap();
        assert!((p.entry_price.to_f64() - 105.0).abs() < 1e-9);
        assert!((p.qty.to_f64() - 2.0).abs() < 1e-9);
    }

    #[test]
    fn reverse_flips_position() {
        let mut acc = Account::new(10_000.0);
        acc.apply_fill(FillRequest {
            ts: ts(1),
            side: Side::Buy,
            price: Price::from_f64(100.0),
            qty: Qty::from_f64(1.0),
            fee: 0.0,
            is_maker: false,
            reason: "open".into(),
        });
        // 反向卖 1.5：平 1.0（亏 5），再开空 0.5
        acc.apply_fill(FillRequest {
            ts: ts(2),
            side: Side::Sell,
            price: Price::from_f64(95.0),
            qty: Qty::from_f64(1.5),
            fee: 0.0,
            is_maker: false,
            reason: "flip".into(),
        });
        let p = acc.position().unwrap();
        assert_eq!(p.side, Side::Sell);
        assert!((p.qty.to_f64() - 0.5).abs() < 1e-9);
        assert!((p.entry_price.to_f64() - 95.0).abs() < 1e-9);
        assert!((acc.cash() - (10_000.0 - 5.0)).abs() < 1e-6);
    }

    #[test]
    fn conservation_holds_over_many_fills() {
        let mut acc = Account::new(50_000.0);
        let prices = [67000.0, 67100.0, 66900.0, 67200.0, 66800.0, 67300.0];
        let sides = [
            Side::Buy,
            Side::Buy,
            Side::Sell,
            Side::Sell,
            Side::Buy,
            Side::Sell,
        ];
        for (i, (&px, &sd)) in prices.iter().zip(sides.iter()).enumerate() {
            acc.apply_fill(FillRequest {
                ts: ts(i as i64 * 1000),
                side: sd,
                price: Price::from_f64(px),
                qty: Qty::from_f64(0.05),
                fee: 0.5,
                is_maker: false,
                reason: format!("t{}", i),
            });
            let err = acc.conservation_error(Price::from_f64(px));
            assert!(err < 1e-9, "conservation error {} at step {}", err, i);
        }
    }
}

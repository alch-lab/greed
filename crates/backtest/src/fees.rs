//! 费率与滑点模型：taker/maker 费率、可配置滑点。
//!
//! - 费率：taker / maker 分开，按基点（bps）配置。
//! - 滑点：市价单在对手价基础上加滑点；限价单成交在限价（maker，通常无滑点）。
//!
//! 所有费用以 USD 计（f64，费用是估算量不参与守恒对账）。

use tcore::types::{Price, Side};

/// 费率与滑点配置（对应 `config/base.toml [backtest]`)
#[derive(Debug, Clone, Copy)]
pub struct FeeModel {
    /// taker 费率（bps，1bps = 0.01%）
    pub taker_fee_bps: f64,
    /// maker 费率（bps）
    pub maker_fee_bps: f64,
    /// 市价单滑点
    pub slippage_bps: f64,
}

impl Default for FeeModel {
    fn default() -> Self {
        Self {
            taker_fee_bps: 4.0,
            maker_fee_bps: 2.0,
            slippage_bps: 1.0,
        }
    }
}

impl FeeModel {
    /// 市价单成交滑点：买入向不利方向（更高）滑动，卖出向更低滑动
    pub fn market_fill_price(&self, ref_price: Price, side: Side) -> Price {
        let slip = self.slippage_bps / 10_000.0;
        let multiplier = match side {
            Side::Buy => 1.0 + slip,
            Side::Sell => 1.0 - slip,
        };
        Price::from_f64(ref_price.to_f64() * multiplier)
    }

    /// 计算一笔成交的手续费（USD）
    /// `notional` 为名义额（USD），`is_maker` 区分费率
    pub fn fee(&self, notional: f64, is_maker: bool) -> f64 {
        let bps = if is_maker {
            self.maker_fee_bps
        } else {
            self.taker_fee_bps
        };
        notional * bps / 10_000.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_slippage_direction() {
        let f = FeeModel::default();
        let r = Price::from_f64(100.0);
        let buy = f.market_fill_price(r, Side::Buy);
        let sell = f.market_fill_price(r, Side::Sell);
        assert!(buy.to_f64() > 100.0); // 买价上滑
        assert!(sell.to_f64() < 100.0); // 卖价下滑
        assert!((buy.to_f64() - 100.01).abs() < 1e-6); // 1bp
        assert!((sell.to_f64() - 99.99).abs() < 1e-6);
    }

    #[test]
    fn fee_calculation() {
        let f = FeeModel::default();
        // 10 万美元 taker 4bps = 40 美元
        assert!((f.fee(100_000.0, false) - 40.0).abs() < 1e-9);
        // maker 2bps = 20 美元
        assert!((f.fee(100_000.0, true) - 20.0).abs() < 1e-9);
    }
}

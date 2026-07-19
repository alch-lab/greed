//! renko 反转砖引擎。
//!
//! 对应教程 Exocharts `Trend Reversal 200-124`：
//! 顺当前砖方向移动满 T_ren（100 美元）收新砖；
//! 逆向回撤满 R_ren（62 美元）收一根反向砖（形成影线）。
//! 每砖聚合 volume / delta / duration / footprint（分价位买卖量）。

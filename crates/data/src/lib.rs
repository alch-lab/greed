//! data： 数据层
pub mod lake;
pub mod live;

#[cfg(test)]
mod tests {
    #[test]
    fn data_smoke() {
        assert_eq!(2 + 2, 4);
    }
}

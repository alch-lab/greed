#[test]
fn assemble_real_strategy_toml() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/strategy.toml");
    let toml_str = std::fs::read_to_string(path).expect("读 strategy.toml 失败");
    let reg = strategy::builtin_registry();
    let s = strategy::assemble_from_toml(&toml_str, &reg).expect("装配失败");
    println!("装配成功: {}", s.describe());
    assert_eq!(s.trigger.name(), "ExhaustionReversal");
}

use contracts::Contract;
#[derive(Contract)]
struct Target { #[contract(default, default)] id: i64 }
fn main() {}

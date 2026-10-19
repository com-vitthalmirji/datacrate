use contracts::Contract;
#[derive(Contract)]
struct Target { #[contract()] id: i64 }
fn main() {}

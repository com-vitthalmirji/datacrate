use contracts::Contract;
#[derive(Contract)]
struct Target { #[contract(unknown)] id: i64 }
fn main() {}

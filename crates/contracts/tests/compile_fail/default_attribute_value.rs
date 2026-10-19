use contracts::Contract;
#[derive(Contract)]
struct Target { #[contract(default = false)] id: i64 }
fn main() {}

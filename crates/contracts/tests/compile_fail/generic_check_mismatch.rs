use contracts::{Contract, Exact};
#[derive(Contract)] struct Producer { id: i64 }
#[derive(Contract)] struct Target { id: String }
fn main() { contracts::enforce::<Producer, Target, Exact>(); }

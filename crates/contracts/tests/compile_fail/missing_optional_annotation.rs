use contracts::{Contract, Exact, SchemaConforms};

#[derive(Contract)]
struct Producer {
    id: i64,
}

#[derive(Contract)]
struct Contract2 {
    id: i64,
    note: Option<String>,
    #[contract(default)]
    tier: String,
}

const _: () = SchemaConforms::<Producer, Contract2, Exact>::CHECK;

fn main() {}

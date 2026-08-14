use contracts::{Contract, Exact, SchemaConforms};

#[derive(Contract)]
struct Producer {
    id: i64,
    name: String,
}

#[derive(Contract)]
struct Contract2 {
    id: i64,
    name: String,
    note: String,
}

const _: () = SchemaConforms::<Producer, Contract2, Exact>::CHECK;

fn main() {}

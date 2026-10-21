use connectors_derive::Connector;
use contracts::Contract;

#[derive(Contract, Connector)]
struct Order {
    id: i64,
    amount: i32,
}

fn main() {
    let _ = Order::col_quantity();
}

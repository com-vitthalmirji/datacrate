use connectors_derive::Connector;

#[derive(Connector)]
struct Generic<T> {
    v: T,
}

fn main() {}

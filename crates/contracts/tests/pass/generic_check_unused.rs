#![allow(dead_code)]
use contracts::{Contract, SchemaPolicyMarker};
fn unused<P: Contract, C: Contract, M: SchemaPolicyMarker>() {
    contracts::enforce::<P, C, M>();
}
fn main() {}

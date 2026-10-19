#![allow(dead_code)]
use contracts::{Backward, Contract, Exact, SchemaConforms};
#[derive(Contract)]
struct Producer {}
#[derive(Contract)]
struct Target {
    #[contract(default)]
    id: i64,
}
#[rustfmt::skip]
#[derive(Contract)]
struct TrailingComma {
    #[contract(default,)]
    id: i64,
}
const _: () = SchemaConforms::<Producer, Target, Backward>::CHECK;
const _: () = SchemaConforms::<Producer, TrailingComma, Backward>::CHECK;
#[test]
fn default_metadata_is_only_a_backward_missing_field_allowance() {
    assert!(contracts::conforms(
        Producer::SHAPE,
        Target::SHAPE,
        contracts::SchemaPolicy::Backward
    ));
    assert!(!contracts::conforms(
        Producer::SHAPE,
        Target::SHAPE,
        <Exact as contracts::SchemaPolicyMarker>::POLICY
    ));
    assert!(Target::SHAPE != Producer::SHAPE);
}

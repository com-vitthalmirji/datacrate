#![allow(dead_code)]
use contracts::{Contract, TypeShape};
type Number = i64;
type Key = std::string::String;
type Borrowed = &'static str;
#[derive(Contract)]
struct Aliased {
    number: Number,
    text: std::string::String,
    borrowed: Borrowed,
    values: std::vec::Vec<std::option::Option<Number>>,
    lookup: std::collections::HashMap<Key, Number>,
}
#[derive(Contract)]
struct Canonical {
    number: i64,
    text: String,
    borrowed: &'static str,
    values: Vec<std::option::Option<i64>>,
    lookup: std::collections::BTreeMap<String, i64>,
}
struct Option<T>(T);
impl<T> Contract for Option<T> {
    const SHAPE: TypeShape = TypeShape::Primitive("domain-option");
}
#[derive(Contract)]
struct Shadowed {
    value: Option<i64>,
}
#[test]
fn aliases_and_qualified_containers_use_resolved_types() {
    assert_eq!(Aliased::SHAPE, Canonical::SHAPE);
}
#[test]
fn shadowed_container_uses_its_own_contract() {
    let TypeShape::Struct(fields) = Shadowed::SHAPE else {
        panic!("expected struct")
    };
    assert_eq!(fields[0].shape, TypeShape::Primitive("domain-option"));
}

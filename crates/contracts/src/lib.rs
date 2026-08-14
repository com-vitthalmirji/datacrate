//! Compile-time schema contracts.
//!
//! [`Contract::SHAPE`] normalizes a type's fields into a [`TypeShape`] tree;
//! [`conforms`] structurally compares two shapes under a [`SchemaPolicy`].
//! [`SchemaConforms::CHECK`] runs that comparison inside a `const` context,
//! so a schema mismatch is a compile error at the call site, not something
//! discovered when a pipeline runs.
//!
//! Rust has no stable equivalent of a "missing trait impl" that can be
//! conditioned on a value-level predicate (no specialization on stable), so
//! this reaches the same "drift fails the build" outcome a different way:
//! `SchemaConforms::<Producer, ContractT, Policy>::CHECK` is a `const` whose
//! initializer panics when the shapes don't conform. Referencing it forces
//! the compiler to evaluate that panic at compile time.
//!
//! The one law this module exists to prove: nested optionality is part of a
//! field's shape, not stripped from it. `Vec<Option<T>>` and `Vec<T>` are
//! different shapes, and `HashMap<K, Option<V>>`'s value optionality survives
//! comparison the same way — see `nested_optionality_is_preserved` in the
//! test module.

#![warn(missing_docs)]

// Lets `#[derive(Contract)]`-generated code refer to this crate as
// `::contracts::...` even when the derive is used from within this crate's
// own tests (the derive macro always emits fully-qualified paths).
extern crate self as contracts;

pub use contracts_derive::Contract;

/// A field's name and shape within a [`TypeShape::Struct`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldShape {
    /// The field's name, as written in source.
    pub name: &'static str,
    /// The field's shape.
    pub shape: TypeShape,
}

/// A structural, compile-time representation of a type's shape.
///
/// Built by `#[derive(Contract)]`; compared by [`conforms`]. Optionality is
/// represented at every level it appears (`Sequence(Optional(_))` is a
/// distinct shape from `Sequence(_)`), which is what lets nested optionality
/// survive comparison instead of being flattened away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeShape {
    /// A scalar or otherwise-undecomposed type, identified by name (e.g.
    /// `"i64"`, `"String"`, or a nested custom struct's type name — nested
    /// structs are compared nominally, not decomposed field-by-field).
    Primitive(&'static str),
    /// `Option<T>`.
    Optional(&'static TypeShape),
    /// `Vec<T>`.
    Sequence(&'static TypeShape),
    /// `HashMap<K, V>` or `BTreeMap<K, V>`.
    Map(&'static TypeShape, &'static TypeShape),
    /// A struct, as an ordered list of named fields.
    Struct(&'static [FieldShape]),
}

/// A type whose shape is known at compile time.
///
/// Implemented via `#[derive(Contract)]` for structs with named fields.
pub trait Contract {
    /// This type's normalized shape.
    const SHAPE: TypeShape;
}

/// How strictly a producer's shape must match a contract's shape.
///
/// The ordered-fields and case-insensitive-name variants FlowForge and
/// compile-time-data-contracts both support are not ported in this slice —
/// logged as a follow-up, not silently dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaPolicy {
    /// Producer and contract must have exactly the same fields (by name),
    /// each with an identical shape. No extra fields on either side.
    Exact,
    /// Every contract field must be satisfied: present in the producer with
    /// a conforming shape, or absent but `Optional` in the contract. The
    /// producer may carry extra fields the contract doesn't mention.
    Backward,
    /// Every producer field must exist in the contract with a conforming
    /// shape — the producer may not carry fields the contract doesn't know
    /// about. The contract may have extra `Optional` fields the producer
    /// omits.
    Forward,
    /// Both `Backward` and `Forward` must hold.
    Full,
}

/// A zero-sized marker type carrying a [`SchemaPolicy`] as a compile-time
/// constant, so it can be used as a generic parameter.
pub trait SchemaPolicyMarker {
    /// The policy this marker selects.
    const POLICY: SchemaPolicy;
}

/// Marker for [`SchemaPolicy::Exact`].
#[derive(Debug)]
pub struct Exact;
/// Marker for [`SchemaPolicy::Backward`].
#[derive(Debug)]
pub struct Backward;
/// Marker for [`SchemaPolicy::Forward`].
#[derive(Debug)]
pub struct Forward;
/// Marker for [`SchemaPolicy::Full`].
#[derive(Debug)]
pub struct Full;

impl SchemaPolicyMarker for Exact {
    const POLICY: SchemaPolicy = SchemaPolicy::Exact;
}
impl SchemaPolicyMarker for Backward {
    const POLICY: SchemaPolicy = SchemaPolicy::Backward;
}
impl SchemaPolicyMarker for Forward {
    const POLICY: SchemaPolicy = SchemaPolicy::Forward;
}
impl SchemaPolicyMarker for Full {
    const POLICY: SchemaPolicy = SchemaPolicy::Full;
}

/// Compares `producer` against `contract` under `policy`.
///
/// Non-`Struct` shapes (a bare `Contract` impl on something other than a
/// struct, which the derive macro doesn't currently produce, but the
/// comparer stays total) are compared for exact structural equality
/// regardless of policy.
#[must_use]
pub const fn conforms(producer: TypeShape, contract: TypeShape, policy: SchemaPolicy) -> bool {
    match (producer, contract) {
        (TypeShape::Struct(producer_fields), TypeShape::Struct(contract_fields)) => match policy {
            SchemaPolicy::Exact => exact_conforms(producer_fields, contract_fields),
            SchemaPolicy::Backward => backward_conforms(producer_fields, contract_fields),
            SchemaPolicy::Forward => forward_conforms(producer_fields, contract_fields),
            SchemaPolicy::Full => {
                backward_conforms(producer_fields, contract_fields)
                    && forward_conforms(producer_fields, contract_fields)
            }
        },
        (producer, contract) => shape_eq(&producer, &contract),
    }
}

const fn exact_conforms(producer: &[FieldShape], contract: &[FieldShape]) -> bool {
    producer.len() == contract.len() && forward_conforms(producer, contract)
}

const fn backward_conforms(producer: &[FieldShape], contract: &[FieldShape]) -> bool {
    let mut i = 0;
    while i < contract.len() {
        let field = &contract[i];
        match find_field(producer, field.name) {
            Some(producer_field) => {
                if !shape_eq(&producer_field.shape, &field.shape) {
                    return false;
                }
            }
            None => {
                if !is_optional(&field.shape) {
                    return false;
                }
            }
        }
        i += 1;
    }
    true
}

const fn forward_conforms(producer: &[FieldShape], contract: &[FieldShape]) -> bool {
    let mut i = 0;
    while i < producer.len() {
        let field = &producer[i];
        match find_field(contract, field.name) {
            Some(contract_field) => {
                if !shape_eq(&field.shape, &contract_field.shape) {
                    return false;
                }
            }
            None => return false,
        }
        i += 1;
    }
    true
}

const fn find_field<'a>(fields: &'a [FieldShape], name: &str) -> Option<&'a FieldShape> {
    let mut i = 0;
    while i < fields.len() {
        if str_eq(fields[i].name, name) {
            return Some(&fields[i]);
        }
        i += 1;
    }
    None
}

const fn is_optional(shape: &TypeShape) -> bool {
    matches!(shape, TypeShape::Optional(_))
}

const fn shape_eq(a: &TypeShape, b: &TypeShape) -> bool {
    match (a, b) {
        (TypeShape::Primitive(x), TypeShape::Primitive(y)) => str_eq(x, y),
        (TypeShape::Optional(x), TypeShape::Optional(y)) => shape_eq(x, y),
        (TypeShape::Sequence(x), TypeShape::Sequence(y)) => shape_eq(x, y),
        (TypeShape::Map(k1, v1), TypeShape::Map(k2, v2)) => shape_eq(k1, k2) && shape_eq(v1, v2),
        (TypeShape::Struct(f1), TypeShape::Struct(f2)) => fields_eq(f1, f2),
        _ => false,
    }
}

const fn fields_eq(a: &[FieldShape], b: &[FieldShape]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if !str_eq(a[i].name, b[i].name) || !shape_eq(&a[i].shape, &b[i].shape) {
            return false;
        }
        i += 1;
    }
    true
}

const fn str_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// Proves, at compile time, that `Producer`'s shape conforms to
/// `ContractT`'s shape under `Policy`.
///
/// Reference [`SchemaConforms::CHECK`] (typically from a `const _: () =
/// SchemaConforms::<Producer, ContractT, Policy>::CHECK;` item) to force the
/// comparison to run during compilation.
pub struct SchemaConforms<Producer, ContractT, Policy> {
    _producer: core::marker::PhantomData<Producer>,
    _contract: core::marker::PhantomData<ContractT>,
    _policy: core::marker::PhantomData<Policy>,
}

impl<Producer, ContractT, Policy> SchemaConforms<Producer, ContractT, Policy>
where
    Producer: Contract,
    ContractT: Contract,
    Policy: SchemaPolicyMarker,
{
    /// Panics at compile time if `Producer` does not conform to `ContractT`
    /// under `Policy`.
    pub const CHECK: () = assert!(
        conforms(Producer::SHAPE, ContractT::SHAPE, Policy::POLICY),
        "producer schema does not conform to the contract under this policy"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    // These structs exist only for their derived `Contract::SHAPE` const —
    // the fields themselves are never constructed or read at runtime.
    #[allow(dead_code)]
    #[derive(Contract)]
    struct WideProducer {
        id: i64,
        name: String,
        tags: Vec<Option<String>>,
    }

    #[allow(dead_code)]
    #[derive(Contract)]
    struct ExactContract {
        id: i64,
        name: String,
        tags: Vec<Option<String>>,
    }

    #[allow(dead_code)]
    #[derive(Contract)]
    struct BackwardContract {
        id: i64,
        name: String,
        note: Option<String>,
    }

    #[allow(dead_code)]
    #[derive(Contract)]
    struct TagsWithoutOptional {
        id: i64,
        name: String,
        tags: Vec<String>,
    }

    #[test]
    fn exact_matching_shapes_conform() {
        assert!(conforms(
            WideProducer::SHAPE,
            ExactContract::SHAPE,
            SchemaPolicy::Exact
        ));
    }

    #[test]
    fn backward_allows_a_missing_optional_contract_field() {
        assert!(conforms(
            WideProducer::SHAPE,
            BackwardContract::SHAPE,
            SchemaPolicy::Backward
        ));
    }

    #[test]
    fn exact_rejects_a_producer_missing_a_contract_field() {
        assert!(!conforms(
            WideProducer::SHAPE,
            BackwardContract::SHAPE,
            SchemaPolicy::Exact
        ));
    }

    #[test]
    fn nested_optionality_is_preserved() {
        // Same field name and same outer Sequence shape, but the producer's
        // element type is `Option<String>` and the contract's is `String` —
        // this must NOT conform, proving inner optionality isn't stripped
        // before comparison.
        assert!(!conforms(
            WideProducer::SHAPE,
            TagsWithoutOptional::SHAPE,
            SchemaPolicy::Exact
        ));
    }

    #[test]
    fn schema_conforms_check_passes_for_a_matching_pair() {
        const _: () = SchemaConforms::<WideProducer, ExactContract, Exact>::CHECK;
    }

    #[test]
    fn compile_fail_mismatched_shapes_panic_the_const_check() {
        let t = trybuild::TestCases::new();
        t.compile_fail("tests/compile_fail/*.rs");
    }

    mod shadowed_container_name {
        use super::Contract;

        // A local type literally named `Vec`, with no generics. The derive
        // macro sees only syntax (not resolved types), so it used to assume
        // any segment named "Vec" had exactly one generic argument and
        // panicked reaching for it. It must now fall back to treating this
        // as an opaque primitive instead.
        #[allow(dead_code)]
        struct Vec {
            x: i32,
        }

        #[allow(dead_code)]
        #[derive(Contract)]
        struct ShadowedContainerName {
            v: Vec,
        }

        #[test]
        fn non_generic_type_named_like_a_container_does_not_panic() {
            let _ = ShadowedContainerName::SHAPE;
        }
    }
}

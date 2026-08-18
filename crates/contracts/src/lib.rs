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
    /// A scalar type with no further structure to compare, identified by
    /// name (e.g. `"i64"`, `"String"`). A nested `#[derive(Contract)]` type
    /// is *not* represented this way — it's decomposed into its own
    /// [`TypeShape::Struct`] so mismatches inside it can be traced by path
    /// (e.g. `shipTo.zip`) instead of compared opaquely by name.
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
/// This policy governs only the *top-level* field comparison. A field whose
/// shape is itself a nested `#[derive(Contract)]` struct is always compared
/// under [`SchemaPolicy::Exact`] semantics one level down, regardless of the
/// policy chosen here — `Backward`/`Forward`/`Full` do not recurse into
/// nested structs. For example, under `Backward`, adding a new required
/// field to a nested struct still fails conformance even though adding one
/// at the top level would not.
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
    diagnose(producer, contract, policy).is_none()
}

/// How many `.`-separated segments a mismatch path can hold (e.g.
/// `["shipTo", "zip", "", "", "", ""]`). A mismatch nested deeper than this
/// still fails to conform — the crate already requires an exact structural
/// match inside nested structs (see [`shape_eq`]'s `Struct` arm) regardless
/// of policy — it just reports only its outermost `MAX_PATH_DEPTH` segments
/// instead of the full path.
const MAX_PATH_DEPTH: usize = 6;

/// Compares `producer` against `contract` under `policy`, returning the path
/// (root field first, left-aligned, empty-string-padded past the mismatch)
/// to the first field that fails to conform, or `None` if they conform.
///
/// Reuses the exact same field-by-field traversal `conforms` relies on —
/// `conforms` is defined in terms of this function, so the two can never
/// disagree on whether a given pair conforms.
///
/// If `producer`/`contract` aren't both [`TypeShape::Struct`] (not currently
/// reachable via `#[derive(Contract)]`, but `diagnose` stays total), a
/// mismatch has no field to name — the sentinel `"<root>"` is returned as a
/// single-segment path instead of an actual field name.
const fn diagnose(
    producer: TypeShape,
    contract: TypeShape,
    policy: SchemaPolicy,
) -> Option<[&'static str; MAX_PATH_DEPTH]> {
    match (producer, contract) {
        (TypeShape::Struct(producer_fields), TypeShape::Struct(contract_fields)) => match policy {
            SchemaPolicy::Exact => exact_diagnose(producer_fields, contract_fields),
            SchemaPolicy::Backward => backward_diagnose(producer_fields, contract_fields),
            SchemaPolicy::Forward => forward_diagnose(producer_fields, contract_fields),
            SchemaPolicy::Full => match backward_diagnose(producer_fields, contract_fields) {
                Some(path) => Some(path),
                None => forward_diagnose(producer_fields, contract_fields),
            },
        },
        (producer, contract) => {
            if shape_eq(&producer, &contract) {
                None
            } else {
                Some(single_segment("<root>"))
            }
        }
    }
}

const fn exact_diagnose(
    producer: &[FieldShape],
    contract: &[FieldShape],
) -> Option<[&'static str; MAX_PATH_DEPTH]> {
    match forward_diagnose(producer, contract) {
        Some(path) => Some(path),
        None => {
            if producer.len() == contract.len() {
                None
            } else {
                // forward_diagnose passing means every producer field is
                // present in contract with a matching shape; a length
                // mismatch then means contract has a field producer lacks.
                first_absent(contract, producer)
            }
        }
    }
}

const fn backward_diagnose(
    producer: &[FieldShape],
    contract: &[FieldShape],
) -> Option<[&'static str; MAX_PATH_DEPTH]> {
    let mut i = 0;
    while i < contract.len() {
        let field = &contract[i];
        match find_field(producer, field.name) {
            Some(producer_field) => {
                if !shape_eq(&producer_field.shape, &field.shape) {
                    return Some(blame(field.name, &producer_field.shape, &field.shape));
                }
            }
            None => {
                if !is_optional(&field.shape) {
                    return Some(single_segment(field.name));
                }
            }
        }
        i += 1;
    }
    None
}

const fn forward_diagnose(
    producer: &[FieldShape],
    contract: &[FieldShape],
) -> Option<[&'static str; MAX_PATH_DEPTH]> {
    let mut i = 0;
    while i < producer.len() {
        let field = &producer[i];
        match find_field(contract, field.name) {
            Some(contract_field) => {
                if !shape_eq(&field.shape, &contract_field.shape) {
                    return Some(blame(field.name, &field.shape, &contract_field.shape));
                }
            }
            None => return Some(single_segment(field.name)),
        }
        i += 1;
    }
    None
}

/// Returns the path to the first field in `fields` that has no matching name
/// in `other`, or `None` if every field in `fields` is present there. Always
/// a single-segment path: an absent field has nothing to recurse into.
const fn first_absent(
    fields: &[FieldShape],
    other: &[FieldShape],
) -> Option<[&'static str; MAX_PATH_DEPTH]> {
    let mut i = 0;
    while i < fields.len() {
        if find_field(other, fields[i].name).is_none() {
            return Some(single_segment(fields[i].name));
        }
        i += 1;
    }
    None
}

/// Builds the path for a field named `name` whose `producer_shape` doesn't
/// conform to `contract_shape`. When both sides are [`TypeShape::Struct`],
/// recurses via [`nested_field_diagnose`] to name the specific field inside
/// that clashes, instead of stopping at the outer field's own name.
const fn blame(
    name: &'static str,
    producer_shape: &TypeShape,
    contract_shape: &TypeShape,
) -> [&'static str; MAX_PATH_DEPTH] {
    match (producer_shape, contract_shape) {
        (TypeShape::Struct(producer_fields), TypeShape::Struct(contract_fields)) => {
            prepend_segment(
                name,
                nested_field_diagnose(producer_fields, contract_fields),
            )
        }
        _ => single_segment(name),
    }
}

/// Finds which field differs between two struct field lists already known
/// (by the caller) not to satisfy [`fields_eq`] — mirroring `fields_eq`'s
/// own index-by-index, order-sensitive walk exactly, rather than `find_field`'s
/// by-name lookup, is what keeps `diagnose` and `shape_eq`/`conforms` from
/// ever disagreeing on whether something conforms; this function only
/// decides which field to blame for a failure `fields_eq` already found.
///
/// A field-count mismatch has no single positional field to blame, so it
/// returns an empty path — the caller (via [`blame`]) has already prepended
/// the outer field's own name, so the reported path simply stops there.
const fn nested_field_diagnose(
    producer: &[FieldShape],
    contract: &[FieldShape],
) -> [&'static str; MAX_PATH_DEPTH] {
    if producer.len() != contract.len() {
        return [""; MAX_PATH_DEPTH];
    }
    let mut i = 0;
    while i < producer.len() {
        let p = &producer[i];
        let c = &contract[i];
        if !str_eq(p.name, c.name) || !shape_eq(&p.shape, &c.shape) {
            if str_eq(p.name, c.name) {
                return blame(p.name, &p.shape, &c.shape);
            }
            // Same position, different names: report the producer's name at
            // that position — there's no principled "same field" pairing to
            // prefer once names disagree.
            return single_segment(p.name);
        }
        i += 1;
    }
    [""; MAX_PATH_DEPTH]
}

/// Builds a path with `name` in the first slot and every other slot empty.
const fn single_segment(name: &'static str) -> [&'static str; MAX_PATH_DEPTH] {
    let mut path = [""; MAX_PATH_DEPTH];
    path[0] = name;
    path
}

/// Shifts `inner` right by one slot (dropping anything past the bound) and
/// puts `name` in the first slot.
const fn prepend_segment(
    name: &'static str,
    inner: [&'static str; MAX_PATH_DEPTH],
) -> [&'static str; MAX_PATH_DEPTH] {
    let mut path = [""; MAX_PATH_DEPTH];
    path[0] = name;
    let mut i = 1;
    while i < MAX_PATH_DEPTH {
        path[i] = inner[i - 1];
        i += 1;
    }
    path
}

/// `"."` if `segment` is a real path segment, `""` if it's unused padding —
/// used to join [`diagnose`]'s fixed-size path array without ever printing a
/// stray separator for an empty trailing slot.
const fn sep(segment: &'static str) -> &'static str {
    if segment.is_empty() { "" } else { "." }
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
    /// under `Policy`, naming the dotted path (e.g. `shipTo.zip`) to the
    /// first mismatched field in the panic message.
    ///
    /// Built with `const_panic::concat_panic!` rather than `panic!("{}", ..)`
    /// or `const_format::concatcp!`: the path is only known once
    /// `Producer`/`ContractT`/`Policy` are monomorphized, and both of those
    /// alternatives need the message to already be, or reduce to, a
    /// standalone item that doesn't depend on the enclosing generics —
    /// `concat_panic!` builds the message from a fixed-size `PanicVal` array
    /// sized by argument count, not by string length, so it has no such item
    /// to place. The path's segments are passed as individual arguments
    /// (with a `.` separator computed per slot) rather than pre-joined into
    /// one string, since `concat_panic!` already concatenates its arguments
    /// — no string-building dependency is needed for a fixed number of
    /// slots.
    pub const CHECK: () = {
        if let Some(path) = diagnose(Producer::SHAPE, ContractT::SHAPE, Policy::POLICY) {
            const_panic::concat_panic!(
                const_panic::FmtArg::DISPLAY;
                "producer schema does not conform to the contract: field `",
                path[0],
                sep(path[1]), path[1],
                sep(path[2]), path[2],
                sep(path[3]), path[3],
                sep(path[4]), path[4],
                sep(path[5]), path[5],
                "` does not conform"
            );
        }
    };
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

    mod nested_struct_paths {
        use super::*;

        #[allow(dead_code)]
        #[derive(Contract)]
        struct Address {
            street: String,
            zip: String,
        }

        #[allow(dead_code)]
        #[derive(Contract)]
        struct AddressZipMismatch {
            street: String,
            zip: i64,
        }

        #[allow(dead_code)]
        #[derive(Contract)]
        struct AddressMissingZip {
            street: String,
        }

        #[allow(dead_code)]
        #[derive(Contract)]
        struct ShipToProducer {
            id: i64,
            ship_to: Address,
        }

        #[allow(dead_code)]
        #[derive(Contract)]
        struct ShipToZipMismatch {
            id: i64,
            ship_to: AddressZipMismatch,
        }

        #[allow(dead_code)]
        #[derive(Contract)]
        struct ShipToMissingZip {
            id: i64,
            ship_to: AddressMissingZip,
        }

        #[allow(dead_code)]
        #[derive(Contract)]
        struct Geo {
            lat: f64,
        }

        #[allow(dead_code)]
        #[derive(Contract)]
        struct GeoLatMismatch {
            lat: i64,
        }

        #[allow(dead_code)]
        #[derive(Contract)]
        struct AddressWithGeo {
            geo: Geo,
        }

        #[allow(dead_code)]
        #[derive(Contract)]
        struct AddressWithGeoMismatch {
            geo: GeoLatMismatch,
        }

        #[allow(dead_code)]
        #[derive(Contract)]
        struct OrderProducer {
            ship_to: AddressWithGeo,
        }

        #[allow(dead_code)]
        #[derive(Contract)]
        struct OrderGeoMismatch {
            ship_to: AddressWithGeoMismatch,
        }

        #[test]
        fn nested_struct_paths_are_decomposed_not_opaque() {
            assert!(conforms(
                ShipToProducer::SHAPE,
                ShipToProducer::SHAPE,
                SchemaPolicy::Exact
            ));
        }

        #[test]
        fn one_level_nested_mismatch_reports_dotted_path() {
            let path = diagnose(
                ShipToProducer::SHAPE,
                ShipToZipMismatch::SHAPE,
                SchemaPolicy::Exact,
            )
            .expect("shapes must not conform");
            assert_eq!(path[0], "ship_to");
            assert_eq!(path[1], "zip");
            assert_eq!(path[2], "");
        }

        #[test]
        fn two_level_nested_mismatch_reports_full_path() {
            let path = diagnose(
                OrderProducer::SHAPE,
                OrderGeoMismatch::SHAPE,
                SchemaPolicy::Exact,
            )
            .expect("shapes must not conform");
            assert_eq!(path[0], "ship_to");
            assert_eq!(path[1], "geo");
            assert_eq!(path[2], "lat");
            assert_eq!(path[3], "");
        }

        #[test]
        fn nested_struct_missing_a_field_stops_the_path_at_the_outer_field() {
            let path = diagnose(
                ShipToProducer::SHAPE,
                ShipToMissingZip::SHAPE,
                SchemaPolicy::Exact,
            )
            .expect("shapes must not conform");
            assert_eq!(path[0], "ship_to");
            assert_eq!(path[1], "");
        }
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
        // panicked reaching for it. It must fall through to the
        // primitive/nested-Contract case instead of panicking — since "Vec"
        // isn't a recognized primitive name, that means it's now treated as
        // a nested Contract type, which requires deriving Contract here too.
        #[allow(dead_code)]
        #[derive(Contract)]
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

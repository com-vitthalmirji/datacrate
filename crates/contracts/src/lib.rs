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
    /// Whether this field was annotated `#[contract(default)]`. A missing
    /// contract field with `has_default: true` is tolerated under
    /// [`SchemaPolicy::Backward`] the same way an `Optional` field is —
    /// Rust has no runtime-inspectable equivalent of a Scala default value,
    /// so this is the opt-in stand-in for it.
    pub has_default: bool,
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
/// Every field comparison, at every nesting level, is governed by the same
/// policy — a nested `#[derive(Contract)]` struct is compared under the
/// *same* traversal mode (case sensitivity, ordering, positional-ness) as
/// the top level, not hardcoded to any one of these regardless of what was
/// chosen here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaPolicy {
    /// Producer and contract must have exactly the same fields, matched by
    /// name, case-insensitively, in any order, each with an identical
    /// shape. No extra fields on either side. Matches
    /// `compile-time-data-contracts`'s `Exact`.
    Exact,
    /// Identical to [`SchemaPolicy::Exact`] — kept as a distinct, explicitly
    /// named variant for parity with `compile-time-data-contracts`, which
    /// exposes both names.
    ExactUnorderedCI,
    /// Like [`SchemaPolicy::Exact`], but fields are matched positionally by
    /// index and their names compared case-*sensitively* — this is the
    /// behavior this crate's `Exact` used before this policy was split out.
    ExactOrdered,
    /// Like [`SchemaPolicy::ExactOrdered`], but names are compared
    /// case-insensitively.
    ExactOrderedCI,
    /// Fields are matched purely by index; names are not compared at all.
    /// Only the shape at each position must match. Diagnostic paths for
    /// this policy use the producer's field name at that index (or the
    /// contract's, once the producer runs out) purely for readability —
    /// names play no role in whether something conforms under this policy.
    ExactByPosition,
    /// Every contract field must be satisfied: present in the producer
    /// (matched by name, case-sensitively) with a conforming shape, or
    /// absent but `Optional` or `#[contract(default)]` in the contract. The
    /// producer may carry extra fields the contract doesn't mention.
    Backward,
    /// Every producer field must exist in the contract (matched by name,
    /// case-sensitively) with a conforming shape — the producer may not
    /// carry fields the contract doesn't know about. The contract may have
    /// extra fields the producer omits.
    Forward,
    /// Accepts everything; the compile-time check still runs but can never
    /// fail. Present for parity with `compile-time-data-contracts`, not
    /// because it's commonly useful — prefer `Backward`/`Forward`/an
    /// `Exact*` variant for anything that should actually enforce shape.
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
/// Marker for [`SchemaPolicy::ExactUnorderedCI`].
#[derive(Debug)]
pub struct ExactUnorderedCI;
/// Marker for [`SchemaPolicy::ExactOrdered`].
#[derive(Debug)]
pub struct ExactOrdered;
/// Marker for [`SchemaPolicy::ExactOrderedCI`].
#[derive(Debug)]
pub struct ExactOrderedCI;
/// Marker for [`SchemaPolicy::ExactByPosition`].
#[derive(Debug)]
pub struct ExactByPosition;
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
impl SchemaPolicyMarker for ExactUnorderedCI {
    const POLICY: SchemaPolicy = SchemaPolicy::ExactUnorderedCI;
}
impl SchemaPolicyMarker for ExactOrdered {
    const POLICY: SchemaPolicy = SchemaPolicy::ExactOrdered;
}
impl SchemaPolicyMarker for ExactOrderedCI {
    const POLICY: SchemaPolicy = SchemaPolicy::ExactOrderedCI;
}
impl SchemaPolicyMarker for ExactByPosition {
    const POLICY: SchemaPolicy = SchemaPolicy::ExactByPosition;
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
    diagnose_all(producer, contract, policy).len == 0
}

/// How many `.`-separated segments a mismatch path can hold (e.g.
/// `["shipTo", "zip", "", "", "", ""]`). A mismatch nested deeper than this
/// still fails to conform — it just reports only its outermost
/// `MAX_PATH_DEPTH` segments instead of the full path.
const MAX_PATH_DEPTH: usize = 6;

/// How many diffs a single comparison can accumulate before later ones stop
/// being individually named. `const fn`/CTFE has no `Vec`, so this mirrors
/// `MAX_PATH_DEPTH`'s bounded-array approach; kept modest (rather than the
/// originally-floated 8) because [`SchemaConforms::CHECK`]'s rendering has
/// to hand-unroll one block of `concat_panic!` arguments per slot — a
/// mismatch beyond this bound still fails the check, it just isn't named in
/// the message.
const MAX_DIFFS: usize = 4;

/// One field-level discrepancy found while comparing a producer's shape
/// against a contract's shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiffKind {
    /// A contract field has no conforming match in the producer.
    Missing,
    /// A producer field has no match in the contract.
    Extra,
    /// A field exists on both sides (by whatever matching the policy uses)
    /// but its shape — or, under an ordered policy, its name at that
    /// position — differs.
    Mismatch,
}

/// A single accumulated diff, with the dotted path to the field it blames.
#[derive(Debug, Clone, Copy)]
struct Diff {
    kind: DiffKind,
    path: [&'static str; MAX_PATH_DEPTH],
    /// Only meaningful for `Missing`: true if the contract field is
    /// `Optional` or `#[contract(default)]`, and so tolerable under
    /// `Backward`.
    optional_or_default: bool,
}

/// A bounded accumulator of [`Diff`]s, standing in for CTDC's unbounded
/// `List[Diff]` — `const fn` can't allocate, so this is a fixed-size array
/// plus a count, exactly like [`MAX_PATH_DEPTH`]'s existing precedent.
#[derive(Debug, Clone, Copy)]
struct Diffs {
    items: [Option<Diff>; MAX_DIFFS],
    len: usize,
}

impl Diffs {
    const fn new() -> Self {
        Diffs {
            items: [None; MAX_DIFFS],
            len: 0,
        }
    }

    /// Records `diff`, silently dropping it once [`MAX_DIFFS`] has already
    /// been reached — the check still fails, this only bounds how many
    /// diffs get individually named.
    const fn push(&mut self, diff: Diff) {
        if self.len < MAX_DIFFS {
            self.items[self.len] = Some(diff);
            self.len += 1;
        }
    }
}

/// The three traversal booleans CTDC's `SchemaPolicy` computes, bundled into
/// one value so the recursive comparison functions below take one extra
/// argument instead of three.
#[derive(Debug, Clone, Copy)]
struct Mode {
    /// Compare field names case-insensitively.
    ci: bool,
    /// Match fields by name, in declaration order (index-aligned), rather
    /// than searching by name regardless of position.
    ordered: bool,
    /// Ignore names entirely; match fields purely by index.
    by_pos: bool,
}

/// Derives the traversal mode CTDC's `SchemaPolicy` computes.
const fn policy_mode(policy: SchemaPolicy) -> Mode {
    match policy {
        SchemaPolicy::Exact | SchemaPolicy::ExactUnorderedCI => Mode {
            ci: true,
            ordered: false,
            by_pos: false,
        },
        SchemaPolicy::ExactOrdered => Mode {
            ci: false,
            ordered: true,
            by_pos: false,
        },
        SchemaPolicy::ExactOrderedCI => Mode {
            ci: true,
            ordered: true,
            by_pos: false,
        },
        SchemaPolicy::ExactByPosition => Mode {
            ci: false,
            ordered: false,
            by_pos: true,
        },
        SchemaPolicy::Backward | SchemaPolicy::Forward | SchemaPolicy::Full => Mode {
            ci: false,
            ordered: false,
            by_pos: false,
        },
    }
}

/// Compares `producer` against `contract` under `policy`, returning every
/// diff the traversal finds (bounded by [`MAX_DIFFS`]), already filtered per
/// `policy`'s CTDC-matching rules. `conforms` is defined in terms of this
/// function's result being empty, so the two can never disagree.
const fn diagnose_all(producer: TypeShape, contract: TypeShape, policy: SchemaPolicy) -> Diffs {
    let mode = policy_mode(policy);
    let mut diffs = Diffs::new();
    match (&producer, &contract) {
        (TypeShape::Struct(producer_fields), TypeShape::Struct(contract_fields)) => {
            compare_fields(
                producer_fields,
                contract_fields,
                [""; MAX_PATH_DEPTH],
                0,
                mode,
                &mut diffs,
            );
        }
        (producer, contract) => {
            if !shape_eq(producer, contract) {
                diffs.push(Diff {
                    kind: DiffKind::Mismatch,
                    path: single_segment("<root>"),
                    optional_or_default: false,
                });
            }
        }
    }
    filter_diffs(diffs, policy)
}

/// Applies CTDC's exact per-policy diff-retention rules:
/// ```text
/// miss1 = if isBackward then miss0.filterNot(optional_or_default)
///         else if isForward || isFull then Nil
///         else miss0
/// extra1 = if isBackward || isFull then Nil else extra0
/// mism1  = if isFull then Nil else mism0
/// ```
/// `Full` is a genuine escape hatch: every diff is dropped, so the check
/// always passes.
const fn filter_diffs(diffs: Diffs, policy: SchemaPolicy) -> Diffs {
    if matches!(policy, SchemaPolicy::Full) {
        return Diffs::new();
    }
    let is_backward = matches!(policy, SchemaPolicy::Backward);
    let is_forward = matches!(policy, SchemaPolicy::Forward);
    let mut out = Diffs::new();
    let mut i = 0;
    while i < diffs.len {
        if let Some(diff) = diffs.items[i] {
            let keep = match diff.kind {
                DiffKind::Missing => {
                    if is_backward {
                        !diff.optional_or_default
                    } else {
                        !is_forward
                    }
                }
                DiffKind::Extra => !is_backward,
                DiffKind::Mismatch => true,
            };
            if keep {
                out.push(diff);
            }
        }
        i += 1;
    }
    out
}

/// Dispatches to the traversal shape `ordered`/`by_pos` select, mirroring
/// CTDC's `compareByName`/`compareOrdered`/`compareByPos`.
const fn compare_fields(
    producer: &[FieldShape],
    contract: &[FieldShape],
    path: [&'static str; MAX_PATH_DEPTH],
    depth: usize,
    mode: Mode,
    diffs: &mut Diffs,
) {
    if mode.by_pos {
        compare_by_position(producer, contract, path, depth, mode, diffs);
    } else if mode.ordered {
        compare_ordered(producer, contract, path, depth, mode, diffs);
    } else {
        compare_by_name(producer, contract, path, depth, mode, diffs);
    }
}

/// Matches each contract field to a producer field by name (case-aware);
/// unmatched contract fields are `Missing`, unmatched producer fields are
/// `Extra`, matched pairs recurse via [`compare_shapes`].
const fn compare_by_name(
    producer: &[FieldShape],
    contract: &[FieldShape],
    path: [&'static str; MAX_PATH_DEPTH],
    depth: usize,
    mode: Mode,
    diffs: &mut Diffs,
) {
    let mut i = 0;
    while i < contract.len() {
        let c = &contract[i];
        match find_field_mode(producer, c.name, mode.ci) {
            Some(p) => {
                let (field_path, field_depth) = append_segment(path, depth, c.name);
                compare_shapes(&p.shape, &c.shape, field_path, field_depth, mode, diffs);
            }
            None => {
                let (field_path, _) = append_segment(path, depth, c.name);
                diffs.push(Diff {
                    kind: DiffKind::Missing,
                    path: field_path,
                    optional_or_default: is_optional(&c.shape) || c.has_default,
                });
            }
        }
        i += 1;
    }
    let mut j = 0;
    while j < producer.len() {
        let p = &producer[j];
        if find_field_mode(contract, p.name, mode.ci).is_none() {
            let (field_path, _) = append_segment(path, depth, p.name);
            diffs.push(Diff {
                kind: DiffKind::Extra,
                path: field_path,
                optional_or_default: false,
            });
        }
        j += 1;
    }
}

/// Index-aligned walk: at each shared index, mismatched names (case-aware)
/// are reported as a `Mismatch` without descending further; matching names
/// recurse via [`compare_shapes`]. Trailing contract fields beyond the
/// producer's length are `Missing`; trailing producer fields are `Extra`.
const fn compare_ordered(
    producer: &[FieldShape],
    contract: &[FieldShape],
    path: [&'static str; MAX_PATH_DEPTH],
    depth: usize,
    mode: Mode,
    diffs: &mut Diffs,
) {
    let shared = min_len(producer.len(), contract.len());
    let mut i = 0;
    while i < shared {
        let p = &producer[i];
        let c = &contract[i];
        let names_match = if mode.ci {
            str_eq_ci(p.name, c.name)
        } else {
            str_eq(p.name, c.name)
        };
        if names_match {
            let (field_path, field_depth) = append_segment(path, depth, c.name);
            compare_shapes(&p.shape, &c.shape, field_path, field_depth, mode, diffs);
        } else {
            let (field_path, _) = append_segment(path, depth, c.name);
            diffs.push(Diff {
                kind: DiffKind::Mismatch,
                path: field_path,
                optional_or_default: false,
            });
        }
        i += 1;
    }
    let mut j = shared;
    while j < contract.len() {
        let (field_path, _) = append_segment(path, depth, contract[j].name);
        diffs.push(Diff {
            kind: DiffKind::Missing,
            path: field_path,
            optional_or_default: is_optional(&contract[j].shape) || contract[j].has_default,
        });
        j += 1;
    }
    let mut k = shared;
    while k < producer.len() {
        let (field_path, _) = append_segment(path, depth, producer[k].name);
        diffs.push(Diff {
            kind: DiffKind::Extra,
            path: field_path,
            optional_or_default: false,
        });
        k += 1;
    }
}

/// Purely positional walk: names play no role in matching. Path segments
/// still use the producer's (or, past its length, the contract's) field
/// name at that index — for readability only, not because names matter
/// under this policy.
const fn compare_by_position(
    producer: &[FieldShape],
    contract: &[FieldShape],
    path: [&'static str; MAX_PATH_DEPTH],
    depth: usize,
    mode: Mode,
    diffs: &mut Diffs,
) {
    let shared = min_len(producer.len(), contract.len());
    let mut i = 0;
    while i < shared {
        let p = &producer[i];
        let c = &contract[i];
        let (field_path, field_depth) = append_segment(path, depth, p.name);
        compare_shapes(&p.shape, &c.shape, field_path, field_depth, mode, diffs);
        i += 1;
    }
    let mut j = shared;
    while j < contract.len() {
        let (field_path, _) = append_segment(path, depth, contract[j].name);
        diffs.push(Diff {
            kind: DiffKind::Missing,
            path: field_path,
            optional_or_default: is_optional(&contract[j].shape) || contract[j].has_default,
        });
        j += 1;
    }
    let mut k = shared;
    while k < producer.len() {
        let (field_path, _) = append_segment(path, depth, producer[k].name);
        diffs.push(Diff {
            kind: DiffKind::Extra,
            path: field_path,
            optional_or_default: false,
        });
        k += 1;
    }
}

/// Recursively compares two shapes already known to sit at `path`,
/// appending path segments as it descends into `Struct` (field name),
/// `Sequence` (`"[]"`), and `Map` (`"<key>"`/`"<value>"`). `Optional`
/// unwraps transparently on both sides. Anything else — including a shape
/// mismatch between different `TypeShape` kinds — is a `Mismatch` at the
/// current path.
const fn compare_shapes(
    producer: &TypeShape,
    contract: &TypeShape,
    path: [&'static str; MAX_PATH_DEPTH],
    depth: usize,
    mode: Mode,
    diffs: &mut Diffs,
) {
    match (producer, contract) {
        (TypeShape::Primitive(p), TypeShape::Primitive(c)) => {
            if !str_eq(p, c) {
                diffs.push(Diff {
                    kind: DiffKind::Mismatch,
                    path,
                    optional_or_default: false,
                });
            }
        }
        (TypeShape::Optional(p), TypeShape::Optional(c)) => {
            compare_shapes(p, c, path, depth, mode, diffs);
        }
        (TypeShape::Sequence(p), TypeShape::Sequence(c)) => {
            let (seq_path, seq_depth) = append_segment(path, depth, "[]");
            compare_shapes(p, c, seq_path, seq_depth, mode, diffs);
        }
        (TypeShape::Map(pk, pv), TypeShape::Map(ck, cv)) => {
            let (key_path, key_depth) = append_segment(path, depth, "<key>");
            compare_shapes(pk, ck, key_path, key_depth, mode, diffs);
            let (value_path, value_depth) = append_segment(path, depth, "<value>");
            compare_shapes(pv, cv, value_path, value_depth, mode, diffs);
        }
        (TypeShape::Struct(pf), TypeShape::Struct(cf)) => {
            compare_fields(pf, cf, path, depth, mode, diffs);
        }
        _ => {
            diffs.push(Diff {
                kind: DiffKind::Mismatch,
                path,
                optional_or_default: false,
            });
        }
    }
}

/// Appends `seg` to `path` at `depth`, returning the (possibly unchanged)
/// path and the next depth. Once `depth` reaches [`MAX_PATH_DEPTH`], further
/// segments are silently dropped — the same disclosed truncation
/// `MAX_PATH_DEPTH` has always documented.
const fn append_segment(
    mut path: [&'static str; MAX_PATH_DEPTH],
    depth: usize,
    seg: &'static str,
) -> ([&'static str; MAX_PATH_DEPTH], usize) {
    if depth < MAX_PATH_DEPTH {
        path[depth] = seg;
        (path, depth + 1)
    } else {
        (path, depth)
    }
}

/// Builds a path with `name` in the first slot and every other slot empty.
const fn single_segment(name: &'static str) -> [&'static str; MAX_PATH_DEPTH] {
    let mut path = [""; MAX_PATH_DEPTH];
    path[0] = name;
    path
}

/// `"."` if `segment` is a real path segment, `""` if it's unused padding —
/// used to join a diff's fixed-size path array without ever printing a
/// stray separator for an empty trailing slot.
const fn sep(segment: &'static str) -> &'static str {
    if segment.is_empty() { "" } else { "." }
}

const fn find_field_mode<'a>(
    fields: &'a [FieldShape],
    name: &str,
    ci: bool,
) -> Option<&'a FieldShape> {
    let mut i = 0;
    while i < fields.len() {
        let matches_name = if ci {
            str_eq_ci(fields[i].name, name)
        } else {
            str_eq(fields[i].name, name)
        };
        if matches_name {
            return Some(&fields[i]);
        }
        i += 1;
    }
    None
}

const fn min_len(a: usize, b: usize) -> usize {
    if a < b { a } else { b }
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

/// ASCII case-insensitive byte comparison — sufficient here since field
/// names are Rust identifiers, always ASCII.
const fn str_eq_ci(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if to_ascii_lower(a[i]) != to_ascii_lower(b[i]) {
            return false;
        }
        i += 1;
    }
    true
}

const fn to_ascii_lower(byte: u8) -> u8 {
    if byte.is_ascii_uppercase() {
        byte + 32
    } else {
        byte
    }
}

/// `"missing \`"`/`"extra \`"`/`"mismatch \`"` label opening a diff's
/// rendered segment, or `""` past the end of the accumulated diffs.
const fn diff_kind_label(diffs: &Diffs, i: usize) -> &'static str {
    match diffs.items[i] {
        Some(diff) => match diff.kind {
            DiffKind::Missing => "missing `",
            DiffKind::Extra => "extra `",
            DiffKind::Mismatch => "mismatch `",
        },
        None => "",
    }
}

/// `"; "` before every diff after the first present one, else `""`.
const fn diff_lead_sep(diffs: &Diffs, i: usize) -> &'static str {
    if i > 0 && diffs.items[i].is_some() {
        "; "
    } else {
        ""
    }
}

/// The `j`th path segment of the `i`th diff, or `""` if either index is out
/// of range.
const fn diff_seg(diffs: &Diffs, i: usize, j: usize) -> &'static str {
    match diffs.items[i] {
        Some(diff) => diff.path[j],
        None => "",
    }
}

const fn diff_seg_sep(diffs: &Diffs, i: usize, j: usize) -> &'static str {
    sep(diff_seg(diffs, i, j))
}

/// Closing backtick for a present diff, else `""`.
const fn diff_close(diffs: &Diffs, i: usize) -> &'static str {
    match diffs.items[i] {
        Some(_) => "`",
        None => "",
    }
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
    /// under `Policy`, naming up to [`MAX_DIFFS`] mismatched/missing/extra
    /// fields (dotted path, e.g. `shipTo.zip`) in the panic message.
    ///
    /// Built with `const_panic::concat_panic!` rather than
    /// `const_format::concatcp!`: the diffs are only known once
    /// `Producer`/`ContractT`/`Policy` are monomorphized, and
    /// `concat_panic!`'s argument list already concatenates whatever pieces
    /// it's given, so each diff's fixed-size path segments are passed as
    /// individual arguments (with per-slot separators) rather than
    /// pre-joined into a string — no string-building dependency needed for
    /// a fixed number of slots. Hand-unrolled per [`MAX_DIFFS`] slot, the
    /// same acceptable tradeoff already used for `MAX_PATH_DEPTH`.
    pub const CHECK: () = {
        let diffs = diagnose_all(Producer::SHAPE, ContractT::SHAPE, Policy::POLICY);
        if diffs.len > 0 {
            const_panic::concat_panic!(
                const_panic::FmtArg::DISPLAY;
                "producer schema does not conform to the contract: ",
                diff_lead_sep(&diffs, 0),
                diff_kind_label(&diffs, 0),
                diff_seg(&diffs, 0, 0),
                diff_seg_sep(&diffs, 0, 1), diff_seg(&diffs, 0, 1),
                diff_seg_sep(&diffs, 0, 2), diff_seg(&diffs, 0, 2),
                diff_seg_sep(&diffs, 0, 3), diff_seg(&diffs, 0, 3),
                diff_seg_sep(&diffs, 0, 4), diff_seg(&diffs, 0, 4),
                diff_seg_sep(&diffs, 0, 5), diff_seg(&diffs, 0, 5),
                diff_close(&diffs, 0),
                diff_lead_sep(&diffs, 1),
                diff_kind_label(&diffs, 1),
                diff_seg(&diffs, 1, 0),
                diff_seg_sep(&diffs, 1, 1), diff_seg(&diffs, 1, 1),
                diff_seg_sep(&diffs, 1, 2), diff_seg(&diffs, 1, 2),
                diff_seg_sep(&diffs, 1, 3), diff_seg(&diffs, 1, 3),
                diff_seg_sep(&diffs, 1, 4), diff_seg(&diffs, 1, 4),
                diff_seg_sep(&diffs, 1, 5), diff_seg(&diffs, 1, 5),
                diff_close(&diffs, 1),
                diff_lead_sep(&diffs, 2),
                diff_kind_label(&diffs, 2),
                diff_seg(&diffs, 2, 0),
                diff_seg_sep(&diffs, 2, 1), diff_seg(&diffs, 2, 1),
                diff_seg_sep(&diffs, 2, 2), diff_seg(&diffs, 2, 2),
                diff_seg_sep(&diffs, 2, 3), diff_seg(&diffs, 2, 3),
                diff_seg_sep(&diffs, 2, 4), diff_seg(&diffs, 2, 4),
                diff_seg_sep(&diffs, 2, 5), diff_seg(&diffs, 2, 5),
                diff_close(&diffs, 2),
                diff_lead_sep(&diffs, 3),
                diff_kind_label(&diffs, 3),
                diff_seg(&diffs, 3, 0),
                diff_seg_sep(&diffs, 3, 1), diff_seg(&diffs, 3, 1),
                diff_seg_sep(&diffs, 3, 2), diff_seg(&diffs, 3, 2),
                diff_seg_sep(&diffs, 3, 3), diff_seg(&diffs, 3, 3),
                diff_seg_sep(&diffs, 3, 4), diff_seg(&diffs, 3, 4),
                diff_seg_sep(&diffs, 3, 5), diff_seg(&diffs, 3, 5),
                diff_close(&diffs, 3)
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

    #[test]
    fn exact_is_case_insensitive_and_unordered() {
        #[allow(dead_code)]
        #[derive(Contract)]
        struct Producer {
            name: String,
            id: i64,
        }
        #[allow(dead_code, non_snake_case)]
        #[derive(Contract)]
        struct ContractT {
            ID: i64,
            NAME: String,
        }
        assert!(conforms(
            Producer::SHAPE,
            ContractT::SHAPE,
            SchemaPolicy::Exact
        ));
    }

    #[test]
    fn exact_ordered_is_case_sensitive_and_order_sensitive() {
        #[allow(dead_code)]
        #[derive(Contract)]
        struct Producer {
            name: String,
            id: i64,
        }
        #[allow(dead_code)]
        #[derive(Contract)]
        struct ContractT {
            id: i64,
            name: String,
        }
        assert!(!conforms(
            Producer::SHAPE,
            ContractT::SHAPE,
            SchemaPolicy::ExactOrdered
        ));
    }

    #[test]
    fn exact_by_position_ignores_names() {
        #[allow(dead_code)]
        #[derive(Contract)]
        struct Producer {
            first: i64,
            second: String,
        }
        #[allow(dead_code)]
        #[derive(Contract)]
        struct ContractT {
            alpha: i64,
            beta: String,
        }
        assert!(conforms(
            Producer::SHAPE,
            ContractT::SHAPE,
            SchemaPolicy::ExactByPosition
        ));
    }

    #[test]
    fn full_always_conforms() {
        #[allow(dead_code)]
        #[derive(Contract)]
        struct Producer {
            id: i64,
        }
        #[allow(dead_code)]
        #[derive(Contract)]
        struct ContractT {
            id: String,
            extra: bool,
        }
        assert!(conforms(
            Producer::SHAPE,
            ContractT::SHAPE,
            SchemaPolicy::Full
        ));
    }

    #[test]
    fn backward_tolerates_a_missing_field_with_contract_default() {
        #[allow(dead_code)]
        #[derive(Contract)]
        struct Producer {
            id: i64,
        }
        #[allow(dead_code)]
        #[derive(Contract)]
        struct ContractT {
            id: i64,
            #[contract(default)]
            note: String,
        }
        assert!(conforms(
            Producer::SHAPE,
            ContractT::SHAPE,
            SchemaPolicy::Backward
        ));
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
            let diffs = diagnose_all(
                ShipToProducer::SHAPE,
                ShipToZipMismatch::SHAPE,
                SchemaPolicy::Exact,
            );
            assert_eq!(diffs.len, 1);
            let diff = diffs.items[0].expect("diff must be present");
            assert_eq!(diff.kind, DiffKind::Mismatch);
            assert_eq!(diff.path[0], "ship_to");
            assert_eq!(diff.path[1], "zip");
            assert_eq!(diff.path[2], "");
        }

        #[test]
        fn two_level_nested_mismatch_reports_full_path() {
            let diffs = diagnose_all(
                OrderProducer::SHAPE,
                OrderGeoMismatch::SHAPE,
                SchemaPolicy::Exact,
            );
            assert_eq!(diffs.len, 1);
            let diff = diffs.items[0].expect("diff must be present");
            assert_eq!(diff.kind, DiffKind::Mismatch);
            assert_eq!(diff.path[0], "ship_to");
            assert_eq!(diff.path[1], "geo");
            assert_eq!(diff.path[2], "lat");
            assert_eq!(diff.path[3], "");
        }

        #[test]
        fn nested_struct_missing_a_field_is_named_precisely() {
            // Unlike the old ordered-only engine (which could only blame the
            // outer `ship_to` field when a nested struct's field count
            // differed), the unordered-by-name engine can name exactly
            // which inner field is the culprit: `zip` is present on the
            // producer's nested Address but absent from the contract's, so
            // it's reported as an Extra field, not a vague outer mismatch.
            let diffs = diagnose_all(
                ShipToProducer::SHAPE,
                ShipToMissingZip::SHAPE,
                SchemaPolicy::Exact,
            );
            assert_eq!(diffs.len, 1);
            let diff = diffs.items[0].expect("diff must be present");
            assert_eq!(diff.kind, DiffKind::Extra);
            assert_eq!(diff.path[0], "ship_to");
            assert_eq!(diff.path[1], "zip");
        }
    }

    mod sequence_and_map_paths {
        use super::*;
        use std::collections::HashMap;

        #[allow(dead_code)]
        #[derive(Contract)]
        struct Producer {
            items: Vec<i64>,
            scores: HashMap<String, i64>,
        }

        #[allow(dead_code)]
        #[derive(Contract)]
        struct ContractT {
            items: Vec<String>,
            scores: HashMap<String, String>,
        }

        #[test]
        fn sequence_element_mismatch_appends_bracket_segment() {
            let diffs = diagnose_all(Producer::SHAPE, ContractT::SHAPE, SchemaPolicy::Exact);
            let items_diff = diffs
                .items
                .iter()
                .flatten()
                .find(|d| d.path[0] == "items")
                .expect("items diff must be present");
            assert_eq!(items_diff.path[1], "[]");
        }

        #[test]
        fn map_value_mismatch_appends_value_segment() {
            let diffs = diagnose_all(Producer::SHAPE, ContractT::SHAPE, SchemaPolicy::Exact);
            let scores_diff = diffs
                .items
                .iter()
                .flatten()
                .find(|d| d.path[0] == "scores")
                .expect("scores diff must be present");
            assert_eq!(scores_diff.path[1], "<value>");
        }
    }

    #[test]
    fn multiple_diffs_are_all_reported() {
        #[allow(dead_code)]
        #[derive(Contract)]
        struct Producer {
            id: i64,
        }
        #[allow(dead_code)]
        #[derive(Contract)]
        struct ContractT {
            id: String,
            note: bool,
        }
        let diffs = diagnose_all(Producer::SHAPE, ContractT::SHAPE, SchemaPolicy::Exact);
        assert_eq!(diffs.len, 2);
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

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
//!
//! # Diagnostic limits on the pinned stable toolchain vs. `compile-time-data-contracts`
//!
//! This crate's Scala counterpart, `compile-time-data-contracts` (CTDC),
//! renders an unbounded [`Vec`]-backed diff list with arbitrarily rich
//! per-field text, because its macros run as ordinary compile-time Scala
//! with full heap/`String`/`List` access. This implementation uses fixed
//! arrays and static labels for stable const evaluation on Rust 1.97.1:
//! at most `MAX_DIFFS` rejections and `MAX_PATH_DEPTH` path segments are
//! rendered. These display limits never determine the compatibility verdict.
//! Future language capabilities may allow richer diagnostics without changing
//! the policy relation.

// `SchemaConforms::CHECK`'s `seq_macro`-generated `concat_panic!` call has
// ~3072 arguments (128 diff slots x 24 pieces each) — well past
// `concat_panic!`'s default recursion budget.
#![recursion_limit = "4096"]
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
/// Implemented via `#[derive(Contract)]` for concrete structs with named fields,
/// and for standard scalars and containers. Aliases use their resolved type.
/// References inherit the referent's shape; separate manual reference
/// implementations are intentionally unsupported.
pub trait Contract {
    /// This type's normalized shape.
    const SHAPE: TypeShape;
}

mod resolved;

/// Fallible physical schema mapping, enabled by the `arrow` feature.
#[cfg(feature = "arrow")]
pub mod arrow;

/// Evaluates declaration compatibility for an instantiated generic call.
///
/// An incompatible call fails during a build's const evaluation. `cargo check`
/// alone may not instantiate the call, and unused generic code is not proof.
/// For a concrete unconditional check, prefer an item-level
/// `const _: () = SchemaConforms::<P, C, M>::CHECK;`.
/// This checks declarations, not external data or transformed batches.
pub fn enforce<P: Contract, C: Contract, M: SchemaPolicyMarker>() {
    const { SchemaConforms::<P, C, M>::CHECK };
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
    !compare(producer, contract, policy).rejected
}

/// How many `.`-separated segments a mismatch path can hold (e.g.
/// `["shipTo", "zip", "", "", "", "", "", ""]`). A mismatch nested deeper
/// than this still fails to conform — it just reports only its outermost
/// `MAX_PATH_DEPTH` segments instead of the full path.
///
/// Raising this requires also extending the fixed, hand-written sequence of
/// `diff_seg_sep`/`diff_seg` calls inside [`SchemaConforms::CHECK`]'s
/// `seq_macro::seq!` body — that list is written once (not once per diff
/// slot, see `MAX_DIFFS`), so extending it is a one-time, proportional edit,
/// not a multiplicative one.
const MAX_PATH_DEPTH: usize = 8;

/// How many diffs a single comparison can accumulate before later ones stop
/// being individually named. `const fn`/CTFE has no `Vec`, so this mirrors
/// `MAX_PATH_DEPTH`'s bounded-array approach. [`SchemaConforms::CHECK`]
/// generates one `concat_panic!` argument block per slot via
/// `seq_macro::seq!(I in 0..MAX_DIFFS { ... })` rather than hand-unrolling
/// each slot, so raising this bound is the one-line constant change here
/// plus matching the `seq!` range literal at the call site (`seq!` expands
/// via raw token substitution, so its range must be a literal, not a
/// reference to this const) — not a multiplicative amount of new source. A
/// mismatch beyond this bound still fails the check, it just isn't named in
/// the message.
const MAX_DIFFS: usize = 128;

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
    /// `" (optional)"`/`" (default)"` for a tolerable `Missing`, else `""`
    /// — selected from a fixed set of literals via [`optional_annotation`],
    /// so it can be spliced directly into [`SchemaConforms::CHECK`]'s
    /// message with no further composition needed.
    annotation: &'static str,
    /// The contract side's type/shape label for a `Mismatch`, or `""` when
    /// not applicable (every other diff kind, or a mismatch this crate
    /// can't usefully label). Kept as a separate field rather than
    /// pre-joined into `annotation`, since `expected`/`found` are ordinary
    /// runtime-computed `&'static str` values that `const_format::concatcp!`
    /// cannot concatenate at push time (it requires its arguments to be
    /// const-promotable, not function parameters) — so
    /// [`SchemaConforms::CHECK`] instead splices `expected`/`found` in as
    /// their own `concat_panic!` arguments, the same way path segments
    /// already are.
    expected: &'static str,
    /// The producer side's type/shape label for a `Mismatch`, or `""`. See
    /// `expected`.
    found: &'static str,
}

/// `" (optional)"` if `is_optional`, else `" (default)"` if `has_default`,
/// else `""`. Mirrors CTDC's richer `Missing attributes: ... (optional)`/
/// `(default)` rendering as closely as a bounded, statically-composed
/// message can.
const fn optional_annotation(is_optional: bool, has_default: bool) -> &'static str {
    if is_optional {
        " (optional)"
    } else if has_default {
        " (default)"
    } else {
        ""
    }
}

/// A short, static label for a shape's kind: a `Primitive`'s own type name,
/// or the container kind (`"Optional"`/`"Sequence"`/`"Map"`/`"Struct"`)
/// otherwise — used to fill in `expected`/`found` for a shape-kind clash
/// that isn't a `Primitive`-vs-`Primitive` mismatch.
const fn shape_label(shape: &TypeShape) -> &'static str {
    match shape {
        TypeShape::Primitive(name) => name,
        TypeShape::Optional(_) => "Optional",
        TypeShape::Sequence(_) => "Sequence",
        TypeShape::Map(_, _) => "Map",
        TypeShape::Struct(_) => "Struct",
    }
}

/// A `Missing` diff, annotated `" (optional)"`/`" (default)"` when the
/// contract field is tolerable-if-absent under `Backward`.
const fn missing_diff(
    path: [&'static str; MAX_PATH_DEPTH],
    is_optional: bool,
    has_default: bool,
) -> Diff {
    Diff {
        kind: DiffKind::Missing,
        path,
        optional_or_default: is_optional || has_default,
        annotation: optional_annotation(is_optional, has_default),
        expected: "",
        found: "",
    }
}

/// An `Extra` diff — no annotation applies.
const fn extra_diff(path: [&'static str; MAX_PATH_DEPTH]) -> Diff {
    Diff {
        kind: DiffKind::Extra,
        path,
        optional_or_default: false,
        annotation: "",
        expected: "",
        found: "",
    }
}

/// A `Mismatch` diff labeled with what the contract expected and what the
/// producer actually had — CTDC's `expected X, found Y` rendering, as
/// closely as this crate's bounded, statically-composed message can
/// reproduce it.
const fn mismatch_diff_labeled(
    path: [&'static str; MAX_PATH_DEPTH],
    expected: &'static str,
    found: &'static str,
) -> Diff {
    Diff {
        kind: DiffKind::Mismatch,
        path,
        optional_or_default: false,
        annotation: "",
        expected,
        found,
    }
}

/// A bounded accumulator of [`Diff`]s, standing in for CTDC's unbounded
/// `List[Diff]` — `const fn` can't allocate, so this is a fixed-size array
/// plus a count, exactly like `MAX_PATH_DEPTH`'s existing precedent.
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

    /// Records `diff`, silently dropping it once `MAX_DIFFS` has already
    /// been reached — the check still fails, this only bounds how many
    /// diffs get individually named.
    const fn push(&mut self, diff: Diff) {
        if self.len < MAX_DIFFS {
            self.items[self.len] = Some(diff);
            self.len += 1;
        }
    }
}

/// Owns the policy verdict independently of the bounded diagnostic display.
struct Comparison {
    policy: SchemaPolicy,
    rejected: bool,
    diffs: Diffs,
}

impl Comparison {
    const fn new(policy: SchemaPolicy) -> Self {
        Self {
            policy,
            rejected: false,
            diffs: Diffs::new(),
        }
    }

    const fn push(&mut self, diff: Diff) {
        if rejects(diff, self.policy) {
            self.rejected = true;
            self.diffs.push(diff);
        }
    }
}

/// Applies compatibility policy before a difference consumes display capacity.
const fn rejects(diff: Diff, policy: SchemaPolicy) -> bool {
    match policy {
        SchemaPolicy::Full => false,
        SchemaPolicy::Backward => match diff.kind {
            DiffKind::Missing => !diff.optional_or_default,
            DiffKind::Extra => false,
            DiffKind::Mismatch => true,
        },
        SchemaPolicy::Forward => !matches!(diff.kind, DiffKind::Missing),
        _ => true,
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

/// Traverses the shapes once, recording rejection independently of rendering.
const fn compare(producer: TypeShape, contract: TypeShape, policy: SchemaPolicy) -> Comparison {
    let mode = policy_mode(policy);
    let mut diffs = Comparison::new(policy);
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
                diffs.push(mismatch_diff_labeled(
                    single_segment("<root>"),
                    shape_label(contract),
                    shape_label(producer),
                ));
            }
        }
    }
    diffs
}

/// Dispatches to the traversal shape `ordered`/`by_pos` select, mirroring
/// CTDC's `compareByName`/`compareOrdered`/`compareByPos`.
const fn compare_fields(
    producer: &[FieldShape],
    contract: &[FieldShape],
    path: [&'static str; MAX_PATH_DEPTH],
    depth: usize,
    mode: Mode,
    diffs: &mut Comparison,
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
    diffs: &mut Comparison,
) {
    if let Some(name) = duplicate_field_name(producer, mode.ci) {
        let (field_path, _) = append_segment(path, depth, name);
        diffs.push(mismatch_diff_labeled(field_path, "unique field name", name));
        return;
    }
    if let Some(name) = duplicate_field_name(contract, mode.ci) {
        let (field_path, _) = append_segment(path, depth, name);
        diffs.push(mismatch_diff_labeled(field_path, "unique field name", name));
        return;
    }
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
                diffs.push(missing_diff(
                    field_path,
                    is_optional(&c.shape),
                    c.has_default,
                ));
            }
        }
        i += 1;
    }
    let mut j = 0;
    while j < producer.len() {
        let p = &producer[j];
        if find_field_mode(contract, p.name, mode.ci).is_none() {
            let (field_path, _) = append_segment(path, depth, p.name);
            diffs.push(extra_diff(field_path));
        }
        j += 1;
    }
}

/// Index-aligned walk: at each shared index, a name mismatch (case-aware)
/// is reported as its own `Mismatch` (path suffixed `(name)`, distinguishing
/// it from a shape diff at the same field) — but unlike a by-name mismatch,
/// this does *not* stop the traversal from also descending into
/// [`compare_shapes`] at that index, matching CTDC's `compareOrdered`, which
/// unconditionally recurses into the paired shapes regardless of whether the
/// names at that position agree. Trailing contract fields beyond the
/// producer's length are `Missing`; trailing producer fields are `Extra`.
const fn compare_ordered(
    producer: &[FieldShape],
    contract: &[FieldShape],
    path: [&'static str; MAX_PATH_DEPTH],
    depth: usize,
    mode: Mode,
    diffs: &mut Comparison,
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
        let (field_path, field_depth) = append_segment(path, depth, c.name);
        if !names_match {
            let (name_path, _) = append_segment(field_path, field_depth, "(name)");
            diffs.push(mismatch_diff_labeled(name_path, c.name, p.name));
        }
        compare_shapes(&p.shape, &c.shape, field_path, field_depth, mode, diffs);
        i += 1;
    }
    let mut j = shared;
    while j < contract.len() {
        let (field_path, _) = append_segment(path, depth, contract[j].name);
        diffs.push(missing_diff(
            field_path,
            is_optional(&contract[j].shape),
            contract[j].has_default,
        ));
        j += 1;
    }
    let mut k = shared;
    while k < producer.len() {
        let (field_path, _) = append_segment(path, depth, producer[k].name);
        diffs.push(extra_diff(field_path));
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
    diffs: &mut Comparison,
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
        diffs.push(missing_diff(
            field_path,
            is_optional(&contract[j].shape),
            contract[j].has_default,
        ));
        j += 1;
    }
    let mut k = shared;
    while k < producer.len() {
        let (field_path, _) = append_segment(path, depth, producer[k].name);
        diffs.push(extra_diff(field_path));
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
    diffs: &mut Comparison,
) {
    match (producer, contract) {
        (TypeShape::Primitive(p), TypeShape::Primitive(c)) => {
            if !str_eq(p, c) {
                diffs.push(mismatch_diff_labeled(path, c, p));
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
            diffs.push(mismatch_diff_labeled(
                path,
                shape_label(contract),
                shape_label(producer),
            ));
        }
    }
}

/// Appends `seg` to `path` at `depth`, returning the (possibly unchanged)
/// path and the next depth. Once `depth` reaches `MAX_PATH_DEPTH`, further
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

/// Name-based matching requires a one-to-one correspondence on each side.
const fn duplicate_field_name(fields: &[FieldShape], ci: bool) -> Option<&'static str> {
    let mut i = 0;
    while i < fields.len() {
        let mut j = i + 1;
        while j < fields.len() {
            let same = if ci {
                str_eq_ci(fields[i].name, fields[j].name)
            } else {
                str_eq(fields[i].name, fields[j].name)
            };
            if same {
                return Some(fields[j].name);
            }
            j += 1;
        }
        i += 1;
    }
    None
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

/// ASCII case-insensitive byte comparison. Non-ASCII identifier bytes must
/// match exactly; this does not perform Unicode case folding.
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

/// A `Missing` diff's `" (optional)"`/`" (default)"` label, or `""`.
const fn diff_annotation(diffs: &Diffs, i: usize) -> &'static str {
    match diffs.items[i] {
        Some(diff) => diff.annotation,
        None => "",
    }
}

/// `" (expected \`"` if this diff carries `expected`/`found` labels, else
/// `""` — opens the CTDC-style `expected X, found Y` clause.
const fn expected_open(diffs: &Diffs, i: usize) -> &'static str {
    match diffs.items[i] {
        Some(diff) if !diff.expected.is_empty() => " (expected `",
        _ => "",
    }
}

/// The `i`th diff's `expected` label, or `""`.
const fn diff_expected(diffs: &Diffs, i: usize) -> &'static str {
    match diffs.items[i] {
        Some(diff) => diff.expected,
        None => "",
    }
}

/// `"\`, found \`"` between `expected` and `found`, or `""`.
const fn expected_mid(diffs: &Diffs, i: usize) -> &'static str {
    match diffs.items[i] {
        Some(diff) if !diff.expected.is_empty() => "`, found `",
        _ => "",
    }
}

/// The `i`th diff's `found` label, or `""`.
const fn diff_found(diffs: &Diffs, i: usize) -> &'static str {
    match diffs.items[i] {
        Some(diff) => diff.found,
        None => "",
    }
}

/// `"\`)"` closing the `expected X, found Y` clause, or `""`.
const fn expected_close(diffs: &Diffs, i: usize) -> &'static str {
    match diffs.items[i] {
        Some(diff) if !diff.expected.is_empty() => "`)",
        _ => "",
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
    /// under `Policy`, naming up to `MAX_DIFFS` mismatched/missing/extra
    /// fields (dotted path, e.g. `shipTo.zip`) in the panic message.
    ///
    /// Built with `const_panic::concat_panic!` rather than
    /// `const_format::concatcp!`: the diffs are only known once
    /// `Producer`/`ContractT`/`Policy` are monomorphized, and
    /// `concat_panic!`'s argument list already concatenates whatever pieces
    /// it's given, so each diff's fixed-size path segments are passed as
    /// individual arguments (with per-slot separators) rather than
    /// pre-joined into a string — no string-building dependency needed for
    /// a fixed number of slots.
    ///
    /// The per-diff-slot argument block (one `#(...)*` group below) is
    /// generated once via `seq_macro::seq!(I in 0..128 { ... })` instead of
    /// being hand-unrolled `MAX_DIFFS` times — raising `MAX_DIFFS` is now
    /// this doc comment's range literal plus the constant itself (`seq!`
    /// expands via raw token substitution, so its range must be written as
    /// a literal, not a reference to the `MAX_DIFFS` const). The inner
    /// path-segment list (`diff_seg`/`diff_seg_sep` pairs, one per
    /// `MAX_PATH_DEPTH` slot) is still written out once by hand inside that
    /// group rather than mechanized by a second, nested counter — a
    /// two-dimensional `seq!`/`seq!` or `seq!`/`macro_rules!` nesting was
    /// considered and rejected: a macro invocation sitting at a single
    /// `concat_panic!` argument position must expand to exactly one
    /// expression, so it cannot splice several sibling comma-separated
    /// arguments into the surrounding list. Raising `MAX_PATH_DEPTH`
    /// therefore still requires manually extending this one list — a
    /// proportional, one-time edit, not a multiplicative one.
    pub const CHECK: () = {
        let comparison = compare(Producer::SHAPE, ContractT::SHAPE, Policy::POLICY);
        let diffs = comparison.diffs;
        if comparison.rejected {
            seq_macro::seq!(I in 0..128 {
                const_panic::concat_panic!(
                    const_panic::FmtArg::DISPLAY;
                    "producer schema does not conform to the contract: ",
                    #(
                        diff_lead_sep(&diffs, I),
                        diff_kind_label(&diffs, I),
                        diff_seg(&diffs, I, 0),
                        diff_seg_sep(&diffs, I, 1), diff_seg(&diffs, I, 1),
                        diff_seg_sep(&diffs, I, 2), diff_seg(&diffs, I, 2),
                        diff_seg_sep(&diffs, I, 3), diff_seg(&diffs, I, 3),
                        diff_seg_sep(&diffs, I, 4), diff_seg(&diffs, I, 4),
                        diff_seg_sep(&diffs, I, 5), diff_seg(&diffs, I, 5),
                        diff_seg_sep(&diffs, I, 6), diff_seg(&diffs, I, 6),
                        diff_seg_sep(&diffs, I, 7), diff_seg(&diffs, I, 7),
                        diff_close(&diffs, I),
                        diff_annotation(&diffs, I),
                        expected_open(&diffs, I),
                        diff_expected(&diffs, I),
                        expected_mid(&diffs, I),
                        diff_found(&diffs, I),
                        expected_close(&diffs, I),
                    )*
                );
            });
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
    fn exact_ordered_name_mismatch_still_recurses_into_shape() {
        // Matches CTDC's `compareOrdered`: a name mismatch at an index does
        // not stop the traversal from also comparing the shapes at that
        // index — both the name diff and the shape diff are reported.
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
        let diffs = compare(
            Producer::SHAPE,
            ContractT::SHAPE,
            SchemaPolicy::ExactOrdered,
        )
        .diffs;
        // Index 0: producer `name`(String) vs contract `id`(i64) — name
        // mismatch AND shape mismatch. Index 1: mirror image. 4 diffs total.
        assert_eq!(diffs.len, 4);
        let kinds: std::vec::Vec<DiffKind> = diffs.items.iter().flatten().map(|d| d.kind).collect();
        assert!(kinds.iter().all(|k| *k == DiffKind::Mismatch));
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
            let diffs = compare(
                ShipToProducer::SHAPE,
                ShipToZipMismatch::SHAPE,
                SchemaPolicy::Exact,
            )
            .diffs;
            assert_eq!(diffs.len, 1);
            let diff = diffs.items[0].expect("diff must be present");
            assert_eq!(diff.kind, DiffKind::Mismatch);
            assert_eq!(diff.path[0], "ship_to");
            assert_eq!(diff.path[1], "zip");
            assert_eq!(diff.path[2], "");
        }

        #[test]
        fn two_level_nested_mismatch_reports_full_path() {
            let diffs = compare(
                OrderProducer::SHAPE,
                OrderGeoMismatch::SHAPE,
                SchemaPolicy::Exact,
            )
            .diffs;
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
            let diffs = compare(
                ShipToProducer::SHAPE,
                ShipToMissingZip::SHAPE,
                SchemaPolicy::Exact,
            )
            .diffs;
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
            let diffs = compare(Producer::SHAPE, ContractT::SHAPE, SchemaPolicy::Exact).diffs;
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
            let diffs = compare(Producer::SHAPE, ContractT::SHAPE, SchemaPolicy::Exact).diffs;
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
        let diffs = compare(Producer::SHAPE, ContractT::SHAPE, SchemaPolicy::Exact).diffs;
        assert_eq!(diffs.len, 2);
    }

    #[test]
    fn diff_count_between_old_and_new_max_diffs_is_fully_captured() {
        const PRODUCER: TypeShape = TypeShape::Struct(&[]);
        const TARGET: TypeShape = TypeShape::Struct(&[
            FieldShape {
                name: "a",
                shape: TypeShape::Primitive("i64"),
                has_default: false,
            },
            FieldShape {
                name: "b",
                shape: TypeShape::Primitive("i64"),
                has_default: false,
            },
            FieldShape {
                name: "c",
                shape: TypeShape::Primitive("i64"),
                has_default: false,
            },
            FieldShape {
                name: "d",
                shape: TypeShape::Primitive("i64"),
                has_default: false,
            },
            FieldShape {
                name: "e",
                shape: TypeShape::Primitive("i64"),
                has_default: false,
            },
            FieldShape {
                name: "f",
                shape: TypeShape::Primitive("i64"),
                has_default: false,
            },
            FieldShape {
                name: "g",
                shape: TypeShape::Primitive("i64"),
                has_default: false,
            },
            FieldShape {
                name: "h",
                shape: TypeShape::Primitive("i64"),
                has_default: false,
            },
            FieldShape {
                name: "i",
                shape: TypeShape::Primitive("i64"),
                has_default: false,
            },
            FieldShape {
                name: "j",
                shape: TypeShape::Primitive("i64"),
                has_default: false,
            },
            FieldShape {
                name: "k",
                shape: TypeShape::Primitive("i64"),
                has_default: false,
            },
            FieldShape {
                name: "l",
                shape: TypeShape::Primitive("i64"),
                has_default: false,
            },
        ]);
        // 12 missing fields: beyond the old MAX_DIFFS = 8, within the raised
        // MAX_DIFFS = 16 — proves the cap increase actually took effect, not
        // just that `seq_macro` produced equivalent code for the old bound.
        let diffs = compare(PRODUCER, TARGET, SchemaPolicy::Exact).diffs;
        assert_eq!(diffs.len, 12);
    }

    /// Records `diff` in `diffs` iff `policy` rejects it — the `Vec`-backed
    /// counterpart to `Comparison::push`.
    fn push_if_rejected(diffs: &mut Vec<Diff>, diff: Diff, policy: SchemaPolicy) {
        if rejects(diff, policy) {
            diffs.push(diff);
        }
    }

    /// Test-only, genuinely unbounded counterpart to `compare`: every pure
    /// helper below the sink layer (`append_segment`, `shape_eq`,
    /// `find_field_mode`, ...) is reused unchanged, since those are plain
    /// calculations with no `Diffs`/`Comparison` coupling. Only the thin
    /// "where does this diff go" layer below is duplicated against a
    /// `Vec<Diff>` instead of `compare`'s fixed `[Option<Diff>; MAX_DIFFS]`.
    /// This can't be unified with `compare` behind a generic sink: a
    /// `const fn` must typecheck as const-valid on every reachable branch,
    /// so a function that can call `Vec::push` can never be `const fn` —
    /// not even behind a branch skipped at const-eval time. `compare`
    /// itself stays the one path `SchemaConforms::CHECK` relies on; this
    /// exists only so a test can see every mismatch a pathological schema
    /// produces, not just the first `MAX_DIFFS`.
    fn compare_unbounded(
        producer: TypeShape,
        contract: TypeShape,
        policy: SchemaPolicy,
    ) -> Vec<Diff> {
        let mode = policy_mode(policy);
        let mut diffs = Vec::new();
        match (&producer, &contract) {
            (TypeShape::Struct(producer_fields), TypeShape::Struct(contract_fields)) => {
                compare_fields_unbounded(
                    producer_fields,
                    contract_fields,
                    [""; MAX_PATH_DEPTH],
                    0,
                    mode,
                    policy,
                    &mut diffs,
                );
            }
            (producer, contract) => {
                if !shape_eq(producer, contract) {
                    push_if_rejected(
                        &mut diffs,
                        mismatch_diff_labeled(
                            single_segment("<root>"),
                            shape_label(contract),
                            shape_label(producer),
                        ),
                        policy,
                    );
                }
            }
        }
        diffs
    }

    fn compare_fields_unbounded(
        producer: &[FieldShape],
        contract: &[FieldShape],
        path: [&'static str; MAX_PATH_DEPTH],
        depth: usize,
        mode: Mode,
        policy: SchemaPolicy,
        diffs: &mut Vec<Diff>,
    ) {
        if mode.by_pos {
            compare_by_position_unbounded(producer, contract, path, depth, mode, policy, diffs);
        } else if mode.ordered {
            compare_ordered_unbounded(producer, contract, path, depth, mode, policy, diffs);
        } else {
            compare_by_name_unbounded(producer, contract, path, depth, mode, policy, diffs);
        }
    }

    fn compare_by_name_unbounded(
        producer: &[FieldShape],
        contract: &[FieldShape],
        path: [&'static str; MAX_PATH_DEPTH],
        depth: usize,
        mode: Mode,
        policy: SchemaPolicy,
        diffs: &mut Vec<Diff>,
    ) {
        if let Some(name) = duplicate_field_name(producer, mode.ci) {
            let (field_path, _) = append_segment(path, depth, name);
            push_if_rejected(
                diffs,
                mismatch_diff_labeled(field_path, "unique field name", name),
                policy,
            );
            return;
        }
        if let Some(name) = duplicate_field_name(contract, mode.ci) {
            let (field_path, _) = append_segment(path, depth, name);
            push_if_rejected(
                diffs,
                mismatch_diff_labeled(field_path, "unique field name", name),
                policy,
            );
            return;
        }
        for c in contract {
            match find_field_mode(producer, c.name, mode.ci) {
                Some(p) => {
                    let (field_path, field_depth) = append_segment(path, depth, c.name);
                    compare_shapes_unbounded(
                        &p.shape,
                        &c.shape,
                        field_path,
                        field_depth,
                        mode,
                        policy,
                        diffs,
                    );
                }
                None => {
                    let (field_path, _) = append_segment(path, depth, c.name);
                    push_if_rejected(
                        diffs,
                        missing_diff(field_path, is_optional(&c.shape), c.has_default),
                        policy,
                    );
                }
            }
        }
        for p in producer {
            if find_field_mode(contract, p.name, mode.ci).is_none() {
                let (field_path, _) = append_segment(path, depth, p.name);
                push_if_rejected(diffs, extra_diff(field_path), policy);
            }
        }
    }

    fn compare_ordered_unbounded(
        producer: &[FieldShape],
        contract: &[FieldShape],
        path: [&'static str; MAX_PATH_DEPTH],
        depth: usize,
        mode: Mode,
        policy: SchemaPolicy,
        diffs: &mut Vec<Diff>,
    ) {
        let shared = min_len(producer.len(), contract.len());
        for i in 0..shared {
            let p = &producer[i];
            let c = &contract[i];
            let names_match = if mode.ci {
                str_eq_ci(p.name, c.name)
            } else {
                str_eq(p.name, c.name)
            };
            let (field_path, field_depth) = append_segment(path, depth, c.name);
            if !names_match {
                let (name_path, _) = append_segment(field_path, field_depth, "(name)");
                push_if_rejected(
                    diffs,
                    mismatch_diff_labeled(name_path, c.name, p.name),
                    policy,
                );
            }
            compare_shapes_unbounded(
                &p.shape,
                &c.shape,
                field_path,
                field_depth,
                mode,
                policy,
                diffs,
            );
        }
        for c in &contract[shared..] {
            let (field_path, _) = append_segment(path, depth, c.name);
            push_if_rejected(
                diffs,
                missing_diff(field_path, is_optional(&c.shape), c.has_default),
                policy,
            );
        }
        for p in &producer[shared..] {
            let (field_path, _) = append_segment(path, depth, p.name);
            push_if_rejected(diffs, extra_diff(field_path), policy);
        }
    }

    fn compare_by_position_unbounded(
        producer: &[FieldShape],
        contract: &[FieldShape],
        path: [&'static str; MAX_PATH_DEPTH],
        depth: usize,
        mode: Mode,
        policy: SchemaPolicy,
        diffs: &mut Vec<Diff>,
    ) {
        let shared = min_len(producer.len(), contract.len());
        for i in 0..shared {
            let p = &producer[i];
            let c = &contract[i];
            let (field_path, field_depth) = append_segment(path, depth, p.name);
            compare_shapes_unbounded(
                &p.shape,
                &c.shape,
                field_path,
                field_depth,
                mode,
                policy,
                diffs,
            );
        }
        for c in &contract[shared..] {
            let (field_path, _) = append_segment(path, depth, c.name);
            push_if_rejected(
                diffs,
                missing_diff(field_path, is_optional(&c.shape), c.has_default),
                policy,
            );
        }
        for p in &producer[shared..] {
            let (field_path, _) = append_segment(path, depth, p.name);
            push_if_rejected(diffs, extra_diff(field_path), policy);
        }
    }

    fn compare_shapes_unbounded(
        producer: &TypeShape,
        contract: &TypeShape,
        path: [&'static str; MAX_PATH_DEPTH],
        depth: usize,
        mode: Mode,
        policy: SchemaPolicy,
        diffs: &mut Vec<Diff>,
    ) {
        match (producer, contract) {
            (TypeShape::Primitive(p), TypeShape::Primitive(c)) => {
                if !str_eq(p, c) {
                    push_if_rejected(diffs, mismatch_diff_labeled(path, c, p), policy);
                }
            }
            (TypeShape::Optional(p), TypeShape::Optional(c)) => {
                compare_shapes_unbounded(p, c, path, depth, mode, policy, diffs);
            }
            (TypeShape::Sequence(p), TypeShape::Sequence(c)) => {
                let (seq_path, seq_depth) = append_segment(path, depth, "[]");
                compare_shapes_unbounded(p, c, seq_path, seq_depth, mode, policy, diffs);
            }
            (TypeShape::Map(pk, pv), TypeShape::Map(ck, cv)) => {
                let (key_path, key_depth) = append_segment(path, depth, "<key>");
                compare_shapes_unbounded(pk, ck, key_path, key_depth, mode, policy, diffs);
                let (value_path, value_depth) = append_segment(path, depth, "<value>");
                compare_shapes_unbounded(pv, cv, value_path, value_depth, mode, policy, diffs);
            }
            (TypeShape::Struct(pf), TypeShape::Struct(cf)) => {
                compare_fields_unbounded(pf, cf, path, depth, mode, policy, diffs);
            }
            _ => {
                if !shape_eq(producer, contract) {
                    push_if_rejected(
                        diffs,
                        mismatch_diff_labeled(path, shape_label(contract), shape_label(producer)),
                        policy,
                    );
                }
            }
        }
    }

    #[test]
    fn unbounded_diff_report_names_every_mismatch_past_max_diffs() {
        seq_macro::seq!(I in 0..200 {
            const MANY_FIELDS: TypeShape = TypeShape::Struct(&[
                #(
                    FieldShape {
                        name: concat!("f", I),
                        shape: TypeShape::Primitive("i64"),
                        has_default: false,
                    },
                )*
            ]);
        });

        let diffs = compare_unbounded(TypeShape::Struct(&[]), MANY_FIELDS, SchemaPolicy::Exact);

        // 200 missing fields: past MAX_DIFFS (128), proving the Vec-backed
        // path names every one of them instead of truncating like `compare`.
        assert_eq!(diffs.len(), 200);
        assert!(diffs.len() > MAX_DIFFS);

        let report: String = diffs
            .iter()
            .map(|diff| format!("missing `{}`", diff.path[0]))
            .collect::<Vec<_>>()
            .join(", ");
        assert_eq!(report.matches("missing `").count(), 200);
    }

    const LATE_CONTRACT: TypeShape = TypeShape::Struct(&[
        FieldShape {
            name: "a",
            shape: TypeShape::Optional(&TypeShape::Primitive("i64")),
            has_default: false,
        },
        FieldShape {
            name: "b",
            shape: TypeShape::Optional(&TypeShape::Primitive("i64")),
            has_default: false,
        },
        FieldShape {
            name: "c",
            shape: TypeShape::Optional(&TypeShape::Primitive("i64")),
            has_default: false,
        },
        FieldShape {
            name: "d",
            shape: TypeShape::Optional(&TypeShape::Primitive("i64")),
            has_default: false,
        },
        FieldShape {
            name: "e",
            shape: TypeShape::Optional(&TypeShape::Primitive("i64")),
            has_default: false,
        },
        FieldShape {
            name: "f",
            shape: TypeShape::Optional(&TypeShape::Primitive("i64")),
            has_default: false,
        },
        FieldShape {
            name: "g",
            shape: TypeShape::Optional(&TypeShape::Primitive("i64")),
            has_default: false,
        },
        FieldShape {
            name: "h",
            shape: TypeShape::Optional(&TypeShape::Primitive("i64")),
            has_default: false,
        },
        FieldShape {
            name: "id",
            shape: TypeShape::Primitive("i64"),
            has_default: false,
        },
    ]);
    const LATE_PRODUCER: TypeShape = TypeShape::Struct(&[FieldShape {
        name: "id",
        shape: TypeShape::Primitive("String"),
        has_default: false,
    }]);

    #[test]
    fn backward_rejects_mismatch_after_tolerated_missing_fields() {
        assert!(!conforms(
            LATE_PRODUCER,
            LATE_CONTRACT,
            SchemaPolicy::Backward
        ));
    }

    #[test]
    fn forward_rejects_mismatch_after_tolerated_missing_fields() {
        assert!(!conforms(
            LATE_PRODUCER,
            LATE_CONTRACT,
            SchemaPolicy::Forward
        ));
    }

    #[test]
    fn policies_keep_rejections_beyond_diagnostic_capacity() {
        for policy in [
            SchemaPolicy::Exact,
            SchemaPolicy::ExactUnorderedCI,
            SchemaPolicy::ExactOrdered,
            SchemaPolicy::ExactOrderedCI,
            SchemaPolicy::ExactByPosition,
            SchemaPolicy::Backward,
            SchemaPolicy::Forward,
        ] {
            assert!(
                !conforms(LATE_PRODUCER, LATE_CONTRACT, policy),
                "{policy:?}"
            );
        }
        assert!(conforms(LATE_PRODUCER, LATE_CONTRACT, SchemaPolicy::Full));
    }

    #[test]
    fn mismatch_below_display_path_limit_still_rejects() {
        const PRODUCER: TypeShape = TypeShape::Struct(&[FieldShape {
            name: "deep",
            shape: TypeShape::Sequence(&TypeShape::Sequence(&TypeShape::Sequence(
                &TypeShape::Sequence(&TypeShape::Sequence(&TypeShape::Sequence(
                    &TypeShape::Sequence(&TypeShape::Primitive("String")),
                ))),
            ))),
            has_default: false,
        }]);
        const TARGET: TypeShape = TypeShape::Struct(&[FieldShape {
            name: "deep",
            shape: TypeShape::Sequence(&TypeShape::Sequence(&TypeShape::Sequence(
                &TypeShape::Sequence(&TypeShape::Sequence(&TypeShape::Sequence(
                    &TypeShape::Sequence(&TypeShape::Primitive("i64")),
                ))),
            ))),
            has_default: false,
        }]);
        assert!(!conforms(PRODUCER, TARGET, SchemaPolicy::Exact));
        assert_eq!(compare(PRODUCER, TARGET, SchemaPolicy::Exact).diffs.len, 1);
    }

    #[test]
    fn backward_rejects_after_tolerated_nested_extras() {
        const PRODUCER: TypeShape = TypeShape::Struct(&[
            FieldShape {
                name: "group",
                shape: LATE_CONTRACT,
                has_default: false,
            },
            FieldShape {
                name: "id",
                shape: TypeShape::Primitive("String"),
                has_default: false,
            },
        ]);
        const TARGET: TypeShape = TypeShape::Struct(&[
            FieldShape {
                name: "group",
                shape: TypeShape::Struct(&[]),
                has_default: false,
            },
            FieldShape {
                name: "id",
                shape: TypeShape::Primitive("i64"),
                has_default: false,
            },
        ]);
        assert!(!conforms(PRODUCER, TARGET, SchemaPolicy::Backward));
    }

    #[test]
    fn compile_fail_mismatched_shapes_panic_the_const_check() {
        let t = trybuild::TestCases::new();
        // A pass case makes locked trybuild use cargo build, which catches
        // monomorphization-time inline-const failures that cargo check misses.
        t.pass("tests/pass/*.rs");
        t.compile_fail("tests/compile_fail/*.rs");
    }

    mod shadowed_container_name {
        use super::Contract;

        // A local Vec follows its own Contract instead of std's sequence shape.
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

# Compile-time schema contracts

`crates/contracts` (plus its derive macro, `crates/contracts-derive`) checks
whether one struct's shape is compatible with another's — the "producer" and
the "contract" — entirely at compile time. If the shapes don't conform under
the chosen policy, the crate fails to compile with an `assertion failed`
message pointing at the check, not a test failure discovered later.

## The problem this solves

Two parts of a pipeline agree on a schema by convention: a struct produced
by one stage, and a struct expected by the next. Nothing stops them from
drifting apart — a field gets renamed, a type changes from `String` to
`Option<String>` — and the mismatch is usually only caught when real data
flows through and something panics or silently drops a column. This crate
turns that drift into a build failure instead.

## How it works

1. `#[derive(Contract)]` walks a struct's fields and builds a `TypeShape`
   tree describing them — recursing through `Option<T>`, `Vec<T>`, and
   `HashMap`/`BTreeMap<K, V>` so nested optionality is preserved rather than
   flattened (`Vec<Option<T>>` and `Vec<T>` are different shapes, and stay
   different all the way through comparison).
2. `conforms(producer, contract, policy)` is a `const fn` that structurally
   compares two `TypeShape`s under a [`SchemaPolicy`](#policies).
3. `SchemaConforms::<Producer, ContractT, Policy>::CHECK` is a `const` whose
   initializer panics if the shapes don't conform. Referencing it from a
   `const _: () = ...;` item forces the compiler to evaluate that panic
   during compilation — so a schema mismatch becomes a real compile error at
   the reference site.

```rust,ignore
use contracts::{Backward, Contract, SchemaConforms};

#[derive(Contract)]
struct Producer {
    id: i64,
    name: String,
    tags: Vec<Option<String>>,
}

#[derive(Contract)]
struct Contract {
    id: i64,
    name: String,
    note: Option<String>, // missing from Producer, but Optional — allowed under Backward
}

// Fails to compile if Producer no longer conforms to Contract under Backward:
const _: () = SchemaConforms::<Producer, Contract, Backward>::CHECK;
```

## Policies

| Policy     | Rule                                                                                          |
|------------|-------------------------------------------------------------------------------------------------|
| `Exact`    | Same fields on both sides, by name, each with an identical shape. No extras either way.        |
| `Backward` | Every contract field is satisfied — present with a matching shape, or absent but `Optional`. Producer may carry extra fields. |
| `Forward`  | Every producer field exists in the contract with a matching shape. Contract may have extra `Optional` fields the producer omits. |
| `Full`     | Both `Backward` and `Forward` must hold.                                                        |

Not ported in this slice: ordered-field and case-insensitive-name policy
variants — see `docs/internals/notes/decisions.md` for the tracked backlog.

## No engine dependency

`contracts` depends only on `contracts-derive`, which depends only on
`syn`/`quote`/`proc-macro2` — no Arrow, no DataFusion, no execution engine
anywhere in either crate. `TypeShape` comes straight from a struct's own
field types, so the same contract check works regardless of what (if
anything) later reads that struct's data.

## Try it

```sh
cargo test -p contracts -p contracts-derive
```

Includes `trybuild` cases proving an `Exact`-policy mismatch is a genuine
compile error, and a case proving `#[derive(Contract)]` itself is rejected
on generic structs (a bare type parameter has no shape the macro can see).

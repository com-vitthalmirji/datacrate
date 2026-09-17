# Rust fundamentals for JVM & Python engineers

This chapter is the vocabulary you need before anything else in datacrate
makes sense. If you already know what ownership, borrowing, `Option`/`Result`,
traits, and closures mean in Rust, skip straight to [Usage](usage.md) - you
won't miss anything.

Most of the bridges below lean on Scala, because Scala's type system maps
onto Rust's most directly - `sealed trait`, `Option`, `Either`, `for`-
comprehensions all have a near-exact Rust counterpart. Where the more useful
comparison is Python or PySpark instead (dynamic typing, `None`, exceptions,
DataFrame transformations), it's called out explicitly.

This is deliberately **not** a Rust language tour. It covers exactly the
concepts `datacrate` leans on, in the order it leans on them, and nothing
else. Async/await, `Send`/`Sync`, and a couple of trickier generic patterns
are intentionally left out of this chapter - they get their own proper
introduction later, in the chapters where they first matter, instead of being
rushed through here before you have anywhere to hang them.

Every concept below has a runnable home in `crates/dtl-core` - a small
library built for exactly this purpose. Read the doc comments
(`cargo doc -p dtl-core --open`), run the tests, and come back. For a single
end-to-end tour of ownership, borrowing, references, and slices in one
binary, run:

```sh
cargo run -p dtl-core --example fundamentals_demo
```

## The one big idea: ownership

A JVM never asks "whose job is it to free this object?" - the garbage
collector figures it out at runtime, by tracing what's still reachable.
Rust asks that question at *compile* time, and the answer is always the
same: **exactly one binding owns a value, and when that binding goes out of
scope, the value is dropped.** No tracing, no pauses, no runtime cost.

The consequence that trips people up first: passing a value by default
*moves* it, it doesn't copy it. The everyday version of this: you hand a
friend the one physical notebook you were writing in. They can read it, write
in it, hand it back - but *you* don't have it anymore while they do, and if
you try to flip through "your" notebook in the meantime, there's nothing
there. Ownership is that same rule, just enforced by the compiler instead of
by "well, obviously, it's a single physical object":

```rust
fn takes_and_gives_back(value: String) -> String {
    value // ownership passed in, then passed back out
}

let draft = String::from("first draft of the report");
let returned = takes_and_gives_back(draft);
// `draft` is no longer usable here - the notebook left your hands the moment
// it was passed to `takes_and_gives_back`. `returned` is the same notebook,
// handed back.
```

That's not a restriction Rust adds for fun; it's what makes "who's
responsible for this data, and for how long" a question with one obvious
answer everywhere in the codebase, including in code you didn't write.

**JVM bridge**: you already know the *rule* - `val`s should be immutable,
side-effecting mutation should be visible and controlled. Rust just enforces
the "who owns this" half of that discipline at compile time instead of by
convention. `Clone` (see below) is your escape hatch when you genuinely want
a copy instead of a move - it's `.clone()`ing a Scala case class, spelled
the same way, but in Rust it's opt-in per call site instead of the default.

**Python bridge**: CPython also tracks "who's responsible for freeing this" -
via reference counting, at runtime, on every assignment and every scope exit.
Rust asks the same question but answers it once, at compile time: for a plain
owned value like the `String` above there is no counter at all, at runtime,
anywhere - the compiler has already worked out, before your program runs,
the single point where it's safe to free the value. Two separate things
CPython blurs into one mechanism are worth keeping apart here: *moving*
(the notebook - `draft` - handed to `takes_and_gives_back`, `draft` no
longer usable) happens at the point of use, the same instant a Python name
would still be
perfectly valid; *dropping* (the value's memory actually freed) happens
later, when the owner's scope ends - closer to what CPython's refcount
hitting zero triggers, just decided ahead of time instead of counted at
runtime.

```console
$ cargo test -p dtl-core transfers_ownership_back_to_the_caller -- --nocapture
$ cargo test -p dtl-core clones_owned_text_explicitly -- --nocapture
```

## Borrowing: reading without owning

Moving ownership every time you want to *look* at a value would make
functions unusable - you'd have to hand a value back from every function
that only wanted to read it. Borrowing is the fix: a reference lets code use
a value without taking it. Back to the notebook: instead of handing it over,
you let someone read it over your shoulder - you still have it, they can see
it, and the moment they're done looking, nothing about ownership changed.

```rust
fn calculate_length(value: &str) -> usize {
    value.len() // borrows, doesn't own - nothing to give back
}
```

Two kinds of reference, and exactly one rule governs both:

- `&T` - a **shared** borrow. Any number of people can read over your
  shoulder at once, as long as nobody's writing in it.
- `&mut T` - an **exclusive** borrow. This is you actually handing someone
  the pen - while they're writing, nobody else gets to read *or* write,
  not even you, until they hand the pen back.

**Many readers, or one writer. Never both, at the same time, to the same
value.** The compiler checks this statically and refuses to build code that
violates it - this is "the borrow checker," and it's not a linter you can
silence, it's the type system. [Ownership and streaming](ownership.md) walks
through a real case where this rule rejects a first draft of working-looking
code, and what the fix actually looks like.

**JVM bridge**: Scala has no equivalent check. A `var` captured in a
closure that outlives its enclosing scope, or a mutable collection handed to
two callers who both think they have exclusive access, compiles fine and
fails at runtime (or silently gives the wrong answer). Rust's borrow checker
is that class of bug moved from "hope you wrote a test for it" to "does not
compile."

**Python bridge**: Python has no equivalent check either. The GIL only
guarantees that one thread's bytecode instruction finishes before another
starts - it says nothing about your program's logic, and most operations
(`counter += 1`, a `list` append followed by a read elsewhere) compile to
*several* bytecode instructions, so a thread switch mid-operation is exactly
how two references aliasing the same `list` or `dict` end up racing each
other, GIL and all. Rust's borrow checker catches the aliasing itself at
compile time, before the code runs - a stronger guarantee than "one
instruction at a time," not a restatement of it.

```console
$ cargo test -p dtl-core observes_without_taking_ownership -- --nocapture
$ cargo test -p dtl-core mutates_through_an_exclusive_borrow -- --nocapture
```

## Lifetimes, in one paragraph

A borrow can't outlive the value it points into - the compiler has to prove
that at every call site, and the way it proves it is by tracking a
**lifetime**, written `'a`. Read `'a` as a label the compiler attaches to
"how long is this borrow allowed to be valid," not a duration you configure.
Most of the time you'll never write one - the compiler infers it. You'll see
one explicitly on `CsvBatch<'a>` in [Ownership and streaming](ownership.md#zero-copy-row-splitting-dtl-corecsv_zero_copy):
it exists because `CsvBatch` holds `&'a [u8]` slices borrowed from the
caller's buffer, and the struct's own lifetime has to be tied to that
buffer's so the compiler can guarantee those slices never outlive it.

## No null, no exceptions: `Option` and `Result`

Rust has no `null`. A value that might be absent is `Option<T>` -
`Some(value)` or `None` - and the compiler forces you to handle both before
you can get at the value inside.

```rust
let note: Option<String> = None;
match note {
    Some(text) => println!("{text}"),
    None => println!("no note"),
}
```

An operation that might fail returns `Result<T, E>` - `Ok(value)` or
`Err(error)` - instead of throwing. There is no `try`/`catch` for ordinary
failures anywhere in this codebase; a function that can fail says so in its
return type, and the caller cannot ignore it without writing the word
`unwrap` where anyone reviewing the diff will see it.

**JVM bridge**: `Option<T>` is `Option[T]`, unsurprising. `Result<T, E>` is
`Either[E, A]` with the sides fixed (`Err` is always the left/failure side,
never ambiguous about which side means what). The `?` operator is your
`for`-comprehension's short-circuit, inlined at every call site instead of
requiring a comprehension block:

```rust,ignore
let field = record.get(column).ok_or(CliError::ColumnOutOfRange { .. })?;
//                                                                  ^ returns Err early, same as
//                                                                    a Left short-circuiting a
//                                                                    for-comprehension in Scala
```

[Ownership and streaming](ownership.md#-guarantees-no-partial-output-on-a-bad-column)
has a real example of `?` chaining through several calls to guarantee no
output is written before an error is detected.

**Python bridge**: Python has `None` for absence, but nothing stops a
function from returning `None` where the type hints promised a `str` - it's
a convention, checked (if at all) by a separate tool like `mypy`. `Option<T>`
makes "might be absent" part of the type itself, checked by the same compiler
that checks everything else. `Result`/`?` plays the role your `try`/`except`
plays, except the possibility of failure is visible in the function's
signature instead of hidden until it's raised at runtime - closer to a
PySpark `DataFrame` operation failing at `.collect()` time than to a Python
built-in raising deep inside a call stack you can't see from the signature.

## Errors are just enums

This codebase doesn't use an error-handling library - no `thiserror`, no
`anyhow`. Every error type (`CliError`, `PipelineIoError`, `PipelineError`,
`DownloadStepError`) is the same three-step hand-written pattern, because
the pattern itself is only a few lines:

```rust,ignore
#[derive(Debug)]
pub enum PipelineIoError {
    InvalidId { row: usize, value: String },
    // ...other variants, one per distinct failure...
}

impl std::fmt::Display for PipelineIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidId { row, value } => {
                write!(f, "row {row}: invalid id {value:?}")
            }
            // ...
        }
    }
}

impl std::error::Error for PipelineIoError {}
```

An `enum` variant per distinct failure, a `Display` impl that `match`es
every variant (the compiler rejects a missing arm - see the next section),
and an empty `impl std::error::Error` to opt into the standard error trait.
That's the whole recipe. [The DataFusion basics chapter](datafusion-basics.md#what-can-go-wrong)
walks through `PipelineIoError`'s full variant list once you have fixtures
to run it against.

**JVM bridge**: this is a `sealed trait DomainError` with `case class`
variants, spelled as one `enum` instead of a trait-plus-case-classes
hierarchy - and, unlike a Scala sealed trait, Rust's compiler enforces
exhaustive matching *everywhere* you pattern-match on it, not just in code
you remembered to run through a linter.

## Enums are ADTs, and `match` doesn't let you forget a case

A Rust `enum` is a proper algebraic data type - each variant can carry its
own data, and `match` on it must cover every variant or the code doesn't
compile:

```rust,ignore
match schema_policy {
    SchemaPolicy::Exact => { /* ... */ }
    SchemaPolicy::Backward => { /* ... */ }
    // forgetting a variant here is a compile error, not a silent bug
}
```

[Compile-time schema contracts](contracts.md) is built almost entirely out
of this: `TypeShape` and `SchemaPolicy` are enums, and the whole crate's
correctness rests on `match` arms the compiler has already checked are
exhaustive.

**JVM bridge**: same discipline as a `sealed trait` with exhaustiveness
checking turned on - except in Rust it's not a compiler flag you have to
remember to enable, it's the only way `match` works.

**Python bridge**: Python's closest thing is an `Enum` plus a chain of
`if`/`elif` - and nothing stops you from forgetting a branch; you find out
at runtime, on whichever input finally hits the missing case. Rust's `match`
finds that same bug for you before the code ever runs.

## Traits are interfaces - resolved differently than you'd guess

A `trait` is a set of methods a type promises to implement - the closest
Rust equivalent to a Scala `trait` used as an interface, or a Java
interface. `Display`, `Clone`, `Debug`, and `std::error::Error` above are
all traits from the standard library; `SchemaPolicyMarker` in
`crates/contracts` is a trait this codebase defines itself.

The part that *doesn't* map onto the JVM cleanly: Scala's implicit search
will hunt through companion objects and imports to find "something that
fits" your type - that's the mechanism behind things like automatic JSON
codecs, where you never write the encoder yourself and it just shows up.
Rust has nothing like that search. A trait implementation is resolved by
looking at the concrete type and the trait bound written right there in the
function signature - nothing is ever found by rummaging through scope for a
fit. When you see
`fn read_csv_rows<T>(...) -> Result<Vec<T>, PipelineIoError>`
(`crates/pipeline/src/lib.rs:216`), the `<T>` is a generic parameter with no
trait bound at all here - `T` can be anything, because this function never
inspects `T`, it just collects whatever the caller's parse closure produces.
Where a bound *is* needed, it's written explicitly: `fn foo<T: Display>(x: T)`
reads as "any `T`, as long as it implements `Display`" - visible at the
function signature, not inferred from an implicit search.

## Closures and `Box<dyn Fn>`

A closure is an anonymous function that can capture variables from its
surrounding scope - `|args| { ... }`, same shape as Scala's
`(args) => { ... }` or Python's `lambda args: ...`. [The typestate pipeline
builder](typestate.md) stores closures behind
`Box<dyn Fn(RecordBatch) -> RecordBatch>`: `Box` puts the closure on the heap
(closures can have different sizes depending on what they capture, so they
can't live directly in a struct field without one), and `dyn Fn` means "any
type implementing the `Fn` trait, decided at runtime" rather than "one
specific closure type fixed at compile time."

**JVM bridge**: `Box<dyn Fn(RecordBatch) -> RecordBatch>` is doing the job
of `RecordBatch => RecordBatch` (a `Function1` value) - Scala's function
types are reference types by default, so you get the heap allocation and
dynamic dispatch for free; Rust makes both explicit in the type because it
otherwise defaults to zero-cost, statically-dispatched closures.

**Python bridge**: every Python function is already this - a first-class
object living on the heap, dispatched dynamically, no annotation needed.
`Box<dyn Fn(...)>` is Rust spelling out, in the type signature, the exact
runtime shape Python gives you for free by default. A PySpark `udf` passed a
`lambda` is doing the same job as a closure stored in `typestate`'s
transform step - a function value handed to something else to call later.

## Two smart pointers worth knowing: `Box` and `Arc`

- **`Box<T>`** - a value on the heap instead of the stack. Used above to
  store a closure of unknown size, and anywhere a value needs to be one
  fixed size regardless of what's actually stored inside it.
- **`Arc<T>`** - an *atomically reference-counted* pointer: multiple owners
  can share the same value, and it's freed once the last owner drops it.
  `schema()` (`crates/pipeline/src/lib.rs:171`) returns `Arc<Schema>` -
  every query and every batch that shares a schema shares one allocation,
  not a copy each.

Rust also has `Rc<T>` - the same reference-counting idea, but only safe on a
single thread. This codebase never uses it: everything that needs shared
ownership here also needs to survive being handed across an async task or a
thread, so it reaches for `Arc` directly rather than starting with `Rc` and
upgrading later.

**JVM bridge**: neither has a direct equivalent, because the JVM's GC
already gives you shared ownership for free, everywhere, without asking. The
closest mental model is "an immutable value shared across threads without
copying it" - which on the JVM you also get for free, but Rust has to spell
out explicitly because nothing is shared by default.

**Python bridge**: same story as ownership above - CPython's refcounting
already gives every object shared ownership for free, at the cost of
incrementing/decrementing a counter on every assignment (and paying for the
GIL to make that safe across threads). `Arc<T>`'s atomic counter is doing
the same job explicitly, only where you actually need it, and it works
without a GIL because the counter itself is what's made thread-safe.

## Iterators and the `.map`/`.collect` habit

Rust's `Iterator` trait is lazy, chainable, and idiomatic in exactly the way
Scala's `List`/`Iterator` API is - `.map`, `.filter`, `.collect` all exist
and mean what you'd expect. `batch_from_rows`
(`crates/pipeline/src/lib.rs:238`) is the clearest example in this codebase:

```rust,ignore
let ids: Int64Array = rows.iter().map(|r| r.id).collect();
```

One pass over `rows`, one column built. `.collect()` needs a target type it
can build (`Int64Array` here) - Rust infers *what* to collect into from
context, the same way Scala's `.to(List)` or an explicit type annotation
tells `.collect` what shape to produce.

**Python bridge**: this is the same habit as a list comprehension
(`[r.id for r in rows]`), or a PySpark `.select(col("id"))` - one declarative
pass building one output, instead of a hand-rolled loop with an accumulator
you append to. The difference is what `.collect()` builds: a Python
comprehension always builds a `list`; Rust's `.collect()` builds whatever
type the surrounding code says it needs - here, an Arrow `Int64Array`, not a
`Vec`.

## Modules, crates, and visibility

A Rust **crate** is a compilation unit - roughly a Scala/sbt module.
`datacrate` is a Cargo *workspace*: multiple crates (`dtl-core`, `pipeline`,
`typestate`, `contracts`, ...) built together, each with its own
`Cargo.toml`, listed in [Getting Started](getting-started.md#workspace-layout).
Inside a crate, `mod` declares a **module** - a namespace, not a separate
compilation unit - and every item defaults to private to its module unless
marked otherwise:

- `pub` - visible outside the crate (`pub fn schema()`, part of the crate's
  public API).
- `pub(crate)` - visible anywhere inside the crate, nowhere outside it
  (`pub(crate) fn parse_id`, `crates/pipeline/src/lib.rs:192` - an internal
  helper, not part of the API contract).
- nothing - private to the module that defines it.

**JVM bridge**: `pub` is roughly `public`, `pub(crate)` has no clean Scala
equivalent (closest is `private[mypackage]`), and Rust's default (no
modifier = private to the module) is the opposite of Scala's default
(`public` unless you say otherwise) - worth remembering the first time a
field you expected to be visible isn't.

**Python bridge**: Python has no enforced visibility at all - `_leading_
underscore` is a convention, not a wall; anything can still be reached from
outside the module if you really want it. Rust's `pub`/`pub(crate)`/private
is that convention turned into a compiler-checked rule: reach for something
you shouldn't and the build fails, not just a linter warning.

## What's deliberately not here

- **`async`/`.await`, `Future`, `Send`/`Sync`** - first used starting in
  [the DataFusion chapters](datafusion-basics.md), properly introduced in
  [the object-store chapter](object-store.md), which exists specifically to
  slow down and explain the sync/async boundary rather than rushing it here.
- **The zero-sized marker-type trick** (`Missing`/`Present<T>` encoding
  builder state in the type system) - [Typestate pipeline builder](typestate.md)
  builds this from scratch; repeating it here would just be a worse copy of
  that chapter.
- **`const fn` evaluation forcing a panic at compile time** - the mechanism
  behind compile-time schema contracts, covered where it's used in
  [Compile-time schema contracts](contracts.md).

You now have enough vocabulary to read every other chapter in datacrate as
real code, not as syntax to take on faith. [Usage](usage.md) is next.

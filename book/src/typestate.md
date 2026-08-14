# Typestate pipeline builder

`crates/typestate` builds a pipeline (source → transform → sink) where an
incomplete pipeline fails to *compile*, not to *run*. There's no `unwrap()`
or `Result` check guarding against a missing stage — if you forget one,
`.build()` simply doesn't exist as a method, and rustc tells you so at the
call site.

## Why this instead of a runtime check

A conventional builder might look like this:

```rust,ignore
let pipeline = PipelineBuilder::new()
    .source(path)
    .sink(identity)
    .build()?; // returns Err("no transform set") at runtime
```

That bug — forgetting `.transform(...)` — only surfaces when the code
actually runs, possibly in production, possibly long after the builder was
written. The typestate pattern moves that check to compile time by giving
each required stage its own type parameter:

```rust,ignore
pub struct PipelineBuilder<Src, Xf, Snk> {
    source: Src,
    transform: Xf,
    sink: Snk,
}
```

`Src`, `Xf`, and `Snk` each start as a zero-sized `Missing` marker and
become `Present<T>` once you call the matching builder method. `.build()`
is only defined in the one `impl` block where all three parameters are
`Present<_>`:

```rust,ignore
impl PipelineBuilder<Present<PathBuf>, Present<Transform>, Present<Sink>> {
    pub fn build(self) -> Result<RecordBatch, pipeline::FixtureError> { .. }
}
```

Call `.build()` on any other combination and it's a plain "no method named
`build` found" compiler error — the same class of mistake as calling a
method that was never written, because as far as the type system is
concerned, it wasn't.

## Stages can be supplied in any order

Because `source`, `transform`, and `sink` are independent type parameters
(not one combined "pipeline state" enum), calling them in a different order
still narrows the right parameter:

```rust,ignore
PipelineBuilder::new()
    .transform(identity)
    .sink(identity)
    .source(path)   // order doesn't matter — each call narrows its own parameter
    .build()?;
```

## Try it

```sh
cargo test -p typestate
```

This runs the happy-path tests plus a `trybuild` suite
(`crates/typestate/tests/compile_fail/`) that asserts `.build()` is a
compile error when any one stage is missing — proof that the guarantee is
real, not just documented.

# Ownership and streaming: `csv-select` walked through

This chapter is a line-by-line tutorial through `crates/csv-cli/src/bin/csv-select.rs`
and `crates/dtl-core/src/csv_zero_copy.rs` - real code, not illustrative
pseudocode. If you're coming from Scala/Spark, each section starts with the
assumption that background invites, then the correction, then the Rust
concept it's actually demonstrating. Run the commands as you read; nothing
here is hypothetical.

## The CLI, in one command

```console
csv-select <INPUT> --column <ZERO_BASED_INDEX> [--no-headers]
```

```console
$ cargo run --bin csv-select -- fixtures/m1/headers.csv --column 2
note
"quoted, comma"
"line one
line two"
""
```

That output is `fixtures/m1/headers-column-2.csv` byte-for-byte - the
integration test `headers_mode_selects_column_by_index` in
`crates/csv-cli/tests/csv_select.rs` asserts exactly that equality. Run it
yourself: `cargo test -p csv-select-cli headers_mode_selects_column_by_index`.

## Streaming is a property of the type, not the loop

In Spark you'd write:

```scala
spark.read.option("header", "true").csv(path).select("note")
```

and never think about what "streaming" costs, because Catalyst plans lazily
and the driver never materializes the whole file. It's tempting to assume
`csv::Reader` gives you the same thing for free just because you write
`for record in reader.records()` instead of `.collect()`.

It doesn't come from the loop syntax. `for record in reader.records()`
(`csv-select.rs:129,141`) is streaming because `csv::Reader` itself never
loads the file into memory - it pulls one record at a time off the
underlying `Read`. `reader.records()` returns a lazy `Iterator`: one CSV
record allocated per `.next()` call, not a `Vec<StringRecord>` built up
front. If `run()` had instead written
`reader.records().collect::<Result<Vec<_>, _>>()` before writing anything,
you'd have silently defeated the entire point of streaming - same `for`
syntax at the call site, opposite memory behavior at the boundary that
actually matters. The habit to build: streaming is a property of the type
you're iterating over, not the loop you wrap around it.

## The borrow checker rejects a header held by reference

The obvious first draft of "print headers, then rows" reaches for a
reference:

```rust,ignore
let headers = reader.headers()?;   // &StringRecord, borrowed from `reader`
select_and_write(writer, headers, column)?;
for record in reader.records() { ... }   // reader.records() needs &mut reader
```

This does not compile. `reader.headers()` returns `&StringRecord` borrowed
from `reader`; `reader.records()` two lines later needs `&mut reader`. You
cannot hold a shared borrow of `reader` across a call that needs an
exclusive borrow of the same `reader` - the borrow checker rejects it, full
stop, no lifetime annotation fixes it, because the two borrows genuinely
overlap in time.

What the real code does (`csv-select.rs:120-127`):

```rust
let headers = reader
    .headers()
    .map_err(|source| CliError::ReadRecord { source })?
    .clone();
select_and_write(writer, &headers, column)?;

for record in reader.records() {
```

`.clone()` ends the borrow immediately - `headers` now owns its own
`StringRecord`, independent of `reader`, so the later `&mut reader` borrow
for `.records()` is uncontested. This is a **conscious clone**: it copies
one header row once per run, not per data row, so the cost is negligible
against the actual per-record I/O that follows. Every `clone()` in this
codebase is a decision, not a reflex - if you find yourself reaching for one
inside the per-row loop instead of before it, stop and ask why.

**Scala bridge**: this is the same shape as capturing a `var` in a closure
that outlives the scope holding it - Scala's compiler doesn't stop you,
because Scala doesn't track *when* a reference is alive, only *whether* one
exists. Rust's borrow checker does alias tracking Scala never does at all;
`.clone()` here is the manual equivalent of "copy it out so I don't have to
reason about lifetime overlap."

## Fusing lookup and write so a borrow never outlives its use

```rust
fn select_and_write<W: std::io::Write>(
    writer: &mut csv::Writer<W>,
    record: &csv::StringRecord,
    column: usize,
) -> Result<(), CliError> {
    let field = record.get(column).ok_or(CliError::ColumnOutOfRange {
        column,
        available: record.len(),
    })?;
    writer
        .write_record([field])
        .map_err(|source| CliError::WriteOutput { source })
}
```

`record.get(column)` returns `Option<&str>` - a borrow scoped to `record`.
Lookup and write are one function instead of two so that borrow never has to
survive past the function that created it. Splitting them would work today
(no intervening mutation), but it's exactly the shape that breaks the moment
someone inserts a line that needs `&mut record` in between. This is also why
`write_with_headers` and `write_no_headers` (`csv-select.rs:115-146`) look so
similar instead of being unified - they differ only in whether the first
record is a header or data, and threading a `bool` through `select_and_write`
for a distinction only the *caller* needs to know about would leak that
distinction into a function that shouldn't care.

## `?` guarantees no partial output on a bad column

```console
$ cargo run --bin csv-select -- fixtures/m1/headers.csv --column 99
csv-select: column 99 is out of range: only 3 column(s) present
$ echo $?
1
```

Nothing is written to stdout before that error - not even the header row,
even though the header row is written before the data-row loop starts
(`csv-select.rs:127`, before the `for` at `:129`). That's not an accident of
ordering; it's guaranteed by `select_and_write` returning `Result` and every
caller propagating with `?` (`csv-select.rs:125,127,130,131`). The moment
`record.get(column)` returns `None`, the function returns `Err` before
`writer.write_record` is ever called for that record - and because headers
go through the same `select_and_write` path, a bad column on a headers-mode
file fails on the header row itself, before any data row is even read.
`invalid_column_fails_before_writing_any_output` asserts
`output.stdout.is_empty()` for exactly this reason.

**Scala/Spark bridge**: this is the discipline you already apply with
`Either` chains in a for-comprehension - a `Left` on step 2 means step 3
never runs, no matter how step 3 is written. `?` gives you the same
short-circuit for free at every call site, without an explicit `match`.

## The one guarantee this design does *not* make

The module doc comment says so directly:

```rust
//! Current limitation: output is written record-by-record as the input is
//! streamed, so a malformed record partway through the file can leave
//! already-selected rows on stdout before the error is detected and the
//! process exits non-zero. Buffering the whole output to avoid this would
//! defeat the point of streaming, so it is accepted rather than fixed.
```

The "no partial output" guarantee from the previous section holds for an
*invalid column* (checked before any write happens) but not for a
*malformed record partway through the file* (the error only surfaces once
the reader reaches the bad record - by which point earlier good records are
already on stdout). Naming which guarantee holds, which doesn't, and why
fixing the second would cost you the entire reason to stream is the actual
skill here - not a gap to quietly patch over.

Four failure paths, four tests, all in `crates/csv-cli/tests/csv_select.rs`:

| Failure | Fixture / input | Test | `CliError` variant |
|---|---|---|---|
| Missing file | `fixtures/m1/does-not-exist.csv` | `missing_input_file_fails_with_nonzero_exit_and_stderr` | `OpenInput` |
| Malformed CSV | `fixtures/m1/malformed.csv` | `malformed_csv_fails_with_nonzero_exit_and_stderr` | `ReadRecord` |
| Column out of range | `--column 99` | `invalid_column_fails_before_writing_any_output` | `ColumnOutOfRange` |
| Missing required flag | no `--column` | `missing_required_flag_fails_with_clap_usage_error` | clap usage error |

```console
$ cargo test -p csv-select-cli
```

`CliError` (`csv-select.rs:29-35`) is a **closed, exhaustive error type** -
`main()` matches only `Ok`/`Err`, never reaches for `unwrap()` or `expect()`
on anything derived from `args.input` or file content, and the compiler
rejects a fifth variant with no `Display` arm.

## Zero-copy row splitting: `dtl-core::csv_zero_copy`

`crates/dtl-core/src/csv_zero_copy.rs` is the lower-level sibling of the same
idea: split a CSV line into fields *without* allocating a `String` per
field. Every field it returns is a `&str` borrowed from the original line
buffer - no copy, no heap allocation, one pass over the bytes. Read the
module's own doc-tested examples (`cargo doc -p dtl-core --open`, or
`docs.rs/dtl-core`) for the exact API; the point worth internalizing here is
the same one from the section above, pushed one layer lower: a borrow into
existing memory is free to create and free to destroy, and a Rust API that
returns `&str` instead of `String` is making a deliberate promise about
that.

## Try it yourself

```sh
cargo test -p csv-select-cli
cargo test -p dtl-core
```

Every claim in this chapter is backed by a test you can run - if a line
number above has drifted from the current source, that's a sign this
chapter needs updating, not that the claim was ever asserted without proof.

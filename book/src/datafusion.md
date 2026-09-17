# The DataFusion pipeline: failure paths and resource control

`crates/pipeline` is where the CSV/Arrow fundamentals turn into an actual
query engine: `bounded.rs` runs a bounded CSV → Parquet pipeline with
cancellation and crash-safe output, `datafusion_query.rs` runs real SQL
through DataFusion under a configurable memory limit. This chapter walks
both, live-test by live-test — code first, then the misconception a
Scala/Spark background invites, then the correction, then the bridge back to
something you already know.

## Cancellation leaves no partial output behind

**Claim**: cancelling before output is published leaves no partial output —
the pipeline either finishes cleanly or leaves nothing.

**Code**: `crates/pipeline/src/bounded.rs`, the `CancellationToken` type and
`run_bounded_pipeline`'s check of it (`bounded.rs:256`), plus the test
`a_pre_cancelled_token_stops_the_pipeline_before_any_output_is_published`
(`bounded.rs:461`).

```console
$ cargo test -p pipeline a_pre_cancelled_token_stops_the_pipeline_before_any_output_is_published -- --nocapture
```

The test creates a `CancellationToken`, calls `.cancel()` on it *before*
calling `run_bounded_pipeline`, then asserts three things: the call returns
`Err`, the error matches `PipelineError::Cancelled`, and — the part worth
pausing on — **both** `!output.exists()` **and**
`!staging_path_for(&output).exists()`. Not just "no final file," but "no
final file *and* no leftover staging file either." The writer stages its
output under a separate temp path and only renames it into place after every
batch has been consumed successfully — cancellation short-circuits before
that rename happens, so neither path is ever left behind.

**Scala/Spark bridge**: `CancellationToken` is the same idea as checking
`TaskContext.get().isInterrupted()` inside a long-running Spark task, or
racing a fiber against a `Deferred`/cancel signal in Cats Effect — a
cooperative flag that owning code must check at safe points, not a
preemptive kill. The more interesting difference is the stage-then-rename
pattern: Spark's own output committers (`FileOutputCommitter` et al.) solve
the same "don't publish a partial file" problem at the *job/task* level via
a two-phase commit (write to a task-attempt directory, promote to the final
path only once the task/job commits). `run_bounded_pipeline` does the same
two-phase idea — stage under a temp path, rename into place only after
success — at the scale of a single process's single output file, with no
separate commit coordinator needed because there's no distributed set of
tasks to reconcile.

## A failed writer publishes nothing, not a truncated file

**Claim**: if the writer fails partway through, the pipeline never leaves a
truncated, corrupt-looking file where a caller might mistake it for
complete output.

**Code**: `bounded.rs:481`, `writer_open_failure_cleans_up_and_publishes_nothing`.
It builds an output path *inside a subdirectory that doesn't exist*, so
`File::create` for the staging file fails before any row is written.

```console
$ cargo test -p pipeline writer_open_failure_cleans_up_and_publishes_nothing -- --nocapture
```

The assertion: `err` matches
`PipelineError::Io(PipelineIoError::OpenOutput { .. })` and
`!output.exists()`. Contrast this with a naive `File::create(output_path)` +
write-as-you-go implementation, which would leave a partial file at exactly
the path callers expect complete output to be — dangerous specifically
because a downstream job polling for the output path existing would pick up
a truncated file and silently process incomplete data. The staged-then-
renamed pattern makes "the file exists at the final path" and "the file is
complete" the same guarantee.

**Scala/Spark bridge**: this is exactly the failure mode Spark's output
committers exist to prevent — a job that crashes mid-write without a commit
protocol can leave a partial part-file in the output directory, which is why
`_SUCCESS` marker files exist: not a log line, but the one signal a
downstream job should poll for instead of trusting "the directory has files
in it." `!output.exists()` here is the same idea taken one step further:
there's no stray part-file left behind *at all*, even for a caller that
doesn't check for a marker.

## Slicing shares the buffer — provably, not just by claim

**Claim**: slicing an Arrow array/batch shares the underlying allocation
instead of copying it — the reason the pipeline can stream large inputs
without multiplying memory use per stage.

**Code**: `crates/pipeline/src/lib.rs:541`,
`slicing_shares_the_underlying_buffer_instead_of_copying`:

```rust
let ids = Int64Array::from(vec![1, 2, 3, 4, 5]);
let sliced = ids.slice(1, 3);

let original_ptr = ids.to_data().buffers()[0].data_ptr();
let sliced_ptr = sliced.to_data().buffers()[0].data_ptr();

assert_eq!(
    original_ptr, sliced_ptr,
    "Array::slice should reuse the original buffer allocation, not copy it"
);
assert_eq!(sliced.values(), &[2, 3, 4]);
```

```console
$ cargo test -p pipeline slicing_shares_the_underlying_buffer_instead_of_copying -- --nocapture
```

The deliberate choice is `data_ptr()` over `as_ptr()`. `ids.slice(1, 3)`
doesn't copy — it creates a *view* into the same allocation starting at a
different logical offset. `as_ptr()` on the two arrays would differ, because
it accounts for each array's own offset; comparing those would make two
views of the same buffer look like different buffers. `data_ptr()` points at
the start of the underlying allocation itself, ignoring the view's offset,
so it correctly proves "these two arrays share one buffer" rather than
"these two arrays start reading from the same place."

**Scala/Spark bridge**: Spark's own in-memory columnar format (Tungsten's
off-heap `UnsafeRow`/`ColumnVector`, itself Arrow-influenced) makes the same
zero-copy claim for `.limit()` or a partition-local `.filter()`, but you
can't *prove* it from Scala the way this test proves it from Rust — the JVM
gives you no `data_ptr()`-equivalent, no legal way to compare two object
references for "do these share the same backing array" short of
`sun.misc.Unsafe` or a heap dump. In Rust it's an ordinary, safe assertion in
a unit test, because `Int64Array::slice` returns an owned Rust value whose
fields (an `Arc`-backed buffer plus an offset/length) are fully inspectable —
the JVM's object model treats reference identity as something you're not
meant to introspect from safe code.

## What a memory limit actually gates

```rust
pub fn context_with_memory_limit(
    max_bytes: usize,
    spill_dir: &Path,
) -> Result<SessionContext, DataFusionError> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(max_bytes, 1.0)
        .with_temp_file_path(spill_dir)
        .build_arc()?;
    Ok(SessionContext::new_with_config_rt(SessionConfig::new(), runtime))
}
```

(`datafusion_query.rs:436`)

**Misconception**: that `.with_temp_file_path(spill_dir)` is what *enables*
spilling — that without it, DataFusion refuses to write temp files and just
fails outright under memory pressure.

**Correction**, sourced against the pinned `datafusion-execution = 55.1.0`
source, not assumed from docs: `DiskManagerBuilder::default()` sets
`mode: DiskManagerMode::OsTmpDirectory` — spilling is on by default,
everywhere, no config needed. The `"temporary files are not enabled"` error
only fires under `DiskManagerMode::Disabled`, which nothing here sets.
`.with_temp_file_path(spill_dir)` exists purely so tests (and this chapter)
can point at a known, inspectable directory instead of the shared OS temp
dir. What `.with_memory_limit(max_bytes, 1.0)` actually gates is how much
memory an operator may reserve *before it must either spill or fail* — the
memory limit doesn't cause spilling on its own, and spilling doesn't require
a memory limit to be configured. They're independent knobs that happen to
interact: a low `max_bytes` is just the fastest way to *force* an operator
that already knows how to spill into actually doing it, instead of waiting
for a multi-gigabyte dataset to trigger the same path.

**Spark bridge**: the same trap as assuming `spark.local.dir` being set is
what makes shuffle spill possible. It isn't — shuffle spills to local disk
by default under memory pressure regardless; `spark.local.dir` just tells
Spark *where*. Same shape here: `with_temp_file_path` tells DataFusion
where, not whether.

## A memory limit doesn't mean everything spills the same way

Given one `context_with_memory_limit(1200, spill_dir)`, you'd expect a
constrained aggregate and a constrained join to fail — or succeed — the same
way. **They don't.** Run both:

```console
$ cargo test -p pipeline memory_limited_aggregate_spills_and_still_completes -- --nocapture
$ cargo test -p pipeline memory_limited_join_fails_with_resources_exhausted -- --nocapture
```

The aggregate test (`datafusion_query.rs:1195`) builds a `GROUP BY note`
over the 1200-byte-capped context and asserts it **completes**, with all 7
groups present, and that spill files were actually written:

```rust
let progress = ctx.runtime_env().spilling_progress();
assert!(
    progress.active_files_count > 0
        || spill_dir.path().read_dir().expect("read spill dir").count() > 0,
    ...
);
```

The join test (`datafusion_query.rs:1226`), under the *identical* 1200-byte
limit, asserts the query **fails**:

```rust
let err = result.expect_err("query must fail under this memory limit");
assert!(
    matches!(err.find_root(), DataFusionError::ResourcesExhausted(_)),
    "hash join build side has no spill fallback in datafusion 55.1.0 ..."
);
```

**Correction**: this is a real, documented asymmetry in DataFusion 55.1.0,
sourced directly from
`datafusion-physical-plan-55.1.0/src/joins/hash_join/exec.rs`,
`collect_left_input`, ~line 2265: the hash-join build side has a comment
`// Decide if we spill or not` immediately followed by
`state.reservation.try_grow(batch_size)?` — **with no spill branch**. A
failed `try_grow` just propagates `ResourcesExhausted` straight through `?`.
Hash-aggregation, by contrast, is mature/GA spillable — since the v27-28
vectorized rework it groups values into a single Arrow-Row-format
allocation with a hash table storing indexes, and that structure has a real
spill-to-disk path. Two distinct demos, not an inconsistency: one operator
in this DataFusion version can spill, the other genuinely can't.

**Scala bridge**: `err.find_root()` (used in the join test) matters for the
same reason unwrapping a chained `Throwable`'s `getCause()` matters on the
JVM — the join's `ORDER BY` adds a sort stage after the hash join, so
DataFusion may wrap the underlying `ResourcesExhausted` in a `Context(..)`
error from that later stage. `find_root()` is the Rust-side equivalent of
walking `getCause()` until you hit the actual failure instead of asserting
against whatever wrapper happened to be outermost.

## No GC doesn't mean queries can't pause

It's tempting, once you've internalized "Rust has no GC," to overcorrect
into "so a DataFusion query never blocks or pauses for memory bookkeeping."
Both tests above disprove this — a memory-limited aggregate *does* pause: it
stops, spills, and resumes.

**Correction**: Spark's `UnifiedMemoryManager` shares one JVM heap between
execution and storage regions elastically, execution given priority, storage
evicted LRU-first when execution needs space; both regions can spill.
DataFusion's `MemoryPool` trait (`GreedyMemoryPool`, `FairSpillPool`,
`TrackConsumersPool`) is a pluggable **per-query/per-operator reservation
strategy**, not a single shared heap region — a different design, not "Rust
doesn't need memory management." The precise, defensible claim about GC is
narrower: DataFusion's memory accounting doesn't stop-the-world across
unrelated queries the way a tracing GC pause can; it's explicit reservation
and explicit spill logic per operator. "No tracing GC" ≠ "queries can't
pause" — they can, deliberately, for exactly the reason you'd expect (not
enough memory for the current step), and you can watch it happen via
`spilling_progress()`.

## Dropping a stream cleans up — no separate `Resource` API needed

The temptation from a Cats Effect background is to look for something like
DataFusion's own `Resource`/`bracket` — a `.use { }` block, an explicit
cancel-and-cleanup API — and be surprised there isn't one.

```rust
let mut stream = ctx.sql(GROUP_BY_NOTE_SQL).await?.execute_stream().await?;
stream.next().await;
drop(stream);
```

(`datafusion_query.rs:1259`, from `dropping_stream_early_cleans_up_spill_files`, test at `:1250`)

```console
$ cargo test -p pipeline dropping_stream_early_cleans_up_spill_files -- --nocapture
```

**Correction**: there's no separate cleanup step to look for, because
there's no separate `Resource` type in the first place. This is Rust's
ordinary `Drop` semantics on the stream value itself — pull one batch, then
`drop(stream)`, and whatever temp-file guards the streaming execution was
holding (`RefCountedTempFile`) run their `Drop` impl the moment the stream
goes out of scope, deterministically. The test polls rather than asserting
instantly, though:

```rust
let mut progress = ctx.runtime_env().spilling_progress();
for _ in 0..1000 {
    if progress.active_files_count == 0 { break; }
    tokio::task::yield_now().await;
    progress = ctx.runtime_env().spilling_progress();
}
assert_eq!(progress.active_files_count, 0, ...);
```

Why: the aggregation's spilling runs on a spawned task that observes the
dropped receiver *asynchronously*, not synchronously on drop. Cleanup is
deterministic in the "it will definitely happen, via ordinary scope exit"
sense, but not synchronous in the "happens on the exact line `drop(stream)`
runs" sense — don't mix up the two.

**Cats Effect bridge**: this is the same guarantee `Resource`/`bracket`
gives at runtime — acquire, use, release, release runs even on early exit or
failure — except the borrow checker enforces the *shape* of it at compile
time instead of a runtime interpreter running a release action on your
behalf. A struct that owns a file handle or spill directory releases it the
moment it goes out of scope. No `.use { }` block, because there's nothing to
opt into — it's the default behavior of every owned value in Rust, not a
library feature layered on top.

## Run the whole chapter live

```console
$ RUST_LOG=pipeline=debug cargo test -p pipeline memory_limited -- --nocapture
$ RUST_LOG=pipeline=debug cargo test -p pipeline dropping_stream_early_cleans_up_spill_files -- --nocapture
$ cargo test -p pipeline
```

`context_with_memory_limit` is `#[tracing::instrument(...)]`
(`datafusion_query.rs:435`) — with `RUST_LOG=pipeline=debug` and
`--nocapture`, the span prints the spill directory actually in use.

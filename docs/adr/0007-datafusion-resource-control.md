# 0007. Bound DataFusion query execution with a memory pool, spill, and cancellation-safe streams

## Status
Accepted

## Context
Once queries run against inputs too large to fit in memory, "it works on my laptop's small fixture"
stops being good enough - the pipeline needs a real, enforced memory bound, a way to spill
gracefully instead of aborting, and a guarantee that cancelling a running query cleans up whatever
spill files it created. DataFusion exposes these knobs through `RuntimeEnvBuilder`, but the specific
combination that gives a correct, testable bound isn't the default configuration.

Two things needed independent verification rather than being taken on faith from documentation:
- Whether temp-file spilling is on by default (an earlier assumption, carried into project notes,
  said it wasn't - checking the pinned DataFusion version's `DiskManagerBuilder` showed it defaults
  to `DiskManagerMode::OsTmpDirectory`, i.e. spilling is already enabled by default).
- Whether a dropped, partially-consumed result stream reliably cleans up its spill files, since
  that cleanup runs on a background task and is inherently a race against the stream's `Drop`.

## Decision
Built query execution around `RuntimeEnvBuilder::new().with_memory_pool(...).with_temp_file_path(...)
.build_arc()`, paired with `SessionContext::new_with_config_rt`, so a query run through this
context gets an explicit, enforced memory ceiling with a known spill directory. Chose a
`FairSpillPool` (wrapped in `TrackConsumersPool` for the "top memory consumers" diagnostic in
error messages) over a `GreedyMemoryPool` because it lets spillable operators wait and spill
predictably under memory pressure instead of failing the first operator that asks for more memory
than is free.

This ADR originally documented that choice but the code didn't match it: it called
`.with_memory_limit(max_bytes, 1.0)`, whose own doc comment on the pinned `datafusion = "55.1.0"`
states it builds a `GreedyMemoryPool` (there is no way to get a `FairSpillPool` through that
method) - a drift bug caught by a CI failure
(`memory_limited_aggregate_spills_and_still_completes` failing with `ResourcesExhausted` on a
`greedy(...)` pool). Fixed by switching to `.with_memory_pool(Arc::new(TrackConsumersPool::new(
FairSpillPool::new(max_bytes), NonZeroUsize::new(5).unwrap())))`, the only way to actually select
`FairSpillPool`.

Also pinned `SessionConfig::new().with_target_partitions(1)`, replacing the previous unset default
(host CPU core count). Two independent, evidence-backed reasons:
- `target_partitions` unset makes a memory-tight test's pass/fail depend on the host's core count -
  confirmed against DataFusion upstream issues describing the same failure class
  (apache/datafusion#25423, apache/datafusion#25047: concurrent final-aggregate-stream consumers
  starved of a few KiB after spilling, timing-dependent on how many partitions were scheduled).
- `FairSpillPool` divides its budget evenly across however many spillable streams are running
  concurrently. At `target_partitions(2)` and a 1200-byte pool, each partition's fair share (600 B)
  was smaller than the ~623 B a single post-spill aggregate batch needs, so the test failed
  deterministically at 2 partitions - not host-CPU-count flaky, but partition-count flaky in the
  same way. This mirrors a real-world case in the Lance project
  (lance-format/lance#9183: `FairSpillPool` sized without accounting for the actual spawned
  partition count, causing load-/CPU-count-dependent `ResourcesExhausted` failures). Pinning to 1
  partition gives the single aggregate stream the whole budget, deterministically.

Verified cancellation safety with a test that drops a stream mid-read and polls (via a yield loop,
not a fixed sleep) until the spill directory is confirmed empty. Run repeatedly to rule out
flakiness from the asynchronous cleanup task.

## Consequences
For callers that build their context through this memory-limited path, failures under memory
pressure are now distinguishable, catchable errors (`DataFusionError::ResourcesExhausted`,
unwrapped through `find_root()` when DataFusion wraps it in context) rather than OOM kills, and
spillable operators degrade gracefully instead of failing outright. The bound is opt-in per
`SessionContext`, not automatic - a caller that builds a plain `SessionContext::new()` instead
still runs unbounded. One operator class remains uncovered even under the bounded path, since it
has no spill path at all - see [0007.1](0007.1-hash-join-build-side-spill-gap.md).

`target_partitions(1)` trades away intra-query parallelism for a deterministic, CI-stable memory
bound on the specific tight-budget lab tests in this crate - it is not a general recommendation for
production DataFusion tuning, where `FairSpillPool`'s budget must instead be sized to the chosen
partition count (`max_bytes` large enough that `max_bytes / target_partitions` covers one
partition's peak post-spill allocation), not the other way around. A future ADR should record that
sizing rule explicitly if a production deployment path is added.

# 0007. Bound DataFusion query execution with a memory pool, spill, and cancellation-safe streams

## Status
Accepted

## Context
Once queries run against inputs too large to fit in memory, "it works on my laptop's small fixture"
stops being good enough — the pipeline needs a real, enforced memory bound, a way to spill
gracefully instead of aborting, and a guarantee that cancelling a running query cleans up whatever
spill files it created. DataFusion exposes these knobs through `RuntimeEnvBuilder`, but the specific
combination that gives a correct, testable bound isn't the default configuration.

Two things needed independent verification rather than being taken on faith from documentation:
- Whether temp-file spilling is on by default (an earlier assumption, carried into project notes,
  said it wasn't — checking the pinned DataFusion version's `DiskManagerBuilder` showed it defaults
  to `DiskManagerMode::OsTmpDirectory`, i.e. spilling is already enabled by default).
- Whether a dropped, partially-consumed result stream reliably cleans up its spill files, since
  that cleanup runs on a background task and is inherently a race against the stream's `Drop`.

## Decision
Built query execution around `RuntimeEnvBuilder::new().with_memory_limit(...).with_temp_file_path(...)
.build_arc()`, paired with `SessionContext::new_with_config_rt`, so a query run through this
context gets an explicit, enforced memory ceiling with a known spill directory. Chose a `FairSpillPool` over a
`GreedyMemoryPool` because it lets spillable operators wait and spill predictably under
memory pressure instead of failing the first operator that asks for more memory than is free.
Verified cancellation safety with a test that drops a stream mid-read and polls (via a yield loop,
not a fixed sleep) until the spill directory is confirmed empty. Run repeatedly to rule out
flakiness from the asynchronous cleanup task.

## Consequences
For callers that build their context through this memory-limited path, failures under memory
pressure are now distinguishable, catchable errors (`DataFusionError::ResourcesExhausted`,
unwrapped through `find_root()` when DataFusion wraps it in context) rather than OOM kills, and
spillable operators degrade gracefully instead of failing outright. The bound is opt-in per
`SessionContext`, not automatic — a caller that builds a plain `SessionContext::new()` instead
still runs unbounded. One operator class remains uncovered even under the bounded path, since it
has no spill path at all — see [0007.1](0007.1-hash-join-build-side-spill-gap.md).

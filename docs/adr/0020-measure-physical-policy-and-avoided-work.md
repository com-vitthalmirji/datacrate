# 0020. Measure physical policy and avoided work

## Status

Proposed - 20 October 2026. The bounded physical-layout comparison is implemented;
production defaults remain unchanged. Evidence retains stage versus process measurement
boundaries and unavailable metrics. Initial host-contended timings are exploratory;
performance acceptance awaits controlled reruns. Other conditional evaluations remain separate.
Extends ADR 0006 and preserves ADR 0007/0007.1 resource-control limitations.

## Context

Logical types and physical layout have separate contracts. Cheap policy checks, deferred
estimation and deterministic samples still have costs and refusal conditions. Counters
for requested lengths can include failed reads; they are not successful network wire bytes
or independent backend service latency.

Datacrate already has runtime projection/filter evidence and a compression comparison.
The earlier comparison ran Snappy then default-level Zstd once on one ordered dataset. That could not
support a broad default-policy migration. Existing Spark/DataFusion timings retain their
published timing boundaries and establish none of the new candidates' benefits.

## Decision

Run small reproducible comparisons in the current Arrow/Parquet/DataFusion stack before
adding policy APIs. Preserve Snappy defaults. Initial candidates are current writer settings,
explicit Zstd level 3 with the same row-group limit, and Snappy with a deliberate alternate
row-group limit. Capture the pinned default, then use the explicit alternate 65,536-row limit;
inspect metadata rather than equating one RecordBatch with one row group.

Use identical logical values for sorted/shuffled variants, short/long repeated and
high-cardinality strings, nulls, narrow selective queries, and full-scan controls. Keep
sequential ingest and analytical reads separate workloads. Start with 200,000 rows and seed
42; increase only after correctness and resource checks. Do not regenerate the 91GB dataset.

Before timing, declare the named workload's primary objective, acceptable regressions and
selection rule. Use one warmup and at least five raw samples per candidate, interleaving
candidate order deterministically. Report median/range and all raw samples. Do not infer p99
from this sample count or call an inconclusive result a win. Builds, fixture generation and
instrumentation overhead belong outside the stated timing boundary unless explicitly measured.

Record revision, lockfile/toolchain, machine/OS/CPU/RAM, seed, schema, ordering/partitioning,
effective codec/level/row-group/statistics settings, file bytes, write/read wall and CPU time,
and measurement method for memory. Profile allocations separately from timing using external
tools; no unsafe global allocator instrumentation enters Datacrate.

### Metric meanings

| Metric | Observation point and limit |
| --- | --- |
| rows/columns selected | Logical result and expected-value oracle |
| decoder rows pruned | Reader runtime metric; not proof of reduced network bytes |
| engine bytes scanned | Engine/reader counter with pinned definition; not automatically wire bytes |
| range calls / requested bytes | ObjectStore wrapper at attempted calls, including errors |
| returned bytes | Successful payload bytes observed at that wrapper |
| retries / wire bytes | Only if backend exposes them; otherwise unavailable, not zero |
| duration | Per-call start-to-completion or whole-run elapsed, explicitly named |
| admitted bytes | ADR 0017 charge rule/lifetime; separate from RSS and query-pool accounting |

Keep instrumentation on the same RuntimeEnv/ObjectStore route as the query being measured.
Use a wrapper only to observe a missing invariant/counter. Preserve residual filters,
projection and DataFusion's own scheduling/coalescing. Add an I/O scheduler only if a measured workload requires one.

Compare pruning enabled/disabled with absent statistics, nulls and range boundaries.
Unknown facts must preserve rows. Prefer exact result checks before runtime-work comparisons.
If fewer requests require over-read, report both costs. Default tuning constants from
another storage engine are not workload evidence.

Do not add a writer-policy type unless the experiment justifies controls used by a real
writer caller. If earned, expose only codec/level and row-group rows initially; validate them
before file creation. Keep logical schema independent. Statistics/page/dictionary knobs are
later additions only if a named query demonstrates need.

Evaluate one-slot queue specialization, metadata caching, adaptive sampling and hasher setup
only against measured allocation/repeated-read/selection/setup cost. A negative or neutral
evaluation is a completed result. A cache needs namespace, content identity, policy/version
and invalidation; sampling needs held-out/full-data verification and measured selection cost.
Do not introduce a tuner, registry, custom codec, compressed executor, GPU/IPC layer or masks.

### Acceptance

- Independent complete result/schema/null checks pass for all small policy/workload cases.
- Every performance claim has a before path, after path, raw runs and a named mechanism.
  Counter changes explain avoided work without conflating filtering with remote traffic.
- Include full-scan and write-cost regressions; retain defaults when benefit is inconclusive.
- Streaming consumers demonstrate incremental consumption and early-drop cleanup; ordered
  sorts may still require blocking work and spill.
- Update book narratives as earlier design, observed limitation, new mechanism and measured
  consequence. Keep public documentation independent of private notes/plans.
- The talk presents implemented/tested Datacrate behavior. External benchmark results are
  never relabeled as Datacrate measurements.

## Consequences

Experiments and counter definitions take engineering time but reduce misleading optimizations.
A fixed candidate set gives less automation and less tuning complexity. Physical policy may
trade write CPU for space/read work; there is no universal best setting.

The pure comparison/selection calculations remain separate from file reads, query execution,
profiling and timing actions. Missing tooling or backend evidence remains an incomplete check,
not a passed gate. No new Rust build/test result is claimed by this documentation task.

## Sources

- [Datacrate comparison](../../crates/pipeline/examples/compression_comparison.rs),
  [query metrics](../../crates/pipeline/src/datafusion_query.rs)

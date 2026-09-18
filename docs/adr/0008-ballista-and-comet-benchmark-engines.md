# 0008. Add Ballista and Comet as benchmark comparison engines, evaluated together

## Status
Accepted

## Context
Demonstrating DataFusion's story credibly needs more than a single-node benchmark: it needs a real
distributed-execution comparison (does a DataFusion-based query engine scale across machines the
way Spark does?) and a real Spark-acceleration comparison (how much of Spark's cost is JVM/shuffle
overhead that a Rust-based execution engine removes even without replacing Spark?). Ballista answers
the first question — it's a distributed query engine built on DataFusion. Comet answers the second —
it's a native acceleration layer that runs *inside* Spark, speeding up Spark's own execution and
shuffle stages without replacing the Spark driver.

These are different engines answering different questions, but were scoped as one unit: if schedule
pressure forced cutting one, both would be cut together rather than keeping one and dropping the
other. A benchmark story covering only half the comparison (distribution *or* acceleration, not
both) is weaker than either scoped narrowly from the start.

A version conflict also needed resolving: Ballista's published crate release pins an older
DataFusion major version than this pipeline's own pinned version, which is not backward compatible.

## Decision
Added both Ballista and Comet as benchmark-only comparison engines, scoped and evaluated together.
Resolved the DataFusion version conflict by pinning Ballista to a specific commit on its unreleased
main branch (rather than downgrading the pipeline's own DataFusion version, or blocking on
Ballista's next release), verified with a single, uniform DataFusion version resolving across the
whole workspace. Ballista's client/scheduler code lives inside the same crate as the rest of the
pipeline's benchmark binaries rather than in a separate isolated crate, accepting that a
git-commit-pinned dependency is a heavier, more unusual dependency than anything else the pipeline
pins from a package registry.

## Consequences
The benchmark suite demonstrates genuine distributed execution (partitioned scan, aggregate, and
shuffle-join queries spread across multiple executors, confirmed via `EXPLAIN`'s distributed-plan
output) and genuine Spark-shuffle acceleration (Comet measured larger relative improvement on
shuffle-heavy join queries than simple scans, consistent with Comet targeting that stage) in the
same pass. The cost is a git-pinned dependency that needs manual attention when Ballista releases
a compatible version, and a benchmark binary set that only builds with an explicit feature flag
(see [0010](0010-optional-cargo-features-for-benchmark-engines.md)) rather than by default.

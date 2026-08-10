# Workshop contract

## Delivery

- Delivery date: 29 October 2026.
- Final preflight and stop-coding date: 28 October 2026.
- Duration: 150 minutes for Mode A; Mode B preserves a tested 120-minute core and uses 30 minutes for guided diagnosis and questions.
- Working title: **Build a typed data pipeline with Arrow and DataFusion in Rust**.

## Audience

The hands-on lane is for experienced backend or data engineers who can already explain a Rust move, an immutable borrow, and propagation with `Result`/`?`. Participants who do not pass the prework readiness check use a clearly labelled follow-along lane and are not counted in the hands-on completion metric.

## Participant outcome

By the end of the tested workshop mode, a hands-on participant can:

1. Inspect an Arrow `RecordBatch` and distinguish shared buffers from allocating operations.
2. Register Parquet data with DataFusion, execute a query, and interpret the relevant physical-plan nodes.
3. Run a bounded object-storage-to-DataFusion-to-Parquet path and observe a controlled failure or recovery.
4. Complete a typed pipeline assembly and observe one compile-time-invalid transition.
5. Validate Spark/DataFusion result parity before interpreting benchmark evidence.
6. State where single-process DataFusion fits, where Ballista begins, and where Spark remains the stronger system.

## Required evidence

- A clean checkout passes format, Clippy, tests, and locked release build.
- Starter and solution checkpoints are tagged and recoverable offline.
- Every exercise completes in 10–20 minutes in participant testing.
- Correctness fixtures and logical-content digests are deterministic.
- Memory, spill, cancellation, object-store, and partial-publication failure paths are tested.
- Benchmark claims retain commands, versions, hardware, input description, repetitions, raw results, and correctness validation.
- Two 150-minute dress rehearsals pass before travel, followed by the assessment and assurance runs in the canonical schedule.

## Non-goals

- Writing `unsafe` Rust, custom allocators, SIMD, lock-free structures, custom futures, or pin projection.
- HTTP services, PyO3, Python wheels, custom DataFusion optimizers/operators, or multi-node Ballista implementation.
- Claiming a production Spark migration, deterministic latency, universal zero-copy execution, or a universal performance multiplier.
- Teaching Rust from A0 during the hands-on lane.

## Scope rule

The delivered mode is the highest mode that has passed end to end by 26 October. Ownership/error fluency, Arrow memory, DataFusion execution and `EXPLAIN`, bounded-resource behaviour, correctness parity, and recovery paths are never traded for optional breadth.

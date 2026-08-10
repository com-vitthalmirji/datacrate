# Workshop evidence ledger

Evidence states are `hypothesis`, `demonstrated`, `contradicted`, or `retired`. A statement becomes `demonstrated` only through executable repository evidence or an authoritative source. A benchmark chart without retained raw data is not evidence.

| Statement | State | Required proof | Current evidence |
|---|---|---|---|
| The pinned Rust toolchain builds the repository reproducibly | hypothesis | Clean locked debug/test/release commands | Toolchain pinned; clean-check evidence not yet recorded |
| Vitthal can explain ownership and the CLI error boundary without notes | hypothesis | Passing M1 implementation plus timed 15-minute explanation | None |
| The workshop demonstrates a genuinely shared Arrow slice | hypothesis | Buffer-identity demonstration and test | None |
| The workshop distinguishes operations that allocate from those that share buffers | hypothesis | Tested examples backed by Arrow documentation | None |
| DataFusion performs expected Parquet projection/predicate pushdown | hypothesis | Version-pinned `EXPLAIN` evidence and golden results | None |
| Constrained execution spills or fails in a controlled, explainable way | hypothesis | Memory-limited integration test and accounted temporary files | None |
| Dropping/cancelling execution releases remaining work and resources | hypothesis | Integration test against the pinned DataFusion version | None |
| Object-store publication never presents partial output as complete | hypothesis | Staging/manifest protocol plus injected-failure tests | None |
| Typestate prevents incomplete pipeline assembly | hypothesis | Compile-fail tests | None |
| Spark and DataFusion return logically equivalent results for the disclosed workload | hypothesis | Normalised schema, rows/nulls, stable ordering, and logical digest | None |
| DataFusion is faster for the disclosed workload | hypothesis | Correctness-first reproducible benchmark with raw runs | None |
| Another engineer can finish the tested exercises within the time box | hypothesis | Participant-driven beta and rehearsal logs | None |

## Evidence-entry format

For each new measurement or demonstration, record:

- Date and commit.
- Claim being tested.
- Toolchain and dependency versions.
- Hardware/runtime environment.
- Fixture, row count, byte size, file layout, and seed where applicable.
- Exact command.
- Correctness oracle or digest.
- Raw result location.
- What the result proves.
- What it does not prove.

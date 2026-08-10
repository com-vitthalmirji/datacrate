# Run of show — version 0

Every segment remains unproven until participant testing and the evidence ledger demonstrate it.

| Time | Segment | Participant action | Current state |
|---:|---|---|---|
| 0–15 min | Scope, evidence, and non-goals | Run preflight and inspect the deterministic fixture | unproven |
| 15–40 min | Arrow memory without mythology | Build and inspect a `RecordBatch`; identify sharing/allocation | unproven |
| 40–75 min | DataFusion execution | Register Parquet, execute SQL, and inspect the physical plan | unproven |
| 75–100 min | Object storage and bounded resources | Run the pipeline with a memory/failure control | unproven |
| 100–125 min | Typed boundaries | Complete typestate assembly and trigger a compile-fail path | unproven |
| 125–140 min | Spark comparison | Validate logical parity before reading measurements | unproven |
| 140–150 min | Boundaries and questions | Explain DataFusion/Ballista/Spark placement | unproven |

## Mode-reduction candidates

Cut in this order when gates or timing require it: custom UDF, unnecessary worker experiment, extra Parquet comparisons, optional upstream contribution, live-cloud validation, and benchmark breadth. The reduced mode must be rehearsed end to end; it cannot be assembled for the first time on delivery day.

# Architecture decision records

Each file is a decision, using [`TEMPLATE.md`](TEMPLATE.md): Status, Context,
Decision, Consequences.

An Accepted ADR is not edited once its decision changes. Write a new ADR that
supersedes it, and mark the old one's Status as `Superseded by ADR-XXXX`.

## Index

- [0001. Proc-macro build-script diagnostics](0001-proc-macro-build-script-diagnostics.md)
- [0002. Cargo workspace restructure](0002-cargo-workspace-restructure.md)
- [0003. DSA practice crate isolation](0003-dsa-practice-crate-isolation.md)
- [0004. Object-store I/O scope and async shim](0004-object-store-io-scope-and-async-shim.md)
- [0005. Bounded pipeline backpressure](0005-bounded-pipeline-backpressure.md)
  - [0005.1. Stream object-store I/O in chunks](0005.1-stream-object-store-io-in-chunks.md)
- [0006. DataFusion query parity and pushdown proof](0006-datafusion-query-parity-and-pushdown-proof.md)
- [0007. DataFusion resource control](0007-datafusion-resource-control.md)
  - [0007.1. Hash-join build-side spill gap](0007.1-hash-join-build-side-spill-gap.md)
- [0008. Ballista and Comet benchmark engines](0008-ballista-and-comet-benchmark-engines.md)
  - [0008.1. DataFusion/Polars comparison is scale-dependent](0008.1-datafusion-polars-comparison-is-scale-dependent.md)

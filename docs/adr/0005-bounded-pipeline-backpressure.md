# 0005. Bound pipeline memory with `thread::scope` and a bounded channel, not async

## Status
Accepted

## Context
Converting CSV input into Parquet output needs to run under a bounded memory budget — the pipeline
must not hold the entire input in memory before writing anything out, and the workshop's own claims
about that bound need to be true, not just asserted. Rust offers two natural shapes for a
producer/consumer pipeline: an async task pair connected by an async channel, or a pair of OS
threads connected by a synchronous bounded channel.

Async wasn't needed anywhere else in the pipeline's core (see
[0004](0004-object-store-io-scope-and-async-shim.md) for the one deliberate exception), and adding
a runtime just for this would spread async through code that has no other reason to carry it.

## Decision
Built the pipeline as a synchronous producer/consumer pair using `std::thread::scope` and
`std::sync::mpsc::sync_channel`. The producer streams CSV rows into fixed-size batches and pushes
them onto the bounded channel; a full channel blocks the producer, which is exactly the
backpressure the memory bound depends on. The consumer writes each batch, and only renames the
output into place from a `.tmp` staging path after the writer closes successfully — on error, or on
a cancellation signal, the staging file is cleaned up instead of left half-written.

## Consequences
Memory use is governed by two small, auditable numbers — batch size and channel capacity — instead
of an implicit "how much does the async runtime buffer" question. The staged-write-then-rename
shape means a reader never observes a partially-written output file. The tradeoff is that this
pipeline can't overlap I/O the way an async version might. That cost was accepted because
correctness and a provable memory bound matter more than throughput, and nothing measured so
far has shown the synchronous shape to be a bottleneck.

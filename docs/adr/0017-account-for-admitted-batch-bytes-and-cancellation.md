# 0017. Account for admitted batch bytes and cancellation

## Status

Proposed - 20 October 2026. The optional byte-budget transport is implemented.
Controlled multipart straggler progress is covered by independently released parts.
Full integration and physical memory measurements remain separate evidence gates.
Extends ADR 0005 and ADR 0015; the existing multipart fix is a prerequisite, not new work here.

## Context

A count-limited queue is useful but does not bound variable-width Arrow payload bytes.
Construction, queued input, active transformed output, Parquet buffering, and upload work
have different owners. A buffered-byte reservation can release accounting on Drop,
but tracking alone is not admission. Queue specialization must preserve Datacrate's
supported zero-capacity rendezvous semantics.

Datacrate already limits multipart submissions to two aligned 8 MiB parts. That establishes
neither a transport byte budget nor progress when one part remains a straggler.

## Decision

Add an optional `PipelineConfig.max_admitted_bytes: Option<NonZeroUsize>`, default `None`
to preserve existing configuration behavior. Preserve `channel_capacity = 0` and current
batch defaults. Report current/peak admitted bytes and the charge rule independently of RSS.

Name the accounting domain: a producer's finished batch, once admitted, remains charged
through queue residence and the consumer's write/digest step. Use
`RecordBatch::get_array_memory_size()` as the declared charge estimate, checked for overflow.
Shared/sliced buffers may be counted more than once; this is conservative accounting under
that API, not unique physical allocation attribution. One producer batch under construction,
CSV parser/record storage, callback internals, Parquet/backend buffers, and allocator overhead
remain outside this ceiling and must be reported as separate allowances.

An owned, non-cloneable batch lease carries the reservation. Success, receive failure,
callback/validation/write failure, receiver closure, and cancellation release it exactly once.
If a single charge exceeds the total budget, fail with a typed oversized-batch error before
waiting. Admission occurs after construction; it cannot stop allocation of an arbitrarily
large CSV record. Do not advertise a hard whole-ingest bound without a separate record limit.

After a transform, conservatively retain at least the original charge and account for a larger
returned batch before writer/digest use. A consumer must never wait for extra bytes held by
queued batches it alone can drain: growth uses a nonblocking reservation adjustment and returns
a typed budget error when insufficient. Memory temporarily created inside a callback remains
outside the admission guarantee.

### Coordination boundary

Hide count/rendezvous/byte admission in one private transport module, keeping the public
pipeline API small. Compare keeping `sync_channel` plus a separate byte wait with one
mutex/condition-variable transport. Select the latter for the budgeted path: blocking
`SyncSender::send` has no independent cancellation wakeup, and two admission mechanisms
make shutdown and ownership harder to reason about. Keep count-only behavior on the existing
path until the budgeted implementation has parity evidence; do not introduce a second runtime.

The private state owns queued messages, charged bytes, receiver-waiting/rendezvous
acknowledgment, and terminal state. All wait predicates are checked in loops under the same
mutex that protects their transitions. Receiver removal frees a count slot; lease drop frees
bytes; closure/cancellation notifies all waiters. Rendezvous send completes only after receive
acknowledgment, never merely because an inline slot was filled.

Preserve shared-token cancellation across multiple runs. A token may register weak references
to per-run transport controls; cancellation closes/notifies those controls without retaining
completed runs. Register/check cancellation without a lost-wakeup window. Never hold the token
registry lock while acquiring a transport lock. This is a specific wakeup mechanism, not a
general callback/event registry. Arbitrary user callbacks and blocking file/backend I/O remain
cooperative boundaries and are not forcibly interrupted.

### Acceptance

- Controlled slow-consumer tests establish count, admitted-byte high water, reservation release,
  and both count/byte wait wakeups. Cover rendezvous and positive capacities.
- A batch equal to the budget succeeds; an oversized batch fails without waiting. Cover zero
  rows, wide strings, shared buffers, charge overflow, and transformed-batch growth.
- Cancellation before registration, during registration, during count/byte waiting, receiver
  failure, and multiple runs sharing a token terminate without leaked reservations.
- Keep one upload part blocked while completing another; prove a freed slot admits the next
  part before the straggler completes. Include finish's short part and error paths.
- Timeouts detect hangs only. Permits, barriers, and recorded active counts prove the ordering.
  Do not claim physical remote abort from logical cancellation or dropped futures.

## Consequences

The optional budget gives operators a named and testable retention policy at the cost of
coordination complexity and conservative rejection. The budgeted path may reject a transform
that expands data even when eventual memory could be recovered by draining other work.
This is preferable to a consumer waiting on itself.

Metrics are observations; mutex-protected transitions enforce admission. Owned local mutation
is confined to the transport action boundary. No global memory manager, automatic queue
specialization, custom async executor, or exact-RSS promise is introduced.

## Sources

- [Datacrate bounded pipeline](../../crates/pipeline/src/bounded.rs),
  [multipart implementation](../../crates/pipeline/src/object_store_io.rs)
- [ADR 0005](0005-bounded-pipeline-backpressure.md),
  [runtime contract](0016-check-runtime-ingest-batches-before-writing-and-digesting.md)

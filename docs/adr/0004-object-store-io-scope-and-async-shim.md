# 0004. Object-store I/O: pull it forward narrowly, use `object_store`, confine async to one module

## Status
Accepted

## Context
Later pipeline work needs to read and write against S3-compatible object storage. That capability
was originally scoped for a later milestone, but a smoke test needed it earlier to validate the
pipeline's memory envelope against something closer to real infrastructure than local disk. This was
a deliberate, narrow exception, not a general schedule slip.

Two decisions had to be made explicitly rather than picked by default:

1. **Which object-store client.** `aws-sdk-s3` is the obvious default, but it locks the
   implementation to AWS specifically. `object_store` (the crate DataFusion's own ecosystem uses)
   is backend-agnostic — S3, GCS, Azure, or a local MinIO instance all speak the same trait.
2. **Async.** The rest of the pipeline is deliberately synchronous — no Tokio runtime, no `async
   fn` anywhere — because nothing before this needed it. `object_store`'s `ObjectStore` trait is
   `async fn`-only, so satisfying it without runtime async spreading through the whole pipeline
   needed a decision, not a default.

## Decision
Adopted `object_store` over `aws-sdk-s3`, since backend-agnosticism matters more here than deep
AWS-specific features, and it keeps the pipeline aligned with DataFusion's own dependency choices
rather than introducing a second, divergent object-storage abstraction.

Confined async to one module (`object_store_io.rs`), using a
`tokio::runtime::Builder::new_current_thread()` + `block_on` shim around the object-store calls.
Everything else — the producer/consumer thread pair, the channel-based backpressure — stays
synchronous.

## Consequences
The rest of the pipeline's code never reasons about async cancellation, executor scheduling,
or `Send`/`Sync` bounds on futures — those concerns stay within two functions. Adding a second
storage backend later means implementing one more `object_store` provider, not rewriting the
pipeline's core. The cost is a small, deliberate exception to "no async until it's needed": one
module carries a Tokio runtime that the rest of the crate doesn't. This is worth calling out to
anyone reading the code fresh so it doesn't look like async crept in by accident.

# 0003. Isolate the Rust practice-and-playground crate by dependency, not by directory

## Status
Accepted

## Context
Alongside the data-pipeline work, this repository hosts a general Rust practice and playground
area, moved from a separate Python habit so the practice itself doubles as Rust reps. That
practice has nothing to do with the data-pipeline crates' purpose and risks becoming a distraction
if it leaks into their review surface or dependency graph.

The practice code still needs a home somewhere in the workspace, since maintaining a second,
unrelated repository just for it is more overhead than the practice is worth.

## Decision
Give the practice its own workspace crate, `crates/rusty-ready/`, with one hard rule: it is never
imported by any of the pipeline/CLI/typestate crates, and it never imports them. The isolation is
enforced at the dependency-graph level, not by hiding the crate in a separate directory tree or
repository - a crate that plainly exists in the workspace but is structurally unreachable from the
pipeline code is easier to audit than one that's merely out of sight.

## Consequences
`cargo build`/`cargo test` at the workspace root compiles the practice crate too, so its mistakes
don't block pipeline work and vice versa. Anyone reviewing pipeline-crate changes never needs to
read `rusty-ready`'s diffs, and vice versa - the dependency graph enforces that guarantee
mechanically rather than relying on discipline. The tradeoff is one extra crate in the workspace
listing with no bearing on the pipeline itself. Kept anyway, because "never imported" is a stronger
and cheaper guarantee than "conventionally not imported."

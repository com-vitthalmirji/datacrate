# 0011. Use the same release profile locally and in CI

## Status
Accepted

## Context
The local verification task's release-build step was building with the default `[profile.release]`
- fat LTO, `codegen-units = 1` - which is the right, expensive profile for a shipped binary but
not for a routine local correctness check. CI's workflow was already building with a lighter
`[profile.ci-release]` (thin LTO, 16 codegen units) for that reason. The two had quietly diverged:
local verification paid full release-build cost on every run for no benefit over the lighter
profile, while CI never did.

## Decision
Changed the local verification task's build step to `cargo build --profile ci-release --locked`,
matching what CI already does. `[profile.release]`'s fat-LTO settings remain reserved for the actual
binary-artifact release workflow, which is the only place that needs the extra optimization cost.

## Consequences
Local verification now costs roughly what CI's build step costs, instead of silently paying for a
fully optimized release build every time. Nothing about the shipped binaries changes - they still
build under the original, fully optimized `[profile.release]` through the dedicated release
workflow.

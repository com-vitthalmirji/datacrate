# 0006. Prove DataFusion parity and optimizer behavior with metrics, not plan text

## Status
Accepted

## Context
The pipeline's query layer needs to support both SQL and DataFrame-style access to the same
underlying data. It must also demonstrate that hand-written user-defined functions get the same
optimizer treatment as DataFusion's built-in functions — otherwise "just write it in Rust" is a
weaker claim than it sounds. Two specific behaviors needed proof rather than assumption:

1. **Predicate pushdown.** DataFusion's `pushdown_filters` config defaults to `false`. A plan that
   *looks* like it pushes a filter into the Parquet scan doesn't prove it actually pruned any rows —
   plan text can show an intended optimization without confirming it fired.
2. **UDF optimizer parity.** A user-defined function registered without the right volatility hint
   is invisible to constant-folding and simplification passes that built-in functions get.

## Decision
Standardized on `EXPLAIN ANALYZE`'s runtime metrics (e.g. `pushdown_rows_pruned`, `bytes_scanned`)
as the evidence for pushdown claims, not the presence of a filter node in the plan text. For UDFs,
registered them with `Volatility::Immutable` where the function is a pure computation, and verified
via `EXPLAIN` that the optimizer folds/simplifies calls to it exactly as it would a built-in.

## Consequences
Every pushdown or optimizer-parity claim in this codebase is backed by a metric value that changes
when the behavior regresses, not by a plan shape that can look right while proving nothing. The
cost is that these tests are slightly more verbose — they parse or assert on metrics output instead
of a simpler plan-string match — but that verbosity is the point: a plan-text assertion would pass
even if pushdown were silently disabled.

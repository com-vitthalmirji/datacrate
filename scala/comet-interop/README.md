# comet-interop

A scaffold for generating Spark Comet operator-replacement rules from
`derives CometOperator` declarations, the same build-time-scan-plus-compiler-
checked-typeclass mechanism a `derives Resource` macro would use: a
build-time text scan finds candidates, and the compiler is the actual judge
of whether each one resolves.

## Toolchain

This is a separate sbt/Scala project, not part of the `datacrate` Cargo
workspace. It has its own toolchain:

- sbt 1.10.7 (`project/build.properties`)
- Scala 3.3.8 for `core` and `comet-ops`
- `project/CometOpsScannerPlugin.scala` is sbt meta-build code, compiled with
  sbt's own Scala 2.12, not Scala 3 - that is why it uses brace syntax while
  everything else in this project uses braceless syntax.

Run `sbt compile` and `sbt test` from inside `comet-interop/` to build it.

## Why this doesn't extend real Spark/Comet classes

The Spark and Comet jars checked into this repo
(`benchmark/spark-comet/comet-spark-spark4.1_2.13-0.16.0.jar`) are published
for Scala 2.13 only. A Scala 3 module cannot extend their traits
(`SparkPlan`, `ColumnarRule`, `SparkSessionExtensions`) directly - Scala's
binary compatibility is per major version. This scaffold uses its own
stand-in types, `PlanNode` and `ColumnarRule` (`core/src/main/scala/
cometinterop/`), so the derive/typeclass mechanism can be designed and
tested in isolation. Wiring the generated rules into a real
`SparkSessionExtensions` needs a thin Scala 2.13 bridge module that is not
built here.

## How it fits together

- `CometOperatorSpec` (`core/.../CometOperator.scala`) is the trait every
  operator config implements: the Spark node it targets, its Comet
  replacement, and the input fields it requires.
- `derives CometOperator` resolves to a bounded `given` instance for any
  `T <: CometOperatorSpec` - there is no Mirror-based shape derivation here,
  because the typeclass only needs the bound, not the case class's field
  shape.
- `comet-ops/.../RowCountFilterOp.scala` is an example spec, written as an
  `object` so generated code can reference it by name.
- `CometOpsScannerPlugin` text-scans `comet-ops`'s sources for the literal
  `derives CometOperator`, and writes a generated file listing every match.
  A false positive here costs one dead line in the generated list, never a
  wrong operator swap - the compiler still has to resolve `CometOperator`
  for whatever it names.

## Operator coverage

Each operator below is a one-field-set `CometOperatorSpec` instance, proving
the same derive/scanner mechanism across the scan, transform, and join
categories - no new Scala mechanism is introduced per operator.

| Operator | Category | Spark node | Comet replacement |
|---|---|---|---|
| `RowCountFilterOp` | filter | `FilterExec` | `CometFilterExec` |
| `ParquetScanOp` | scan/ingestion | `FileSourceScanExec` | `CometScanExec` |
| `ProjectOp` | project | `ProjectExec` | `CometProjectExec` |
| `HashAggregateOp` | aggregate | `HashAggregateExec` | `CometHashAggregateExec` |
| `BroadcastHashJoinOp` | join/shuffle | `BroadcastHashJoinExec` | `CometBroadcastHashJoinExec` |

There is no sink-category operator: Comet accelerates Spark's read and
compute path, not the write side, so there is nothing for a "sink operator"
to replace.

## Testing

`core/src/test/scala/cometinterop/CometOperatorSuite.scala` covers the
derive/typeclass logic directly, using a local `FilterSpec`: a matching node
gets replaced, a non-matching node name or a missing required field falls
back to the original node unchanged.

`comet-ops/src/test/scala/comet_ops/CometOperatorBreadthSuite.scala` runs the
same match/fallback shape against the four real operators above
(`ParquetScanOp`, `ProjectOp`, `HashAggregateOp`, `BroadcastHashJoinOp`),
proving the mechanism holds across the scan/transform/join categories, not
just for one hand-written spec. It lives in `comet-ops`, not `core`, because
`core` cannot see `comet_ops`'s types - `comet-ops` depends on `core`, not
the reverse.

The scanner itself (`OperatorScanner.scan`) is meta-build code under
`project/`, outside the test framework reachable by `sbt test`. It is
exercised end-to-end by `sbt compile`, which runs it as a
`sourceGenerators` task and writes `GeneratedCometOperators.scala` -
if the scan misses or misnames a candidate, the generated file fails to
compile against the real `comet-ops` sources.

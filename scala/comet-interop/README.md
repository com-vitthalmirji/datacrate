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

Run `sbt compile` and `sbt test` from inside `scala/comet-interop/` to build it.

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

## `reflect-check`: compile-time validation of a reflection target (POC)

A separate idea, proven as a standalone proof of concept in `reflect-check/`,
not wired into `core`/`comet-ops` above. `docs/internals/datafusion-comet-story.md`
(Jordan Epstein's own account of Comet's Iceberg integration) describes both
Comet's existing read path and its planned write path reaching into *private*
fields on Spark/Iceberg Java classes via Scala reflection, because those
fields have no public API - a field rename on a dependency upgrade breaks
that `getDeclaredField` call silently at runtime, not at build time.

`ClasspathField.requireFieldOfType` (`reflect-check/src/main/scala/
cometinterop/reflectcheck/ClasspathField.scala`) is a Scala 3 macro, using
the same `scala.quoted` mechanism `derives CometOperator` above already
relies on, that runs the equivalent `Class.forName` + `getDeclaredField`
lookup while the call site itself is being compiled. `WriteConfigFields.scala`
calls it against `IcebergWriteConfigStub` - a stand-in for the kind of
write-config object the transcript describes (target Parquet file size, rows
per row group, partition path, whether to use Comet for the write at all; not
a confirmed real Iceberg class or field name) - for all four fields.

**Demonstrated, not just designed** - both directions were run and the output
captured (**measured**, this repo, 2026-10-22):
- Positive: `sbt reflectCheck/test` compiles `WriteConfigFields.scala` against
  the real, compiled `IcebergWriteConfigStub` class and passes 4 tests.
- Negative: renaming `targetFileSizeBytes` to `targetFileSizeByte` on the stub
  and running `sbt reflectCheck/clean reflectCheck/compile` fails the build
  with the real field drift it's meant to catch:
  ```
  [error] -- Error: .../WriteConfigFields.scala:18:35
  [error] 18 |  ClasspathField.requireFieldOfType(className, "targetFileSizeBytes", "long")
  [error]    |  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  [error]    |compile-time reflection check failed: cometinterop.reflectcheck.stub.IcebergWriteConfigStub
  [error]    |has no field named 'targetFileSizeBytes' (available: targetFileSizeByte,
  [error]    |rowsPerRowGroup, partitionPath, useCometForWrite)
  [error] one error found
  ```
  This is the exact failure mode a renamed Iceberg write-config field would
  hit in production today, surfaced at `sbt compile` instead.

**What this is not**: there is no confirmed, Comet-maintainer-reviewed
contribution here - the idea targets the write path's reflection surface the
transcript names as still-future work, using a stand-in class, not the real
Iceberg jar's actual field names (unverified against it). It is a POC proving
the mechanism works, not a patch proposed to `apache/datafusion-comet`.

## `reflect-check-iceberg`: the same macro against the real Iceberg class

`reflect-check-iceberg/` points `ClasspathField` at the real target instead of
a stand-in: `org.apache.iceberg.spark.source.SparkWrite`, the class Comet's
own `IcebergReflection.scala` (`org.apache.comet.iceberg.IcebergReflection`,
in `apache/datafusion-comet`) already reflects into via `getSparkWriteField`
for its planned native write path. That file's own doc comment names five
fields it reads unconditionally or with a version fallback - `queryId`,
`targetFileSize`, `outputSpecId`, `writeSchema`, `useFanoutWriter` (with an
older-version fallback to `partitionedFanoutEnabled`) - and states they are
"present on `SparkWrite` across the Iceberg 1.5.2 / 1.8.1 / 1.10.0 versions
Comet supports."

This is kept as its own sbt module, not folded into `reflect-check`, because
it needs a real dependency on `org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.10.0`
(plus `org.apache.spark:spark-sql_2.13:3.5.9`, which `SparkWrite` needs on the
classpath because it implements a Spark interface `Class.forName` must also
resolve) - special-purpose code that targets one real class is kept separate
from `ClasspathField`, which stays general-purpose and dependency-free.
1.10.0 was chosen because it is one of the three versions Comet's own comment
names as confirmed, not because it's the newest available (1.12.0 is).

**Demonstrated, not just designed** - both directions run against the real
Iceberg 1.10.0 jar and the output captured (**measured**, this repo,
2026-10-22):
- Field names and types were read first via `javap -p` on the real
  `SparkWrite.class` inside the fetched jar, not assumed - this is what
  determined `queryId: String`, `targetFileSize: long`, `outputSpecId: int`,
  `writeSchema: org.apache.iceberg.Schema`, `useFanoutWriter: boolean`.
  `partitionedFanoutEnabled` is not checked: it does not exist as a field on
  1.10.0's `SparkWrite` at all, so there is nothing on this classpath to check
  it against - this is named here, not silently skipped.
- Positive: `sbt reflectCheckIceberg/test` compiles `SparkWriteFields.scala`
  against the real jar and passes.
- Negative: renaming `queryId` to `queryIdRenamed` in `SparkWriteFields.scala`
  and running `sbt reflectCheckIceberg/clean reflectCheckIceberg/compile`
  fails with:
  ```
  [error] -- Error: .../SparkWriteFields.scala:27:35
  [error] 27 |  ClasspathField.requireFieldOfType(className, "queryIdRenamed", "java.lang.String")
  [error]    |  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  [error]    |compile-time reflection check failed: org.apache.iceberg.spark.source.SparkWrite
  [error]    |has no field named 'queryIdRenamed' (available: LOG, sparkContext, writeConf,
  [error]    |table, queryId, format, applicationId, wapEnabled, wapId, outputSpecId, branch,
  [error]    |targetFileSize, writeSchema, dsSchema, extraSnapshotMetadata, useFanoutWriter,
  [error]    |writeRequirements, writeProperties, cleanupOnAbort)
  [error] one error found
  ```

**What this still is not**: this is a compile-time guard against a real
Iceberg field renaming out from under Comet's reflection code, running in
this standalone repo - it has not been proposed to, reviewed by, or merged
into `apache/datafusion-comet`, and it only pins Iceberg 1.10.0 (not 1.5.2 or
1.8.1, which are not available to check against here). It would need to be
wired into Comet's own build (e.g. a `compile-fail`-style module there) to
protect that project directly, which has not been done.

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

package cometinterop.reflectcheck.iceberg

import cometinterop.reflectcheck.ClasspathField

/** Points `ClasspathField` at the real reflection target named in Comet's
  * own `IcebergReflection.scala`: `org.apache.iceberg.spark.source.SparkWrite`,
  * the class whose private fields back Comet's planned native Iceberg write
  * path (`getSparkWriteField` and its callers - `getOperationIdFromSparkWrite`,
  * `getTargetFileSizeFromSparkWrite`, `getUseFanoutWriterFromSparkWrite`,
  * `getOutputSpecIdFromSparkWrite`, `getWriteSchemaFromSparkWrite`). Field
  * names and types were confirmed from the real `iceberg-spark-runtime-3.5_2.13:1.10.0`
  * jar via `javap -p`, not guessed: `queryId: String`, `targetFileSize: long`,
  * `outputSpecId: int`, `writeSchema: org.apache.iceberg.Schema`,
  * `useFanoutWriter: boolean`.
  *
  * 1.10.0 is one of the three versions (1.5.2 / 1.8.1 / 1.10.0) Comet's own
  * doc comment names as confirmed-compatible - this check pins to that one
  * real version. It does not prove the fields are unchanged on 1.5.2/1.8.1,
  * and `partitionedFanoutEnabled` (the older name `getUseFanoutWriterFromSparkWrite`
  * falls back to via `.orElse`) is not checked here: it isn't a field on
  * 1.10.0's `SparkWrite` at all, so there is nothing on this classpath to
  * check it against.
  */
object SparkWriteFields:
  private inline val className = "org.apache.iceberg.spark.source.SparkWrite"

  ClasspathField.requireFieldOfType(className, "queryId", "java.lang.String")
  ClasspathField.requireFieldOfType(className, "targetFileSize", "long")
  ClasspathField.requireFieldOfType(className, "outputSpecId", "int")
  ClasspathField.requireFieldOfType(className, "writeSchema", "org.apache.iceberg.Schema")
  ClasspathField.requireFieldOfType(className, "useFanoutWriter", "boolean")

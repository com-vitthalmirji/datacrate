package cometinterop.reflectcheck.stub

/** Stands in for the Java/Iceberg write-config object the Comet transcript
  * describes as the planned reflection target for a native write path
  * (`docs/internals/datafusion-comet-story.md`): target Parquet file size,
  * rows per row group, the partition path, and whether Comet should even
  * be used for a given write. These are not the real Iceberg class's field
  * names - no such class/field pairing has been confirmed against the real
  * Iceberg jar in this repo - this is a stand-in shaped like the fields the
  * transcript names, used only to prove `ClasspathField` catches a renamed
  * field at compile time instead of at the moment reflection runs.
  *
  * Declared `private` like the real target fields would be - reflection
  * is the only way in from outside the class, same as the production case
  * this models. `describe` reads all four internally, the same way the
  * real class's own logic reads its private fields - a Scala 3 private
  * field with no internal reader gets dead-code-eliminated entirely (no
  * backing field at all), which would make this stub useless as a model
  * of a field that genuinely exists on the compiled class.
  */
final class IcebergWriteConfigStub:
  private val targetFileSizeBytes: Long = 134217728L
  private val rowsPerRowGroup: Int = 1_000_000
  private val partitionPath: String = "/warehouse/orders/dt=2026-10-22"
  private val useCometForWrite: Boolean = true

  def describe(): String =
    s"targetFileSizeBytes=$targetFileSizeBytes rowsPerRowGroup=$rowsPerRowGroup " +
      s"partitionPath=$partitionPath useCometForWrite=$useCometForWrite"

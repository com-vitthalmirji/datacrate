package arrowinterop

import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowFileWriter}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}

import scala.sys.process.*

/** Phase E1 (`docs/internals/experiment.md` section 13): measures the full
  * Scala-object -> Arrow -> Rust-kernel -> Arrow -> Scala-object round
  * trip, reporting five phases separately -- object-to-Arrow packing,
  * boundary export, Rust kernel execution (a subprocess, not JNI -- see
  * `arrow-interop-kernel`'s own doc comment for why), boundary import +
  * object reconstruction, and total duration -- per the plan's explicit
  * requirement that these never be collapsed into one number.
  *
  * Domain type: [[Order]], a four-field case class. Only `value` crosses
  * into Arrow/Rust; `id`/`placedAt`/`note` are carried through this
  * process's own memory and re-attached during reconstruction. The plan
  * asks for "the required Scala objects" to be reconstructed, not only
  * the one field that happened to cross the boundary.
  *
  * Every Arrow Java method signature used here (`ArrowFileReader`,
  * `ArrowFileWriter`, `VectorSchemaRoot`, `IntVector`, `Field`/`Schema`/
  * `ArrowType`) was verified directly against the real arrow-vector
  * 19.0.0 jar's class files (`javap -p`), not assumed from memory or
  * training data.
  *
  * JVM configuration: whatever `-jvm-opts` the CI job applies (none, by
  * default); no steady-state-only JMH-style warmup. This measures a real
  * wall-clock run including JIT and GC, intentionally -- the plan asks
  * for "the treatment of garbage collection" to be recorded, not excluded
  * by a warmup that would hide it.
  */
object ObjectMaterializationDriver:

  final case class Order(id: Long, value: Int, placedAt: Long, note: String)

  private def generateOrders(rows: Int): IndexedSeq[Order] =
    (0 until rows).map { i =>
      Order(id = i.toLong, value = i % 1_000_000, placedAt = 1_700_000_000L + i.toLong, note = s"order-$i")
    }

  private def valueField: Field =
    Field.notNullable("value", new ArrowType.Int(32, true))

  /** Phase 1: Scala object-to-Arrow packing. Only `value` crosses into
    * the vector; `id`/`placedAt`/`note` stay in this process's own
    * `orders` sequence and are re-attached in [[readIpcAndReconstruct]] --
    * packing one field out of a four-field object is itself the cost this
    * phase measures, matching the Rust side's own row-shaped
    * `SourceRow` (`materialization-vs-handoff.rs`): the iteration still
    * walks the real, full-width `Order` sequence, it simply only writes
    * one field per element into Arrow.
    */
  private def packValueColumn(orders: IndexedSeq[Order], allocator: RootAllocator): VectorSchemaRoot =
    val schema = new Schema(java.util.List.of(valueField))
    val root = VectorSchemaRoot.create(schema, allocator)
    val vector = root.getVector("value").asInstanceOf[IntVector]
    vector.allocateNew(orders.length)
    orders.zipWithIndex.foreach { case (order, i) => vector.set(i, order.value) }
    vector.setValueCount(orders.length)
    root.setRowCount(orders.length)
    root

  /** Phase 2: boundary export -- one IPC file written to `path`. */
  private def writeIpc(root: VectorSchemaRoot, path: Path): Unit =
    val channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
    try
      val writer = new ArrowFileWriter(root, new DictionaryProvider.MapDictionaryProvider(), channel)
      writer.start()
      writer.writeBatch()
      writer.end()
      writer.close()
    finally channel.close()

  /** Phase 3: Rust kernel execution via subprocess -- not JNI, per
    * Decision 1's prohibition on a custom bridge.
    */
  private def runRustKernel(kernelBinary: Path, inputPath: Path, outputPath: Path, k: Int): Unit =
    val command = Seq(
      kernelBinary.toString,
      "--input", inputPath.toString,
      "--output", outputPath.toString,
      "--k", k.toString
    )
    val exitCode = command.!
    if exitCode != 0 then
      throw new RuntimeException(s"arrow-interop-kernel exited with code $exitCode")

  /** Phase 4: boundary import + object reconstruction. Reads the result
    * `value` column back and zips it against the *original* orders to
    * rebuild the required `Order` instances, including the fields that
    * never crossed the boundary.
    */
  private def readIpcAndReconstruct(path: Path, allocator: RootAllocator, originalOrders: IndexedSeq[Order]): IndexedSeq[Order] =
    val channel = FileChannel.open(path, StandardOpenOption.READ)
    try
      val reader = new ArrowFileReader(channel, allocator)
      try
        reader.loadNextBatch()
        val root = reader.getVectorSchemaRoot
        val vector = root.getVector("value").asInstanceOf[IntVector]
        originalOrders.zipWithIndex.map { case (order, i) => order.copy(value = vector.get(i)) }
      finally reader.close()
    finally channel.close()

  private def parseArg(args: Array[String], name: String): Option[String] =
    args.find(_.startsWith(s"--$name=")).map(_.stripPrefix(s"--$name="))

  def main(args: Array[String]): Unit =
    val rows = parseArg(args, "rows").map(_.toInt).getOrElse(100_000)
    val kernelBinaryPath = parseArg(args, "kernel-binary").getOrElse(
      sys.error("--kernel-binary=<path to arrow-interop-kernel> is required")
    )
    val k = 7

    val allocator = new RootAllocator()
    val tempDir = Files.createTempDirectory("arrow-interop-e1")
    val inputPath = tempDir.resolve("input.arrow")
    val outputPath = tempDir.resolve("output.arrow")

    // `sys.exit` terminates the JVM immediately, bypassing any pending
    // `finally` -- so `allocator.close()` below must run to completion
    // first. `failed` records the verdict; `sys.exit` is called exactly
    // once, after the try/finally has already fully completed.
    var failed = false

    try
      val totalStart = System.nanoTime()

      val orders = generateOrders(rows)

      val packStart = System.nanoTime()
      val root = packValueColumn(orders, allocator)
      val packDuration = System.nanoTime() - packStart

      val exportStart = System.nanoTime()
      writeIpc(root, inputPath)
      root.close()
      val exportDuration = System.nanoTime() - exportStart

      val kernelStart = System.nanoTime()
      runRustKernel(Path.of(kernelBinaryPath), inputPath, outputPath, k)
      val kernelDuration = System.nanoTime() - kernelStart

      val reconstructStart = System.nanoTime()
      val reconstructed = readIpcAndReconstruct(outputPath, allocator, orders)
      val reconstructDuration = System.nanoTime() - reconstructStart

      val totalDuration = System.nanoTime() - totalStart

      // wrapping_add is standard two's-complement wraparound; every
      // `value` here is in 0 until 1_000_000, so plain Int addition never
      // wraps and is a correct independent oracle -- it does not call,
      // import, or reimplement the Rust kernel's own logic.
      val expected = orders.map(order => order.copy(value = order.value + k))

      if reconstructed != expected then
        println("arrow-interop-e1: FAIL: reconstructed orders do not match the independent oracle")
        failed = true
      else
        def ms(nanos: Long): Double = nanos / 1e6

        println(s"arrow-interop-e1: rows=$rows")
        println(s"  1. object-to-Arrow packing:            ${ms(packDuration)} ms")
        println(s"  2. boundary export (write IPC):        ${ms(exportDuration)} ms")
        println(s"  3. Rust kernel execution (subprocess):  ${ms(kernelDuration)} ms")
        println(s"  4. boundary import + reconstruction:    ${ms(reconstructDuration)} ms")
        println(s"  5. total:                               ${ms(totalDuration)} ms")
        println("arrow-interop-e1: PASS")
    finally
      allocator.close()

    if failed then sys.exit(1)

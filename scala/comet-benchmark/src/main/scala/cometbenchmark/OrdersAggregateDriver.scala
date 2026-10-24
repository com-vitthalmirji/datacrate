package cometbenchmark

import java.math.{BigDecimal => JBigDecimal}
import java.sql.Timestamp

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/** Minimal Spark driver for the Scenario B/C Comet comparison (decision:
  * option (a) — reuse an operation Comet already natively supports;
  * `wrapping_add_simd` stays Scenario A, a separate, already-integrated
  * DataFusion path in `crates/pipeline/src/simd_add_udf.rs`, and this
  * driver never calls it).
  *
  * All Spark/Comet configuration (`--master`, `--jars`, `spark.comet.enabled`,
  * the Comet shuffle manager) is supplied externally by whoever launches
  * this via `spark-submit`, not hardcoded here — the same compiled driver
  * runs unmodified for both the baseline and the Comet-accelerated
  * invocation, satisfying "separate Spark application invocations so
  * session/configuration state does not leak between scenarios" by
  * construction: there are two separate `spark-submit` processes, not one
  * process branching on a flag.
  *
  * Deliberately NOT added to `scala/comet-interop`: that module is a
  * self-contained simulation of Comet's operator-matching idea with no
  * real Spark dependency; folding a real Spark/Comet driver into it would
  * blur that distinction, which is exactly what the simulation is not.
  *
  * Query: the same filter + aggregate `benchmark/spark-comet/query.sql`
  * already uses (`COUNT(*)`, `SUM(amount) WHERE amount > 50.00`) — both
  * operations are inside Comet's documented, already-native operator set,
  * unlike a custom scalar kernel.
  */
object OrdersAggregateDriver {

  private val FilterThreshold = new JBigDecimal("50.00")

  /** Same four-column shape as `crates/pipeline::datafusion_query::orders_schema`
    * (`id: Int64`, `amount: Decimal(10,2)`, `placed_at: Timestamp`,
    * `note: Utf8`) — reused as a shape, not as shared code: this driver
    * intentionally carries no Rust dependency, so the fixture is generated
    * independently in Scala, with the same deterministic formula
    * documented at [[amountFor]].
    */
  private val Schema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("amount", DataTypes.createDecimalType(10, 2), nullable = false),
      StructField("placed_at", TimestampType, nullable = false),
      StructField("note", StringType, nullable = false)
    )
  )

  /** Same deterministic cycle as the Rust fixture generator
    * (`crates/pipeline/examples/comet_benchmark_dataset.rs`): amount cycles
    * 1.00..=100.00 via `(id % 100) + 1`, so the `amount > 50.00` filter
    * matches exactly half of all rows, and the expected count/total are
    * computable independently of any engine under test — the correctness
    * oracle below never calls Spark at all.
    */
  private def amountFor(id: Long): JBigDecimal =
    JBigDecimal.valueOf((id % 100 + 1) * 100, 2)

  private final case class Expected(count: Long, total: JBigDecimal)

  private def expectedResult(rowCount: Long): Expected = {
    val matching = (0L until rowCount).map(amountFor).filter(_.compareTo(FilterThreshold) > 0)
    val total = matching.foldLeft(JBigDecimal.ZERO)(_ add _)
    Expected(matching.length.toLong, total)
  }

  private def generateFixture(spark: SparkSession, path: String, rowCount: Long): Unit = {
    val placedAt = Timestamp.valueOf("2026-01-01 00:00:00")
    val rows = (0L until rowCount).map { id =>
      Row(id, amountFor(id), placedAt, "synthetic")
    }
    val rdd = spark.sparkContext.parallelize(rows)
    val df = spark.createDataFrame(rdd, Schema)
    df.write.mode("overwrite").parquet(path)
  }

  /** A focused invariant, not a full-plan snapshot: a snapshot would be
    * brittle to Comet's own internal operator renames across versions,
    * and this only needs to answer "did native execution happen at all,"
    * not reproduce the plan's exact shape (decision §5: "avoid a brittle
    * full-plan snapshot when a focused invariant is sufficient").
    */
  private def planUsesCometNatively(plan: String): Boolean =
    plan.contains("Comet")

  private final case class Args(
      output: String,
      rows: Long,
      generateFixture: Boolean,
      requireComet: Option[Boolean]
  )

  private def parseArgs(argv: Array[String]): Args = {
    val generateFixture = argv.contains("--generate-fixture")
    val rest = argv.filterNot(_ == "--generate-fixture")
    val values = rest
      .sliding(2, 2)
      .collect { case Array(key, value) if key.startsWith("--") => key.drop(2) -> value }
      .toMap

    val output = values.getOrElse("output", sys.error("--output is required"))
    val rows = values.get("rows").map(_.toLong).getOrElse(2000L)
    val requireComet = values.get("require-comet").map(_.toBoolean)
    Args(output, rows, generateFixture, requireComet)
  }

  def main(argv: Array[String]): Unit = {
    val args = parseArgs(argv)
    val spark = SparkSession.builder().appName("comet-benchmark-orders-aggregate").getOrCreate()

    var failure: Option[String] = None

    try {
      if (args.generateFixture) {
        generateFixture(spark, args.output, args.rows)
      }

      val df = spark.read.parquet(args.output)
      df.createOrReplaceTempView("orders")
      val result = spark.sql(
        "SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount FROM orders WHERE amount > 50.00"
      )

      val plan = result.queryExecution.executedPlan.toString
      println(s"executed plan:\n$plan")

      val rows = result.collect()
      if (rows.length != 1) {
        failure = Some(s"expected exactly one result row, got ${rows.length}")
      } else {
        val actualCount = rows(0).getAs[Long]("order_count")
        val actualTotal = rows(0).getAs[JBigDecimal]("total_amount")
        val expected = expectedResult(args.rows)

        println(s"actual:   count=$actualCount total=$actualTotal")
        println(s"expected: count=${expected.count} total=${expected.total}")

        if (actualCount != expected.count || actualTotal.compareTo(expected.total) != 0) {
          failure = Some("correctness check failed: actual result does not match the independent oracle")
        } else {
          args.requireComet.foreach { expectNative =>
            val native = planUsesCometNatively(plan)
            if (expectNative && !native) {
              failure = Some("required native Comet execution did not appear in the executed plan")
            } else if (!expectNative && native) {
              failure = Some("unexpected Comet operator present in a baseline (non-Comet) run")
            }
          }
        }
      }
    } finally {
      spark.stop()
    }

    failure match {
      case Some(message) =>
        System.err.println(s"comet-benchmark: FAIL: $message")
        sys.exit(1)
      case None =>
        println("comet-benchmark: PASS")
    }
  }
}

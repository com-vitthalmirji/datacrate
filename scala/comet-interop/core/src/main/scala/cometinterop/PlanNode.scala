package cometinterop

/** Stands in for the plan node types a real integration would rewrite
  * (`org.apache.spark.sql.execution.SparkPlan` and friends). Spark and Comet
  * in this repo are published for Scala 2.13 only (see
  * `benchmark/spark-comet/comet-spark-spark4.1_2.13-0.16.0.jar`), so a
  * Scala 3 module cannot extend their traits directly. Wiring this
  * scaffold's generated rules into a real `SparkSessionExtensions` needs a
  * thin Scala 2.13 bridge module, which this scaffold does not build.
  */
final case class PlanNode(nodeName: String, outputFields: List[String])

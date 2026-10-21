package comet_ops

import cometinterop.CometOperator
import cometinterop.CometOperatorSpec

/** Replaces a Parquet scan node with Comet's native columnar scan when the
  * input carries the one field the Comet kernel needs.
  */
object ParquetScanOp extends CometOperatorSpec derives CometOperator:
  val sparkNodeName: String = "FileSourceScanExec"
  val cometReplacementName: String = "CometScanExec"
  val supportedFields: List[String] = List("order_id")

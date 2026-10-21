package comet_ops

import cometinterop.CometOperator
import cometinterop.CometOperatorSpec

/** Replaces a hash-aggregate node with its Comet-native equivalent when the
  * input carries the one field the Comet kernel needs.
  */
object HashAggregateOp extends CometOperatorSpec derives CometOperator:
  val sparkNodeName: String = "HashAggregateExec"
  val cometReplacementName: String = "CometHashAggregateExec"
  val supportedFields: List[String] = List("amount")

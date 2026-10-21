package comet_ops

import cometinterop.CometOperator
import cometinterop.CometOperatorSpec

/** Replaces a projection node with its Comet-native equivalent when the
  * input carries the one field the Comet kernel needs.
  */
object ProjectOp extends CometOperatorSpec derives CometOperator:
  val sparkNodeName: String = "ProjectExec"
  val cometReplacementName: String = "CometProjectExec"
  val supportedFields: List[String] = List("amount")

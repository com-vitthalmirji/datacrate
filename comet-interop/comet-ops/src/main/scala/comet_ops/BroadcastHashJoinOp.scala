package comet_ops

import cometinterop.CometOperator
import cometinterop.CometOperatorSpec

/** Replaces a broadcast hash-join node with its Comet-native equivalent
  * when the input carries the one field the Comet kernel needs.
  * The join family is where a schema mismatch is costliest to get wrong,
  * since it is the operator most likely to run across a shuffle boundary -
  * the derived typeclass's fallback-to-original-node behavior on a missing
  * field is what keeps a mismatch from becoming a correctness bug here.
  */
object BroadcastHashJoinOp extends CometOperatorSpec derives CometOperator:
  val sparkNodeName: String = "BroadcastHashJoinExec"
  val cometReplacementName: String = "CometBroadcastHashJoinExec"
  val supportedFields: List[String] = List("order_id")

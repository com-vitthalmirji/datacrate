package comet_ops

import cometinterop.CometOperator
import cometinterop.CometOperatorSpec

/** Example operator: replaces a row-count filter node with its Comet-native
  * equivalent when the input carries the one field the Comet kernel needs.
  * An object, not a case class, so the scanner's generated registration can
  * reference it by name directly.
  */
object RowCountFilterOp extends CometOperatorSpec derives CometOperator:
  val sparkNodeName: String = "FilterExec"
  val cometReplacementName: String = "CometFilterExec"
  val supportedFields: List[String] = List("row_count")

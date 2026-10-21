package cometinterop

/** One compiled operator-replacement rule: swap `node` for its Comet-native
  * replacement when `replace` recognizes it, otherwise return `node`
  * unchanged. Falling back to the original node (rather than failing) is
  * deliberate: an unsupported input shape must fall back to Spark, not
  * produce a wrong replacement.
  */
final case class ColumnarRule(sparkNodeName: String, replace: PlanNode => PlanNode)

package cometinterop

class CometOperatorSuite extends munit.FunSuite:
  private case class FilterSpec(
      sparkNodeName: String,
      cometReplacementName: String,
      supportedFields: List[String]
  ) extends CometOperatorSpec derives CometOperator

  private val spec = FilterSpec("FilterExec", "CometFilterExec", List("row_count"))

  test("replaces a matching node with its Comet-native name") {
    val rule = CometOperator.ruleFor(spec)
    val input = PlanNode("FilterExec", List("row_count", "value"))
    assertEquals(rule.replace(input), PlanNode("CometFilterExec", List("row_count", "value")))
  }

  test("falls back to the original node when the node name does not match") {
    val rule = CometOperator.ruleFor(spec)
    val input = PlanNode("ProjectExec", List("row_count"))
    assertEquals(rule.replace(input), input)
  }

  test("falls back to the original node when a required field is missing") {
    val rule = CometOperator.ruleFor(spec)
    val input = PlanNode("FilterExec", List("value"))
    assertEquals(rule.replace(input), input)
  }

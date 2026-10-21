package comet_ops

import cometinterop.CometOperator
import cometinterop.PlanNode

/** One match + one fallback test per operator added beyond the original
  * `RowCountFilterOp` example, proving breadth across the scan,
  * project/aggregate, and join categories - all through the same
  * derive/typeclass mechanism `CometOperatorSuite` already covers, so this
  * suite only needs to vary the spec, not the mechanism.
  */
class CometOperatorBreadthSuite extends munit.FunSuite:
  test("ParquetScanOp replaces a matching scan node") {
    val rule = CometOperator.ruleFor(ParquetScanOp)
    val input = PlanNode("FileSourceScanExec", List("order_id", "amount"))
    assertEquals(rule.replace(input), PlanNode("CometScanExec", List("order_id", "amount")))
  }

  test("ParquetScanOp falls back when the required field is missing") {
    val rule = CometOperator.ruleFor(ParquetScanOp)
    val input = PlanNode("FileSourceScanExec", List("amount"))
    assertEquals(rule.replace(input), input)
  }

  test("ProjectOp replaces a matching projection node") {
    val rule = CometOperator.ruleFor(ProjectOp)
    val input = PlanNode("ProjectExec", List("amount"))
    assertEquals(rule.replace(input), PlanNode("CometProjectExec", List("amount")))
  }

  test("ProjectOp falls back when the node name does not match") {
    val rule = CometOperator.ruleFor(ProjectOp)
    val input = PlanNode("FilterExec", List("amount"))
    assertEquals(rule.replace(input), input)
  }

  test("HashAggregateOp replaces a matching aggregate node") {
    val rule = CometOperator.ruleFor(HashAggregateOp)
    val input = PlanNode("HashAggregateExec", List("amount"))
    assertEquals(rule.replace(input), PlanNode("CometHashAggregateExec", List("amount")))
  }

  test("HashAggregateOp falls back when the required field is missing") {
    val rule = CometOperator.ruleFor(HashAggregateOp)
    val input = PlanNode("HashAggregateExec", List("row_count"))
    assertEquals(rule.replace(input), input)
  }

  test("BroadcastHashJoinOp replaces a matching join node") {
    val rule = CometOperator.ruleFor(BroadcastHashJoinOp)
    val input = PlanNode("BroadcastHashJoinExec", List("order_id"))
    assertEquals(
      rule.replace(input),
      PlanNode("CometBroadcastHashJoinExec", List("order_id"))
    )
  }

  test("BroadcastHashJoinOp falls back on a schema mismatch (missing join key)") {
    val rule = CometOperator.ruleFor(BroadcastHashJoinOp)
    val input = PlanNode("BroadcastHashJoinExec", List("amount"))
    assertEquals(rule.replace(input), input)
  }

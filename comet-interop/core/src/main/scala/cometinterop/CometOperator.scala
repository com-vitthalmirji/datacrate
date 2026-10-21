package cometinterop

/** Describes one operator replacement: the Spark plan node it targets, the
  * Comet-native replacement, and the fields that must be present on the
  * input for the replacement to be safe.
  */
trait CometOperatorSpec:
  def sparkNodeName: String
  def cometReplacementName: String
  def supportedFields: List[String]

/** Turns a `CometOperatorSpec` into a runnable `ColumnarRule`. The `derived`
  * given below resolves for any `T <: CometOperatorSpec`, so writing
  * `derives CometOperator` on a spec is really just asking the compiler to
  * confirm that bound holds - the same role `derives Resource` plays for an
  * eezo case class.
  */
trait CometOperator[T <: CometOperatorSpec]:
  def toColumnarRule(spec: T): ColumnarRule

object CometOperator:
  given derived[T <: CometOperatorSpec]: CometOperator[T] with
    def toColumnarRule(spec: T): ColumnarRule =
      ColumnarRule(
        sparkNodeName = spec.sparkNodeName,
        replace = node =>
          val isMatch = node.nodeName == spec.sparkNodeName
            && spec.supportedFields.forall(node.outputFields.contains)
          if isMatch then node.copy(nodeName = spec.cometReplacementName) else node
      )

  /** Builds the rule for one spec, looked up at the call site where its
    * concrete type is known - this is the generated scanner output's only
    * job (see `CometOpsScannerPlugin`).
    */
  def ruleFor[T <: CometOperatorSpec](spec: T)(using ev: CometOperator[T]): ColumnarRule =
    ev.toColumnarRule(spec)

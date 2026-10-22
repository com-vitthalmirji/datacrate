package cometinterop.reflectcheck

import scala.quoted.*

/** Checks that a named private field still exists, with the expected
  * declared type, on a class pulled from the actual compile classpath -
  * at compile time, not at the moment Comet's reflection call runs in
  * production.
  *
  * This targets the failure mode described for Comet's Iceberg
  * integration: both the existing read path and the planned write path
  * reach into private fields on Spark/Iceberg Java classes because those
  * fields have no public API, so a field rename or move on a dependency
  * upgrade breaks the reflective `getDeclaredField` call silently at
  * runtime. `requireField` turns that into a build failure instead, by
  * running the same lookup while the macro itself expands.
  */
object ClasspathField:

  inline def requireField(inline className: String, inline fieldName: String): Unit =
    ${ requireFieldImpl('className, 'fieldName) }

  inline def requireFieldOfType(inline className: String, inline fieldName: String, inline expectedType: String): Unit =
    ${ requireFieldOfTypeImpl('className, 'fieldName, 'expectedType) }

  private def requireFieldImpl(classNameExpr: Expr[String], fieldNameExpr: Expr[String])(using Quotes): Expr[Unit] =
    import quotes.reflect.report
    val className = classNameExpr.valueOrAbort
    val fieldName = fieldNameExpr.valueOrAbort
    lookupField(className, fieldName) match
      case Left(message) => report.errorAndAbort(message)
      case Right(_)       => '{ () }

  private def requireFieldOfTypeImpl(
      classNameExpr: Expr[String],
      fieldNameExpr: Expr[String],
      expectedTypeExpr: Expr[String]
  )(using Quotes): Expr[Unit] =
    import quotes.reflect.report
    val className = classNameExpr.valueOrAbort
    val fieldName = fieldNameExpr.valueOrAbort
    val expectedType = expectedTypeExpr.valueOrAbort
    lookupField(className, fieldName) match
      case Left(message) => report.errorAndAbort(message)
      case Right(field) =>
        val actualType = field.getType.getName
        if actualType == expectedType then '{ () }
        else
          report.errorAndAbort(
            s"compile-time reflection check failed: $className.$fieldName has type " +
              s"'$actualType', expected '$expectedType'"
          )

  /** Pure classpath lookup - a Calculation, not an Action: same class/field
    * name, same result, called from the macro at compile time or from a
    * test at runtime.
    */
  private[reflectcheck] def lookupField(className: String, fieldName: String): Either[String, java.lang.reflect.Field] =
    try
      val cls = Class.forName(className)
      try Right(cls.getDeclaredField(fieldName))
      catch
        case _: NoSuchFieldException =>
          val available = cls.getDeclaredFields.map(_.getName).mkString(", ")
          Left(
            s"compile-time reflection check failed: $className has no field named " +
              s"'$fieldName' (available: $available)"
          )
    catch
      case _: ClassNotFoundException =>
        Left(s"compile-time reflection check failed: class not found on compile classpath: $className")

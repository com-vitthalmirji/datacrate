import sbt._
import sbt.Keys._

import scala.io.Source

/** One scanned candidate: a package and an object name found in source text,
  * believed (not proven) to carry `derives CometOperator`. The compiler is
  * the real judge - a false positive here costs one dead line in the
  * generated file, never a wrong registration.
  */
final case class OperatorCandidate(packageName: String, objectName: String)

/** Pure text scan - a Calculation, not an Action. It never touches sbt's
  * task machinery or writes anything; `CometOpsScannerPlugin` below is the
  * Action that calls this and writes the result to disk.
  *
  * This file lives under `project/`, which sbt compiles with its own meta-
  * build Scala version (2.12), not this repo's Scala 3 - so it uses Scala 2
  * brace syntax throughout, deliberately, not by oversight.
  */
object OperatorScanner {
  private val packagePattern = """^\s*package\s+([\w.]+)\s*$""".r
  private val derivesPattern = """object\s+(\w+)\s+extends\s+\w+(\[.*\])?\s+derives\s+CometOperator""".r

  def scan(sourceDir: File): List[OperatorCandidate] =
    if (!sourceDir.exists()) Nil
    else (sourceDir ** "*.scala").get().toList.flatMap(scanFile)

  private def scanFile(file: File): List[OperatorCandidate] = {
    val lines = Source.fromFile(file).getLines().toList
    val packageName = lines.collectFirst { case packagePattern(name) => name }.getOrElse("")
    derivesPattern
      .findAllMatchIn(lines.mkString("\n"))
      .map(m => OperatorCandidate(packageName, m.group(1)))
      .toList
  }
}

object CometOpsScannerPlugin extends AutoPlugin {
  object autoImport {
    val cometOpsScannerTask = taskKey[Seq[File]](
      "Scans comet-ops sources for `derives CometOperator` objects and generates their registration list"
    )
  }

  import autoImport._

  override def requires: Plugins = sbt.plugins.JvmPlugin
  override def trigger: PluginTrigger = noTrigger

  override lazy val projectSettings: Seq[Setting[_]] = Seq(
    cometOpsScannerTask := generateRegistrations(
      (Compile / scalaSource).value,
      (Compile / sourceManaged).value
    )
  )

  private def generateRegistrations(sourceDir: File, outputDir: File): Seq[File] = {
    val candidates = OperatorScanner.scan(sourceDir)
    val registrations = candidates
      .map(c => s"      cometinterop.CometOperator.ruleFor(${c.packageName}.${c.objectName})")
      .mkString(",\n")

    val content =
      s"""package cometinterop.generated
         |
         |object GeneratedCometOperators:
         |  val rules: List[cometinterop.ColumnarRule] = List(
         |$registrations
         |  )
         |""".stripMargin

    val outputFile = outputDir / "cometinterop" / "generated" / "GeneratedCometOperators.scala"
    IO.write(outputFile, content)
    Seq(outputFile)
  }
}

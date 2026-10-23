// Scala 2.13, not 3.3.8 like comet-interop: this module links directly
// against org.apache.spark's Scala API (`import spark.implicits._`,
// SparkSession), which is published for Scala 2.13 only (matching the
// pinned Comet jar's own `_2.13` artifact suffix). Scala 3 and Scala 2.13
// are not generally binary-compatible for this, so this module's Scala
// version is fixed by Spark's own published cross-version, not a separate
// choice.
ThisBuild / scalaVersion := "2.13.15"
ThisBuild / organization := "com.vitthalmirji.cometbenchmark"
// Pinned explicitly (sbt otherwise defaults to "0.1.0-SNAPSHOT") so the CI
// workflow's `docker compose cp` step can reference a fixed jar filename
// instead of discovering it at runtime.
ThisBuild / version := "0.1.0"
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")

lazy val root = project
  .in(file("."))
  .settings(
    name := "comet-benchmark",
    // "provided": the pinned Spark Docker image already carries these jars
    // at /opt/spark/jars/*, and spark-submit puts them on the classpath
    // automatically. Declaring them here is for compiling against the
    // matching API only, not for bundling into the driver's own thin jar
    // (sbt's default `package`, no assembly plugin needed).
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.1.3" % Provided,
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,
    testFrameworks += new TestFramework("munit.Framework")
  )

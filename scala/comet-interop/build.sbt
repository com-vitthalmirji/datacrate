ThisBuild / scalaVersion := "3.3.8"
ThisBuild / organization := "com.vitthalmirji.cometinterop"
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")

lazy val core = project
  .in(file("core"))
  .settings(
    name := "comet-interop-core",
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,
    testFrameworks += new TestFramework("munit.Framework")
  )

lazy val cometOps = project
  .in(file("comet-ops"))
  .dependsOn(core)
  .settings(
    name := "comet-ops",
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,
    testFrameworks += new TestFramework("munit.Framework"),
    Compile / sourceGenerators += cometOpsScannerTask.taskValue
  )
  .enablePlugins(CometOpsScannerPlugin)

lazy val reflectCheck = project
  .in(file("reflect-check"))
  .settings(
    name := "reflect-check",
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,
    testFrameworks += new TestFramework("munit.Framework")
  )

// Depends on the real `iceberg-spark-runtime` jar so `ClasspathField` can be
// pointed at `org.apache.iceberg.spark.source.SparkWrite`'s actual private
// fields, not a stand-in. Kept as its own module (not folded into
// `reflect-check`) so `reflect-check`'s own build doesn't need to fetch a
// ~80MB Iceberg jar just to test the macro mechanism against the stub -
// special-purpose code (checks against one real class) is separated from
// general-purpose code (the macro itself).
lazy val reflectCheckIceberg = project
  .in(file("reflect-check-iceberg"))
  .dependsOn(reflectCheck)
  .settings(
    name := "reflect-check-iceberg",
    // 1.10.0 chosen deliberately: it is one of the three Iceberg versions
    // (1.5.2 / 1.8.1 / 1.10.0) Comet's own `IcebergReflection.scala` doc
    // comment names as confirmed-compatible for `SparkWrite`'s private
    // fields - not just the latest available version.
    libraryDependencies += "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.13" % "1.10.0",
    // SparkWrite implements org.apache.spark.sql.connector.write.Write -
    // Class.forName must resolve that superinterface too, so spark-sql
    // (matching the Spark 3.5 line iceberg-spark-runtime-3.5 targets) is
    // needed on the classpath even though nothing here calls into Spark.
    libraryDependencies += "org.apache.spark" % "spark-sql_2.13" % "3.5.9",
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,
    testFrameworks += new TestFramework("munit.Framework")
  )

lazy val root = project
  .in(file("."))
  .aggregate(core, cometOps, reflectCheck, reflectCheckIceberg)
  .settings(
    name := "comet-interop",
    publish / skip := true
  )

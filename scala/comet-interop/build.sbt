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

lazy val root = project
  .in(file("."))
  .aggregate(core, cometOps)
  .settings(
    name := "comet-interop",
    publish / skip := true
  )

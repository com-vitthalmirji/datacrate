// Scala 3.3.8, matching comet-interop: no Spark dependency forces a
// cross-version choice here the way comet-benchmark's Scala 2.13.15 is
// forced by Spark's own published artifacts. Arrow Java is a plain JVM
// library with no Scala-version-specific publication.
ThisBuild / scalaVersion := "3.3.8"
ThisBuild / organization := "com.vitthalmirji.arrowinterop"
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")

lazy val root = project
  .in(file("."))
  .settings(
    name := "arrow-interop",
    // 19.0.0 confirmed current (Maven Central metadata <release>) and
    // lockstep across arrow-vector/arrow-memory-netty at the time this was
    // added. Method signatures used by this module (ArrowFileReader,
    // ArrowFileWriter, VectorSchemaRoot, IntVector, Field/Schema/ArrowType)
    // were verified directly against the real 19.0.0 jar's class files
    // (javap -p), not assumed from memory.
    libraryDependencies += "org.apache.arrow" % "arrow-vector" % "19.0.0",
    // Runtime BufferAllocator implementation; arrow-vector only declares
    // the API, not an allocator.
    libraryDependencies += "org.apache.arrow" % "arrow-memory-netty" % "19.0.0",
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,
    testFrameworks += new TestFramework("munit.Framework")
  )

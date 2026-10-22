package cometinterop.reflectcheck

/** Proves the mechanism against a real class on the compile classpath
  * (`IcebergWriteConfigStub`, compiled in the same build), not a hand-rolled
  * string. Each call below runs `Class.forName` + `getDeclaredField` while
  * this file itself is being compiled - renaming or removing any of these
  * four fields on the stub fails this module's build, which is the point:
  * the equivalent rename on the real Iceberg write-config class today only
  * fails at the moment Comet's planned write path calls it in production.
  *
  * The class name is a literal, not `classOf[...].getName`: `inline`
  * macro parameters must reduce to a literal constant at the call site,
  * and `classOf[X].getName` is a runtime computation, not a constant.
  */
object WriteConfigFields:
  private inline val className = "cometinterop.reflectcheck.stub.IcebergWriteConfigStub"

  ClasspathField.requireFieldOfType(className, "targetFileSizeBytes", "long")
  ClasspathField.requireFieldOfType(className, "rowsPerRowGroup", "int")
  ClasspathField.requireFieldOfType(className, "partitionPath", "java.lang.String")
  ClasspathField.requireFieldOfType(className, "useCometForWrite", "boolean")

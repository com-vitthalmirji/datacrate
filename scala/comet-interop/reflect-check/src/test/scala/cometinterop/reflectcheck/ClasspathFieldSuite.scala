package cometinterop.reflectcheck

class ClasspathFieldSuite extends munit.FunSuite:

  private val stubClassName = "cometinterop.reflectcheck.stub.IcebergWriteConfigStub"

  test("lookupField finds an existing field") {
    val result = ClasspathField.lookupField(stubClassName, "targetFileSizeBytes")
    assert(result.isRight, result)
  }

  test("lookupField reports a missing field by name, with the real available fields listed") {
    val result = ClasspathField.lookupField(stubClassName, "targetFileSizeByte")
    result match
      case Left(message) =>
        assert(message.contains("no field named 'targetFileSizeByte'"), message)
        assert(message.contains("targetFileSizeBytes"), message)
      case Right(_) => fail("expected a missing-field error")
  }

  test("lookupField reports a class missing from the classpath") {
    val result = ClasspathField.lookupField("cometinterop.reflectcheck.stub.NoSuchStub", "anyField")
    result match
      case Left(message) => assert(message.contains("class not found"), message)
      case Right(_)       => fail("expected a class-not-found error")
  }

  test("WriteConfigFields compiled - every macro-checked field still exists on the stub") {
    // Merely referencing the object proves its static initializer (the four
    // ClasspathField.requireFieldOfType calls) already ran successfully at
    // compile time - this test documents that fact, it doesn't re-run it.
    assert(WriteConfigFields != null)
  }

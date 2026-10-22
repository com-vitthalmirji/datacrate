package cometinterop.reflectcheck.iceberg

class SparkWriteFieldsSuite extends munit.FunSuite:

  test("SparkWriteFields compiled - every macro-checked field still exists on the real SparkWrite") {
    // Merely referencing the object proves its static initializer (the five
    // ClasspathField.requireFieldOfType calls against the real Iceberg jar)
    // already ran successfully at compile time - this test documents that
    // fact, it doesn't re-run it.
    assert(SparkWriteFields != null)
  }

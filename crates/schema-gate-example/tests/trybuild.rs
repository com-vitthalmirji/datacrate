#[test]
fn compile_fail_schema_drift_fails_the_build() {
    let t = trybuild::TestCases::new();
    // A pass case makes locked trybuild use cargo build, which catches
    // monomorphization-time inline-const failures that cargo check misses.
    t.pass("tests/pass/*.rs");
    t.compile_fail("tests/compile_fail/*.rs");
}

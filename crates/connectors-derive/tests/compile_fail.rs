#[test]
fn connector_derive_rejects_generics_at_compile_time() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
}

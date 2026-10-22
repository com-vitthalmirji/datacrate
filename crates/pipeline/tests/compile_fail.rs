#[test]
fn layout_rejects_a_row_batch_where_a_columnar_batch_is_required() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
}

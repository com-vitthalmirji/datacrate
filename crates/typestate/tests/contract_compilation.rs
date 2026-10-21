#[test]
fn automatic_connections_and_stage_states_are_checked_by_builds() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/contract_pass/*.rs");
    cases.compile_fail("tests/contract_compile_fail/*.rs");
}

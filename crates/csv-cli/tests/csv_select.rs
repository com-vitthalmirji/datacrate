use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn bin() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_csv-select"));
    cmd.current_dir(workspace_root());
    cmd
}

#[test]
fn headers_mode_selects_column_by_index() {
    let output = bin()
        .args(["fixtures/m1/headers.csv", "--column", "2"])
        .output()
        .expect("failed to run csv-select");

    assert!(output.status.success());
    let expected = fs::read(workspace_root().join("fixtures/m1/headers-column-2.csv"))
        .expect("expected fixture must exist");
    assert_eq!(output.stdout, expected);
}

#[test]
fn headers_mode_round_trips_unicode() {
    // fixtures/m1/headers.csv column 1 ("name") contains "Zoë" — the only
    // Unicode content in the fixture set. column-2 tests (quoted commas,
    // embedded newlines, empty fields) don't exercise this.
    let output = bin()
        .args(["fixtures/m1/headers.csv", "--column", "1"])
        .output()
        .expect("failed to run csv-select");

    assert!(output.status.success());
    assert_eq!(output.stdout, "name\nAda\nZoë\n\"\"\n".as_bytes());
}

#[test]
fn no_headers_mode_selects_column_by_index() {
    let output = bin()
        .args([
            "fixtures/m1/no-headers.csv",
            "--column",
            "1",
            "--no-headers",
        ])
        .output()
        .expect("failed to run csv-select");

    assert!(output.status.success());
    let expected = fs::read(workspace_root().join("fixtures/m1/no-headers-column-1.csv"))
        .expect("expected fixture must exist");
    assert_eq!(output.stdout, expected);
}

#[test]
fn no_headers_mode_invalid_column_fails_before_writing_any_output() {
    let output = bin()
        .args([
            "fixtures/m1/no-headers.csv",
            "--column",
            "99",
            "--no-headers",
        ])
        .output()
        .expect("failed to run csv-select");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).expect("stderr must be valid utf-8");
    assert!(
        stderr.contains("column 99 is out of range"),
        "stderr was: {stderr}"
    );
}

#[test]
fn missing_input_file_fails_with_nonzero_exit_and_stderr() {
    let output = bin()
        .args(["fixtures/m1/does-not-exist.csv", "--column", "0"])
        .output()
        .expect("failed to run csv-select");

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr must be valid utf-8");
    assert!(
        stderr.contains("does-not-exist.csv"),
        "stderr was: {stderr}"
    );
    assert!(stderr.contains("cannot read"), "stderr was: {stderr}");
}

#[test]
fn malformed_csv_fails_with_nonzero_exit_and_stderr() {
    let output = bin()
        .args(["fixtures/m1/malformed.csv", "--column", "0"])
        .output()
        .expect("failed to run csv-select");

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr must be valid utf-8");
    assert!(stderr.contains("malformed CSV"), "stderr was: {stderr}");
}

#[test]
fn invalid_column_fails_before_writing_any_output() {
    let output = bin()
        .args(["fixtures/m1/headers.csv", "--column", "99"])
        .output()
        .expect("failed to run csv-select");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).expect("stderr must be valid utf-8");
    assert!(
        stderr.contains("column 99 is out of range"),
        "stderr was: {stderr}"
    );
}

#[test]
fn missing_required_flag_fails_with_clap_usage_error() {
    let output = bin()
        .args(["fixtures/m1/headers.csv"])
        .output()
        .expect("failed to run csv-select");

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr must be valid utf-8");
    assert!(stderr.contains("--column"), "stderr was: {stderr}");
}

#[test]
fn help_flag_documents_the_cli_contract() {
    let output = bin()
        .args(["--help"])
        .output()
        .expect("failed to run csv-select");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout must be valid utf-8");
    assert!(stdout.contains("--column"), "stdout was: {stdout}");
    assert!(stdout.contains("--no-headers"), "stdout was: {stdout}");
}

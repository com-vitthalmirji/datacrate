use std::process::Command;
#[test]
fn invalid_batch_sizes_fail_before_local_or_remote_work() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let input = dir.path().join("missing.csv");
    let output = dir.path().join("output.parquet");
    for size in ["0".to_owned(), format!("{}0", usize::MAX)] {
        let local = Command::new(env!("CARGO_BIN_EXE_bounded-csv-to-parquet"))
            .args(["--input"])
            .arg(&input)
            .arg("--output")
            .arg(&output)
            .args(["--batch-size", &size])
            .output()
            .expect("run local CLI");
        assert!(!local.status.success());
        assert!(String::from_utf8_lossy(&local.stderr).contains("--batch-size"));
        assert!(!output.exists());
        let remote = Command::new(env!("CARGO_BIN_EXE_run-s3-pipeline"))
            .args([
                "--bucket",
                "unused",
                "--input-key",
                "missing",
                "--output-key",
                "out",
                "--batch-size",
                &size,
            ])
            .env_remove("AWS_ACCESS_KEY_ID")
            .env_remove("AWS_SECRET_ACCESS_KEY")
            .output()
            .expect("run remote CLI");
        assert!(!remote.status.success());
        let error = String::from_utf8_lossy(&remote.stderr);
        assert!(error.contains("--batch-size"));
        assert!(!error.contains("configure S3"));
    }
}

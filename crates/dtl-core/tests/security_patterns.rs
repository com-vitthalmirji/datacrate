//! Proves the CVE-backed security patterns in
//! `dtl_core::security_patterns` deterministically — no real race, no
//! network, no umask-dependent assumption.

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::symlink;

#[cfg(target_os = "linux")]
use dtl_core::security_patterns::read_filename_safe;
use dtl_core::security_patterns::{
    create_file_atomically, is_within_naive, is_within_safe, mkdir_atomic_with_mode,
};
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;

#[test]
fn symlink_planted_before_create_new_is_rejected_atomically() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let target = dir.path().join("target");
    let planted = dir.path().join("planted-link");
    fs::write(&target, b"attacker-controlled").expect("write symlink target");
    symlink(&target, &planted).expect("plant symlink at the create path");

    let result = create_file_atomically(&planted);

    assert!(
        matches!(&result, Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists),
        "create_new must fail atomically when a symlink already occupies the path, got: \
         {result:?}"
    );
}

#[test]
fn naive_prefix_check_disagrees_with_canonicalized_check() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let base = dir.path().join("base");
    let outside = dir.path().join("outside");
    fs::create_dir(&base).expect("create base dir");
    fs::create_dir(&outside).expect("create outside dir");

    // False positive: a candidate string that textually starts with
    // `base`'s path but resolves (via `..`) to a directory outside it.
    let traversal_candidate = base.join("..").join("outside");
    assert!(
        is_within_naive(
            base.to_str().expect("utf8 path"),
            traversal_candidate.to_str().expect("utf8 path"),
        ),
        "naive check must false-positive on a textual prefix match"
    );
    assert_eq!(
        is_within_safe(&base, &traversal_candidate),
        Some(false),
        "canonicalized check must see through the `..` and reject it"
    );

    // False negative: a symlink whose name doesn't start with `base`'s
    // path at all, but which resolves into `base`.
    let link = dir.path().join("link-into-base");
    symlink(&base, &link).expect("create symlink into base");
    assert!(
        !is_within_naive(
            base.to_str().expect("utf8 path"),
            link.to_str().expect("utf8 path")
        ),
        "naive check must false-negative on a same-target symlink with a different name"
    );
    assert_eq!(
        is_within_safe(&base, &link),
        Some(true),
        "canonicalized check must resolve the symlink and accept it"
    );
}

// APFS (macOS) enforces UTF-8 filename normalization at the file-system
// level and rejects this write outright; ext4 (Linux, also in CI) imposes
// no such requirement and treats a filename as an opaque byte string.
#[test]
#[cfg(target_os = "linux")]
fn non_utf8_filename_is_unreadable_as_str_but_readable_as_osstr() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let raw_name = std::ffi::OsStr::from_bytes(b"caf\xFF");
    fs::write(dir.path().join(raw_name), b"non-utf8 filename").expect("create non-utf8 file");

    let entry = fs::read_dir(dir.path())
        .expect("read dir")
        .next()
        .expect("one entry")
        .expect("entry readable");

    assert!(
        entry.file_name().to_str().is_none(),
        "the crafted filename must not be representable as &str"
    );
    assert_eq!(
        read_filename_safe(&entry).as_bytes(),
        raw_name.as_bytes(),
        "the OsStr-based safe path must read the exact bytes without panicking"
    );
}

#[test]
fn atomic_mkdir_mode_has_no_group_or_other_bits_regardless_of_umask() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("create tempdir");
    let target = dir.path().join("mode-0700");

    mkdir_atomic_with_mode(&target, 0o700).expect("create dir with explicit mode");

    let mode = fs::metadata(&target)
        .expect("read metadata")
        .permissions()
        .mode();
    assert_eq!(
        mode & 0o077,
        0,
        "requesting 0o700 must leave zero group/other bits set regardless of umask, got: \
         {mode:#o}"
    );
}

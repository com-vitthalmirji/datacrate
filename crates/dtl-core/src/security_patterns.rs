//! File-system security patterns, backed by real uutils coreutils CVEs.
//!
//! uutils coreutils (a Rust rewrite of GNU coreutils) has accumulated 44 CVEs
//! with zero memory-safety bugs — every one is a boundary/logic error
//! (<https://byteiota.com/44-rust-cves-but-zero-memory-bugs-what-this-reveals/>).
//! Ownership eliminates one whole class of bug; it says nothing about
//! TOCTOU races, path traversal, or non-UTF-8 input. This module reproduces
//! three of those categories as runnable before/after pairs, plus one
//! narrative-only case where no safe reproduction exists.
//!
//! Grokking Simplicity tagging: every `is_*`/`would_*` function below is a
//! Calculation (pure, same input → same output). Every `create_*`/`mkdir_*`
//! function is an Action — it touches the file system and returns
//! `io::Result`, so its effect is visible in the signature, not hidden.
//!
//! Depth Doctrine framing: [`create_file_atomically`] and
//! [`mkdir_atomic_with_mode`] define their race windows out of existence —
//! the unsafe intermediate state (file created, permissions not yet set;
//! or path checked, not yet created) is simply not representable by the
//! API chosen, rather than being handled after the fact.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::Path;

/// Returns whether a file-creation strategy leaves a TOCTOU race window.
///
/// Calculation: pure, documents *why* `create_new` closes the gap that
/// check-then-create leaves open (CVE-2026-35354, `mv`).
#[must_use]
pub fn would_be_racy(uses_create_new: bool) -> bool {
    !uses_create_new
}

/// Creates `path`, but checks for existence before creating — the
/// vulnerable shape. Another process (or an attacker-planted symlink) can
/// occupy `path` between the check and the create.
///
/// # Errors
///
/// Returns the underlying I/O error if the existence check or the create
/// fails.
pub fn create_file_racy(path: &Path) -> io::Result<File> {
    if path.exists() {
        return Err(io::Error::new(io::ErrorKind::AlreadyExists, "path exists"));
    }
    File::create(path)
}

/// Creates `path` atomically: `O_CREAT | O_EXCL` under the hood via
/// [`OpenOptions::create_new`], so there is no separate check-then-act step
/// for a race to land in. Fails with `AlreadyExists` if anything — file or
/// symlink — already occupies `path`.
///
/// # Errors
///
/// Returns `io::ErrorKind::AlreadyExists` if `path` is already occupied, or
/// another I/O error from the underlying `open` call.
pub fn create_file_atomically(path: &Path) -> io::Result<File> {
    OpenOptions::new().write(true).create_new(true).open(path)
}

/// Returns whether `candidate` textually starts with `base` — the naive,
/// exploitable check (`chmod` historically accepted `/../` this way).
///
/// Calculation: pure string comparison, no file-system access. This is the
/// "before" counter-example: it does not resolve `..` components, so a
/// string that merely starts with `base`'s prefix can still resolve outside
/// `base` on disk.
#[must_use]
pub fn is_within_naive(base: &str, candidate: &str) -> bool {
    candidate.starts_with(base)
}

/// Returns whether `candidate` resolves to a path inside `base`, after
/// canonicalizing both. `None` if either path does not exist (canonicalize
/// requires the path to exist).
///
/// Calculation: deterministic given the file system's current state —
/// classified as a Calculation here because it takes no mutable action,
/// though its result depends on disk state rather than its arguments alone.
#[must_use]
pub fn is_within_safe(base: &Path, candidate: &Path) -> Option<bool> {
    let base = fs::canonicalize(base).ok()?;
    let candidate = fs::canonicalize(candidate).ok()?;
    Some(candidate.starts_with(base))
}

/// Reads a directory entry's file name without assuming it is valid UTF-8.
///
/// `sort --files0-from` (CVE-2026-35377) panicked on non-UTF-8 filenames by
/// forcing a `&str` conversion. `OsStr` makes no such assumption: on Unix, a
/// filename is just a byte string, not a UTF-8 string, so comparing and
/// printing it by `OsStr` never requires a conversion that can fail.
#[must_use]
pub fn read_filename_safe(entry: &fs::DirEntry) -> std::ffi::OsString {
    entry.file_name()
}

/// Creates a directory, but sets its permissions in a second syscall after
/// creation — the vulnerable shape (CVE-2026-35353, `mkdir -m`). Between
/// `create_dir` and `set_permissions`, the directory briefly exists with
/// whatever default mode the OS assigned (subject to umask), not `mode`.
///
/// # Errors
///
/// Returns the underlying I/O error if either syscall fails.
#[cfg(unix)]
pub fn mkdir_racy_then_chmod(path: &Path, mode: u32) -> io::Result<()> {
    fs::create_dir(path)?;
    let mut permissions = fs::metadata(path)?.permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, mode);
    fs::set_permissions(path, permissions)
}

/// Creates a directory with `mode` already applied in the single `mkdir(2)`
/// syscall — no window where the directory exists with the wrong
/// permissions.
///
/// # Errors
///
/// Returns the underlying I/O error if the create fails.
#[cfg(unix)]
pub fn mkdir_atomic_with_mode(path: &Path, mode: u32) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    fs::DirBuilder::new().mode(mode).create(path)
}

// Trust-boundary violation (CVE-2026-35368, `chroot` + shared-library
// loading): a process that `chroot()`s into a restricted directory but
// still dynamically links a shared library can have that library resolved
// from outside the chroot, defeating the sandbox. This is a logic/trust-
// boundary bug, not a memory-safety bug — Rust's ownership model does not
// prevent it, because nothing about the bug involves an invalid memory
// access. No safe in-repo reproduction exists (it requires an actual
// `chroot()` call, which needs root and affects the whole process). This is
// exactly why this repo's `contracts`/`typestate` layers exist: ownership is
// the floor a Rust program starts from, not the ceiling of what a trusted
// boundary needs to enforce.

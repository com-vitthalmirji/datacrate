# Security patterns: what ownership does and doesn't buy you

The [ownership chapter](ownership.md) proved Rust eliminates use-after-free
and data races at compile time. This chapter proves the other half of that
claim by testing its boundary: ownership is a memory-safety guarantee, not
a *correctness* guarantee. A program with zero `unsafe` blocks, borrow
checker fully satisfied, can still open a file at the wrong path, race
against an attacker-planted symlink, or panic on a filename it assumed was
valid UTF-8. `dtl_core::security_patterns` takes four real, CVE-numbered
bugs from GNU coreutils (`mv`, `chmod`, `mkdir`, `sort`) - a C codebase,
not a Rust one - and reproduces the vulnerable *shape* and the fixed shape
side by side, so each fix is something you can run and watch fail, not
just read about.

If you last touched raw pointers in a college C course, this is the
chapter to reactivate that memory for: every bug here is a pointer/syscall
discipline problem that C makes easy to get wrong and that Rust's std
library, not the borrow checker, makes easy to get right.

## TOCTOU: the gap between checking and acting

**Claim**: a security bug doesn't need a data race between two threads to
be a race condition. It's enough for *one* thread to check a condition,
then act on it a moment later - if anything in the world can change
state in that gap, that gap is exploitable.

**Code**: `create_file_racy` is the vulnerable shape - check whether the
path exists, then create the file:

```rust
pub fn create_file_racy(path: &Path) -> io::Result<File> {
    if path.exists() {
        return Err(io::Error::new(io::ErrorKind::AlreadyExists, "..."));
    }
    File::create(path)
}
```

Between the `exists()` check returning `false` and `File::create`
running, an attacker who can write to the same directory plants a symlink
at `path` pointing wherever they like. `File::create` opens through the
symlink and writes attacker-chosen content to attacker-chosen content to
an attacker-chosen target - this is CVE-2026-35354, a real bug in GNU
`mv`. The fix isn't a smarter check - no check run before the create can
ever close this gap, because the gap is the time between the check and
the create, however small. The fix is an API that makes "check" and
"create" the same atomic operation:

```rust
pub fn create_file_atomically(path: &Path) -> io::Result<File> {
    OpenOptions::new().write(true).create_new(true).open(path)
}
```

`create_new(true)` maps to the `O_CREAT | O_EXCL` flag pair at the syscall
level - the kernel fails the whole call atomically if *anything* already
occupies that path, file or symlink, with no window for anything to be
planted in between.

```console
$ cargo test -p dtl-core symlink_planted_before_create_new_is_rejected_atomically -- --nocapture
```

The test plants a symlink at the target path *before* calling
`create_file_atomically`, and asserts the call fails with
`ErrorKind::AlreadyExists` - deterministically, with no real race or
second thread needed, because the bug being tested is "no atomicity," not
"sometimes loses a race."

**The lens worth naming**: this is Ousterhout's "define errors out of
existence," applied to a security boundary instead of an API ergonomics
one. `create_file_racy` has a bug a reviewer has to notice by reasoning
about timing. `create_file_atomically` has no equivalent bug to notice,
because `O_CREAT | O_EXCL` makes the racy intermediate state
unrepresentable - there's no way to ask the kernel "does this exist?"
and get a stale answer, because the question and the action are one
syscall.

**Dormant-C reactivation**: this is the same category of bug `fopen`
without `O_EXCL` has always had in C - `access()` followed by `open()` is
the textbook TOCTOU anti-pattern taught (or not taught) in every systems
course. The fix is identical at the syscall level in both languages
(`O_CREAT | O_EXCL`); what's different is that Rust's std library exposes
it as a named, discoverable builder method (`create_new`) rather than a
raw flag constant you have to already know to go looking for.

**Scala/Spark bridge**: there's no clean analogue inside the JVM's own
file APIs - `java.nio.file.Files.createFile` actually has the same
`CREATE_NEW`-equivalent atomicity by default, so this specific bug class
is less commonly hit from Java/Scala. The closer parallel is a distributed
one: an executor checking whether an output path exists before writing a
partition, then writing - exactly the shape of bug idempotent-write
designs in Spark pipelines have to guard against, usually by writing to a
temp path and atomically renaming, not by checking first.

## Path traversal: a prefix match is not a containment check

**Claim**: whether one path is "inside" another is a question about where
the path actually *resolves* on disk, not about what characters its string
representation happens to start with. Checking only the string produces
both false positives and false negatives.

**Code**: `is_within_naive` does the tempting, wrong thing - string prefix
comparison:

```rust
pub fn is_within_naive(base: &str, candidate: &str) -> bool {
    candidate.starts_with(base)
}
```

`is_within_safe` resolves both paths to their canonical form first, then
compares:

```rust
pub fn is_within_safe(base: &Path, candidate: &Path) -> Option<bool> {
    let base = fs::canonicalize(base).ok()?;
    let candidate = fs::canonicalize(candidate).ok()?;
    Some(candidate.starts_with(&base))
}
```

```console
$ cargo test -p dtl-core naive_prefix_check_disagrees_with_canonicalized_check -- --nocapture
```

The test proves both directions of disagreement, not just one:

- **False positive**: `base/../outside` textually starts with `base`'s
  own path string, so `is_within_naive` says "inside" - but `..` walks
  back up and out, so the path actually resolves to a sibling directory
  entirely outside `base`. This is the `chmod`/path-traversal shape: a
  string that *looks* contained but isn't.
- **False negative**: a symlink named `link-into-base`, sitting next to
  `base` with a name sharing none of `base`'s prefix, whose target *is*
  `base`. `is_within_naive` says "outside" because the string doesn't
  match - but the path resolves into exactly the directory it's meant to
  be checked against.

`is_within_safe` gets both right, because `fs::canonicalize` resolves
every `..` and every symlink before the comparison ever runs - the
check happens on the place the path actually points, not on the
characters used to spell it.

**Dormant-C reactivation**: this is `realpath(3)` doing the same
resolution C programs have called for this exact reason since before Rust
existed - `fs::canonicalize` is a thin std wrapper over the same
primitive. If you've ever seen a `chroot`-adjacent security review flag
"always canonicalize before comparing paths," this is why.

**Scala/Spark bridge**: the closest everyday parallel is validating an S3
key prefix or a mounted path before a connector reads from it - checking
`key.startsWith(allowedPrefix)` on the *string* has exactly this same
false-positive shape if the key contains an encoded `../`-equivalent
segment the object store will still honor. The fix in both worlds is the
same principle: resolve first, compare second - never compare on the
unresolved representation.

## Non-UTF-8 filenames: the type that admits the problem exists

**Claim**: Rust's `&str` is a *guarantee*, not a convenience - a
compile-time promise that this particular byte sequence is valid UTF-8.
A filesystem makes no such promise about filenames, so any code that
converts a filename straight to `&str` is asserting something the
filesystem never agreed to.

**Code**: on Linux, a filename is just a sequence of bytes - no UTF-8
requirement at all. `read_filename_safe` holds the name in
`OsString`/`&OsStr`, the type that makes no UTF-8 claim, and only reaches
for a lossy or checked conversion at the point something actually needs
text:

```rust
pub fn read_filename_safe(entry: &fs::DirEntry) -> OsString {
    entry.file_name()
}
```

```console
$ cargo test -p dtl-core non_utf8_filename_is_unreadable_as_str_but_readable_as_osstr -- --nocapture
```

The test (Linux-only - APFS enforces UTF-8 normalization at the
filesystem level and would reject the write outright on macOS) constructs
a genuinely invalid-UTF-8 name via `OsStr::from_bytes(b"caf\xFF")` and
writes a real file with it, then shows `entry.file_name().to_str()`
returns `None` - proving the panic surface a naive `.to_str().unwrap()`
would hit actually exists on a real filesystem - while
`read_filename_safe`'s `OsString` reads the exact same bytes back without
ever needing to decide whether they're valid UTF-8.

This is CVE-2026-35377: GNU `sort --files0-from` assumed filenames were
valid UTF-8 text and panicked when fed one that wasn't.

**Dormant-C reactivation**: this is the same fact C never hid and Rust's
type system makes impossible to forget - in C, a filename is `char*`, an
opaque byte string, and nothing stops you from treating it as text; the
bug is assuming text where the OS only promised bytes. `OsStr` is Rust's
way of keeping that distinction visible in the type instead of letting it
quietly disappear the first time someone calls `.to_str()`.

**Scala/Spark bridge**: the closer-to-home version of this bug is a
Spark job that assumes every value in a `String` column is valid
UTF-8 because the JVM's `String` type enforces it internally - but the
*source* bytes (a legacy file, a byte-array column from a non-UTF-8
system) may not have been valid to begin with, and the encoding step that
produced the `String` either already lost information or already threw.
`OsStr` is what Rust gives you to defer that decision instead of forcing
it at the point of least information.

## Atomic permission creation: the window between `mkdir` and `chmod`

**Claim**: creating a directory with restrictive permissions is not the
same thing as creating a directory and *then* restricting its
permissions, even though both end in the same final state - the
difference is whether a window exists in between where the directory is
briefly world-readable.

**Code**: `mkdir_racy_then_chmod` is the vulnerable shape - two syscalls,
a directory momentarily created with default (umask-dependent, often
permissive) permissions before the second syscall narrows them:

```rust
pub fn mkdir_racy_then_chmod(path: &Path, mode: u32) -> io::Result<()> {
    fs::create_dir(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
}
```

Between `create_dir` returning and `set_permissions` running, anything
with access to the parent directory can read or write into the
newly-created, not-yet-restricted directory. This is CVE-2026-35353, a
real bug in GNU `mkdir -m`. `mkdir_atomic_with_mode` collapses the two
syscalls into one, via `DirBuilderExt::mode` - the directory is created
*with* its final permission bits already set, with no intermediate state
to exploit:

```rust
pub fn mkdir_atomic_with_mode(path: &Path, mode: u32) -> io::Result<()> {
    DirBuilder::new().mode(mode).create(path)
}
```

```console
$ cargo test -p dtl-core atomic_mkdir_mode_has_no_group_or_other_bits_regardless_of_umask -- --nocapture
```

The test requests mode `0o700` and asserts the resulting directory has
zero group/other bits set - deliberately checked this way rather than
asserting an exact mode, because a process's umask can only *clear* bits
from a requested mode, never add extra ones, so "no group/other bits"
holds regardless of whatever umask the test happens to run under.

**Dormant-C reactivation**: the vulnerable shape here is literally
`mkdir()` followed by `chmod()`, the exact pattern generations of C
security advisories have warned against - the single-syscall fix
(`mkdir2` with an explicit mode argument, which is what `mkdir(2)` itself
actually always supported) has existed the whole time; the bug is reaching
for the two-call version instead of passing the mode to the call that
already accepts one.

**Scala/Spark bridge**: no close JVM equivalent - the JVM's own
`Files.createDirectory` doesn't expose POSIX mode bits directly, so this
specific race is more of a native-interop concern than something a pure
Scala pipeline hits directly. The transferable idea is the general one:
wherever an API offers a combined "create with these final properties"
call versus "create, then adjust," the combined call is the one without a
window.

## Trust boundaries: what ownership deliberately doesn't cover

Not every security bug in this family is reproducible safely, and one is
worth naming even without a runnable example: CVE-2026-35368, a GNU
`chroot` privilege-escalation bug involving shared-library loading across
a `chroot` boundary. It's a logic bug, not a memory-safety bug - the code
does exactly what it was told to do, and what it was told to do turned out
to let a process load a library from outside the boundary it was supposed
to be confined to.

Ownership and borrowing - the whole mechanism the [ownership
chapter](ownership.md) proves - has nothing to say about this class of
bug. A `chroot` boundary, a permission check, a trust boundary between
"data from this file" and "data from that file" are all decisions a
programmer makes about *meaning*, not about memory. This is exactly the
gap this book's [typestate chapter](typestate.md) and [contracts
chapter](contracts.md) exist to narrow - encoding a trust or sequencing
invariant into a type so the compiler enforces *that*, the same way the
borrow checker enforces memory safety. Ownership is the floor. It was
never the ceiling.

## Run the whole chapter live

```console
$ cargo test -p dtl-core security_patterns
```

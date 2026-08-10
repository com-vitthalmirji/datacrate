# Compiler journal

Use one entry per meaningful diagnostic. Predict first, compile second, and ask for help only after reducing the problem and attempting the smallest justified correction.

## Entry template

### Date — short description

- Goal:
- Prediction before compiling:
- Minimal code or test:
- Exact command:
- Diagnostic code and essential message:
- Ownership graph before the failure:
- My original mental model:
- Smallest attempted correction:
- Why the accepted correction works:
- Did the correction allocate, clone, widen ownership, or change concurrency?
- Closed-book reproduction result:
- Official source consulted:
- Remaining question:

### 09 Aug 2026 — E0382: using a `String` after moving it

> Mentor-assisted retrospective: this entry was reconstructed after the first
> review because I had not recorded my prediction or diagnostic during the
> original exercise. Future entries must be written before asking for help.

- Goal: Understand exactly why passing a `String` by value transfers ownership and prevents the caller from using the old binding.
- Prediction before compiling: No contemporaneous prediction was recorded. For the reproduced probe, the expected result was `E0382` because `consume(value)` moves the `String`, while the later `println!` attempts to borrow the moved value.
- Minimal code or test:

  ```rust
  fn consume(value: String) {
      println!("consumed: {value}");
  }

  fn main() {
      let value = String::from("ownership");
      consume(value);
      println!("after move: {value}");
  }
  ```

- Exact command: `rustc --crate-name moved_value_probe --edition 2024 notes/.moved_value_probe.rs`
- Diagnostic code and essential message: `error[E0382]: borrow of moved value: value`. The compiler identified the `String` creation as the original ownership point, `consume(value)` as the move, and the later formatting expression as a borrow after the move.
- Ownership graph before the failure:
  1. `main::value` owns the `String` and its heap buffer.
  2. Calling `consume(value)` moves ownership into `consume::value` because the parameter type is `String`.
  3. `consume::value` is dropped when `consume` returns.
  4. The binding in `main` no longer owns a valid value, so it cannot be borrowed by `println!`.
- My original mental model: I understood that `String` does not implement `Copy`, but my notes did not yet prove that I could trace the owner across a function boundary or distinguish a real Cargo target from an uncompiled source file.
- Smallest attempted correction: Because `consume` only observes the text, change its parameter to `&str` and call it with `consume(&value)`. If the callee genuinely required ownership, alternatives would be to return the `String` to the caller or clone explicitly after accepting the allocation cost.
- Why the accepted correction works: Borrowing gives `consume` temporary read access without transferring ownership. The borrow ends after the call, so `main::value` remains the owner and can be used by the later `println!`.
- Did the correction allocate, clone, widen ownership, or change concurrency? No. It replaced an ownership transfer with a shared borrow. It did not allocate, clone, introduce shared ownership, or affect concurrency.
- Closed-book reproduction result: The mentor reproduced `E0382` on `rustc 1.97.1`. My own closed-book reproduction and explanation remain the final verification for this entry.
- Official sources consulted:
  - [The Rust Programming Language — What Is Ownership?](https://doc.rust-lang.org/book/ch04-01-what-is-ownership.html)
  - [`rustc --explain E0382`](https://doc.rust-lang.org/error_codes/E0382.html)
- Remaining question: When should an API deliberately consume a value instead of borrowing it? Current answer: consume when the callee must store, transform irreversibly, transfer, or control the value's lifetime; borrow when it only needs temporary access.

#### Vitthal's closed-book verification — 10 Aug 2026

- Attempt result: I needed mentor help, so independent reproduction is not yet demonstrated.
- What the compiler reported: `E0382: borrow of moved value: value`.
- Mentor-assisted correction: capture the returned owner with `let value = consume(value);`.
- Why it works: the first binding is moved into `consume`; the returned `String` is then owned by the new, shadowing binding.
- Result: `NEEDS PRACTICE` for M1; valid honest baseline evidence for M0.

### 09 Aug 2026 — Build evidence: why the first green test run was false

- Observation: `cargo test` initially passed even though the ownership source failed when compiled directly.
- Cause: The package had no `src/main.rs` or `src/lib.rs`, so Cargo discovered only the files under `tests/` as integration-test executables. The ownership and borrowing source files were not Cargo targets and were never compiled.
- Additional false signal: One test executed the host `ls` command rather than `data-tools`. Another called `process::exit(0)`, which terminated its test process before the harness could report a normal result.
- Correction: Add explicit conventional targets (`src/lib.rs` and `src/main.rs`), expose the four topic modules through the library, replace the false tests with behavior tests for those modules, and make `just verify` test all targets and features.
- Verification: Cargo now discovers one library, one binary, and four integration-test targets. `just verify`, `cargo run --locked`, and `cargo nextest run --locked` pass; 11 behavior tests complete normally.
- Lesson: A green command is evidence only after confirming which targets and behavior the command covers.
- Official sources consulted:
  - [Cargo targets](https://doc.rust-lang.org/cargo/reference/cargo-targets.html)
  - [`std::process::exit`](https://doc.rust-lang.org/std/process/fn.exit.html)

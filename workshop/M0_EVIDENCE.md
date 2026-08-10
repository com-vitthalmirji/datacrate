# M0 evidence — 10 August 2026

## Mentor-assisted ownership probe

- Attempt result: I needed help; this is honest baseline evidence, not independent fluency.
- Failing example: [`../exercises/m0/e0382_move_error.rs`](../exercises/m0/e0382_move_error.rs)
- Diagnostic: `E0382: borrow of moved value: value`.
- Correction: bind the `String` returned by `consume` to `value` again.
- Why: the call moves ownership into `consume`; returning and rebinding the
  `String` gives the caller a valid owner again. Shadowing reuses the name—it
  does not undo the move.
- Corrected example: [`../exercises/m0/e0382_fixed.rs`](../exercises/m0/e0382_fixed.rs)

## Vitthal's short closeout

- Five-minute explanation attempted after study: `DONE`
- Actual Rusty/workshop hours, 7–10 August: Rusty 4h; workshop 18h; total 22h.
- Capacity: safe for the mandatory Amber week.
- One remaining uncertainty: I need more hands-on practice and greater conceptual clarity in Rust.

## Engineering evidence

- Failing example produces E0382: `PASS`
- Corrected example compiles and runs: `PASS`
- `just verify`: `PASS`
- Initial commit and clean-clone verification: `PASS` (see repository history)

## Decision

M0 status: `PASS`.

Reason: the restart established an honest baseline, runnable and reproducible
engineering evidence, a risk/gap statement, and a safe capacity decision.

Next action: begin Own I under mandatory Amber capacity. M1, due 24 August,
still requires independent ownership explanation and diagnosis.

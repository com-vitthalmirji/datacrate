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

- Five-minute explanation attempted after study: `TODO(Vitthal: DONE)`
- Actual Rusty/workshop hours, 7–10 August: `TODO(Vitthal)`
- Capacity: `TODO(Vitthal: safe for Amber / reduce further)`
- One remaining uncertainty: `TODO(Vitthal)`

## Engineering evidence

- Failing example produces E0382: `PASS`
- Corrected example compiles and runs: `PASS`
- `just verify`: `PASS`
- Initial commit and clean-clone verification: `PASS` (see repository history)

## Decision

M0 status: `READY TO CLOSE`.

Close it after the four short Vitthal inputs above and the initial commit. M1,
due 24 August, still requires independent ownership explanation and diagnosis.

# Risk register

| Risk | Signal | Early probe / mitigation | Decision date | Current state |
|---|---|---|---|---|
| Rust ownership remains explainable only with notes | Random borrow/clone changes or inability to predict diagnostics | Compiler journal, closed-book rebuilds, weekly timed teaching | 24 Aug | open |
| Arrow/DataFusion version skew breaks examples | Duplicate Arrow versions or incompatible public types | Pin versions; prefer DataFusion re-exports; inspect dependency tree | 31 Aug | open |
| Cold compilation consumes live time | Fresh-machine build exceeds exercise allowance | Fetch/build before rehearsals; cache dependencies; provide checkpoints | 7 Sep | open |
| Docker/local object storage is unavailable | Preflight cannot reach local daemon/service | Start daemon and run isolated list/write/delete smoke test early | 7 Sep | open; CLI installed, daemon stopped on 9 Aug |
| Memory/spill/cancellation claims are not reproducible | Behaviour varies or cleanup cannot be observed | Version-pinned integration tests with controlled limits and temp paths | 21 Sep | open |
| Spark/DataFusion semantics diverge | Decimal, timestamp, null, ordering, or window result mismatch | Begin tiny parity probes before benchmark integration | 21 Sep | open |
| Workshop depends on hidden machine state or network | Fresh participant setup fails | Clean-user rehearsals, offline fixtures, cached artifacts, restore commands | 12 Oct | open |
| Participant cannot finish exercises in 10–20 minutes | Facilitator intervenes or completion falls below 80% | Shrink the exercise; retain the capability and failure boundary | 12 Oct | open |
| Workshop title exceeds evidence | Headline implies universal Spark defeat | Keep evidence-bounded title unless benchmark earns a narrow comparison | 26 Oct | controlled |
| Schedule pressure displaces sleep or recovery | Repeated poor sleep, symptoms, concentration below 3/5 | Enter Amber/Red and cut optional scope; removed hours do not become debt | weekly | open |

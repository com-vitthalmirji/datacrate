# Usage

## `csv-select`

`csv-select` picks a single column out of a CSV file and writes it to
stdout, streaming the input rather than buffering the whole file.

```sh
cargo run -p csv-select-cli --bin csv-select -- <input.csv> --column <index>
```

| Flag           | Meaning                                              |
|----------------|-------------------------------------------------------|
| `<input>`      | Path to the input CSV file                           |
| `--column`     | Zero-based index of the column to select              |
| `--no-headers` | Treat the first record as data instead of headers    |

By default the first row is treated as a header and is selected/emitted like
any other row. Pass `--no-headers` if the input has no header row.

```sh
cargo run -p csv-select-cli --bin csv-select -- fixtures/m1/headers.csv --column 1
```

Note: output is written record-by-record as the input is streamed, so a
malformed record partway through the file can leave already-selected rows on
stdout before the error is detected and the process exits non-zero.

## `dtl-core`

`dtl-core` is a hands-on Rust fundamentals library, organized by topic:
`ownership`, `borrowing`, `references`, `slices`, and `csv_zero_copy` (a
lifetime-bound, zero-copy CSV row splitter). Each module and public item has
rustdoc comments with runnable examples — the full API reference is generated
from those comments and published at
[docs.rs/dtl-core](https://docs.rs/dtl-core) and
[alongside this guide](api/dtl_core/index.html) (or locally via
`cargo doc -p dtl-core --open`).

This guide describes the shape of the crate; the doc comments in the source
are the source of truth for exact signatures and behavior.

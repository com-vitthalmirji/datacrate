set shell := ["zsh", "-cu"]

fmt:
    cargo fmt --all --check

lint:
    cargo clippy --all-targets --all-features -- -D warnings

test:
    cargo test --all-targets --all-features --locked

release:
    cargo build --release --locked

docs:
    RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p dtl-core -p csv-select-cli
    mdbook build book

verify: fmt lint test release
    git diff --check

install-hooks:
    git config core.hooksPath .githooks

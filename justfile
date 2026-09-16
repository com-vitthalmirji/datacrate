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

preflight:
    scripts/preflight.sh

install-hooks:
    git config core.hooksPath .githooks

minio-up:
    docker compose -f docker-compose.minio.yml up -d

minio-down:
    docker compose -f docker-compose.minio.yml down

minio-reset:
    docker compose -f docker-compose.minio.yml down -v

# Pinned by commit, not branch — see docs/internals/notes/decisions.md, 2026-09-15 entry.
ballista-install:
    cargo install --git https://github.com/apache/datafusion-ballista --rev ae9bff026ffd18b256d39b10890a2ca96e1fc836 ballista-scheduler ballista-executor

ballista-scheduler:
    ballista-scheduler --bind-port 50050

ballista-executor-1:
    ballista-executor --scheduler-port 50050 --bind-port 50061 --bind-grpc-port 50062 --bind-health-port 50063 --work-dir /tmp/ballista-executor-1

ballista-executor-2:
    ballista-executor --scheduler-port 50050 --bind-port 50071 --bind-grpc-port 50072 --bind-health-port 50073 --work-dir /tmp/ballista-executor-2

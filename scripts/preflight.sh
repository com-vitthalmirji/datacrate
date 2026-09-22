#!/usr/bin/env bash
# Workshop preflight: catches toolchain drift, stale Cargo.lock, fixture
# tampering, and a broken pipeline build before a session starts.
set -euo pipefail
cd "$(dirname "$0")/.."

fail() {
    echo "PREFLIGHT FAILED: $1" >&2
    exit 1
}

echo "== toolchain =="
pinned=$(grep -m1 '^channel' rust-toolchain.toml | sed -E 's/channel = "(.*)"/\1/')
actual=$(rustc --version | awk '{print $2}')
[[ "$actual" == "$pinned" ]] || fail "rustc $actual does not match pinned toolchain $pinned"
echo "ok: rustc $actual matches pinned toolchain"

echo "== Cargo.lock =="
cargo check --locked --workspace --quiet || fail "Cargo.lock is stale (cargo check --locked failed)"
echo "ok: Cargo.lock is up to date"

echo "== fixture digests =="
check_digest() {
    local path="$1" expected="$2"
    local actual
    actual=$(shasum -a 256 "$path" | awk '{print $1}')
    [[ "$actual" == "$expected" ]] || fail "$path digest changed: expected $expected, got $actual"
}
check_digest fixtures/m1/headers.csv 03c409d49b546c73fd6480e1a9c6d7b275f50af39f17ba40d3f893b95720bf92
check_digest fixtures/m3/orders.csv 5c73354c86d6d86413f9f84d92dd5dfefa568a682a71e171aefb5f62e1177ade
echo "ok: fixture digests match"

echo "== pipeline build =="
cargo build -p pipeline --locked --quiet || fail "pipeline crate does not build"
echo "ok: pipeline builds"

echo "== Docker (object-store tests need it) =="
docker info >/dev/null 2>&1 || fail "Docker daemon is not reachable; the object_store_io failure-path tests start their own MinIO container via testcontainers and will fail with a cryptic error, not this one"
echo "ok: Docker daemon is reachable"

echo "PREFLIGHT PASSED"

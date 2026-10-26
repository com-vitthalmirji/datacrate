"""Verify the legacy facade forwards to patched registry code, without auditing it by name."""
from pathlib import Path
import re
import sys
import tomllib

REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
FACADE_VERSION = "0.13.3+datacrate.1"
FACADE_SOURCE = "#![forbid(unsafe_code)]\npub use zstd_patched::*;"
LEGACY_FEATURES = (
    "arrays", "bindgen", "debug", "doc-cfg", "experimental", "fat-lto", "legacy",
    "no_asm", "pkg-config", "thin", "thin-lto", "wasm", "zdict_builder", "zstdmt",
)


def load_inputs(root):
    facade = root / "compat/zstd-013"
    return [
        tomllib.loads((root / "Cargo.toml").read_text()),
        tomllib.loads((facade / "Cargo.toml").read_text()),
        (facade / "src/lib.rs").read_text(),
        tomllib.loads((root / "Cargo.lock").read_text()),
    ]


def release_version(value):
    core = value.split("+", 1)[0]
    return tuple(map(int, core.split("."))) if re.fullmatch(r"\d+\.\d+\.\d+", core) else ()


def violations(root, facade, source, lock):
    """Pure checks: a local package being absent from RustSec is not sufficient."""
    errors = []
    if root.get("patch", {}).get("crates-io", {}).get("zstd") != {"path": "compat/zstd-013"}:
        errors.append("zstd must use the reviewed local facade")
    if "compat/zstd-013" not in root.get("workspace", {}).get("exclude", []):
        errors.append("the facade must not enable every legacy build-tool feature in workspace CI")
    package = facade.get("package", {})
    if package.get("name") != "zstd" or package.get("version") != FACADE_VERSION:
        errors.append("unexpected facade identity")
    if package.get("publish") is not False or package.get("build") is not False:
        errors.append("the facade must remain private and have no build script")
    if set(facade) != {"package", "dependencies", "features"}:
        errors.append("unexpected facade targets or dependency sections")
    expected_dependency = {
        "zstd-patched": {"package": "zstd", "version": "=0.14.3", "default-features": False}
    }
    if facade.get("dependencies") != expected_dependency:
        errors.append("facade must depend only on pinned patched upstream zstd")
    expected_features = {name: ["zstd-patched/" + name] for name in LEGACY_FEATURES}
    expected_features["default"] = ["legacy", "arrays", "zdict_builder"]
    if facade.get("features") != expected_features:
        errors.append("legacy feature forwarding changed")
    if source.strip() != FACADE_SOURCE:
        errors.append("facade must contain only the unsafe-code prohibition and upstream re-export")

    packages = lock.get("package", [])
    local = [p for p in packages if p["name"] == "zstd" and "source" not in p]
    if len(local) != 1 or local[0].get("version") != FACADE_VERSION:
        errors.append("lockfile must contain exactly the reviewed local facade")
    elif local[0].get("dependencies") != ["zstd 0.14.3"]:
        errors.append("local facade must resolve directly to patched zstd")
    minimums = {"zstd": (0, 14, 3), "zstd-safe": (8, 2, 0), "zstd-sys": (2, 1, 2)}
    seen = set()
    for item in packages:
        name = item["name"]
        if name not in minimums:
            continue
        if name == "zstd" and "source" not in item and item.get("version") == FACADE_VERSION:
            continue
        seen.add(name)
        if item.get("source") != REGISTRY or release_version(item["version"]) < minimums[name]:
            errors.append(f"unreviewed or vulnerable compression package: {name} {item['version']}")
    if seen != set(minimums):
        errors.append("patched zstd, zstd-safe and zstd-sys must all be present")
    return errors


def main():
    errors = violations(*load_inputs(Path(__file__).resolve().parents[1]))
    for error in errors:
        print(error, file=sys.stderr)
    if not errors:
        print("Compression graph contains only the forwarding facade and patched registry implementations")
    return bool(errors)


if __name__ == "__main__":
    sys.exit(main())

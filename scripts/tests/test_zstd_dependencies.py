"""Reject audit-green graphs that still contain an unsafe compression branch."""
import copy
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from check_zstd_dependencies import load_inputs, violations


class CompressionPolicyTests(unittest.TestCase):
    def setUp(self):
        self.inputs = copy.deepcopy(load_inputs(ROOT))

    def test_current_graph_uses_only_patched_implementations(self):
        self.assertEqual(violations(*self.inputs), [])

    def test_registry_legacy_crate_cannot_replace_the_facade(self):
        facade = next(p for p in self.inputs[3]["package"]
                      if p["name"] == "zstd" and "source" not in p)
        facade["source"] = "registry+https://github.com/rust-lang/crates.io-index"
        self.assertTrue(violations(*self.inputs))

    def test_old_native_wrapper_cannot_return(self):
        self.inputs[3]["package"].append({
            "name": "zstd-safe", "version": "7.3.0",
            "source": "registry+https://github.com/rust-lang/crates.io-index",
        })
        self.assertTrue(violations(*self.inputs))

    def test_missing_native_implementation_is_not_an_empty_success(self):
        self.inputs[3]["package"] = [p for p in self.inputs[3]["package"]
                                     if p["name"] != "zstd-sys"]
        self.assertTrue(violations(*self.inputs))

    def test_forwarding_source_cannot_hide_extra_code(self):
        self.inputs[2] += "\npub fn unrelated() {}\n"
        self.assertTrue(violations(*self.inputs))

    def test_patched_dependency_must_stay_pinned(self):
        self.inputs[1]["dependencies"]["zstd-patched"]["version"] = "0.14"
        self.assertTrue(violations(*self.inputs))

    def test_feature_cannot_silently_stop_forwarding(self):
        self.inputs[1]["features"]["zstdmt"] = []
        self.assertTrue(violations(*self.inputs))

    def test_other_patch_source_is_rejected(self):
        self.inputs[0]["patch"]["crates-io"]["zstd"] = {"git": "https://example.com/fork"}
        self.assertTrue(violations(*self.inputs))

    def test_build_script_cannot_be_enabled(self):
        self.inputs[1]["package"]["build"] = "build.rs"
        self.assertTrue(violations(*self.inputs))

    def test_non_registry_native_dependency_is_rejected(self):
        native = next(p for p in self.inputs[3]["package"] if p["name"] == "zstd-sys")
        native["source"] = "git+https://example.com/fork#1234"
        self.assertTrue(violations(*self.inputs))


if __name__ == "__main__":
    unittest.main()

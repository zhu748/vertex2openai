"""Negative fixtures ensure the architecture check rejects dependency regressions."""

import ast
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from check_architecture import LEAF_BOUNDARIES, check_modules


def check_sources(sources, packages=frozenset()):
    modules = {name: "" for name in LEAF_BOUNDARIES}
    modules.update(sources)
    return check_modules(
        {name: ast.parse(source) for name, source in modules.items()}, packages
    )


class ArchitectureTests(unittest.TestCase):
    def test_adapters_can_use_leaf_modules(self):
        self.assertEqual(
            check_sources(
                {
                    "upstreams.cookie_proxy": "from upstreams.cookie_transport import stream_events",
                    "upstreams.cookie_transport": "from retry_policy import get_retry_settings",
                    "retry_policy": "import asyncio",
                }
            ),
            [],
        )

    def test_lazy_reverse_import_is_rejected(self):
        errors = check_sources(
            {
                "upstreams.cookie_transport": "def call():\n    import upstreams.cookie_proxy",
            }
        )
        self.assertTrue(
            any(
                "imports upper-layer upstreams.cookie_proxy" in error
                for error in errors
            )
        )

    def test_relative_reverse_import_is_rejected(self):
        errors = check_sources(
            {"upstreams.cookie_transport": "from . import cookie_native"}
        )
        self.assertTrue(
            any(
                "imports upper-layer upstreams.cookie_native" in error
                for error in errors
            )
        )

    def test_conditional_eager_cycle_is_rejected(self):
        errors = check_sources({"a": "if True:\n    import b", "b": "import a"})
        self.assertIn("Import cycle: a -> b -> a", errors)

    def test_lazy_dependency_is_not_an_import_time_cycle(self):
        self.assertEqual(
            check_sources({"a": "import b", "b": "def call():\n    import a"}), []
        )

    def test_package_relative_cycle_is_rejected(self):
        errors = check_sources(
            {
                "feature": "from . import worker",
                "feature.worker": "import feature",
            },
            packages={"feature"},
        )
        self.assertIn("Import cycle: feature -> feature.worker -> feature", errors)

    def test_package_can_export_its_child_without_a_cycle(self):
        self.assertEqual(
            check_sources(
                {"feature": "from . import worker", "feature.worker": ""},
                packages={"feature"},
            ),
            [],
        )

    def test_missing_leaf_module_is_rejected(self):
        errors = check_modules({})
        self.assertTrue(
            all(f"Missing owned module: {name}" in errors for name in LEAF_BOUNDARIES)
        )


if __name__ == "__main__":
    unittest.main()

"""Runs the Transparent binary-upgrade tests with the deploy suite, which discovers only this directory."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from transparent.ops.tests import test_binary_upgrade  # noqa: E402


def load_tests(loader, tests, pattern):
    return loader.loadTestsFromModule(test_binary_upgrade)

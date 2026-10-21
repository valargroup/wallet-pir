"""Runs the Transparent continuous-load repair tests with the deploy suite, which discovers only this directory."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.transparent.tests import test_load_repair  # noqa: E402


def load_tests(loader, tests, pattern):
    return loader.loadTestsFromModule(test_load_repair)

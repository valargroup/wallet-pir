"""Transparent product operations and retained source-bound qualification tools.

The retained qualification modules use sibling module names when run by their
pinned-source wrappers. Their search root is this owned directory in both the
checkout and relocatable bundle, never a separate checkout or current directory.
"""
from pathlib import Path
import sys

_library = str(Path(__file__).resolve().parent)
if _library not in sys.path:
    sys.path.insert(0, _library)

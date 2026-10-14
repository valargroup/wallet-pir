#!/usr/bin/env python3
"""Assemble, locate and verify exact-SHA release artifacts from full CI."""
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'ops/lib'))
from wallet_pir_ops.release import *  # noqa: F401,F403,E402

if __name__ == '__main__':
    main()

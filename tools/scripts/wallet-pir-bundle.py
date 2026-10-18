#!/usr/bin/env python3
"""Build, verify or unpack an operations release; no host or service changes."""
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from wallet_pir_ops.bundle import main  # noqa: E402

if __name__ == '__main__':
    main()

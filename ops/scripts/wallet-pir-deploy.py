#!/usr/bin/env python3
"""Transactional deploys of Enhance PIR and Status PIR; see `--help`.

Run from the Enhance coordinator or a workstation with the production SSH
access the inventory names. Mutating commands hold the production lock.
"""
from pathlib import Path
import sys

# Every invocation, including checksum-bound recipe commands without -B, must
# leave the immutable reviewed source tree unchanged.
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from wallet_pir_ops.deploy.cli import main  # noqa: E402

if __name__ == '__main__':
    sys.exit(main())

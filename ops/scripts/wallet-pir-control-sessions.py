#!/usr/bin/env python3
"""Supervised restricted SSH control forwards for Enhance, Status and Transparent.

    wallet-pir-control-sessions.py [--config PATH] run|check|validate
    wallet-pir-control-sessions.py [--config PATH] authorized-keys --user U --public-key FILE
    wallet-pir-control-sessions.py [--config PATH] sshd-match --user U --authorized-keys-file PATH

The configuration defaults to /etc/wallet-pir/control-sessions.json. See
docs/control-sessions.md and ops/lib/wallet_pir_ops/control_sessions.py.
"""
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
# Installed with a copy of the library in lib/ beside this script; in a
# checkout the library is ops/lib.
for LIB in (HERE / 'lib', HERE.parent / 'lib'):
    if (LIB / 'wallet_pir_ops/control_sessions.py').is_file():
        sys.path.insert(0, str(LIB))
        break
from wallet_pir_ops import control_sessions  # noqa: E402

if __name__ == '__main__':
    try:
        sys.exit(control_sessions.main())
    except (OSError, ValueError, RuntimeError) as error:
        print(f'wallet-pir-control-sessions: {error}', file=sys.stderr)
        sys.exit(2)

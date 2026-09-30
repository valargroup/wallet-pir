#!/usr/bin/env python3
"""Time an explicit check phase using the same recorder as the feedback gate."""
import sys
from fast import run

if __name__ == '__main__':
    if len(sys.argv) < 4 or sys.argv[2] != '--':
        raise SystemExit('usage: stage.py <phase> -- <command>')
    run(sys.argv[3:], stage=sys.argv[1])

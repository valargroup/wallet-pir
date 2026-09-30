#!/usr/bin/env python3
"""Time one command without importing Cargo/selection dependencies."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]


def run(command, *, env=None, stage='helper'):
    start = time.monotonic()
    print('+ ' + ' '.join(command), flush=True)
    result = subprocess.run(command, cwd=ROOT, env=env)
    elapsed = time.monotonic() - start
    record = {'stage': stage, 'seconds': round(elapsed, 3), 'exit': result.returncode}
    print('CHECK_STAGE ' + json.dumps(record), flush=True)
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as out:
            out.write(f'| {stage} | {elapsed:.2f}s | {result.returncode} |\n')
    result.check_returncode()



if __name__ == '__main__':
    if len(sys.argv) < 4 or sys.argv[2] != '--':
        raise SystemExit('usage: stage.py <phase> -- <command>')
    run(sys.argv[3:], stage=sys.argv[1])

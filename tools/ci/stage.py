#!/usr/bin/env python3
"""Time one command without importing Cargo/selection dependencies.

When WALLET_PIR_STAGE_LOG is set (CI), each stage is also appended there with
its parent stage so job reports count nested wrappers once.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'shared/dev'))
from target_lease import inherited_fds  # noqa: E402

PARENT = 'WALLET_PIR_STAGE_PARENT'


def run(command, *, env=None, stage='helper'):
    identifier = uuid.uuid4().hex[:12]
    child = dict(os.environ if env is None else env, **{PARENT: identifier})
    started = time.time()
    start = time.monotonic()
    print('+ ' + ' '.join(command), flush=True)
    result = subprocess.run(command, cwd=ROOT, env=child, **inherited_fds(child))
    elapsed = time.monotonic() - start
    record = {'stage': stage, 'seconds': round(elapsed, 3), 'exit': result.returncode}
    print('CHECK_STAGE ' + json.dumps(record), flush=True)
    log = os.environ.get('WALLET_PIR_STAGE_LOG')
    if log:
        with open(log, 'a') as out:
            out.write(json.dumps({**record, 'id': identifier, 'parent': os.environ.get(PARENT),
                                  'started': round(started, 3)}) + '\n')
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as out:
            out.write(f'| {stage} | {elapsed:.2f}s | {result.returncode} |\n')
    result.check_returncode()



if __name__ == '__main__':
    if len(sys.argv) < 4 or sys.argv[2] != '--':
        raise SystemExit('usage: stage.py <phase> -- <command>')
    run(sys.argv[3:], stage=sys.argv[1])

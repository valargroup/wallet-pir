#!/usr/bin/env python3
"""Build an immutable source export in one persistent, externally limited Cargo lane."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--sha', required=True)
    parser.add_argument('--target', type=Path, required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--unit', required=True)
    args = parser.parse_args()
    if len(args.sha) != 40 or any(c not in '0123456789abcdef' for c in args.sha):
        parser.error('a full source SHA is required')
    args.evidence.mkdir(parents=True, exist_ok=True)
    args.target.mkdir(parents=True, exist_ok=True)
    lane_lock = (args.target / '.activity-build.lock').open('a+')
    fcntl.flock(lane_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    result = args.evidence / 'result.json'
    if result.exists():
        parser.error('result already exists; retain it and use a new owner')
    owner = dict(source_sha=args.sha, source=str(args.source), profile='release-fast',
                 target=str(args.target), unit=args.unit, pid=os.getpid(),
                 log=str(args.evidence / 'build.log'), result=str(result), started_at=time.time())
    (args.evidence / 'owner.json').write_text(json.dumps(owner, indent=2) + '\n')
    env = dict(os.environ, CARGO_TARGET_DIR=str(args.target), CARGO_BUILD_JOBS='4')
    commands = [
        ['cargo', 'build', '--locked', '--profile', 'release-fast',
         '-p', 'transparent-filter-server', '-p', 'transparent-shard-server',
         '-p', 'transparent-loadtest', '-p', 'transparent-measure',
         '-p', 'transparent-regression', '--bins'],
        ['cargo', 'build', '--locked', '--profile', 'release-fast',
         '-p', 'transparent-shard-server', '--example', 'rate-query', '--example', 'native_certificate'],
        ['cargo', 'test', '--locked', '--profile', 'release-fast',
         '-p', 'transparent-events', '--lib'],
        ['cargo', 'test', '--locked', '--profile', 'release-fast',
         '-p', 'transparent-shard', '--lib', 'compact'],
        ['cargo', 'test', '--locked', '--profile', 'release-fast',
         '-p', 'transparent-wallet-store', '--lib'],
        ['cargo', 'test', '--locked', '--profile', 'release-fast',
         '-p', 'transparent-filter-server', '--lib', 'extract::'],
        ['cargo', 'test', '--locked', '--profile', 'release-fast',
         '-p', 'transparent-filter-server', '--lib', 'events::'],
        ['cargo', 'test', '--locked', '--profile', 'release-fast',
         '-p', 'transparent-filter-server', '--lib', 'publication::'],
        ['cargo', 'test', '--locked', '--profile', 'release-fast',
         '-p', 'transparent-filter-server', '--bin', 'event-spotcheck'],
    ]
    stages = []
    failure = None
    with (args.evidence / 'build.log').open('x') as log:
        for command in commands:
            started = time.monotonic()
            completed = subprocess.run(command, cwd=args.source, env=env, stdout=log, stderr=subprocess.STDOUT)
            stages.append(dict(command=command, seconds=time.monotonic() - started, exit_code=completed.returncode))
            log.flush()
            if completed.returncode:
                failure = completed.returncode
                break
    binaries = {}
    if failure is None:
        for name in ['transparent-event-ingest', 'shard-publish', 'event-spotcheck',
                     'shard-verify', 'script-sample', 'transparent-shard-server',
                     'transparent-filter-server', 'transparent-loadtest', 'transparent-measure']:
            path = args.target / 'release-fast' / name
            binaries[name] = dict(path=str(path), sha256=hashlib.file_digest(path.open('rb'), 'sha256').hexdigest())
    compiler = subprocess.check_output(['rustc', '-Vv'], cwd=args.source, env=env, text=True)
    lock_hash = hashlib.file_digest((args.source / 'Cargo.lock').open('rb'), 'sha256').hexdigest()
    result.write_text(json.dumps(dict(**owner, ended_at=time.time(), stages=stages,
        status='passed' if failure is None else 'failed', compiler=compiler,
        cargo_lock_sha256=lock_hash, binaries=binaries), indent=2) + '\n')
    return failure or 0


if __name__ == '__main__':
    raise SystemExit(main())

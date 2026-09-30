#!/usr/bin/env python3
"""Build an immutable source export in one persistent, externally limited Cargo lane."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import shutil
import tomllib
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--sha', required=True)
    parser.add_argument('--target', type=Path, required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--unit', required=True)
    parser.add_argument('--profile', choices=('release-fast', 'release'), default='release-fast')
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
    if args.profile == 'release':
        profile = tomllib.loads((args.source / 'Cargo.toml').read_text())['profile']['release']
        if profile.get('lto') != 'fat' or profile.get('codegen-units') != 1:
            parser.error('production requires the fat-LTO release profile')
    owner = dict(source_sha=args.sha, source=str(args.source), profile=args.profile,
                 driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                 target=str(args.target), unit=args.unit, pid=os.getpid(),
                 log=str(args.evidence / 'build.log'), result=str(result), started_at=time.time())
    (args.evidence / 'owner.json').write_text(json.dumps(owner, indent=2) + '\n')
    env = dict(os.environ, CARGO_TARGET_DIR=str(args.target), CARGO_BUILD_JOBS='4')
    commands = [
        ['cargo', 'build', '--locked', '--profile', args.profile,
         '-p', 'transparent-filter-server', '-p', 'transparent-shard-server',
         '-p', 'transparent-loadtest', '-p', 'transparent-measure',
         '-p', 'transparent-regression', '--bins'],
        ['cargo', 'build', '--locked', '--profile', args.profile,
         '-p', 'transparent-shard-server', '--example', 'rate-query', '--example', 'native_certificate'],
        ['cargo', 'test', '--locked', '--profile', args.profile,
         '-p', 'transparent-events', '--lib'],
        ['cargo', 'test', '--locked', '--profile', args.profile,
         '-p', 'transparent-shard', '--lib', 'compact'],
        ['cargo', 'test', '--locked', '--profile', args.profile,
         '-p', 'transparent-wallet-store', '--lib'],
        ['cargo', 'test', '--locked', '--profile', args.profile,
         '-p', 'transparent-wallet-store', '--test', 'store_semantics', 'metadata'],
        ['cargo', 'build', '--locked', '--profile', args.profile,
         '-p', 'transparent-wallet-store', '--example', 'activity-reopen'],
        ['cargo', 'test', '--locked', '--profile', args.profile,
         '-p', 'transparent-filter-server', '--lib', 'extract::'],
        ['cargo', 'test', '--locked', '--profile', args.profile,
         '-p', 'transparent-filter-server', '--lib', 'events::'],
        ['cargo', 'test', '--locked', '--profile', args.profile,
         '-p', 'transparent-filter-server', '--lib', 'publication::'],
        ['cargo', 'test', '--locked', '--profile', args.profile,
         '-p', 'transparent-filter-server', '--bin', 'event-spotcheck'],
    ]
    commands.extend([
        ['cargo', 'test', '--locked', '--profile', args.profile, '-p', 'transparent-filter-server', '--bin', 'journal-convert-v1'],
        ['cargo', 'test', '--locked', '--profile', args.profile, '-p', 'transparent-wallet', '--features', 'reqwest', '--lib', 'http::'],
        ['cargo', 'test', '--locked', '--profile', args.profile, '-p', 'pir-apm', '--lib'],
    ])
    if args.profile == 'release':
        # Focused tests have their own release-fast owner; do not duplicate them
        # or start sustained qualification inside this production artifact build.
        commands = commands[:2] + [
            ['cargo', 'build', '--locked', '--profile', args.profile,
             '-p', 'transparent-wallet-store', '--example', 'activity-reopen'],
        ]
    stages = []
    failure = None
    with (args.evidence / 'build.log').open('x') as log:
        for command in commands:
            started = time.monotonic()
            completed = subprocess.run(command, cwd=args.source, env=env, stdout=log, stderr=subprocess.STDOUT, pass_fds=(lane_lock.fileno(),))
            stages.append(dict(command=command, seconds=time.monotonic() - started, exit_code=completed.returncode))
            log.flush()
            if completed.returncode:
                failure = completed.returncode
                break
    binaries = {}
    if failure is None:
        for name in ['transparent-event-ingest', 'shard-publish', 'event-spotcheck',
                     'shard-verify', 'script-sample', 'transparent-publish-controller',
                     'shard-cutoff', 'journal-inventory', 'shard-census', 'transparent-shard-server',
                     'transparent-filter-server', 'transparent-loadtest', 'transparent-measure',
                     'examples/rate-query', 'examples/native_certificate', 'examples/activity-reopen']:
            path = args.target / args.profile / name
            retained = args.evidence / 'artifacts' / name
            retained.parent.mkdir(parents=True, exist_ok=True)
            if retained.exists():
                raise RuntimeError('retained artifact already exists')
            shutil.copy2(path, retained)
            digest = hashlib.file_digest(path.open('rb'), 'sha256').hexdigest()
            if digest != hashlib.file_digest(retained.open('rb'), 'sha256').hexdigest():
                raise RuntimeError('retained artifact checksum mismatch')
            binaries[name] = dict(path=str(path), retained_path=str(retained), sha256=digest)
    compiler = subprocess.check_output(['rustc', '-Vv'], cwd=args.source, env=env, text=True)
    lock_hash = hashlib.file_digest((args.source / 'Cargo.lock').open('rb'), 'sha256').hexdigest()
    result.write_text(json.dumps(dict(**owner, ended_at=time.time(), stages=stages,
        status='passed' if failure is None else 'failed', compiler=compiler,
        cargo_lock_sha256=lock_hash, binaries=binaries), indent=2) + '\n')
    return failure or 0


if __name__ == '__main__':
    raise SystemExit(main())

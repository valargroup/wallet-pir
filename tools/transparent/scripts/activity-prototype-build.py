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


ARTIFACTS = (
    'pir-publisher', 'event-spotcheck',
    'shard-verify', 'script-sample',
    'shard-cutoff', 'journal-inventory', 'shard-census', 'shard-assign', 'shard-control',
    'pir-query-server',
    'transparent-loadtest', 'transparent-measure',
    'examples/rate-query', 'examples/native_certificate', 'examples/activity-reopen',
)


# Retained evidence keeps its historical labels while Cargo uses current names.
# The history server is pir-query-server under its own name: earlier results'
# `transparent-shard-server` was the retired flag-driven executable, which
# their frozen pins still describe.
ARTIFACT_SOURCES = {
    'examples/rate-query': 'rate-query',
}


def build_commands(profile):
    """Build service-owned artifacts and qualify their canonical-library consumers.

    Library-internal primitive, engine and store tests run in wallet-libraries CI
    for the exact dependency revision, where their dev dependencies are available.
    This workspace exercises those libraries through its own recovery/HTTP tests.
    """
    commands = [
        ['cargo', 'build', '--locked', '--profile', profile,
         '-p', 'pir-publisher',
         '-p', 'transparent-tools', '--features',
         'pir-publisher/transparent-history,pir-publisher/transparent-detail,pir-publisher/rocksdb-reader,transparent-tools/benchmarks,transparent-tools/rocksdb-reader', '--bins'],
        # Transparent history and transaction-detail serving: the configured host.
        ['cargo', 'build', '--locked', '--profile', profile,
         '-p', 'pir-query-server', '--features', 'transparent-history,transparent-detail', '--bin', 'pir-query-server'],
        ['cargo', 'build', '--locked', '--profile', profile,
         '-p', 'transparent-tools', '--example', 'activity-reopen', '--example', 'native_certificate'],
    ]
    if profile == 'release':
        # Focused tests have their own release-fast owner; this lane retains
        # the same fat-LTO production artifacts without duplicating qualification.
        return commands
    commands.extend([
        ['cargo', 'test', '--locked', '--profile', profile,
         '-p', 'transparent-tools', '--features', 'benchmarks', '--test', 'compact_recovery'],
        ['cargo', 'test', '--locked', '--profile', profile,
         '-p', 'transparent-query-server', '--features', 'transparent-history,transparent-detail', '--test', 'wallet_anchor',
         '--test', 'wallet_continuation', '--test', 'wallet_recut', '--test', 'wallet_sync'],
        ['cargo', 'test', '--locked', '--profile', profile,
         '-p', 'transparent-tools', '--features', 'benchmarks', '--tests'],
        ['cargo', 'test', '--locked', '--profile', profile,
         '-p', 'transparent-publisher', '--features', 'fixtures', '--lib', 'ingest::extract::'],
        ['cargo', 'test', '--locked', '--profile', profile,
         '-p', 'transparent-shared', '--lib', 'journal::events::'],
        ['cargo', 'test', '--locked', '--profile', profile,
         '-p', 'transparent-publisher', '--lib', 'history::publication::'],
        ['cargo', 'test', '--locked', '--profile', profile,
         '-p', 'transparent-tools', '--features', 'publisher-tools', '--bin', 'event-spotcheck'],
        ['cargo', 'test', '--locked', '--profile', profile,
         '-p', 'transparent-tools', '--bin', 'journal-convert-v1'],
        ['cargo', 'test', '--locked', '--profile', profile, '-p', 'pir-apm', '--lib'],
    ])
    return commands


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
    commands = build_commands(args.profile)
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
        for name in ARTIFACTS:
            path = args.target / args.profile / ARTIFACT_SOURCES.get(name, name)
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

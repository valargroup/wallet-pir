#!/usr/bin/env python3
"""Run exact Cargo-reported test programs with a bounded working set.

Cases stay in one process per program so immutable public fixtures can be reused.
Cargo compilation has already finished; no target directory writer runs here.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from stage import run


def execute(programs, jobs, *, budget=False):
    library_directory = subprocess.check_output(['rustc', '--print', 'target-libdir'], text=True).strip()

    def invoke(program):
        directory = program['directory']
        binary = program['executable']
        env = dict(os.environ, RUST_TEST_THREADS='1', CARGO_MANIFEST_DIR=directory,
                   CARGO_MANIFEST_PATH=str(Path(directory) / 'Cargo.toml'),
                   CARGO_PKG_NAME=program['package'], CARGO_PKG_VERSION=program['version'])
        key = 'DYLD_FALLBACK_LIBRARY_PATH' if sys.platform == 'darwin' else 'LD_LIBRARY_PATH'
        env[key] = os.pathsep.join([str(Path(binary).parent), library_directory, env.get(key, '')])
        if budget:
            from test_budget import execute as timed_execute
            # Keep Unix socket paths below macOS's 104-byte address limit even
            # when the caller's TMPDIR is a long per-user or runner path.
            with tempfile.TemporaryDirectory(prefix='wp-', dir='/tmp') as scratch:
                env.update(TMPDIR=scratch, TMP=scratch, TEMP=scratch)
                count = timed_execute([binary, *program['args']], cwd=directory, env=env,
                                      report=env.get('WALLET_PIR_CASE_LOG'),
                                      enforce=env.get('WALLET_PIR_ENFORCE_BUDGET') != '0')
            if program.get('integration') and not count:
                raise ValueError(f'{binary}: integration executable contains no tests')
        else:
            run([binary, *program['args']], cwd=directory, env=env, stage='tests:' + program['package'])

    # Encrypted HTTP fixtures retain their own concurrency. Keep their working
    # sets apart from other Rust programs before overlapping ordinary units.
    for program in programs:
        if program.get('exclusive'):
            invoke(program)
    with ThreadPoolExecutor(max_workers=jobs) as pool:
        futures = [pool.submit(invoke, program) for program in programs
                   if not program.get('exclusive')]
        try:
            for future in as_completed(futures):
                future.result()
        except BaseException:
            if budget:
                from test_budget import stop
                stop()
            for future in futures:
                future.cancel()
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('plan', type=Path)
    parser.add_argument('--jobs', type=int, choices=(1, 2), required=True)
    parser.add_argument('--budget', action='store_true', help='time every case and enforce the correctness budget')
    args = parser.parse_args()
    if args.budget:
        from test_budget import install_cancellation
        install_cancellation()
    execute(json.loads(args.plan.read_text()), args.jobs, budget=args.budget)


if __name__ == '__main__':
    main()

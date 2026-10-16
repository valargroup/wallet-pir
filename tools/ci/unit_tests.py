#!/usr/bin/env python3
"""Run exact Cargo-reported test programs with a bounded working set.

Cases stay in one process per program so immutable public fixtures can be reused.
Cargo compilation has already finished; no target directory writer runs here.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import sys

from stage import run


def execute(programs, jobs):
    library_directory = subprocess.check_output(['rustc', '--print', 'target-libdir'], text=True).strip()

    def invoke(program):
        directory = program['directory']
        binary = program['executable']
        env = dict(os.environ, RUST_TEST_THREADS='1', CARGO_MANIFEST_DIR=directory,
                   CARGO_MANIFEST_PATH=str(Path(directory) / 'Cargo.toml'),
                   CARGO_PKG_NAME=program['package'], CARGO_PKG_VERSION=program['version'])
        key = 'DYLD_FALLBACK_LIBRARY_PATH' if sys.platform == 'darwin' else 'LD_LIBRARY_PATH'
        env[key] = os.pathsep.join([str(Path(binary).parent), library_directory, env.get(key, '')])
        run([binary, *program['args']], cwd=directory, env=env, stage='tests:' + program['package'])

    with ThreadPoolExecutor(max_workers=jobs) as pool:
        futures = [pool.submit(invoke, program) for program in programs]
        try:
            for future in futures:
                future.result()
        except BaseException:
            for future in futures:
                future.cancel()
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('plan', type=Path)
    parser.add_argument('--jobs', type=int, choices=(1, 2), required=True)
    args = parser.parse_args()
    execute(json.loads(args.plan.read_text()), args.jobs)


if __name__ == '__main__':
    main()

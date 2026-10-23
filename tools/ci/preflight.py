#!/usr/bin/env python3
"""Pre-push checks that self-check and the hub preflight run from .github/preflight.json.

Each check covers what the branch changed since it left origin/main, plus local
edits and untracked files: the hygiene check and affected fast checks that CI
runs on every push, and the transparent clippy lint that CI full runs when the
change selects the transparent family. Qualification and the full suites stay
in CI.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]


def base():
    """The commit where HEAD left origin/main, so commits pushed since are not counted as this branch's changes."""
    found = subprocess.run(['git', 'merge-base', 'origin/main', 'HEAD'], cwd=ROOT, capture_output=True, text=True)
    if found.returncode:
        sys.exit('preflight: no merge base with origin/main; run `git fetch origin main` and retry')
    return found.stdout.strip()


def run(argv, env=None):
    print('preflight: ' + ' '.join(argv), flush=True)
    return subprocess.call(argv, cwd=ROOT, env=env)


def transparent_lint(start):
    selected = subprocess.run(['python3', 'tools/ci/fast.py', '--base', start, '--select-only'],
                              cwd=ROOT, capture_output=True, text=True)
    if selected.returncode:
        sys.stderr.write(selected.stdout + selected.stderr)
        return selected.returncode
    if 'transparent' not in json.loads(selected.stdout)['groups']:
        print('preflight: transparent lint skipped; the change does not select the transparent family', flush=True)
        return 0
    env = os.environ.copy()
    if platform.system() == 'Linux' and platform.machine() == 'x86_64':
        # Same as the CI job: librocksdb-sys needs -mpclmul for its CRC32C code on x86_64 hosts.
        env.update(CFLAGS='-mpclmul', CXXFLAGS='-mpclmul')
    return run(['python3', 'tools/ci/full_packages.py', '--group', 'transparent', '--lint'], env=env)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('check', choices=['hygiene', 'fast', 'transparent-lint'])
    check = parser.parse_args().check
    start = base()
    if check == 'hygiene':
        return run(['python3', 'tools/ci/hygiene.py', '--base', start])
    if check == 'fast':
        return run(['python3', 'tools/ci/fast.py', '--base', start])
    return transparent_lint(start)


if __name__ == '__main__':
    sys.exit(main())

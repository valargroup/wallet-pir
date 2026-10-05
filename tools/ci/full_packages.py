#!/usr/bin/env python3
"""Fail closed when workspace packages are unclassified; run each full family.

--compile-only builds the same test units without running them (hosted cache
priming); it is never a substitute for the full suite.
"""
import argparse
import json
from pathlib import Path
import subprocess
from fast import ROOT, workspace_packages
from stage import run


def inventory(packages=None, groups=None):
    packages = workspace_packages() if packages is None else packages
    groups = json.loads((ROOT / 'tools/ci/full-packages.json').read_text()) if groups is None else groups
    if set(groups) != {'shared', 'enhance', 'transparent'}:
        raise ValueError('full package groups must be shared, enhance and transparent')
    names = [name for group in groups.values() for name in group]
    if len(names) != len(set(names)):
        raise ValueError('full coverage contains duplicate packages')
    expected = {p['name'] for p in packages}
    if expected != set(names):
        raise ValueError(f'full coverage mismatch: unclassified={sorted(expected-set(names))}, stale={sorted(set(names)-expected)}')
    if 'enhance-pir-server' not in groups['enhance']:
        raise ValueError('Enhance server must retain classified integration coverage')
    return groups


def commands(group, groups, lint=False, compile_only=False):
    names = groups[group]
    if lint:
        return [['cargo', 'clippy', '--locked', '--all-targets', '--all-features',
                 *[arg for name in names for arg in ('-p', name)], '--', '-D', 'warnings']]
    # This server's full registry includes lib, bins and every integration target.
    ordinary = [name for name in names if name != 'enhance-pir-server']
    runner = ['cargo', 'test', '--no-run'] if compile_only else ['bash', 'tools/ci/full-test.sh']
    result = [[*runner, '--locked', '--profile', 'release-fast',
               *[arg for name in ordinary for arg in ('-p', name)],
               *(['--features', 'enhance-pir/cli'] if group == 'enhance' else
                 ['--features', 'transparent-filter/cli'] if group == 'transparent' else [])]]
    if 'enhance-pir-server' in names:
        result.append(['python3', 'tools/ci/enhance_tests.py', '--tier', 'full',
                       *(['--compile-only'] if compile_only else [])])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--group', choices=['shared', 'enhance', 'transparent'])
    parser.add_argument('--lint', action='store_true')
    parser.add_argument('--compile-only', action='store_true')
    args = parser.parse_args()
    groups = inventory()
    print('Full workspace package classification is complete', flush=True)
    if args.group:
        for command in commands(args.group, groups, args.lint, args.compile_only):
            if command[0] == 'cargo':
                run(command, stage=f'{"lint" if args.lint else "compile"}:{args.group}')
            else:
                print('+ ' + ' '.join(command), flush=True)
                subprocess.run(command, cwd=ROOT, check=True)


if __name__ == '__main__':
    main()

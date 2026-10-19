#!/usr/bin/env python3
"""Fail closed when workspace packages are unclassified; run each full family.

--compile-only builds the same test units without running them (hosted cache
priming); it is never a substitute for the full suite.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tomllib
from fast import ROOT, workspace_packages
from stage import run


def inventory(packages=None, groups=None):
    packages = workspace_packages() if packages is None else packages
    groups = json.loads((ROOT / 'tools/ci/full-packages.json').read_text()) if groups is None else groups
    if set(groups) != {'shared', 'enhance', 'transparent', 'receiver'}:
        raise ValueError('full package groups must be shared, enhance, transparent and receiver')
    names = [name for group in groups.values() for name in group]
    if len(names) != len(set(names)):
        raise ValueError('full coverage contains duplicate packages')
    expected = {p['name'] for p in packages}
    if expected != set(names):
        raise ValueError(f'full coverage mismatch: unclassified={sorted(expected-set(names))}, stale={sorted(set(names)-expected)}')
    if 'enhance-publisher' not in groups['enhance']:
        raise ValueError('Enhance publisher must retain classified integration coverage')
    return groups


def qualification_features(names):
    """Enable declared test campaigns only in explicit full qualification."""
    selected = set(names)
    return sorted(package['name'] + '/qualification-tests'
                  for package in workspace_packages() if package['name'] in selected
                  and 'qualification-tests' in tomllib.loads(
                      Path(package['manifest_path']).read_text()).get('features', {}))


def commands(group, groups, lint=False, compile_only=False):
    names = groups[group]
    if lint:
        return [['cargo', 'clippy', '--locked', '--all-targets', '--all-features',
                 *[arg for name in names for arg in ('-p', name)], '--', '-D', 'warnings']]
    runner = ['cargo', 'test', '--no-run'] if compile_only else ['bash', 'tools/ci/full-test.sh']
    features = {
        'enhance': ['enhance-query-server/native-reinspiring', 'status-query-server/native-reinspiring',
                    'enhance-publisher/native-reinspiring', 'status-publisher/native-reinspiring', 'status-publisher/cli',
                    'enhance-pir-client/native-reinspiring', 'enhance-pir-load-test/native-reinspiring',
                    'native-certificate/native-reinspiring',
                    'pir-publisher/enhance', 'pir-publisher/status', 'pir-publisher/transparent',
                    'pir-publisher/receiver', 'pir-publisher/native-reinspiring',
                    'pir-query-server/enhance', 'pir-query-server/status', 'pir-query-server/transparent',
                    'pir-query-server/native-reinspiring'],
        'shared': ['pir-control/admission'],
        'transparent': ['transparent-publisher/rocksdb-reader', 'transparent-tools/test-support', 'transparent-tools/compact-export'],
    }.get(group, []) + qualification_features(names)
    result = [[*runner, '--locked', '--profile', 'release-fast',
               *[arg for name in names for arg in ('-p', name)],
               *(['--features', ','.join(features)] if features else [])]]
    if group == 'transparent':
        # This analysis tool has its own workspace. Its journal-reader identity
        # and compiled-codec contracts run first, so another service test's
        # failure cannot hide whether those contracts passed on Linux.
        result.insert(0, [*runner, '--locked', '--profile', 'release-fast',
                          '--manifest-path', 'tools/txid/txid-sizing/export/Cargo.toml'])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--group', choices=['shared', 'enhance', 'transparent', 'receiver'])
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
                # Qualification workload knobs are for explicit campaigns, not
                # the bounded correctness suite or its compile-only twin.
                env = {key: value for key, value in os.environ.items()
                       if not key.startswith('QUALIFY_')}
                subprocess.run(command, cwd=ROOT, env=env, check=True)


if __name__ == '__main__':
    main()

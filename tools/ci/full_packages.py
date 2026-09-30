#!/usr/bin/env python3
"""Fail closed when workspace packages are unclassified; run each full family."""
import argparse
import json
from pathlib import Path
import subprocess
from fast import ROOT, workspace_packages


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


def commands(group, groups, lint=False):
    names = groups[group]
    if lint:
        return [['cargo', 'clippy', '--locked', '--all-targets', '--all-features',
                 *[arg for name in names for arg in ('-p', name)], '--', '-D', 'warnings']]
    # This server's full registry includes lib, bins and every integration target.
    ordinary = [name for name in names if name != 'enhance-pir-server']
    result = [['bash', 'tools/ci/full-test.sh', '--locked', '--profile', 'release-fast',
               *[arg for name in ordinary for arg in ('-p', name)]]]
    if 'enhance-pir-server' in names:
        result.append(['python3', 'tools/ci/enhance_tests.py', '--tier', 'full'])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--group', choices=['shared', 'enhance', 'transparent'])
    parser.add_argument('--lint', action='store_true')
    args = parser.parse_args()
    groups = inventory()
    print('Full workspace package classification is complete', flush=True)
    if args.group:
        for command in commands(args.group, groups, args.lint):
            print('+ ' + ' '.join(command), flush=True)
            subprocess.run(command, cwd=ROOT, check=True)


if __name__ == '__main__':
    main()

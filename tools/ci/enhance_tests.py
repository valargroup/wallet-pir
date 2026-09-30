#!/usr/bin/env python3
"""Validate and run classified Enhance server integration tests.

Full includes fast targets and library/binary tests. Expensive ignored fixtures
remain opt-in hardware checks; passing this suite does not qualify a deployment.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tomllib

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = 'enhance-pir-server'


def validate(metadata, registry):
    if set(registry) != {'fast', 'full'}:
        raise ValueError('Enhance test registry must contain exactly fast and full tiers')
    assigned = []
    for tier, names in registry.items():
        if not isinstance(names, list) or not all(isinstance(name, str) for name in names):
            raise ValueError(f'{tier} must be a list of target names')
        assigned.extend(names)
    if len(assigned) != len(set(assigned)):
        raise ValueError('Enhance integration target assigned more than once')
    package = next(p for p in metadata['packages'] if p['name'] == PACKAGE)
    targets = {t['name'] for t in package['targets'] if 'test' in t['kind']}
    missing, stale = targets - set(assigned), set(assigned) - targets
    if missing or stale:
        raise ValueError(f'Enhance integration classification mismatch: unclassified={sorted(missing)}, stale={sorted(stale)}')


def cargo_args(registry, tier):
    names = registry['fast'] if tier == 'fast' else registry['fast'] + registry['full']
    args = ['--locked', '--profile', 'release-fast', '-p', PACKAGE]
    if tier == 'full':
        args += ['--lib', '--bins']
    return args + [arg for name in sorted(names) for arg in ('--test', name)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tier', choices=['fast', 'full'])
    parser.add_argument('--offline', action='store_true')
    args = parser.parse_args()
    if args.tier:
        metadata = json.loads(subprocess.check_output(
            ['cargo', 'metadata', '--locked', '--no-deps', '--format-version', '1', *(['--offline'] if args.offline else [])], cwd=ROOT))
    else:
        package_root = ROOT / 'enhance/services/enhance-pir-server'
        manifest = tomllib.loads((package_root / 'Cargo.toml').read_text())
        names = {t['name'] for t in manifest.get('test', [])}
        if manifest['package'].get('autotests', True):
            names.update(p.stem for p in (package_root / 'tests').glob('*.rs'))
        metadata = {'packages': [{'name': PACKAGE, 'targets': [
            {'name': name, 'kind': ['test']} for name in names]}]}
    registry = json.loads((ROOT / 'tools/ci/enhance-tests.json').read_text())
    validate(metadata, registry)
    print('Enhance integration target classification is complete', flush=True)
    if args.tier:
        env = dict(os.environ, RUST_TEST_THREADS='1')
        # Qualification overrides belong to explicit manual runs, not CI defaults.
        env = {key: value for key, value in env.items() if not key.startswith('QUALIFY_')}
        command = ['bash', str(ROOT / 'tools/ci/full-test.sh'), *cargo_args(registry, args.tier), *(['--offline'] if args.offline else [])]
        print('+ ' + ' '.join(command), flush=True)
        subprocess.run(command, cwd=ROOT, env=env, check=True)
        if args.tier == 'full':
            # Explicit lib/bin/integration selectors do not run library doctests.
            subprocess.run(['python3', 'tools/ci/stage.py', 'doctests', '--',
                            'cargo', 'test', '--locked', '--profile', 'release-fast',
                            '-p', PACKAGE, '--doc', *(['--offline'] if args.offline else [])],
                           cwd=ROOT, env=env, check=True)


if __name__ == '__main__':
    main()

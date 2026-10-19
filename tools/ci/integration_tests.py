#!/usr/bin/env python3
"""Classify every workspace integration target and execute the requested tier.

The registry selects whole executables, so a fast run does not compile the full
campaign harnesses. Full package checks continue to run every target.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tomllib
import tempfile

from fast import ROOT, workspace_packages
from full_packages import qualification_features
from stage import run, inherited_fds


def targets(packages=None):
    result = {}
    for package in workspace_packages() if packages is None else packages:
        manifest = Path(package['manifest_path'])
        data = tomllib.loads(manifest.read_text())
        names = {item['name'] for item in data.get('test', [])}
        if data['package'].get('autotests', True):
            names.update(p.stem for p in (manifest.parent / 'tests').glob('*.rs'))
            names.update(p.parent.name for p in (manifest.parent / 'tests').glob('*/main.rs'))
        if names:
            result[package['name']] = names
    return result


def validate(discovered, registry):
    if set(discovered) != set(registry):
        raise ValueError(f'integration packages mismatch: unclassified={sorted(set(discovered)-set(registry))}, '
                         f'stale={sorted(set(registry)-set(discovered))}')
    for package, names in discovered.items():
        tiers = registry[package]
        if set(tiers) != {'fast', 'full'}:
            raise ValueError(f'{package}: expected fast and full tiers')
        assigned = []
        for tier, entries in tiers.items():
            if not isinstance(entries, list) or not all(isinstance(n, str) for n in entries):
                raise ValueError(f'{package}/{tier}: expected target names')
            assigned.extend(entries)
        if len(assigned) != len(set(assigned)):
            raise ValueError(f'{package}: duplicate integration target')
        if names != set(assigned):
            raise ValueError(f'{package}: integration classification mismatch: '
                             f'unclassified={sorted(names-set(assigned))}, stale={sorted(set(assigned)-names)}')


def inventory():
    registry = json.loads((ROOT / 'tools/ci/integration-tests.json').read_text())
    validate(targets(), registry)
    return registry


def cargo_args(package, tiers, tier):
    names = tiers['fast'] if tier == 'fast' else tiers['fast'] + tiers['full']
    args = ['--locked', '--profile', 'release-fast', '-p', package]
    if tier == 'full':
        args += ['--lib', '--bins']
    return args + [arg for name in sorted(names) for arg in ('--test', name)]


def target_features(package, names):
    manifest = next(p['manifest_path'] for p in workspace_packages() if p['name'] == package)
    data = tomllib.loads(Path(manifest).read_text())
    features = {feature for target in data.get('test', []) if target['name'] in names
                for feature in target.get('required-features', [])}
    return ['--features', ','.join(sorted(features))] if features else []


def execute(packages, tier, *, offline=False, compile_only=False, smoke_only=False, repeat=1):
    registry = inventory()
    packages = set(packages)
    env = {k: v for k, v in dict(os.environ, RUST_TEST_THREADS='1').items() if not k.startswith('QUALIFY_')}
    prepared = []
    for package in sorted(packages):
        if package not in registry or (tier == 'fast' and not registry[package]['fast']):
            continue
        tiers = registry[package]
        if smoke_only:
            if 'ci_smoke' not in tiers['fast']:
                continue
            tiers = {'fast': ['ci_smoke'], 'full': []}
        features = target_features(package, tiers['fast'] if tier == 'fast' else tiers['fast'] + tiers['full'])
        if tier == 'full' and (campaigns := qualification_features([package])):
            features += ['--features', ','.join(campaigns)]
        args = cargo_args(package, tiers, tier) + features + (['--offline'] if offline else [])
        if compile_only:
            run(['cargo', 'test', '--no-run', *args], env=env, stage=f'compile:integration-{package}')
        elif tier == 'fast':
            run(['cargo', 'test', '--no-run', *args], env=env, stage=f'compile:integration-{package}')
            for target in tiers['fast']:
                single = cargo_args(package, {'fast': [target], 'full': []}, 'fast') + features + (['--offline'] if offline else [])
                enabled = subprocess.check_output(['cargo', 'test', *single, '--', '--list'],
                                                  cwd=ROOT, env=env, text=True, **inherited_fds(env))
                ignored = subprocess.check_output(['cargo', 'test', *single, '--', '--ignored', '--list'],
                                                  cwd=ROOT, env=env, text=True, **inherited_fds(env))
                names = lambda output: {line[:-6] for line in output.splitlines() if line.endswith(': test')}
                if not names(enabled) - names(ignored):
                    raise ValueError(f'{package}/{target}: no enabled, unignored fast tests')
            prepared.append((package, args))
        else:
            run(['bash', 'tools/ci/full-test.sh', *args], env=env, stage=f'integration:{package}')
            run(['cargo', 'test', '--locked', '--profile', 'release-fast', '-p', package,
                 '--doc', *(['--offline'] if offline else [])], env=env, stage=f'doctests:{package}')
    if prepared:
        # One complete compile boundary and one reclaim, rather than a whole
        # build-cache scan and duplicate no-run build for every small program.
        run(['python3', 'tools/ci/reclaim-build-cache.py'], env=env, stage='reclaim')
        with tempfile.TemporaryDirectory(prefix='wallet-pir-test-runtime-') as cache:
            env = dict(env, WALLET_PIR_TEST_RUNTIME_CACHE=cache)
            for package, args in prepared:
                run(['cargo', 'test', *args], env=env, stage=f'integration:{package}')
                for attempt in range(1, repeat):
                    run(['cargo', 'test', *args], env=env, stage=f'tests:{package}-recovery-{attempt + 1}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--package', action='append')
    parser.add_argument('--tier', choices=['fast', 'full'])
    parser.add_argument('--offline', action='store_true')
    parser.add_argument('--compile-only', action='store_true')
    parser.add_argument('--smoke-only', action='store_true', help='select only the classified ci_smoke executables')
    parser.add_argument('--repeat', type=int, default=1, help='independent smoke executions; maximum 50')
    args = parser.parse_args()
    if not 1 <= args.repeat <= 50 or (args.repeat != 1 and not args.smoke_only):
        parser.error('--repeat must be 1..50 and repeated runs require --smoke-only')
    if args.smoke_only and args.tier != 'fast':
        parser.error('--smoke-only requires --tier fast')
    registry = inventory()
    if args.package and set(args.package) - set(registry):
        parser.error(f'no classified integration targets: {sorted(set(args.package)-set(registry))}')
    print('Workspace integration target classification is complete', flush=True)
    if args.tier:
        execute(args.package or registry, args.tier, offline=args.offline, compile_only=args.compile_only,
                smoke_only=args.smoke_only, repeat=args.repeat)


if __name__ == '__main__':
    main()

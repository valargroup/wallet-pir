#!/usr/bin/env python3
"""One affected-check planner for local iteration and CI. Selection needs no Rust.

Unknown paths broaden coverage. Full CI on main always qualifies the whole
workspace; this feedback gate never qualifies artifacts.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/ci"))
from stage import run, inherited_fds  # noqa: E402
from target_lease import local_target  # noqa: E402
FULL_GROUPS = {'enhance', 'transparent', 'receiver', 'shared', 'ops', 'enhance_infra', 'transparent_infra'}
OPS = {'check-ops-enhance', 'check-ops-shared', 'check-ops-control-sessions',
       'check-ops-deploy', 'check-ops-contracts', 'check-ops-parents', 'check-ops-fleet',
       'check-ops-publication', 'check-ops-burst', 'check-ops-regression-recut',
       'check-ops-regression-fixtures', 'check-ops-observation', 'check-ops-stage-timing',
       'check-ops-membership', 'check-ops-elastic', 'check-ops-scaler-fast', 'check-ops-txid-display'}
HELPERS = OPS | {'check-docs', 'check-tools', 'check-reports'}


def workspace_packages(root=ROOT):
    """Read the same explicit manifests Cargo uses, including renamed dependencies."""
    workspace = tomllib.loads((root / 'Cargo.toml').read_text())['workspace']
    packages = []
    for member in workspace['members']:
        manifests = sorted(root.glob(member + '/Cargo.toml'))
        if not manifests:
            raise ValueError(f'missing workspace manifest: {member}')
        for manifest in manifests:
            data = tomllib.loads(manifest.read_text())
            dependencies = []
            tables = [data, *data.get('target', {}).values()]
            for table in tables:
                for kind in ('dependencies', 'dev-dependencies', 'build-dependencies'):
                    for name, value in table.get(kind, {}).items():
                        if isinstance(value, dict) and value.get('workspace'):
                            value = workspace['dependencies'][name]
                        dependencies.append({'name': value.get('package', name) if isinstance(value, dict) else name})
            packages.append({'name': data['package']['name'], 'manifest_path': str(manifest),
                             'dependencies': dependencies})
    return packages


def owner(packages, path):
    return next((p['name'] for p in packages
                 if path.startswith(Path(p['manifest_path']).parent.relative_to(ROOT).as_posix() + '/')), None)


def route(path):
    """Return helper targets/full groups, or None for an unknown path."""
    if path in {'tools/check-doc-links.sh', 'tools/check-doc-links.py'}:
        return {'check-docs', 'check-tools'}, {'ops'}
    if path in {'receiver/ops/digitalocean/probe-fixture.json', 'receiver/ops/digitalocean/README.md'}:
        # tools/tests pins the probe fixture to the README's monitor config.
        return {'check-docs', 'check-tools'}, {'ops'}
    if path.endswith('.md') or path.startswith(('docs/', 'evidence/', 'enhance/evidence/', 'transparent/evidence/', 'receiver/evidence/')):
        return {'check-docs'}, {'ops'}
    if path.startswith('receiver/ops/'):
        # Receiver deployment files have no helper suite of their own.
        return {'check-docs'}, {'ops'}
    if path.startswith('.github/workflows/deploy-') or path.startswith('.github/workflows/configure-'):
        return {'check-tools', 'check-ops-contracts', 'check-ops-deploy'}, {'ops'}
    if path.startswith('ops/infra/digitalocean/enhance-v4/'):
        return {'check-ops-deploy'}, {'enhance_infra', 'ops'}
    if path.startswith('ops/infra/digitalocean/transparent-elastic/'):
        return {'check-ops-elastic'}, {'transparent_infra', 'ops'}
    if path.startswith('ops/lib/'):
        return OPS, {'ops'}
    if path.startswith('ops/infra/digitalocean/production/'):
        # The txid display deploy pins this root's firewall sources.
        return {'check-ops-shared', 'check-ops-contracts', 'check-ops-deploy', 'check-ops-txid-display'}, {'ops'}
    if path.startswith('ops/tests/control_sessions/') or 'control-session' in path:
        return {'check-ops-control-sessions'}, {'ops'}
    if path.startswith(('ops/tests/deploy/', 'ops/scripts/wallet-pir-deploy', 'ops/scripts/deploy-')):
        return {'check-ops-deploy', 'check-ops-contracts'}, {'ops'}
    if path.startswith('ops/'):
        return {'check-ops-shared', 'check-ops-contracts', 'check-ops-deploy'}, {'ops'}
    if path.startswith('enhance/ops/'):
        return {'check-ops-enhance', 'check-ops-deploy', 'check-ops-contracts'}, {'ops'}
    if path.startswith('transparent/ops/'):
        if 'txid_display' in path or 'txid-display' in path:
            targets = {'check-ops-txid-display'}
        elif 'scaler' in path or 'membership_model' in path:
            targets = {'check-ops-scaler-fast'}
        elif 'elastic' in path or 'test_transparent_plan' in path:
            targets = {'check-ops-elastic'}
        elif 'burst' in path:
            targets = {'check-ops-burst'}
        elif re.search(r'(?:^|[-_/])parents?(?:[-_/.]|$)', path):
            targets = {'check-ops-parents'}
        elif 'regression' in path:
            targets = {'check-ops-regression-recut', 'check-ops-regression-fixtures'}
        elif 'observation' in path:
            targets = {'check-ops-observation'}
        elif 'stage_timing' in path:
            targets = {'check-ops-stage-timing'}
        else:
            targets = {t for t in OPS if t not in {'check-ops-enhance', 'check-ops-control-sessions', 'check-ops-shared'}}
        return targets | {'check-ops-contracts'}, {'ops'}
    if path.startswith('transparent/tools/transparent-loadtest/'):
        return {'check-reports'}, {'transparent', 'ops'}
    if path.startswith(('tools/', 'transparent/tools/filters/', 'transparent/tools/parent-filters/')):
        return {'check-tools'}, {'ops'}
    return None


def select(packages, paths):
    names = {p['name'] for p in packages}
    selected = set()
    for path in paths:
        # Package-owned embedded fixtures (including Markdown) can affect Rust.
        package = owner(packages, path)
        if package:
            selected.add(package)
        elif (path in ('Cargo.toml', 'Cargo.lock', 'Makefile')
              or path.startswith(('.cargo/', '.github/actions/', 'tools/ci/'))
              or path.endswith('/Cargo.toml')):
            return names
        elif route(path) is None:
            return names
    while True:
        expanded = selected | {p['name'] for p in packages if any(d['name'] in selected for d in p['dependencies'])}
        if expanded == selected:
            return selected
        selected = expanded


def plan(packages, paths, *, all_checks=False):
    if all_checks:
        return {'packages': sorted(p['name'] for p in packages), 'helpers': sorted(HELPERS),
                'groups': sorted(FULL_GROUPS), 'reasons': ['complete workspace requested']}
    selected = select(packages, paths)
    helpers, groups, reasons = set(), set(), []
    for path in paths:
        package = owner(packages, path)
        routed = route(path)
        if package:
            if path.endswith('.md'):
                helpers.add('check-docs'); groups.add('ops')
            if path.startswith('transparent/tools/transparent-loadtest/'):
                helpers.add('check-reports')
            # Serialized payloads are consumed by jq/deploy scripts.
            if path.endswith('operator_payloads.rs'):
                helpers.add('check-ops-contracts'); groups.add('ops')
            reasons.append(f'{path}: package {package} and reverse dependencies')
        elif routed and not (path in ('Cargo.toml', 'Cargo.lock', 'Makefile') or path.startswith(('tools/ci/', '.github/actions/'))):
            targets, full = routed
            helpers.update(targets); groups.update(full)
            reasons.append(f'{path}: {", ".join(sorted(targets))}')
        else:
            helpers.update(HELPERS); groups.update(FULL_GROUPS)
            reasons.append(f'{path}: unknown/build configuration; complete coverage')
    coverage = json.loads((ROOT / 'tools/ci/full-packages.json').read_text())
    for package in selected:
        family = next((group for group, names in coverage.items() if package in names), None)
        if family is None:
            groups.update(FULL_GROUPS)
        elif family in ('transparent', 'receiver'):
            groups.add(family)
        elif family == 'shared':
            groups.update({'shared', 'enhance', 'transparent', 'receiver'})
        else:
            groups.add('enhance')
    return {'packages': sorted(selected), 'helpers': sorted(helpers), 'groups': sorted(groups), 'reasons': reasons}


def changed_paths(base='', head='HEAD', *, local=True, root=ROOT):
    def git(*args):
        return subprocess.check_output(['git', *args], cwd=root).decode().split('\0')
    paths = set()
    if base and set(base) != {'0'}:
        paths.update(git('diff', '--name-only', '--no-renames', '-z', base, head))
    if local:
        paths.update(git('diff', '--name-only', '--no-renames', '-z', head))
        paths.update(git('ls-files', '--others', '--exclude-standard', '-z'))
    return sorted(paths - {''})



def rust_checks(selected, *, features='', test='', test_target='', offline=False):
    if not selected:
        return
    metadata = json.loads(subprocess.check_output(['cargo', 'metadata', '--locked', '--no-deps', '--format-version', '1', *(['--offline'] if offline else [])], cwd=ROOT, **inherited_fds()))
    run(['cargo', 'fmt', '--all', '--check'], stage='format')
    options = ['--locked', '--profile', 'release-fast']
    if offline:
        options += ['--offline']
    registry = json.loads((ROOT / 'tools/ci/slow-tests.json').read_text())
    for package in metadata['packages']:
        name = package['name']
        if name not in selected:
            continue
        targets = [t for t in package['targets'] if t.get('test', True) and any(k in ('lib', 'bin') for k in t['kind'])]
        if test_target and not any(t['name'] == test_target and 'test' in t['kind'] for t in package['targets']):
            raise ValueError(f'{name}: unknown integration target: {test_target}')
        kinds = [flag for kind, flag in [('lib', '--lib'), ('bin', '--bins')] if any(kind in t['kind'] for t in targets)]
        if test_target:
            kinds = ['--test', test_target]
        if not kinds:
            continue
        # CLI is a cheap client feature, not a protocol/hardware mode. Cargo
        # otherwise silently skips these required-feature binary test targets.
        required = {f for t in targets for f in t.get('required-features', [])}
        slow = {} if test_target else registry.get(name, {})
        classified_features = {f for entry in slow.values() if isinstance(entry, dict)
                               for f in entry.get('features', [])}
        enabled = features or ','.join(sorted((required & {'cli'}) | classified_features))
        feature_args = ['--features', enabled] if enabled else []
        command = ['cargo', 'test', *options, *feature_args, '-p', name, *kinds]
        run([*command, '--no-run'], stage=f'compile:{name}')
        listing = subprocess.check_output([*command, '--', '--list'], cwd=ROOT, text=True, **inherited_fds())
        discovered = {line.removesuffix(': test') for line in listing.splitlines() if line.endswith(': test')}
        if test_target and not discovered:
            raise ValueError(f'{name}: {test_target} has no enabled tests; select its required FEATURES')
        slow = {} if test_target else registry.get(name, {})
        stale = set(slow) - discovered if not features else set()
        if stale:
            raise ValueError(f'{name}: stale slow-test classification: {sorted(stale)}')
        if test and not any(test in name for name in discovered):
            raise ValueError(f'{name}: test filter matched no tests: {test}')
        args = ['--', *([test] if test else []), *sum((['--skip', n] for n in slow if not test), [])]
        run([*command, *args], env=dict(os.environ, RUST_TEST_THREADS='1'), stage=f'tests:{name}')
    if 'enhance-pir-server' in selected and not test and not test_target and not features:
        command = ['python3', 'tools/ci/enhance_tests.py', '--tier', 'fast']
        if offline:
            command.append('--offline')
        run(command, stage='integration:enhance-fast')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', default='')
    parser.add_argument('--head', default='HEAD')
    parser.add_argument('--committed-only', action='store_true')
    parser.add_argument('--all', action='store_true')
    parser.add_argument('--package', action='append')
    parser.add_argument('--features', default='')
    parser.add_argument('--test', default='')
    parser.add_argument('--test-target', default='')
    parser.add_argument('--offline', action='store_true')
    parser.add_argument('--select-only', action='store_true')
    parser.add_argument('--github-output', action='store_true')
    args = parser.parse_args()
    packages = workspace_packages()
    paths = changed_paths(args.base, args.head, local=not args.committed_only)
    selected = plan(packages, paths, all_checks=args.all or (args.committed_only and (not args.base or set(args.base) == {'0'})))
    if args.package:
        unknown = set(args.package) - {p['name'] for p in packages}
        if unknown:
            parser.error(f'unknown workspace packages: {sorted(unknown)}')
        selected = {'packages': sorted(set(args.package)), 'helpers': [], 'groups': [], 'reasons': ['explicit package check']}
    print(json.dumps(selected, indent=2), flush=True)
    if args.github_output:
        with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
            out.write(f'rust={str(bool(selected["packages"])).lower()}\n')
            out.write('helpers=' + json.dumps(selected['helpers']) + '\n')
            for group in FULL_GROUPS:
                out.write(f'{group}={str(group in selected["groups"]).lower()}\n')
    if args.select_only:
        return
    with local_target(ROOT, enabled=bool(selected['packages'])), ThreadPoolExecutor(max_workers=4) as pool:
        futures = []
        if selected['helpers']:
            futures.append(pool.submit(run, ['make', '-j4', *selected['helpers']], stage='helpers'))
        futures.append(pool.submit(rust_checks, selected['packages'], features=args.features, test=args.test, test_target=args.test_target, offline=args.offline))
        errors = []
        for future in futures:
            try:
                future.result()
            except Exception as error:
                errors.append(error)
        if errors:
            raise errors[0]


if __name__ == '__main__':
    main()

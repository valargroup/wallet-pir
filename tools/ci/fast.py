#!/usr/bin/env python3
"""One affected-check planner for local iteration and CI. Selection needs no Rust.

Unknown paths broaden coverage. Service qualification retains full affected
family suites; this feedback gate never qualifies artifacts.
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
import tempfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/ci"))
from stage import run, inherited_fds  # noqa: E402
from target_lease import local_target  # noqa: E402
FULL_GROUPS = {'enhance', 'transparent', 'receiver', 'shared', 'ops', 'enhance_infra', 'transparent_infra'}
OPS = {'check-ops-enhance', 'check-ops-shared', 'check-ops-control-sessions',
       'check-ops-contracts', 'check-ops-parents', 'check-ops-fleet',
       'check-ops-publication', 'check-ops-burst', 'check-ops-regression-recut',
       'check-ops-regression-fixtures', 'check-ops-observation', 'check-ops-stage-timing',
       'check-ops-membership', 'check-ops-elastic', 'check-ops-scaler-fast', 'check-ops-txid-display'}
HELPERS = OPS | {'check-docs', 'check-tools', 'check-reports'}


def feedback_tooling(path):
    if path.startswith('tools/tests/operations/'):
        return False
    return path in {'.gitignore', '.github/labeler.yml', '.github/labels.json',
                    '.github/workflows/pr-labels.yml'} or path.startswith(('tools/ci/', 'tools/tests/',
        '.github/workflows/ci', '.github/actions/rust-setup/', '.github/actions/qualify-artifacts/'))


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


# The Transparent tools package shares tools/transparent with operations scripts;
# only these inputs belong to the Cargo package.
SHARED_PACKAGE_INPUTS = {
    'tools/transparent/': ('Cargo.toml', 'build.rs', 'src/', 'examples/', 'loadtest/',
                           'fixtures/regression/', 'tests/fixtures/', 'tests/*.rs'),
}


def package_input(directory, path):
    inputs = SHARED_PACKAGE_INPUTS.get(directory)
    if inputs is None:
        return True
    relative = path[len(directory):]
    return any(relative.startswith('tests/') and '/' not in relative[6:] and relative.endswith('.rs')
               if prefix == 'tests/*.rs' else relative.startswith(prefix) for prefix in inputs)


def owner(packages, path):
    for p in packages:
        directory = Path(p['manifest_path']).parent.relative_to(ROOT).as_posix() + '/'
        if path.startswith(directory) and package_input(directory, path):
            return p['name']
    return None


def route(path):
    """Return helper targets/full groups, or None for an unknown path."""
    # A comparison crossing the move can contain deletions at the previous paths.
    if path.startswith('ops/'):
        path = ('tools/tests/operations/' + path[len('ops/tests/'):]
                if path.startswith('ops/tests/') else 'tools/' + path[len('ops/'):])
    if path in {'tools/check-doc-links.sh', 'tools/check-doc-links.py'}:
        return {'check-docs', 'check-tools'}, {'ops'}
    if path == 'tools/enhance/deploy/deploy-inventory.example.json':
        # Shared operations tests pin the receiver's exact check.
        return {'check-ops-enhance', 'check-ops-contracts', 'check-ops-shared'}, {'ops'}
    if path.startswith('tools/receiver/'):
        # Shared operations tests pin the receiver's edge, unit, cloud-init and runbook monitor
        # config and tests its helpers.
        return {'check-docs', 'check-ops-shared'}, {'ops'}
    if path.endswith('.md') or path.startswith(('docs/', 'evidence/', 'evidence/enhance/', 'evidence/transparent/', 'evidence/receiver/')):
        return {'check-docs'}, {'ops'}
    # CI workflows stay on the feedback-tooling path. Publish, deploy, configure
    # and the other operational workflows do not change the Rust graph; selecting
    # them as unknown inputs enables extra reader features and rebuilds the warm
    # fast closure.
    if path.startswith('.github/workflows/') and not path.startswith('.github/workflows/ci'):
        return {'check-tools', 'check-ops-contracts'}, {'ops'}
    if path.startswith(('tools/infra/digitalocean/enhance/worker-pool/', 'tools/infra/digitalocean/enhance-v4/')):
        # Pool plans qualify as Enhance infrastructure. The in-memory deploy suite is local only.
        return set(), {'enhance_infra', 'ops'}
    if path.startswith(('tools/infra/digitalocean/transparent/worker-pool/', 'tools/infra/digitalocean/transparent-elastic/')):
        return {'check-ops-elastic'}, {'transparent_infra', 'ops'}
    if path.startswith('tools/lib/'):
        return OPS, {'ops'}
    if path.startswith(('tools/infra/digitalocean/fleet/', 'tools/infra/digitalocean/production/',
                        'tools/infra/digitalocean/enhance/', 'tools/infra/digitalocean/status/',
                        'tools/infra/digitalocean/transparent/', 'tools/infra/digitalocean/transparent-tx-detail/')):
        # The txid display deploy pins this root's firewall sources.
        return {'check-ops-shared', 'check-ops-control-sessions', 'check-ops-contracts', 'check-ops-txid-display'}, {'enhance_infra', 'ops'}
    if path in {'tools/status/status_qualification.py', 'tools/status/test_status_qualification.py'} or path.startswith('tools/txid/txid-display/'):
        return {'check-tools'}, {'ops'}
    if path.startswith('tools/status/'):
        # Status units and descriptor feed the role tests; its
        # control-session example is pinned by the supervisor tests.
        return {'check-ops-shared', 'check-ops-control-sessions', 'check-ops-contracts'}, {'ops'}
    if path.startswith('tools/txid/'):
        if 'txid_display' in path or 'txid-display' in path:
            return {'check-ops-txid-display', 'check-ops-contracts'}, {'ops'}
        # Its service descriptor and worker role feed the shared role tests.
        return {'check-ops-txid-display', 'check-ops-shared', 'check-ops-contracts'}, {'ops'}
    if path.startswith('tools/tests/operations/control_sessions/') or 'control-session' in path:
        return {'check-ops-control-sessions'}, {'ops'}
    if path.startswith('tools/tests/operations/deploy/'):
        # Local development suite. CI does not run it.
        return set(), {'ops'}
    if path.startswith(('tools/scripts/wallet-pir-deploy', 'tools/scripts/deploy-')):
        return {'check-ops-contracts'}, {'ops'}
    if path.startswith(('tools/transparent/filters/', 'tools/transparent/parent-filters/')):
        return {'check-tools'}, {'ops'}
    if path.startswith('tools/enhance/'):
        return {'check-ops-enhance', 'check-ops-contracts'}, {'ops'}
    if path.startswith('tools/transparent/'):
        if 'scaler' in path or 'membership_model' in path:
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
    if path.startswith(('tools/tests/operations/', 'tools/scripts/', 'tools/deploy/', 'tools/infra/')):
        return {'check-ops-shared', 'check-ops-contracts'}, {'ops'}
    if path.startswith(('tools/transparent/loadtest/', 'tools/transparent/src/loadtest/')):
        return {'check-reports'}, {'transparent', 'ops'}
    if path.startswith(('tools/', 'tools/transparent/filters/', 'tools/transparent/parent-filters/')):
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
        if feedback_tooling(path):
            helpers.add('check-tools')
            if path.startswith('.github/'):
                helpers.add('check-ops-contracts')
            # Full qualification remains conservative for workflow/selector edits.
            groups.update(FULL_GROUPS)
            reasons.append(f'{path}: CI tools and workflow contract checks')
            continue
        if path in ('Cargo.toml', 'Cargo.lock', 'Makefile') or path.startswith('.cargo/'):
            # Native coverage stays conservative. These inputs do not change
            # product deployment logic; full qualification still runs it all.
            helpers.update({'check-tools', 'check-ops-contracts'})
            groups.update(FULL_GROUPS)
            reasons.append(f'{path}: workspace native checks and build/tool contracts')
            continue
        package = owner(packages, path)
        routed = route(path)
        if path.endswith('Cargo.toml') or path == 'Cargo.lock':
            # Every manifest is inside the dependency boundary (tools/ci/boundary.py).
            helpers.add('check-tools'); groups.add('ops')
        if package:
            if path.endswith('.md'):
                helpers.add('check-docs'); groups.add('ops')
            if path.startswith(('tools/transparent/loadtest/', 'tools/transparent/src/loadtest/')):
                helpers.add('check-reports')
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
    # Changing CI orchestration needs exhaustive helper/selector checks, but
    # recompiling unchanged Rust does not validate this orchestration.
    # Manifests, build scripts and unknown inputs
    # retain their broad native checks.
    if any(feedback_tooling(path) for path in paths):
        selected = select(packages, [path for path in paths if not feedback_tooling(path)])
        reasons.append('CI tooling uses helper checks; full family qualification remains required')
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



def diff_products(packages, paths):
    """Products whose host fixtures this diff must compile.

    A product library selects that product. A host test file selects the
    product that test asks for, and a shared test helper selects every product
    the host's tests ask for. The host library and shared crates select none.
    An owned package manifest selects that same product. A workspace manifest
    or any other unscoped path still selects every product.
    Tooling-only paths select none.
    """
    from fast_build import PRODUCT_HOST_FEATURES, product_of_package, products_for_host_path
    relevant = [path for path in paths if not feedback_tooling(path)]
    if not relevant:
        return frozenset()
    by_name = {package['name']: package for package in packages}

    def unscoped(path):
        if path in {'Cargo.toml', 'Cargo.lock', 'Makefile'} or path.startswith('.cargo/'):
            return True
        # An owned manifest stays with its package. A manifest Cargo cannot
        # attribute, like a deleted crate, still selects every product.
        if owner(packages, path):
            return False
        if path.endswith('/Cargo.toml'):
            return True
        return route(path) is None

    if any(unscoped(path) for path in relevant):
        return frozenset(PRODUCT_HOST_FEATURES)
    found = set()
    for path in relevant:
        name = owner(packages, path)
        if not name:
            continue
        host_products = products_for_host_path(by_name[name], path)
        if host_products is not None:
            found.update(host_products)
            continue
        found.add(product_of_package(by_name[name]))
    return frozenset(found & set(PRODUCT_HOST_FEATURES))


def rust_checks(selected, *, features='', test='', test_target='', offline=False, compile_only=False, unit_jobs=1, products=None):
    if not selected:
        return
    if not (features or test or test_target):
        from fast_build import execute
        return execute(selected, offline=offline, compile_only=compile_only, jobs=unit_jobs, products=products)
    metadata = json.loads(subprocess.check_output(['cargo', 'metadata', '--locked', '--no-deps', '--format-version', '1', *(['--offline'] if offline else [])], cwd=ROOT, **inherited_fds()))
    run(['cargo', 'fmt', '--all', '--check'], stage='format')
    options = ['--locked', '--profile', 'release-fast']
    if offline:
        options += ['--offline']
    registry = json.loads((ROOT / 'tools/ci/slow-tests.json').read_text())
    programs = []
    for package in metadata['packages']:
        name = package['name']
        if name not in selected:
            continue
        # Lib and bin tests unless a target is named; full CI runs integration tests with each group's features (full_packages.py).
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
        executables = run([*command, '--no-run'], stage=f'compile:{name}',
                          collect_tests=unit_jobs > 1 and not compile_only)
        if compile_only:
            continue
        if unit_jobs > 1:
            if not executables:
                raise ValueError(f'{name}: Cargo reported no test executables')
            listing = ''.join(subprocess.check_output([binary, '--list'],
                              cwd=Path(package['manifest_path']).parent, text=True,
                              **inherited_fds()) for binary in executables)
        else:
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
        if unit_jobs > 1:
            programs.extend({'executable': binary, 'directory': str(Path(package['manifest_path']).parent),
                             'package': name, 'version': package['version'], 'args': args[1:]}
                            for binary in executables)
        else:
            run([*command, *args], env=dict(os.environ, RUST_TEST_THREADS='1'), stage=f'tests:{name}')
    if programs:
        # Cargo writers finish before bounded execution of their exact reported binaries.
        run(['python3', 'tools/ci/reclaim-build-cache.py'], stage='reclaim')
        with tempfile.TemporaryDirectory(prefix='wallet-pir-unit-plan-') as directory:
            path = Path(directory) / 'programs.json'
            path.write_text(json.dumps(programs))
            run(['python3', 'tools/ci/unit_tests.py', str(path), '--jobs', str(unit_jobs)],
                stage='tests:unit-programs')
    if not test and not test_target and not features:
        # Packages without integration targets need no executable selection.
        from integration_tests import inventory
        classified = inventory()
        command = ['python3', 'tools/ci/integration_tests.py', '--tier', 'fast']
        for name in selected:
            if name in classified and classified[name]['fast']:
                command.extend(['--package', name])
        if len(command) > 4:
            if offline:
                command.append('--offline')
            if compile_only:
                command.append('--compile-only')
            run(command, stage='integration:fast')


def feedback_selection(selected, paths):
    coverage = json.loads((ROOT / 'tools/ci/full-packages.json').read_text())
    families = {family for family, names in coverage.items()
                if set(names) & set(selected['packages'])}
    category = ('shared' if 'shared' in families or len(families) > 1 else next(iter(families))) if families else (
        'ci' if not paths or any(path.startswith(('tools/ci/', 'tools/tests/', '.github/')) for path in paths) else
        'docs' if set(selected['helpers']) <= {'check-docs'} else 'ops')
    return {'category': category, 'families': sorted(families - {'shared'}),
            'package_count': len(selected['packages'])}


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
    parser.add_argument('--unit-jobs', type=int, choices=(1, 2, 3), default=None,
                        help='bounded concurrent test programs, after sequential Cargo builds')
    parser.add_argument('--compile-only', action='store_true', help='prepare fast Rust units without running tests')
    parser.add_argument('--github-output', action='store_true')
    args = parser.parse_args()
    if args.unit_jobs is None:
        args.unit_jobs = 1 if (args.features or args.test or args.test_target) else 3
    if args.unit_jobs > 1 and (args.features or args.test or args.test_target):
        parser.error('--unit-jobs requires the standard fast unit selection')
    if not args.select_only:
        from hygiene import check
        if check(base=args.base or "HEAD", head=args.head, local=not args.committed_only):
            raise SystemExit(1)
    packages = workspace_packages()
    paths = changed_paths(args.base, args.head, local=not args.committed_only)
    from workspace_changes import package_paths
    selection_paths = package_paths(paths, packages, base=args.base, head=args.head,
                                    local=not args.committed_only, root=ROOT)
    selected = plan(packages, selection_paths, all_checks=args.all or (args.committed_only and (not args.base or set(args.base) == {'0'})))
    if {'Cargo.toml', 'Cargo.lock'} & set(paths):
        selected['helpers'] = sorted(set(selected['helpers']) | {'check-tools'})
        selected['groups'] = sorted(set(selected['groups']) | {'ops'})
    if selection_paths != paths:
        selected['reasons'].append('workspace-only manifest/lock edits select changed packages and consumers')
    if args.package:
        unknown = set(args.package) - {p['name'] for p in packages}
        if unknown:
            parser.error(f'unknown workspace packages: {sorted(unknown)}')
        selected = {'packages': sorted(set(args.package)), 'helpers': [], 'groups': [], 'reasons': ['explicit package check']}
    if not args.select_only and __name__ == '__main__':
        print('CI_SELECTION ' + json.dumps(feedback_selection(selected, paths)), flush=True)
    print(json.dumps(selected, indent=2), flush=True)
    if args.github_output:
        with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
            out.write(f'rust={str(bool(selected["packages"])).lower()}\n')
            out.write('helpers=' + json.dumps(selected['helpers']) + '\n')
            for group in FULL_GROUPS:
                out.write(f'{group}={str(group in selected["groups"]).lower()}\n')
    if args.select_only:
        return
    # An explicit package list or a full run derives products from those packages.
    # A diff passes the products its own paths touch, not every dependent.
    products = None
    compiling_everything = args.all or (args.committed_only and (not args.base or set(args.base) == {'0'}))
    if not args.package and not compiling_everything:
        products = diff_products(packages, selection_paths)
    # Workflow steps run scripts by path and no helper target exercises them; always check the paths exist.
    run(['python3', 'tools/ci/workflow_paths.py'], stage='workflow-paths')
    with local_target(ROOT, enabled=bool(selected['packages'])), ThreadPoolExecutor(max_workers=4) as pool:
        futures = []
        if selected['helpers'] and not args.compile_only:
            from helpers import targets
            helper_env = dict(os.environ, OFFLINE='1') if args.offline else os.environ.copy()
            helper_env['WALLET_PIR_FAST_TESTS'] = '1'
            helpers = pool.submit(run, ['make', '-j4', *targets(selected['helpers'])],
                                  env=helper_env, stage='helpers')
            futures.append(helpers)
        futures.append(pool.submit(rust_checks, selected['packages'], features=args.features, test=args.test, test_target=args.test_target, offline=args.offline, compile_only=args.compile_only, unit_jobs=args.unit_jobs, products=products))
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

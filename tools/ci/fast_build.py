"""Compile the selected CPU fast tests, then run their reported binaries.

Cargo unifies features for every package named in one command. Shared crates
and product libraries stay on one command with a fixed feature set, so a warm
cache still hits. The publisher, query server, and end-to-end hosts are a
second command that enables only the products this diff touches. Campaign
targets are never selected. Test programs retain their package directory and
Cargo environment. The Enhance HTTP smokes run alone, as does the filter
library on fewer than eight cores. The remaining programs share a pool of at
most three processes.
"""
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import tomllib

from stage import ROOT, run, inherited_fds


def available_cpus():
    if hasattr(os, 'sched_getaffinity'):
        return len(os.sched_getaffinity(0))
    return os.cpu_count() or 1


def publisher_runs_alone(name, kinds):
    """Archive builds take every core of a 4-core hosted runner.

    The 8-core fast runner still has cores left when the publisher keeps four
    build threads, so it stays in the shared pool there.
    """
    return name == 'transparent-publisher' and 'lib' in kinds and available_cpus() < 8


# Dev-dependencies activate only for roots named in one Cargo command. These
# packages define the fast closure. Their feature set does not follow the diff.
# Only affected packages are executed.
FAST_FEATURE_ROOTS = frozenset({
    'enhance-pir-client',
    'enhance-pir-load-test',
    'enhance-publisher',
    'enhance-query-server',
    'pir-e2e-tests',
    'pir-monitor',
    'pir-publisher',
    'pir-query-server',
    'status-publisher',
    'transparent-publisher',
    'transparent-query-server',
    'transparent-shared',
    'transparent-tools',
})


def compile_roots(selected, available):
    """Affected packages plus the stable fast dev-dependency closure."""
    selected = set(selected)
    available = set(available)
    missing = selected - available
    if missing:
        raise ValueError('selected fast packages are missing from Cargo metadata: '
                         + ', '.join(sorted(missing)))
    return selected | (FAST_FEATURE_ROOTS & available)


# These three hosts were the slow tail: one feature list compiled Enhance,
# Status, and Transparent into each of them, including the wallet client and
# the chain reader's test node.
HOST_PACKAGES = frozenset({'pir-publisher', 'pir-query-server', 'pir-e2e-tests'})

# Each feature pulls one product's wallet client and chain fixtures into the hosts.
PRODUCT_HOST_FEATURES = {
    'enhance': 'pir-e2e-tests/enhance-tests',
    'transparent': 'pir-e2e-tests/transparent-tests',
    'status': 'pir-publisher/status-tests',
}

# A fast target that requires one of these is compiled only for a diff that
# touches that product. Shared crates never receive them.
PRODUCT_FEATURE_GATES = {
    'enhance-tests': 'enhance',
    'transparent-tests': 'transparent',
    'status-tests': 'status',
    'wallet-recovery-tests': 'transparent',
}

PACKAGE_PRODUCTS = ('enhance', 'status', 'transparent', 'receiver')


def product_of_package(package):
    """The product that owns this package, or None for a host or shared crate."""
    manifest = Path(package.get('manifest_path', ''))
    try:
        relative = manifest.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        relative = manifest.as_posix().lstrip('/')
    parts = relative.split('/')
    if len(parts) >= 2 and parts[0] in {'src', 'tools'} and parts[1] in PACKAGE_PRODUCTS:
        return parts[1]
    name = package.get('name', '')
    for product in PACKAGE_PRODUCTS:
        if name.startswith(product + '-'):
            return product
    return None


def products_of_packages(names, packages_by_name):
    """Products named directly by this package list."""
    found = set()
    for name in names:
        package = packages_by_name.get(name)
        if package is None:
            continue
        product = product_of_package(package)
        if product in PRODUCT_HOST_FEATURES:
            found.add(product)
    return frozenset(found)


def feature_serves(feature, products):
    """Whether this feature belongs to one of the products being compiled."""
    gate = PRODUCT_FEATURE_GATES.get(str(feature).rsplit('/', 1)[-1])
    return gate is None or gate in products


def host_features_for(products, package_names):
    """Host features for the products this diff touches."""
    features = set()
    if 'pir-e2e-tests' in package_names:
        if 'enhance' in products:
            features.add(PRODUCT_HOST_FEATURES['enhance'])
        if 'transparent' in products:
            features.add(PRODUCT_HOST_FEATURES['transparent'])
    if 'pir-publisher' in package_names and 'status' in products:
        features.add(PRODUCT_HOST_FEATURES['status'])
    return features


def products_for_host_path(package, path):
    """Products a changed host file needs, or None when the package is not a host.

    One test file selects the products that test asks for. A shared test helper
    selects every product the host's tests ask for. The host library selects none.
    """
    if package.get('name') not in HOST_PACKAGES:
        return None
    manifest_path = Path(package['manifest_path'])
    try:
        root = manifest_path.resolve().parent.relative_to(ROOT).as_posix()
    except ValueError:
        return set()
    data = tomllib.loads(manifest_path.read_text())
    matched, every = [], set()
    for test in data.get('test', []):
        gates = {PRODUCT_FEATURE_GATES[feature.rsplit('/', 1)[-1]]
                 for feature in test.get('required-features', [])
                 if feature.rsplit('/', 1)[-1] in PRODUCT_FEATURE_GATES}
        every |= gates
        test_path = test.get('path')
        if not test_path:
            continue
        repo_path = f'{root}/{test_path}'
        stem = repo_path[:-3] if repo_path.endswith('.rs') else repo_path
        if path == repo_path or path.startswith(stem + '/'):
            matched.append(gates)
    if matched:
        return set().union(*matched)
    if path.startswith(root + '/tests/'):
        return every
    return set()


_TEST_ATTRIBUTE = re.compile(r'#\[(?:[\w:]+::)?test\b|#\[tokio::test\b')


def target_declares_tests(target):
    """Whether a binary's source contains a unit test.

    An empty harness still links the binary's whole dependency graph. Fast
    correctness compiles only binaries that declare a test. Missing source
    stays included so an unread target is not dropped. Qualification builds
    every binary on its own command.
    """
    src = target.get('src_path')
    if not src:
        return True
    path = Path(src)
    if not path.is_file():
        return True
    pending, seen = [path], set()
    while pending:
        current = pending.pop()
        if current in seen or not current.is_file():
            continue
        seen.add(current)
        text = current.read_text(encoding='utf-8', errors='replace')
        if _TEST_ATTRIBUTE.search(text):
            return True
        directory = current.parent
        for name in re.findall(
                r'(?m)^(?:pub(?:\([^)]*\))?\s+)?mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*;', text):
            pending.append(directory / f'{name}.rs')
            pending.append(directory / name / 'mod.rs')
    return False


def test_names(binary, directory, *, ignored=False):
    options = ['--ignored'] if ignored else []
    listing = subprocess.check_output([binary, *options, '--list'], cwd=directory,
                                      text=True, **inherited_fds())
    return {line[:-6] for line in listing.splitlines() if line.endswith(': test')}


def bounded_programs(program, names):
    """Partition large unit executables without increasing the process limit.

    Exact filters are a disjoint cover of discovery, including ignored cases.
    Each process retains immutable fixture reuse across dozens of cases. Keep
    the small-runner publisher protection and real HTTP integrations intact.
    """
    if program['integration'] or program['exclusive'] or len(names) <= 48:
        return [program]
    lanes = min(3, (len(names) + 47) // 48)
    ordered = sorted(names)
    distribution = [name for name in ordered
                    if name.startswith('history::controller::distribution_tests::')]
    if program.get('package') == 'transparent-publisher' and distribution:
        # These cases share expensive immutable native producer bytes. Keep
        # them in one process; each consumer still has its own files and HTTP.
        remaining = sorted(names - set(distribution))
        other_lanes = lanes - 1
        partitions = [distribution, *[remaining[index::other_lanes]
                                     for index in range(other_lanes)]]
    else:
        partitions = [ordered[index::lanes] for index in range(lanes)]
    partitions = [cases for cases in partitions if cases]
    assert sum(map(len, partitions)) == len(names)
    assert set().union(*map(set, partitions)) == set(names)
    return [dict(program, args=[*program['args'], '--exact', *cases],
                 case_count=len(cases)) for cases in partitions]


def _compile_command(group, options):
    """One cargo test --no-run for a feature-stable package group."""
    if not group['names'] or (not group['kinds'] and not group['integrations']):
        return None
    command = ['cargo', 'test', '--locked', '--profile', 'release-fast', '--no-run', *options]
    for name in sorted(group['names']):
        command += ['-p', name]
    if 'lib' in group['kinds']:
        command.append('--lib')
    # --bins also links harnesses for binaries that contain no tests. Name the
    # bins that declare one. With no skip, keep --bins so an unreadable target
    # stays fail-open.
    if 'bin' in group['kinds']:
        skipped_any = any(skipped for skipped, _included in group['bins'])
        if skipped_any:
            included = sorted({bin_name for _skipped, names in group['bins'] for bin_name in names})
            for bin_name in included:
                command += ['--bin', bin_name]
        else:
            command.append('--bins')
    command += [arg for target in sorted(group['integrations']) for arg in ('--test', target)]
    if group['features']:
        command += ['--features', ','.join(sorted(group['features']))]
    return command


def execute(selected, *, offline=False, compile_only=False, jobs=3,
            additional_features=(), environment=None, case_log=None, enforce_budget=True,
            products=None):
    from integration_tests import inventory
    if any(feature.rsplit('/', 1)[-1] == 'cuda' for feature in additional_features):
        raise ValueError('ordinary CPU correctness cannot enable CUDA')
    if any(feature.rsplit('/', 1)[-1] == 'qualification-tests' for feature in additional_features):
        raise ValueError('ordinary correctness cannot enable qualification test campaigns')
    registry = inventory()
    slow = json.loads((ROOT / 'tools/ci/slow-tests.json').read_text())
    options = ['--offline'] if offline else []
    metadata = json.loads(subprocess.check_output(
        ['cargo', 'metadata', '--locked', '--no-deps', '--format-version=1', *options],
        cwd=ROOT, env=environment, **inherited_fds()))
    run_packages = set(selected)
    by_name = {p['name']: p for p in metadata['packages']}
    if products is None:
        products = products_of_packages(run_packages, by_name)
    else:
        unknown = set(products) - set(PRODUCT_HOST_FEATURES)
        if unknown:
            raise ValueError('unknown host products: ' + ', '.join(sorted(unknown)))
        products = frozenset(products)
    compile_packages = compile_roots(run_packages, set(by_name))
    packages = {p['id']: p for p in metadata['packages'] if p['name'] in compile_packages}
    if {p['name'] for p in packages.values()} != compile_packages:
        raise ValueError('selected fast packages are missing from Cargo metadata')

    def integration_allowed(name, target):
        matches = [item for item in by_name[name]['targets']
                   if item['name'] == target and 'test' in item.get('kind', [])]
        if not matches:
            return True
        allowed = products if name in HOST_PACKAGES else frozenset()
        return all(feature_serves(feature, allowed) for feature in matches[0].get('required-features', []))

    compile_integrations = {(name, target) for name in compile_packages
                            for target in registry.get(name, {}).get('fast', [])
                            if integration_allowed(name, target)}
    expected_integrations = {item for item in compile_integrations if item[0] in run_packages}
    integration_names = {target for _, target in compile_integrations}
    # Name collisions are checked for every compiled root. Campaign targets of
    # an anchor must not be pulled in by another package's fast name.
    collisions = {(name, target) for name in compile_packages
                  for target in registry.get(name, {}).get('full', [])
                  if target in integration_names}
    if collisions:
        raise ValueError(f'fast target names also select campaign targets: {sorted(collisions)}')
    groups = {
        'stable': dict(features=set(), kinds=set(), bins=[], integrations=set(), names=set()),
        'hosts': dict(features=set(), kinds=set(), bins=[], integrations=set(), names=set()),
    }
    expected_units = set()
    for package in packages.values():
        name = package['name']
        group = groups['hosts' if name in HOST_PACKAGES else 'stable']
        group['names'].add(name)
        # Product gates stay on the host command. The stable command's features
        # do not change when the diff moves from one product to another.
        allowed = products if name in HOST_PACKAGES else frozenset()
        classified = {f for entry in slow.get(name, {}).values() if isinstance(entry, dict)
                      for f in entry.get('features', [])}
        units = [t for t in package['targets'] if t.get('test', True)
                 and set(t['kind']) & {'lib', 'bin'}]
        enabled = classified | {f for t in units for f in t.get('required-features', []) if f == 'cli'}
        for target in package['targets']:
            if (name, target['name']) in compile_integrations and 'test' in target['kind']:
                enabled.update(feature for feature in target.get('required-features', [])
                               if feature_serves(feature, allowed))
        if any(f.rsplit('/', 1)[-1] in {'native-reinspiring', 'cuda'} for f in enabled):
            raise ValueError(f'{name}: CPU fast builds must keep reference arithmetic separate from native/CUDA features')
        group['features'].update(f if '/' in f else f'{name}/{f}' for f in enabled
                                if feature_serves(f, allowed))
        included_bins, skipped_bin = [], False
        for target in units:
            if not set(target.get('required-features', [])) <= enabled:
                continue
            if 'bin' in target['kind'] and not target_declares_tests(target):
                skipped_bin = True
                continue
            group['kinds'].update(set(target['kind']) & {'lib', 'bin'})
            if name in run_packages:
                expected_units.add((name, target['name']))
            if 'bin' in target['kind']:
                included_bins.append(target['name'])
        group['bins'].append((skipped_bin, included_bins))
        group['integrations'].update(target for pkg, target in compile_integrations if pkg == name)
    groups['hosts']['features'].update(host_features_for(products, groups['hosts']['names']))
    for feature in additional_features:
        if feature_serves(feature, products):
            groups['hosts']['features'].add(feature)
        if feature_serves(feature, frozenset()):
            groups['stable']['features'].add(feature)
    commands = []
    for group in (groups['stable'], groups['hosts']):
        command = _compile_command(group, options)
        if command:
            commands.append(command)
    if not commands:
        return
    # Qualification controls cannot expand a routine correctness fixture.
    env = {key: value for key, value in (os.environ if environment is None else environment).items() if not key.startswith('QUALIFY_')}
    run(['cargo', 'fmt', '--all', '--check'], env=env, stage='format')
    artifacts = []
    for command in commands:
        artifacts.extend(run(command, env=env, stage='compile:fast', collect_tests=not compile_only,
                             test_details=True) or [])
    if compile_only:
        return
    programs = []
    actual_integrations, actual_units = set(), set()
    discovered_units = {name: set() for name in run_packages}
    seen = set()
    for artifact in artifacts:
        binary = artifact['executable']
        if binary in seen:
            continue
        seen.add(binary)
        package = packages.get(artifact['package_id'])
        if package is None:
            raise ValueError('Cargo returned a test from an unselected package')
        name = package['name']
        if 'qualification-tests' in artifact.get('features', []):
            raise ValueError(f'{name}: qualification test campaigns leaked into ordinary correctness')
        # Anchors keep the dev-dependency feature closure compiled. Their
        # programs are not part of this affected run.
        if name not in run_packages:
            continue
        target = artifact['target']
        integration = 'test' in target['kind']
        identity = (name, target['name'])
        directory = str(Path(package['manifest_path']).parent)
        names = test_names(binary, directory)
        if integration:
            actual_integrations.add(identity)
            if not names - test_names(binary, directory, ignored=True):
                raise ValueError(f'{name}/{target["name"]}: no enabled, unignored fast tests')
        else:
            if not set(target['kind']) & {'lib', 'bin'}:
                raise ValueError(f'unexpected fast test target kind: {target["kind"]}')
            actual_units.add(identity)
            discovered_units[name].update(names)
        exclusions = [] if integration else list(slow.get(name, {}))
        program = dict(executable=binary, directory=directory, package=name,
                             version=package['version'],
                             integration=integration,
                             exclusive=publisher_runs_alone(name, target['kind']),
                             case_count=len(names),
                             args=[arg for case in exclusions for arg in ('--skip', case)])
        programs.extend(bounded_programs(program, names))
    if actual_integrations != expected_integrations or expected_units - actual_units:
        raise ValueError(f'fast test executables mismatch: missing_units={sorted(expected_units-actual_units)}, '
                         f'missing_integrations={sorted(expected_integrations-actual_integrations)}, '
                         f'extra_integrations={sorted(actual_integrations-expected_integrations)}')
    # The affected package may have no fast tests. The stable feature roots were
    # still compiled above; there is nothing further to execute.
    if not programs:
        return
    for name, discovered in discovered_units.items():
        stale = set(slow.get(name, {})) - discovered
        if stale:
            raise ValueError(f'{name}: stale slow-test classification: {sorted(stale)}')
    # On a 4-core runner the publisher is exclusive and finishes before this
    # pool. On more cores it starts first in the pool. HTTP programs are next.
    def queue_key(program):
        publisher = (program['package'] == 'transparent-publisher'
                     and not program['integration'] and program['case_count'] > 0)
        group = 0 if publisher else 1 if program['integration'] else 2
        return group, -program['case_count']

    programs.sort(key=queue_key)
    run(['python3', 'tools/ci/reclaim-build-cache.py'], env=env, stage='reclaim')
    with tempfile.TemporaryDirectory(prefix='wallet-pir-fast-tests-') as directory:
        root = Path(directory)
        plan = root / 'programs.json'
        plan.write_text(json.dumps(programs))
        cache = root / 'public-fixtures'
        cache.mkdir()
        env['WALLET_PIR_TEST_RUNTIME_CACHE'] = str(cache)
        env['WALLET_PIR_ENFORCE_BUDGET'] = '1' if enforce_budget else '0'
        cases = Path(case_log) if case_log is not None else root / 'cases.jsonl'
        env['WALLET_PIR_CASE_LOG'] = str(cases)
        try:
            run(['python3', 'tools/ci/unit_tests.py', str(plan), '--budget', '--jobs', str(jobs)],
                env=env, stage='tests:fast-programs')
        finally:
            timings = [json.loads(line) for line in cases.read_text().splitlines()] if cases.exists() else []
            timings = [case for case in timings if case.get("executable")]
            print('CI_FAST_CASES ' + json.dumps(dict(
                completed=len(timings),
                slowest=sorted(timings, key=lambda case: case['seconds'], reverse=True)[:15])), flush=True)

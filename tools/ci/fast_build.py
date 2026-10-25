"""Compile the selected CPU fast tests, then run their reported binaries.

Cargo unifies features for every package named in one command. Shared crates,
product libraries, and the publisher and query hosts stay on one command with
a fixed feature set, so a warm cache still hits. Transparent publication tests
live in those libraries. Enabling history and detail on the publisher binary
links that library again and is not part of this command. The query host's own
Transparent `ci_smoke` names `pir-query-server/transparent-history`. Detail is
pinned beside it on the same command. The host's dev-dependency does not force
those roles on, so the second command can leave them off. Moving the hosts onto
another command changes that unification and rebuilds the closure. The
in-process Enhance hint target stays on that second command with the query
host's Enhance and Status roles only. Transparent history and detail stay on
the first command, so one rustc does not build both stacks. Receiver, status,
and RocksDB chain-node targets stay in qualification.
The Enhance and Transparent reader targets stay on this command: dropping
`testing-node` rebuilds every consumer. Campaign targets are never
selected. Test programs retain their package directory and Cargo environment.
The Enhance and Receiver runtime libraries run alone below eight cores,
counting any cgroup CPU quota. Status stays in one process so its cases share
prepared material instead of copying a multi-gigabyte fixture across lanes.
The remaining programs share a pool of at most three processes.
"""
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import tomllib

from stage import ROOT, run, inherited_fds


def cgroup_cpus(root=Path('/sys/fs/cgroup'), membership=Path('/proc/self/cgroup')):
    """The tightest cgroup v2 CPU quota above this process, or None."""
    try:
        path = next(line[3:] for line in membership.read_text().splitlines()
                    if line.startswith('0::'))
    except (OSError, StopIteration):
        return None
    limits = []
    directory = root / path.lstrip('/')
    while True:
        try:
            quota, period = (directory / 'cpu.max').read_text().split()
            if quota != 'max':
                limits.append(max(1, -(-int(quota) // int(period))))
        except (OSError, ValueError):
            pass
        if directory == root or root not in directory.parents:
            break
        directory = directory.parent
    return min(limits) if limits else None


def available_cpus():
    """CPUs this process may use: its affinity, bounded by any cgroup quota."""
    if hasattr(os, 'sched_getaffinity'):
        cpus = len(os.sched_getaffinity(0))
    else:
        cpus = os.cpu_count() or 1
    quota = cgroup_cpus()
    return min(cpus, quota) if quota else cpus


def publisher_runs_alone(name, kinds):
    """Archive builds take every core of a 4-core hosted runner.

    The 8-core fast runner still has cores left when the publisher keeps four
    build threads, so it stays in the shared pool there.
    """
    return name == 'transparent-publisher' and 'lib' in kinds and available_cpus() < 8


def status_runs_alone(name, kinds, names):
    """One shared Status fixture runs before the ordinary test pool."""
    return (name == 'pir-query-server' and 'lib' in kinds
            and any(case.startswith('status::') for case in names))


def isolate_status_fixture(parts, package, kinds, discovered):
    """Mark Status partitions exclusive without marking the host's other roles."""
    for part in parts:
        args = part['args']
        cases = args[args.index('--exact') + 1:] if '--exact' in args else None
        selected = list(discovered) if cases is None else cases
        status_only = cases is None or all(case.startswith('status::') for case in cases)
        if status_only and status_runs_alone(package, kinds, selected):
            part['exclusive'] = True
    return parts


# Unit cases that build full runtimes on every core, each near the case budget.
RUNTIME_LIBRARIES = frozenset({'enhance-shared', 'receiver-indexer'})


def runtime_runs_alone(name, kinds):
    """Runtime-building unit libraries keep their cores below eight CPUs.

    With a three-CPU quota, overlapping these programs with each other and with
    the Transparent lanes pushed single cases past the unchanged case budget.
    """
    return name in RUNTIME_LIBRARIES and 'lib' in kinds and available_cpus() < 8


# Dev-dependencies activate only for roots named in one Cargo command. These
# packages define the fast closure. Their feature set does not follow the diff.
# Receiver and Status libraries stay named here: adding them only on a diff
# that touches them changes feature unification and rebuilds wallet-libraries
# crates, which leaves the fast job on the plain three-minute budget.
# The chain reader, its RPC transport, and the pipeline stay named for the same
# reason. Their fast reader targets require `testing-node`. Naming those
# packages only on a diff that touches them, then dropping them on the next
# diff, rebuilds every consumer of the cached graph. Receiver, status, and
# RocksDB reader targets are not fast targets and stay out of this set.
# Only affected packages are executed.
FAST_FEATURE_ROOTS = frozenset({
    'enhance-pir-client',
    'enhance-pir-load-test',
    'enhance-shared',
    'enhance-publisher',
    'pir-e2e-tests',
    'pir-monitor',
    'pir-node-rpc',
    'pir-pipeline',
    'pir-publisher',
    'pir-query-server',
    'receiver-indexer',
    'receiver-pir-server',
    'status-publisher',
    'transparent-publisher',
    'transparent-shared',
    'transparent-tools',
    'zakura-server-chain',
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

# Fixture features pull one product's wallet client and chain test node into
# every crate on their command. Fast leaves the chain node in qualification.
# History and detail stay on the stable command. Enhance and Status stay on
# the hint command. One rustc of both stacks does not fit in the fast runner.
TRANSPARENT_QUERY_ROLES = ('pir-query-server/transparent-detail',
                           'pir-query-server/transparent-history')
HINT_HOST_ROLES = ('pir-query-server/enhance', 'pir-query-server/status')

PRODUCT_HOST_FEATURES = {
    'enhance': 'pir-e2e-tests/enhance-fast',
    'transparent': 'pir-e2e-tests/transparent-tests',
    'status': 'pir-publisher/status-tests',
    'receiver': 'pir-query-server/receiver',
}

# A fast target that requires one of these is compiled only for a diff that
# touches that product. Shared crates never receive them.
PRODUCT_FEATURE_GATES = {
    'enhance-fast': 'enhance',
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
    """Fixture features for the hint command. They must stay off the stable command.

    Transparent fast tests do not use `transparent-tests`: that feature enables
    publisher fixtures and compiles the same libraries again. History and detail
    are explicit features of the stable command, not of this one, and not of the
    host dev-dependency. Turning them on for the publisher binary links that
    library a second time. Status `status-tests` enables the chain node for
    fixture campaigns that fast does not run; qualification enables it. The
    Enhance hint target uses `enhance-fast`, not `enhance-tests`.
    """
    features = set()
    if 'pir-e2e-tests' in package_names and 'enhance' in products:
        features.add(PRODUCT_HOST_FEATURES['enhance'])
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
    group = 48
    status = {name for name in names if name.startswith('status::')}
    if (program.get('package') == 'pir-query-server' and status
            and not program['integration'] and not program['exclusive']):
        remaining = sorted(names - status)
        partitions = [remaining, sorted(status)] if remaining else [sorted(status)]
        return [dict(program, args=[*program['args'], '--exact', *cases],
                     case_count=len(cases)) for cases in partitions if cases]
    if program['integration'] or program['exclusive'] or len(names) <= group:
        return [program]
    lanes = min(3, (len(names) + group - 1) // group)
    # Two receiver-indexer lanes are about a minute of wall and finish the
    # pool. A list that is already split can use the third process on the
    # 8-core runner; smaller hosts keep the wider groups.
    if (program.get('package') == 'receiver-indexer' and lanes > 1
            and available_cpus() >= 8):
        lanes = 3
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
            products=None, before_tests=None):
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
    # ci_hint enables enhance-fast. That feature stays on its own command so
    # the stable command can keep a feature set that does not change with the
    # diff. History and detail are not on this command.
    hint_identity = ('pir-e2e-tests', 'ci_hint')
    compile_hint = hint_identity in compile_integrations
    if compile_hint:
        compile_integrations.remove(hint_identity)
    expected_integrations = {item for item in compile_integrations if item[0] in run_packages}
    if compile_hint and hint_identity[0] in run_packages:
        expected_integrations.add(hint_identity)
    integration_names = {target for _, target in compile_integrations}
    if compile_hint:
        integration_names.add(hint_identity[1])
    # Name collisions are checked for every compiled root. Campaign targets of
    # an anchor must not be pulled in by another package's fast name.
    collisions = {(name, target) for name in compile_packages
                  for target in registry.get(name, {}).get('full', [])
                  if target in integration_names}
    if collisions:
        raise ValueError(f'fast target names also select campaign targets: {sorted(collisions)}')
    stable = dict(features=set(), kinds=set(), bins=[], integrations=set(), names=set())
    expected_units = set()
    for package in packages.values():
        name = package['name']
        group = stable
        group['names'].add(name)
        # Chain-node fixture features stay off this command. An Enhance diff
        # and a Transparent diff must compile the same roots and features.
        allowed = frozenset()
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
    for feature in additional_features:
        if feature.rsplit('/', 1)[-1] == 'enhance-fast':
            continue
        if feature_serves(feature, frozenset()) or feature_serves(feature, products):
            stable['features'].add(feature)
    # History and detail are constant on this command. The dev-dependency does
    # not enable them, and ci_smoke only names history.
    if 'pir-query-server' in stable['names']:
        stable['features'].update(TRANSPARENT_QUERY_ROLES)
    commands = []
    command = _compile_command(stable, options)
    if command:
        commands.append(command)
    if compile_hint:
        # Ungated extra features (the native correctness profile) stay visible
        # to this target. Fast feedback passes none, so the oracle stays on the
        # reference profile. History and detail stay off: this rustc is Enhance
        # and Status only.
        features = set(host_features_for(frozenset({'enhance'}), {'pir-e2e-tests'}))
        features.update(feature for feature in additional_features
                        if feature_serves(feature, frozenset())
                        and feature not in TRANSPARENT_QUERY_ROLES)
        features.update(HINT_HOST_ROLES)
        features.update(stable['features'] - set(TRANSPARENT_QUERY_ROLES))
        # Naming only the two hosts drops leaf features the stable roots
        # unify in and rebuilds shared crates. Naming the same roots with the
        # stable features, minus the Transparent roles, resolves those crates
        # as the stable command did. Only the query-host harness compiles again.
        hint_packages = sorted(stable['names'] | {'pir-e2e-tests', 'pir-query-server'})
        commands.append(['cargo', 'test', '--locked', '--profile', 'release-fast', '--no-run', *options,
                         *[arg for name in hint_packages for arg in ('-p', name)], '--lib',
                         '--test', 'ci_hint', '--test', 'operator',
                         '--features', ','.join(sorted(features))])
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
    # Unit cases already run by an earlier command's program for the same target.
    covered_units = {}
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
        # A later command that rebuilds the same harness repeats its cases.
        # Run only the new ones.
        unit = None if integration else (name, target['name'], tuple(target['kind']))
        repeated = set() if unit is None else names & covered_units.get(unit, set())
        if unit is not None:
            covered_units.setdefault(unit, set()).update(names)
        if repeated:
            names = names - repeated
            if not names:
                continue
        program = dict(executable=binary, directory=directory, package=name,
                             version=package['version'],
                             integration=integration,
                             # Its real reference packing and independent oracle
                             # must not compete with the runtime's matrix fixtures.
                             # Only the shared Status fixture stays exclusive;
                             # the host's other roles join the ordinary pool.
                             exclusive=publisher_runs_alone(name, target['kind'])
                             or runtime_runs_alone(name, target['kind'])
                             or identity == ('pir-e2e-tests', 'ci_hint')
                             or (name == 'enhance-publisher' and 'lib' in target['kind']),
                             case_count=len(names),
                             args=[arg for case in exclusions for arg in ('--skip', case)])
        if repeated:
            parts = [dict(part, args=[*part['args'], '--exact', *sorted(names)])
                     if '--exact' not in part['args'] else part
                     for part in bounded_programs(program, names)]
        else:
            parts = bounded_programs(program, names)
        programs.extend(isolate_status_fixture(parts, name, target['kind'], names))
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
            if before_tests is not None:
                before_tests()
            run(['python3', 'tools/ci/unit_tests.py', str(plan), '--budget', '--jobs', str(jobs)],
                env=env, stage='tests:fast-programs')
        finally:
            timings = [json.loads(line) for line in cases.read_text().splitlines()] if cases.exists() else []
            timings = [case for case in timings if case.get("executable")]
            print('CI_FAST_CASES ' + json.dumps(dict(
                completed=len(timings),
                slowest=sorted(timings, key=lambda case: case['seconds'], reverse=True)[:15])), flush=True)

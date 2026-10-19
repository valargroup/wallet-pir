"""Compile the selected CPU fast tests once, then run their reported binaries.

Cargo unifies features for the complete selection. Unit tests and the named
fast integration targets share that build; campaign targets are never selected.
Test programs retain their package directory and Cargo environment. The Enhance
HTTP smokes run alone, as does the filter library on fewer than eight cores.
The remaining programs share a pool of at most three processes.
"""
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

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


def execute(selected, *, offline=False, compile_only=False, jobs=3,
            additional_features=(), environment=None, case_log=None, enforce_budget=True):
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
    selected = set(selected)
    packages = {p['id']: p for p in metadata['packages'] if p['name'] in selected}
    if {p['name'] for p in packages.values()} != selected:
        raise ValueError('selected fast packages are missing from Cargo metadata')
    expected_integrations = {(name, target) for name in selected
                             for target in registry.get(name, {}).get('fast', [])}
    integration_names = {target for _, target in expected_integrations}
    collisions = {(name, target) for name in selected
                  for target in registry.get(name, {}).get('full', [])
                  if target in integration_names}
    if collisions:
        raise ValueError(f'fast target names also select campaign targets: {sorted(collisions)}')
    features = set()
    kinds = set()
    expected_units = set()
    package_bins = []
    for package in packages.values():
        name = package['name']
        classified = {f for entry in slow.get(name, {}).values() if isinstance(entry, dict)
                      for f in entry.get('features', [])}
        units = [t for t in package['targets'] if t.get('test', True)
                 and set(t['kind']) & {'lib', 'bin'}]
        enabled = classified | {f for t in units for f in t.get('required-features', []) if f == 'cli'}
        for target in package['targets']:
            if (name, target['name']) in expected_integrations and 'test' in target['kind']:
                enabled.update(target.get('required-features', []))
        if any(f.rsplit('/', 1)[-1] in {'native-reinspiring', 'cuda'} for f in enabled):
            raise ValueError(f'{name}: CPU fast builds must keep reference arithmetic separate from native/CUDA features')
        features.update(f if '/' in f else f'{name}/{f}' for f in enabled)
        included_bins, skipped_bin = [], False
        for target in units:
            if not set(target.get('required-features', [])) <= enabled:
                continue
            if 'bin' in target['kind'] and not target_declares_tests(target):
                skipped_bin = True
                continue
            kinds.update(set(target['kind']) & {'lib', 'bin'})
            expected_units.add((name, target['name']))
            if 'bin' in target['kind']:
                included_bins.append(target['name'])
        package_bins.append((skipped_bin, included_bins))
    if not kinds and not integration_names:
        return
    features.update(additional_features)
    command = ['cargo', 'test', '--locked', '--profile', 'release-fast', '--no-run', *options]
    for name in sorted(selected):
        command += ['-p', name]
    if 'lib' in kinds:
        command.append('--lib')
    # --bins also links harnesses for binaries that contain no tests. Name the
    # bins that declare one. With no skip, keep --bins so an unreadable target
    # stays fail-open.
    if 'bin' in kinds:
        skipped_any = any(skipped for skipped, _included in package_bins)
        if skipped_any:
            included = sorted({name for _skipped, names in package_bins for name in names})
            for name in included:
                command += ['--bin', name]
        else:
            command.append('--bins')
    command += [arg for target in sorted(integration_names) for arg in ('--test', target)]
    if features:
        command += ['--features', ','.join(sorted(features))]
    # Qualification controls cannot expand a routine correctness fixture.
    env = {key: value for key, value in (os.environ if environment is None else environment).items() if not key.startswith('QUALIFY_')}
    run(['cargo', 'fmt', '--all', '--check'], env=env, stage='format')
    artifacts = run(command, env=env, stage='compile:fast', collect_tests=not compile_only,
                    test_details=True)
    if compile_only:
        return
    programs = []
    actual_integrations, actual_units = set(), set()
    discovered_units = {name: set() for name in selected}
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
    if not programs:
        raise ValueError('Cargo reported no fast test executables')
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

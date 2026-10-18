"""Compile the selected CPU fast tests once, then run their reported binaries.

Cargo unifies features for the complete selection. Unit tests and the named
fast integration targets share that build; campaign targets are never selected.
Test programs retain their package directory, Cargo environment and existing
slow-case exclusions. On fewer than 8 cores the filter library runs alone;
otherwise at most three programs, including that library and HTTP integration
programs, execute together.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile

from stage import ROOT, run, inherited_fds
from test_tiers import check_discovery, gated_cases


def available_cpus():
    if hasattr(os, 'sched_getaffinity'):
        return len(os.sched_getaffinity(0))
    return os.cpu_count() or 1


def publisher_runs_alone(name, kinds):
    """Archive builds take every core of a 4-core hosted runner.

    The 8-core fast runner still has cores left when the publisher keeps four
    build threads, so it stays in the shared pool there.
    """
    return name == 'transparent-filter-server' and 'lib' in kinds and available_cpus() < 8


def test_names(binary, directory, *, ignored=False):
    options = ['--ignored'] if ignored else []
    listing = subprocess.check_output([binary, *options, '--list'], cwd=directory,
                                      text=True, **inherited_fds())
    return {line[:-6] for line in listing.splitlines() if line.endswith(': test')}


def execute(selected, *, offline=False, compile_only=False, jobs=3):
    from integration_tests import inventory
    registry = inventory()
    slow = json.loads((ROOT / 'tools/ci/slow-tests.json').read_text())
    options = ['--offline'] if offline else []
    metadata = json.loads(subprocess.check_output(
        ['cargo', 'metadata', '--locked', '--no-deps', '--format-version=1', *options],
        cwd=ROOT, **inherited_fds()))
    selected = set(selected)
    if 'enhance-pir-server' in selected:
        selected.add('enhance-serving')
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
        for target in units:
            kinds.update(set(target['kind']) & {'lib', 'bin'})
            if set(target.get('required-features', [])) <= enabled:
                expected_units.add((name, target['name']))
    if not kinds and not integration_names:
        return
    command = ['cargo', 'test', '--locked', '--profile', 'release-fast', '--no-run', *options]
    for name in sorted(selected):
        command += ['-p', name]
    command += [flag for kind, flag in [('lib', '--lib'), ('bin', '--bins')] if kind in kinds]
    command += [arg for target in sorted(integration_names) for arg in ('--test', target)]
    if features:
        command += ['--features', ','.join(sorted(features))]
    # Qualification controls cannot expand a routine correctness fixture.
    env = {key: value for key, value in os.environ.items() if not key.startswith('QUALIFY_')}
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
        exclusions = [] if integration else sorted(set(slow.get(name, {})) - gated_cases(name))
        # Full archive runtimes use every core on a 4-core runner and push the
        # encrypted HTTP smokes past their 30s deadline. Run that library alone
        # there; an 8-core runner still has cores left for the shared pool.
        programs.append(dict(executable=binary, directory=directory, package=name,
                             version=package['version'],
                             integration=integration,
                             exclusive=publisher_runs_alone(name, target['kind']),
                             case_count=len(names),
                             args=[arg for case in exclusions for arg in ('--skip', case)]))
    if actual_integrations != expected_integrations or expected_units - actual_units:
        raise ValueError(f'fast test executables mismatch: missing_units={sorted(expected_units-actual_units)}, '
                         f'missing_integrations={sorted(expected_integrations-actual_integrations)}, '
                         f'extra_integrations={sorted(actual_integrations-expected_integrations)}')
    if not programs:
        raise ValueError('Cargo reported no fast test executables')
    for name, discovered in discovered_units.items():
        check_discovery(name, discovered, full=False)
        stale = set(slow.get(name, {})) - gated_cases(name) - discovered
        if stale:
            raise ValueError(f'{name}: stale slow-test classification: {sorted(stale)}')
    # On a 4-core runner the publisher is exclusive and finishes before this
    # pool. On more cores it starts first in the pool. HTTP programs are next.
    def queue_key(program):
        publisher = (program['package'] == 'transparent-filter-server'
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
        env['WALLET_PIR_ENFORCE_BUDGET'] = '1'
        cases = root / 'cases.jsonl'
        env['WALLET_PIR_CASE_LOG'] = str(cases)
        try:
            run(['python3', 'tools/ci/unit_tests.py', str(plan), '--budget', '--jobs', str(jobs)],
                env=env, stage='tests:fast-programs')
        finally:
            timings = [json.loads(line) for line in cases.read_text().splitlines()] if cases.exists() else []
            print('CI_FAST_CASES ' + json.dumps(dict(
                completed=len(timings),
                slowest=sorted(timings, key=lambda case: case['seconds'], reverse=True)[:15])), flush=True)

"""Compile the selected CPU fast tests once, then run their reported binaries.

Cargo unifies features for the complete selection. Unit tests and the named
fast integration targets share that build; campaign targets are never selected.
Test programs retain their package directory, Cargo environment and existing
slow-case exclusions. At most three ordinary programs execute together; integration programs and the
filter library run alone.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile

from stage import ROOT, run, inherited_fds


def test_names(binary, directory, *, ignored=False):
    options = ['--ignored'] if ignored else []
    listing = subprocess.check_output([binary, *options, '--list'], cwd=directory,
                                      text=True, **inherited_fds())
    return {line[:-6] for line in listing.splitlines() if line.endswith(': test')}


def execute(selected, *, offline=False, compile_only=False, jobs=3, before_tests=None):
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
    # Helpers may overlap compilation, but publisher archive preparation needs
    # the same exclusive CPU window that it has among Rust test programs.
    if before_tests is not None:
        before_tests()
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
        exclusions = [] if integration else list(slow.get(name, {}))
        # The filter library prepares full archive runtimes; keep its unchanged
        # cases out of contention with other programs under the 30s case limit.
        programs.append(dict(executable=binary, directory=directory, package=name,
                             version=package['version'],
                             exclusive=integration or (name == 'transparent-filter-server'
                                                       and 'lib' in target['kind']),
                             case_count=len(names),
                             args=[arg for case in exclusions for arg in ('--skip', case)]))
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
    # Cargo can report a freshly rebuilt large executable last. Start binaries
    # with more discovered cases first so that work overlaps smaller programs.
    programs.sort(key=lambda program: program['case_count'], reverse=True)
    run(['python3', 'tools/ci/reclaim-build-cache.py'], env=env, stage='reclaim')
    with tempfile.TemporaryDirectory(prefix='wallet-pir-fast-tests-') as directory:
        root = Path(directory)
        plan = root / 'programs.json'
        plan.write_text(json.dumps(programs))
        cache = root / 'public-fixtures'
        cache.mkdir()
        env['WALLET_PIR_TEST_RUNTIME_CACHE'] = str(cache)
        env['WALLET_PIR_ENFORCE_BUDGET'] = '1'
        run(['python3', 'tools/ci/unit_tests.py', str(plan), '--budget', '--jobs', str(jobs)],
            env=env, stage='tests:fast-programs')

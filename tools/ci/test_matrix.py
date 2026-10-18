#!/usr/bin/env python3
"""Plan complete qualification shards, run their Cargo targets and verify receipts.

The registry assigns every enabled target in each existing feature variant once.
Cargo supplies executable paths; receipts bind completed coverage to the source,
workflow attempt and registry. Caches never substitute for execution receipts.
"""
import argparse
from collections import defaultdict
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time

from full_packages import inventory, commands as full_commands, qualification_features
from stage import ROOT, run, install_cancellation, inherited_fds

REGISTRY = ROOT / 'tools/ci/test-matrix.json'
EXPORT = 'tools/txid/txid-sizing/export/Cargo.toml'
NATIVE = ['enhance-pir/cli', 'enhance-pir/native-reinspiring',
          'enhance-serving/native-reinspiring', 'status-query-server/native-reinspiring',
          'enhance-pir-server/native-reinspiring', 'status-publisher/native-reinspiring',
          'enhance-pir-load-test/native-reinspiring']
CUDA = ['enhance-serving/cuda', 'status-query-server/cuda', 'enhance-pir-server/cuda', 'status-publisher/cuda',
        'enhance-pir-server/native-reinspiring', 'enhance-pir-load-test/native-reinspiring']


def variants(groups):
    return {
        'transparent': dict(family='transparent', packages=groups['transparent'], features=['transparent-filter-server/rocksdb-reader'] + qualification_features(groups['transparent'])),
        'export': dict(family='transparent', packages=['txid-sizing-export'], features=[], manifest=EXPORT),
        'native': dict(family='enhance', packages=groups['enhance'], features=NATIVE + qualification_features(groups['enhance'])),
        'reference': dict(family='enhance', packages=['enhance-serving', 'enhance-pir', 'status-query-server'], features=[]),
        'cuda': dict(family='enhance', packages=['enhance-serving', 'status-query-server', 'enhance-pir-server',
                                             'status-publisher', 'enhance-pir-load-test'], features=CUDA),
        'receiver': dict(family='receiver', packages=groups['receiver'], features=[]),
    }


def metadata(manifest=None):
    return json.loads(subprocess.check_output(
        ['cargo', 'metadata', '--locked', '--no-deps', '--format-version=1',
         *(['--manifest-path', manifest] if manifest else [])], cwd=ROOT, text=True))


def enabled_features(package, features):
    enabled = {'default'} if 'default' in package['features'] else set()
    enabled.update(f.split('/', 1)[1] for f in features if f.startswith(package['name'] + '/'))
    while True:
        expanded = enabled | {f for name in enabled for f in package['features'].get(name, [])
                              if '/' not in f and not f.startswith('dep:')}
        if expanded == enabled:
            return enabled
        enabled = expanded


def target_inventory(contexts, packages):
    """Mirror the original family commands, including implicit example builds."""
    result = {}
    for variant, context in contexts.items():
        for name in context['packages']:
            package = packages[name]
            enabled = enabled_features(package, context['features'])
            for target in package['targets']:
                if not set(target.get('required-features', [])) <= enabled:
                    continue
                kind = target['kind'][0]
                if kind not in ('lib', 'bin', 'test', 'example'):
                    continue
                kinds = []
                if variant == 'reference':
                    if name == 'enhance-serving' and not (kind == 'lib' or kind == 'test' and target['name'] == 'ci_smoke'):
                        continue
                    if kind not in ('lib', 'bin', 'test') or not target.get('test', True):
                        continue
                    kinds = [kind]
                elif variant == 'cuda':
                    # Preserve the existing all-targets check and the two runtime tests.
                    kinds = ['check'] if kind == 'lib' else []
                    if (name, kind, target['name']) in {
                        ('enhance-serving', 'lib', 'enhance_serving'),
                        ('enhance-pir-server', 'test', 'matvec_config'),
                    }:
                        kinds.append('example-test' if kind == 'example' and target.get('test') else kind)
                else:
                    if target.get('test', True) or kind == 'example':
                        kinds.append('example-test' if kind == 'example' and target.get('test') else kind)
                    if kind == 'lib' and target.get('doctest', True):
                        kinds.append('doc')
                for selected in kinds:
                    key = ':'.join((variant, name, selected, target['name']))
                    result[key] = dict(variant=variant, package=name, kind=selected, target=target['name'])
    # A binary-only package still needs the CUDA all-targets compile check.
    result['cuda:enhance-pir-load-test:check:all'] = dict(
        variant='cuda', package='enhance-pir-load-test', kind='check', target='all')
    return result


def load_plan(registry=None, packages=None):
    registry = json.loads(REGISTRY.read_text()) if registry is None else registry
    contexts = variants(inventory())
    if packages is None:
        packages = {p['name']: p for data in (metadata(), metadata(EXPORT)) for p in data['packages']}
    targets = target_inventory(contexts, packages)
    assigned = [key for shard in registry.values() for key in shard['targets']]
    if len(assigned) != len(set(assigned)):
        raise ValueError('duplicate qualification target assignment')
    if set(assigned) != set(targets):
        raise ValueError(f'qualification inventory mismatch: missing={sorted(set(targets)-set(assigned))}, '
                         f'stale={sorted(set(assigned)-set(targets))}')
    for name, shard in registry.items():
        if not shard['targets'] or shard['family'] not in ('transparent', 'enhance', 'receiver'):
            raise ValueError(f'{name}: empty or unknown family')
        if any(contexts[targets[key]['variant']]['family'] != shard['family'] for key in shard['targets']):
            raise ValueError(f'{name}: target from another family')
    digest = hashlib.sha256(json.dumps(dict(registry=registry, contexts=contexts), sort_keys=True).encode()).hexdigest()
    return registry, contexts, targets, packages, digest


def package_features(package, context, packages):
    """Carry family feature unification into each selected package and its direct deps."""
    enabled = set(context['features'])
    while True:
        expanded = set(enabled)
        for entry in enabled:
            owner, feature = entry.split('/', 1)
            for child in packages[owner]['features'].get(feature, []):
                if child.startswith('dep:'):
                    continue
                if '/' in child:
                    dependency, child_feature = child.split('/', 1)
                    dependency = dependency.removesuffix('?')
                    if dependency in packages:
                        expanded.add(dependency + '/' + child_feature)
                else:
                    expanded.add(owner + '/' + child)
        if expanded == enabled:
            break
        enabled = expanded
    direct = {dep.get('rename') or dep['name'] for dep in packages[package]['dependencies']}
    return sorted(feature for feature in enabled if feature.split('/', 1)[0] in direct | {package})


def cargo_commands(keys, contexts, targets, packages):
    """Batch targets within a package without selecting a namesake in another package."""
    batches = defaultdict(list)
    for key in keys:
        target = targets[key]
        category = target['kind'] if target['kind'] in ('doc', 'example', 'check') else 'program'
        batches[target['variant'], target['package'], category].append(target)
    commands = []
    checks = [target for (variant, package, category), selected in batches.items()
              if category == 'check' for target in selected]
    if checks:
        # Keep CUDA's all-targets check in the original unified feature graph;
        # splitting it by package would rebuild dependencies with different flags.
        packages_to_check = sorted({target['package'] for target in checks})
        if set(packages_to_check) != set(contexts['cuda']['packages']):
            raise ValueError('CUDA all-targets check must retain every original package')
        command = ['cargo', 'check', '--locked',
                   *[arg for package in packages_to_check for arg in ('-p', package)],
                   '--all-targets', '--features', ','.join(CUDA)]
        commands.append(('cuda', 'check', checks, command))
    for (variant, package, category), selected in batches.items():
        if category == 'check':
            continue
        context = contexts[variant]
        base = ['--locked', '-p', package]
        if category != 'check':
            base += ['--profile', 'release-fast']
        if context.get('manifest'):
            base += ['--manifest-path', context['manifest']]
        features = package_features(package, context, packages)
        if features:
            base += ['--features', ','.join(features)]
        if category == 'doc':
            command = ['cargo', 'test', *base, '--doc']
        elif category == 'example':
            command = ['cargo', 'build', *base, *[arg for t in selected for arg in ('--example', t['target'])]]
        else:
            flags = {'lib': '--lib', 'bin': '--bin', 'test': '--test', 'example-test': '--example'}
            command = ['cargo', 'test', '--no-run', *base]
            for target in selected:
                command += [flags[target['kind']]]
                if target['kind'] != 'lib':
                    command.append(target['target'])
        commands.append((variant, category, selected, command))
    return commands


def source_identity():
    sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    if os.environ.get('GITHUB_SHA', sha) != sha:
        raise ValueError('workflow SHA differs from checkout')
    if subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=ROOT):
        raise ValueError('qualification checkout has tracked changes')
    return sha


def workflow_identity():
    return {key: os.environ.get(key, '') for key in ('GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT')}


def test_programs(artifacts, variant, packages, env):
    actual, programs, cases = set(), [], {}
    for artifact in artifacts:
        package = next(p for p in packages.values() if p['id'] == artifact['package_id'])
        if (variant in ('transparent', 'native')
                and 'qualification-tests' in package.get('features', {})
                and 'qualification-tests' not in artifact.get('features', [])):
            raise ValueError(f"{package['name']}: qualification fixture feature missing from executable")
        identity = (package['name'], artifact['target']['name'])
        if identity in actual:
            raise ValueError(f'duplicate executable: {identity}')
        actual.add(identity)
        args = []
        if variant == 'reference' and identity == ('enhance-serving', 'enhance_serving'):
            args = ['ipir::tests']
        if variant == 'reference' and identity == ('enhance-serving', 'ci_smoke'):
            # Fast correctness already runs these lifecycle checks with reference
            # arithmetic; native qualification repeats them on the deployed backend.
            # Keep the independent generic-hint oracles and damaged-file checks here.
            args = ['--skip', 'encrypted_http_answers_survive_coordinator_restart',
                    '--skip', 'encrypted_publication_advances_while_another_commit_is_pending']
        if variant == 'cuda' and identity == ('enhance-serving', 'enhance_serving'):
            args = ['backend_load_failure_does_not_rebuild_or_replace_published_units']
        program = dict(executable=artifact['executable'], package=package['name'],
                       version=package['version'], directory=str(Path(package['manifest_path']).parent),
                       integration='test' in artifact['target']['kind'], args=args)
        listing = subprocess.check_output([program['executable'], *args, '--list'],
                                          cwd=program['directory'], env=env, text=True, **inherited_fds(env))
        names = sorted(line[:-6] for line in listing.splitlines() if line.endswith(': test'))
        ignored_listing = subprocess.check_output(
            [program['executable'], *args, '--ignored', '--list'],
            cwd=program['directory'], env=env, text=True, **inherited_fds(env))
        ignored = {line[:-6] for line in ignored_listing.splitlines() if line.endswith(': test')}
        if not set(names) - ignored and program['integration']:
            raise ValueError(f'empty integration executable: {identity}')
        cases[':'.join((variant, *identity))] = dict(tests=names, ignored=sorted(ignored),
                                                   features=sorted(artifact.get('features', [])))
        programs.append(program)
    return actual, programs, cases


def run_shard(name, plan, output, compile_only=False):
    registry, contexts, targets, packages, digest = plan
    shard = registry[name]
    sha = source_identity()
    started = time.monotonic()
    env = {key: value for key, value in os.environ.items() if not key.startswith('QUALIFY_')}
    env['RUST_TEST_THREADS'] = '2' if shard['family'] == 'transparent' else '1'
    cases, completed, queued = {}, [], []
    with tempfile.TemporaryDirectory(prefix='wallet-pir-full-') as directory:
        env['WALLET_PIR_TEST_RUNTIME_CACHE'] = directory
        for variant, category, selected, command in cargo_commands(shard['targets'], contexts, targets, packages):
            stage = 'compile' if category != 'doc' else 'doctests'
            if compile_only and category == 'doc':
                continue  # rustdoc --no-run is unsupported; priming cannot attest doctests.
            artifacts = run(command, env=env, stage=f'{stage}:{name}:{variant}:{selected[0]["package"]}',
                            collect_tests=category == 'program', test_details=True)
            if category == 'program':
                if compile_only:
                    continue
                expected = {(t['package'], t['target']) for t in selected}
                actual, programs, listed = test_programs(artifacts, variant, packages, env)
                cases.update(listed)
                if actual != expected:
                    raise ValueError(f'Cargo executable mismatch: expected={expected}, actual={actual}')
                queued.extend(programs)
            completed.extend(':'.join((t['variant'], t['package'], t['kind'], t['target'])) for t in selected)
        if not compile_only and queued:
            run(['python3', 'tools/ci/reclaim-build-cache.py'], env=env, stage='reclaim')
            # Compile all selected programs before a single reclaim/execution boundary.
            path = Path(directory) / 'programs.json'
            path.write_text(json.dumps(queued))
            run(['python3', 'tools/ci/unit_tests.py', str(path), '--jobs', '1',
                 '--test-threads', env['RUST_TEST_THREADS']], env=env, stage=f'tests:{name}')
    if source_identity() != sha:
        raise ValueError('source changed during qualification')
    receipt = dict(shard=name, family=shard['family'], sha=sha, inventory=digest,
                   workflow=workflow_identity(), passed=not compile_only, compile_only=compile_only,
                   completed=sorted(completed), cases=cases, seconds=round(time.monotonic() - started, 3))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(receipt, indent=2) + '\n')
    print('QUALIFICATION_SHARD ' + json.dumps(receipt), flush=True)


def verify_receipts(family, directory, plan, sha, workflow):
    registry, _, _, _, digest = plan
    expected = {name: shard for name, shard in registry.items() if shard['family'] == family}
    receipts = [json.loads(path.read_text()) for path in directory.rglob('*.json')]
    seen = set()
    for receipt in receipts:
        name = receipt['shard']
        if name not in expected or name in seen:
            raise ValueError(f'unexpected or duplicate shard receipt: {name}')
        seen.add(name)
        if not (receipt['family'] == family and receipt['sha'] == sha and receipt['inventory'] == digest
                and receipt['workflow'] == workflow and receipt['passed'] is True
                and receipt['compile_only'] is False
                and receipt['completed'] == sorted(expected[name]['targets'])):
            raise ValueError(f'invalid qualification receipt: {name}')
    if seen != set(expected):
        raise ValueError(f'missing qualification shards: {sorted(set(expected)-seen)}')
    print(f'Qualification coverage complete: {family}, {len(seen)} shards, {sha}')


def baseline_commands(family):
    """The pre-split qualification commands, retained for same-revision audits."""
    result = [('export' if '--manifest-path' in command else
               'native' if family == 'enhance' else family, command[2:])
              for command in full_commands(family, inventory())]
    if family == 'enhance':
        options = ['--locked', '--profile', 'release-fast']
        result += [
            ('reference', [*options, '-p', 'enhance-serving', '--lib', 'ipir::tests']),
            ('reference', [*options, '-p', 'enhance-serving', '--test', 'ci_smoke']),
            ('reference', [*options, '-p', 'enhance-pir', '-p', 'status-query-server', '--lib', '--tests']),
            ('cuda', [*options, '-p', 'enhance-pir-server', '--features', 'native-reinspiring,cuda', '--test', 'matvec_config']),
            ('cuda', [*options, '-p', 'enhance-serving', '--features', 'native-reinspiring,cuda', '--lib',
                      'backend_load_failure_does_not_rebuild_or_replace_published_units']),
        ]
    return result


def run_baseline(family, plan, output):
    sha = source_identity()
    cases = {}
    started = time.monotonic()
    env = {key: value for key, value in os.environ.items() if not key.startswith('QUALIFY_')}
    env['RUST_TEST_THREADS'] = '2' if family == 'transparent' else '1'
    if family == 'enhance':
        run(['cargo', 'check', '--locked', '-p', 'enhance-serving', '-p', 'status-query-server',
             '-p', 'enhance-pir-server', '-p', 'enhance-pir-load-test', '--all-targets',
             '--features', ','.join(CUDA)], env=env, stage='compile:baseline-cuda')
    for variant, options in baseline_commands(family):
        artifacts = run(['cargo', 'test', '--no-run', *options], env=env,
                        stage='compile:baseline-' + variant, collect_tests=True, test_details=True)
        _, _, listed = test_programs(artifacts, variant, plan[3], env)
        cases.update(listed)
        run(['bash', 'tools/ci/full-test.sh', *options], env=env, stage='tests:baseline-' + variant)
    if source_identity() != sha:
        raise ValueError('source changed during baseline qualification')
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(dict(sha=sha, family=family, passed=True, cases=cases,
                                     seconds=round(time.monotonic()-started, 3)), indent=2) + '\n')


def compare_baseline(family, directory, baseline, plan):
    sha = source_identity()
    verify_receipts(family, directory, plan, sha, workflow_identity())
    expected = json.loads(baseline.read_text())
    if expected['sha'] != sha or expected['family'] != family or expected['passed'] is not True:
        raise ValueError('baseline was not executed successfully at this source revision')
    cases = {}
    for path in directory.rglob('*.json'):
        receipt = json.loads(path.read_text())
        if set(cases) & set(receipt['cases']):
            raise ValueError('test executable ran in more than one shard')
        cases.update(receipt['cases'])
    if cases != expected['cases']:
        raise ValueError('enabled test case inventory differs from the executed baseline')
    print(f'Baseline and matrix cases match: {family}, {sum(len(entry['tests']) for entry in cases.values())} listed cases')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['validate', 'plan', 'run', 'compile-only', 'verify', 'baseline', 'compare'])
    parser.add_argument('--family', choices=['transparent', 'enhance', 'receiver'])
    parser.add_argument('--selected', help='JSON family-to-boolean selection from the workflow')
    parser.add_argument('--github-output', action='store_true')
    parser.add_argument('--shard')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--receipts', type=Path)
    parser.add_argument('--baseline', type=Path)
    args = parser.parse_args()
    if args.command in ('plan', 'verify'):
        registry = json.loads(REGISTRY.read_text())
        contexts = variants(inventory())
        digest = hashlib.sha256(json.dumps(dict(registry=registry, contexts=contexts), sort_keys=True).encode()).hexdigest()
        plan = registry, contexts, {}, {}, digest
    else:
        plan = load_plan()
    if args.command == 'validate':
        print(f'Qualification inventory complete: {len(plan[2])} targets in {len(plan[0])} shards')
    elif args.command == 'plan':
        selected = json.loads(args.selected) if args.selected else dict.fromkeys(('enhance', 'transparent', 'receiver'), True)
        matrix = {'include': [dict(shard=name, family=s['family']) for name, s in plan[0].items()
                              if (not args.family or s['family'] == args.family)
                              and selected.get(s['family']) in (True, 'true')]}
        if args.github_output:
            with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
                output.write('matrix=' + json.dumps(matrix) + '\n')
                output.write('count=' + str(len(matrix['include'])) + '\n')
        print(json.dumps(matrix))
    elif args.command == 'baseline':
        if not args.family or args.output is None:
            parser.error('baseline needs --family and --output')
        install_cancellation()
        run_baseline(args.family, plan, args.output)
    elif args.command == 'compare':
        if not args.family or args.receipts is None or args.baseline is None:
            parser.error('compare needs --family, --receipts and --baseline')
        compare_baseline(args.family, args.receipts, args.baseline, plan)
    elif args.command == 'verify':
        if not args.family or args.receipts is None:
            parser.error('verify needs --family and --receipts')
        verify_receipts(args.family, args.receipts, plan, source_identity(), workflow_identity())
    else:
        if args.shard not in plan[0] or args.output is None:
            parser.error('execution needs a known --shard and --output')
        install_cancellation()
        run_shard(args.shard, plan, args.output, args.command == 'compile-only')


if __name__ == '__main__':
    main()

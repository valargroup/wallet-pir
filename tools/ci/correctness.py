#!/usr/bin/env python3
"""Run all CPU correctness checks, including warm compilation, in 300 seconds.

--record-only collects a complete diagnostic run without relaxing the enforced
command. Both modes retain all bounded unit tests and classified correctness integrations.
Explicit large campaigns, CUDA/hardware and
explicit soak campaigns retain their existing separate qualification paths.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import hashlib
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from test_budget import case_seconds
from full_packages import inventory
from stage import ROOT, run, install_cancellation, stop
from fast import HELPERS, local_target

FEATURES = ','.join([
    'enhance-query-server/native-reinspiring', 'status-query-server/native-reinspiring',
    'enhance-publisher/native-reinspiring', 'status-publisher/native-reinspiring', 'status-publisher/cli',
    'enhance-pir-client/native-reinspiring', 'enhance-pir-load-test/native-reinspiring',
    'native-certificate/native-reinspiring', 'pir-query-server/status',
    'pir-query-server/native-reinspiring', 'pir-control/admission', 'transparent-publisher/rocksdb-reader', 'transparent-tools/test-support', 'transparent-tools/compact-export',
])


def source_identity():
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    digest = hashlib.sha256(subprocess.check_output(['git', 'diff', '--binary', 'HEAD'], cwd=ROOT))
    for name in sorted(subprocess.check_output(
            ['git', 'ls-files', '--others', '--exclude-standard', '-z'], cwd=ROOT).split(b'\0')):
        if name:
            path = ROOT / os.fsdecode(name)
            digest.update(name)
            if path.is_file():
                digest.update(path.read_bytes())
    return dict(head=head, source_sha256=digest.hexdigest())


def artifact_summary(path):
    from cargo_cache import artifacts
    if not path.exists():
        return None
    stages = {'compile:fast', 'compile:all-native', 'compile:reference', 'compile:export', 'doctests', 'lint'}
    records = [json.loads(line) for line in path.read_text().splitlines()]
    return artifacts([entry for entry in records if entry.get('stage') in stages])


def programs(artifacts, metadata):
    packages = {package['id']: package for package in metadata['packages']}
    result = []
    seen = set()
    for artifact in artifacts:
        binary = artifact['executable']
        if binary in seen:
            continue
        seen.add(binary)
        package = packages[artifact['package_id']]
        result.append(dict(executable=binary, directory=str(Path(package['manifest_path']).parent),
                           package=package['name'], version=package['version'], args=[],
                           integration='test' in artifact['target']['kind'],
                           target=artifact['target']['name']))
    return result


def rust_checks(env, temporary, offline):
    metadata = json.loads(subprocess.check_output(
        ['cargo', 'metadata', '--locked', '--no-deps', '--format-version=1', *offline], cwd=ROOT, env=env))
    options = ['--locked', '--profile', 'release-fast', *offline]
    # Use the same executable inventory and discovery validation as affected
    # feedback, with all workspace packages and the required native features.
    # No unit cases are excluded; only explicit qualification integrations and
    # their larger fixture settings belong to the separate full runner.
    from fast_build import execute
    execute([package['name'] for package in metadata['packages']],
            offline=bool(offline), jobs=3, additional_features=FEATURES.split(','),
            environment=env, case_log=env['WALLET_PIR_CASE_LOG'],
            enforce_budget=env['WALLET_PIR_ENFORCE_BUDGET'] == '1')
    # Reference checks have the same package/feature graph and are compiled
    # together. The full native workspace graph is never rebuilt without native.
    reference = run(['cargo', 'test', '--no-run', *options, '-p', 'enhance-query-server', '--lib',
                     '--test', 'routing_contract'],
                    env=env, stage='compile:reference', collect_tests=True, test_details=True)
    reference += run(['cargo', 'test', '--no-run', *options, '-p', 'pir-e2e-tests',
                     '--features', 'enhance-tests', '--test', 'ci_hint'],
                     env=env, stage='compile:reference', collect_tests=True, test_details=True)
    reference += run(['cargo', 'test', '--no-run', *options, '-p', 'status-query-server', '--lib', '--tests'],
                     env=env, stage='compile:reference', collect_tests=True, test_details=True)
    reference_plan = programs(reference, metadata)
    for program in reference_plan:
        # The IPIR units plus the protocol, client and record units the query
        # server absorbed from the former protocol library.
        if program['package'] == 'enhance-query-server' and not program['integration']:
            program['args'] = ['ipir::tests', 'client::', 'protocol::', 'record::', 'record_types::']
    path = temporary / 'reference.json'
    path.write_text(json.dumps(reference_plan))
    reference_fixtures = temporary / 'reference-fixtures'
    reference_fixtures.mkdir()
    reference_env = dict(env, WALLET_PIR_TEST_RUNTIME_CACHE=str(reference_fixtures))
    run(['python3', 'tools/ci/unit_tests.py', str(path), '--jobs=1', '--budget'],
        env=reference_env, stage='tests:reference')
    # Doctests are not executable artifacts in Cargo's no-run output.
    run(['cargo', 'test', '--workspace', '--doc', *options, '--features', FEATURES], env=env, stage='doctests')
    export_manifest = 'tools/txid/txid-sizing/export/Cargo.toml'
    export_metadata = json.loads(subprocess.check_output(
        ['cargo', 'metadata', '--locked', '--no-deps', '--format-version=1',
         '--manifest-path', export_manifest, *offline], cwd=ROOT, env=env))
    export = run(['cargo', 'test', '--no-run', *options, '--manifest-path', export_manifest],
                 env=env, stage='compile:export', collect_tests=True, test_details=True)
    path = temporary / 'export.json'
    path.write_text(json.dumps(programs(export, export_metadata)))
    run(['python3', 'tools/ci/unit_tests.py', str(path), '--jobs=1', '--budget'], env=env, stage='tests:export')
    run(['cargo', 'clippy', '--locked', '--workspace', '--all-targets', *offline,
         '--features', FEATURES, '--', '-D', 'warnings'], env=env, stage='lint')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--record-only', action='store_true')
    parser.add_argument('--offline', action='store_true')
    parser.add_argument('--case-seconds', type=case_seconds, default=30,
                        help='per-case deadline, including setup and teardown (default: 30)')
    args = parser.parse_args()
    if sys.platform != 'linux' and not args.record_only:
        parser.error('complete correctness requires Linux for the exporter executing-image check; '
                     'use --record-only for diagnostics or run focused local checks')
    start = time.monotonic()
    install_cancellation()
    timer = None
    if not args.record_only:
        timer = threading.Timer(300, lambda: os.kill(os.getpid(), signal.SIGTERM))
        timer.daemon = True
        timer.start()
    output = ROOT / '.local-pir/correctness' / uuid.uuid4().hex[:12]
    output.mkdir(parents=True, exist_ok=True)
    case_log = output / 'cases.jsonl'
    case_log.write_text('')
    passed = False
    source = source_identity()
    try:
        inventory()
        with tempfile.TemporaryDirectory(prefix='wallet-pir-correctness-') as directory:
            temporary = Path(directory)
            bootstrap = temporary / 'python'
            bootstrap.mkdir()
            (bootstrap / 'sitecustomize.py').write_text(
                'import sys\n'
                'if any(a == "unittest" or a.rsplit("/",1)[-1].startswith("test_") or a.endswith("scaler_tests.py") for a in sys.orig_argv[1:]):\n'
                ' from python_timing import install\n install()\n')
            cache = temporary / 'public-fixtures'
            cache.mkdir()
            env = {key: value for key, value in os.environ.items() if not key.startswith('QUALIFY_')}
            env.update(WALLET_PIR_CASE_LOG=str(case_log), WALLET_PIR_CASE_SECONDS=str(args.case_seconds), WALLET_PIR_ENFORCE_BUDGET='0' if args.record_only else '1',
                       WALLET_PIR_ARTIFACT_LOG=str(output / 'artifacts.jsonl'),
                       WALLET_PIR_STAGE_LOG=str(output / 'stages.jsonl'),
                       WALLET_PIR_TEST_RUNTIME_CACHE=str(cache),
                       PYTHONPATH=os.pathsep.join([str(bootstrap), str(ROOT / 'tools/ci'), env.get('PYTHONPATH', '')]))
            helpers = ['make', '-j4', 'check-hygiene', *sorted(HELPERS)]
            if args.offline:
                env['OFFLINE'] = '1'
            with local_target(ROOT, enabled=not bool(env.get('CARGO_TARGET_DIR'))), ThreadPoolExecutor(max_workers=2) as pool:
                env = dict(os.environ, **env)
                futures = [pool.submit(run, helpers, env=env, stage='helpers'),
                           pool.submit(rust_checks, env, temporary, ['--offline'] if args.offline else [])]
                try:
                    for future in as_completed(futures):
                        future.result()
                except BaseException:
                    stop(signal.SIGTERM)
                    raise
            run(['cargo', 'fmt', '--all', '--check'], env=env, stage='format')
        passed = source_identity() == source
        if not passed:
            raise RuntimeError('source changed during correctness validation')
    finally:
        if timer:
            timer.cancel()
        elapsed = time.monotonic() - start
        cases = [json.loads(line) for line in case_log.read_text().splitlines()]
        artifacts_path = output / 'artifacts.jsonl'
        units = artifact_summary(artifacts_path)
        report = dict(passed=passed and (args.record_only or elapsed < 300),
                      seconds=round(elapsed, 3), enforced=not args.record_only,
                      source=source, artifacts=units,
                      cases=len(cases), case_seconds=args.case_seconds,
                      over_budget=[case for case in cases if case['seconds'] >= args.case_seconds],
                      slowest=sorted(cases, key=lambda case: case['seconds'], reverse=True)[:15])
        (output / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
        (output.parent / 'latest.json').write_text(json.dumps(dict(report= str(output / 'result.json'), **report), indent=2) + '\n')
        print('CORRECTNESS_RESULT ' + json.dumps(report), flush=True)
    if not args.record_only and elapsed >= 300:
        raise SystemExit('complete correctness run exceeded 300 seconds')


if __name__ == '__main__':
    main()

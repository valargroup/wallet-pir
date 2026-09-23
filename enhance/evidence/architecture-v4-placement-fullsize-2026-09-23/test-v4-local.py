#!/usr/bin/env python3
"""Run disposable v4 processes and an exact-answer HTTP load sweep.

This is local functional/performance evidence, not 8 GiB hardware qualification.
No cloud resources or existing services are touched.
"""
import argparse
import hashlib
import json
import platform
import re
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request


def free_port():
    with socket.socket() as stream:
        stream.bind(('127.0.0.1', 0))
        return stream.getsockname()[1]


def fetch(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def provenance(bin_dir):
    candidate = bin_dir / 'candidate.json'
    if candidate.exists():
        metadata = json.loads(candidate.read_text())
        if (metadata.get('kind') != 'enhance-pir-v4-candidate'
                or metadata.get('qualification') != 'unqualified'
                or metadata.get('protocol_revision') != 'ironwood-enhance-pir-v4'
                or metadata.get('schema_version') != 10
                or not isinstance(metadata.get('source_dirty'), bool)
                or not re.fullmatch('[0-9a-f]{40}', metadata.get('source_revision', ''))):
            raise ValueError('invalid candidate provenance')
        return {'revision': metadata['source_revision'], 'dirty': metadata['source_dirty'],
                'revision_source': 'candidate-metadata'}
    try:
        result = subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True)
    except FileNotFoundError:
        return {'revision': None, 'dirty': None, 'revision_source': 'unavailable'}
    if result.returncode:
        return {'revision': None, 'dirty': None, 'revision_source': 'unavailable'}
    status = subprocess.run(['git', 'status', '--porcelain'], capture_output=True, text=True)
    return {'revision': result.stdout.strip(), 'dirty': bool(status.stdout) if status.returncode == 0 else None,
            'revision_source': 'checkout'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bin-dir', type=Path, default=Path('target/release-fast'))
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--seconds', type=int, default=30)
    parser.add_argument('--concurrency', default='1,2,4,8')
    parser.add_argument('--records', type=int, default=67)
    parser.add_argument('--open-loop', action='store_true')
    parser.add_argument('--require-publications', type=int, default=0,
                        help='Fail unless this many generations publish during the load campaign')
    parser.add_argument('--expand-inventory', action='store_true',
                        help='Register a second idle replica pair while the coordinator serves')
    parser.add_argument('--journal-script', type=Path,
                        help='Exercise durable demand observation before fixture inventory expansion')
    args = parser.parse_args()
    if args.journal_script and not args.expand_inventory:
        parser.error('--journal-script requires --expand-inventory')
    if args.seconds < 1 or args.records < 1:
        parser.error('seconds and records must be positive')
    if args.require_publications < 0:
        parser.error('require-publications must be nonnegative')
    args.out.mkdir(parents=True, exist_ok=False)
    binary = args.bin_dir.resolve() / 'enhance-pir-v4'
    load = args.bin_dir.resolve() / 'enhance-pir-load-test'
    manifest = {'kind': 'local-smoke-not-hardware-qualification', 'platform': platform.platform(),
                **provenance(args.bin_dir.resolve()),
                'binaries': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in [binary, load]},
                'seconds_per_step': args.seconds, 'records': args.records, 'results': []}
    processes = []
    logs = []
    with tempfile.TemporaryDirectory(prefix='enhance-v4-') as work:
        work = Path(work)
        try:
            replicas = []
            for index in range(4 if args.expand_inventory else 2):
                port = free_port()
                replicas.append({'name': f'replica-{index}', 'url': f'http://127.0.0.1:{port}'})
                log = (args.out / f'worker-{index}.log').open('w')
                logs.append(log)
                processes.append(subprocess.Popen([str(binary), 'worker', '--listen', f'127.0.0.1:{port}',
                    '--data-dir', str(work / f'worker-{index}')], stdout=log, stderr=log))
            config = work / 'workers.json'
            groups = [{'name': 'group-1', 'replicas': replicas[:2]}]
            config.write_text(json.dumps({'groups': groups}))
            port = free_port()
            origin = f'http://127.0.0.1:{port}'
            log = (args.out / 'coordinator.log').open('w')
            logs.append(log)
            processes.append(subprocess.Popen([str(binary), 'coordinator', '--listen', f'127.0.0.1:{port}',
                '--data-dir', str(work / 'coordinator'), '--worker-config', str(config), '--isolated-fixture',
                '--fixture-records', str(args.records), '--fixture-append-records', '1', '--poll-seconds', '5',
                '--capacity-fallback-rows-per-second', '8' if args.expand_inventory else '1'], stdout=log, stderr=log))
            deadline = time.monotonic() + 300
            while True:
                if any(p.poll() is not None for p in processes):
                    raise RuntimeError('a v4 process exited during startup')
                try:
                    manifest['initial_manifest'] = fetch(origin + '/v1/enhance/init')
                    break
                except Exception:
                    if time.monotonic() >= deadline:
                        raise RuntimeError('initial publication timed out') from None
                    time.sleep(1)
            if args.expand_inventory:
                manifest['initial_capacity'] = fetch(origin + '/v1/health')['capacity']
                pending = manifest['initial_capacity']['requested']
                if not pending or manifest['initial_capacity']['requests'][pending]['target_groups'] != 2:
                    raise RuntimeError('configured forecast did not request a second replica pair')
                if args.journal_script:
                    policy = work / 'infrastructure-policy.json'
                    policy.write_text(json.dumps({'mode': 'isolated-fixture', 'region': 'ams3',
                                                  'project_id': 'fixture-only', 'vpc_id': 'fixture-only'}))
                    state_dir = work / 'infra-journal'
                    command = [sys.executable, str(args.journal_script.resolve()), '--state-dir', str(state_dir),
                               '--coordinator-url', origin, '--inventory', str(config), '--policy', str(policy)]
                    summaries = []
                    snapshots = []
                    for _ in range(2):
                        result = subprocess.run(command, capture_output=True, text=True, timeout=20, check=True)
                        summaries.append(json.loads(result.stdout))
                        snapshots.append(json.loads((state_dir / 'journal.json').read_text()))
                    if snapshots[0] != snapshots[1] or summaries[0]['operation'] != pending:
                        raise RuntimeError('demand observation did not survive a journal process restart')
                    (args.out / 'expansion-journal.json').write_text(json.dumps(snapshots[1], indent=2))
                    manifest['expansion_journal'] = {'summaries': summaries,
                        'script_sha256': hashlib.sha256(args.journal_script.read_bytes()).hexdigest()}
                groups.append({'name': 'group-2', 'replicas': replicas[2:]})
                replacement = config.with_suffix('.next')
                replacement.write_text(json.dumps({'groups': groups}))
                replacement.replace(config)
                deadline = time.monotonic() + 60
                while True:
                    health = fetch(origin + '/v1/health')
                    if health.get('registered_groups') == 2:
                        if not health['capacity']['requests'][pending]['registered']:
                            raise RuntimeError('registered pair did not satisfy durable expansion demand')
                        manifest['inventory_expansion'] = health
                        break
                    if time.monotonic() >= deadline:
                        raise RuntimeError('online inventory expansion timed out')
                    time.sleep(1)
            manifest['load_start_manifest'] = fetch(origin + '/v1/enhance/init')
            for concurrency in map(int, args.concurrency.split(',')):
                report = args.out / f'load-c{concurrency}.json'
                command = [str(load), '--v4', '--fixture-oracle', '--server', origin,
                    '--parallelism', str(concurrency), '--warmup', '2s', '--duration', f'{args.seconds}s',
                    '--seed', '20260922', '--max-error-rate', '1', '--json-out', str(report)]
                (args.out / f'load-c{concurrency}.command.json').write_text(json.dumps(command, indent=2))
                with (args.out / f'load-c{concurrency}.log').open('w') as output:
                    result = subprocess.run(command, stdout=output, stderr=output, timeout=args.seconds + 180)
                manifest['results'].append({'concurrency': concurrency, 'exit_code': result.returncode,
                                            'report': report.name, 'health': fetch(origin + '/v1/health')})
                print(f'concurrency={concurrency} exit={result.returncode}', flush=True)
                if result.returncode:
                    raise RuntimeError('load driver failed; inspect retained evidence')
            if args.open_loop:
                baseline = json.loads((args.out / 'load-c2.json').read_text())['correct_queries_per_second']
                for fraction in [0.5, 0.75, 1.0, 1.25]:
                    report = args.out / f'open-loop-{fraction}.json'
                    command = [str(load), '--v4', '--fixture-oracle', '--server', origin,
                        '--parallelism', '8', '--rate', str(baseline * fraction), '--warmup', '2s',
                        '--duration', f'{args.seconds}s', '--seed', '20260922', '--max-error-rate', '1',
                        '--json-out', str(report)]
                    (args.out / f'open-loop-{fraction}.command.json').write_text(json.dumps(command, indent=2))
                    with (args.out / f'open-loop-{fraction}.log').open('w') as output:
                        result = subprocess.run(command, stdout=output, stderr=output, timeout=args.seconds + 180)
                    manifest['results'].append({'offered_fraction': fraction, 'exit_code': result.returncode,
                                                'report': report.name, 'health': fetch(origin + '/v1/health')})
                    print(f'offered_fraction={fraction} exit={result.returncode}', flush=True)
                    if result.returncode:
                        raise RuntimeError('open-loop driver failed; inspect retained evidence')
            manifest['final_manifest'] = fetch(origin + '/v1/enhance/init')
            published = (manifest['final_manifest']['generation']
                         - manifest['load_start_manifest']['generation'])
            manifest['publication_gate'] = {'required': args.require_publications,
                                            'observed': published,
                                            'passed': published >= args.require_publications}
            if published < args.require_publications:
                raise RuntimeError('insufficient completed publications during load; inspect retained evidence')
        finally:
            for process in reversed(processes):
                process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            for log in logs:
                log.close()
            (args.out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')


if __name__ == '__main__':
    main()

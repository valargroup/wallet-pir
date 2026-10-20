#!/usr/bin/env python3
"""Run disposable processes and an exact-answer HTTP load sweep.

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


ROOT = Path(__file__).resolve().parents[3]


def free_port():
    with socket.socket() as stream:
        stream.bind(('127.0.0.1', 0))
        return stream.getsockname()[1]


def write_role_config(path, management_listen, settings):
    """One Enhance role for pir-query-server --config."""
    path.write_text(json.dumps({
        'management_listen': management_listen,
        'enhance': {'enabled': True, 'settings': settings},
    }))


def write_worker_config(path, listen, data_dir, management_listen, sealed_shards=6):
    write_role_config(path, management_listen, {
        'role': 'worker',
        'listen': listen,
        'data_dir': str(data_dir),
        'sealed_shards': sealed_shards,
    })


def fetch(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def provenance(bin_dir):
    candidate = bin_dir / 'candidate.json'
    if candidate.exists():
        metadata = json.loads(candidate.read_text())
        if (metadata.get('kind') != 'enhance-pir'
                or metadata.get('qualification') != 'unqualified'
                or metadata.get('protocol_revision') != 'ironwood-enhance-pir-v7'
                or metadata.get('schema_version') != 11
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
    parser.add_argument('--publisher', type=Path, help='Matching pir-publisher binary (default: --bin-dir/pir-publisher)')
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
    publisher = args.publisher.resolve() if args.publisher else args.bin_dir.resolve() / 'pir-publisher'
    query_server = args.bin_dir.resolve() / 'pir-query-server'
    load = args.bin_dir.resolve() / 'enhance-pir-load-test'
    manifest = {'kind': 'local-smoke-not-hardware-qualification', 'platform': platform.platform(),
                **provenance(args.bin_dir.resolve()),
                'binaries': {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                             for p in [publisher, query_server, load]},
                'seconds_per_step': args.seconds, 'records': args.records, 'results': []}
    processes = []
    logs = []
    with tempfile.TemporaryDirectory(prefix='enhance-') as work:
        work = Path(work)
        try:
            replicas = []
            used_ports = set()

            def take_port():
                while True:
                    candidate = free_port()
                    if candidate not in used_ports:
                        used_ports.add(candidate)
                        return candidate

            for index in range(4 if args.expand_inventory else 2):
                port = take_port()
                replicas.append({'name': f'replica-{index}', 'url': f'http://127.0.0.1:{port}'})
                config = work / f'worker-{index}.json'
                write_worker_config(config, f'127.0.0.1:{port}', work / f'worker-{index}',
                                    f'127.0.0.1:{take_port()}')
                log = (args.out / f'worker-{index}.log').open('w')
                logs.append(log)
                processes.append(subprocess.Popen([str(query_server), '--config', str(config)],
                                                   stdout=log, stderr=log))
            # Wallets and the load driver reach query ingress; the coordinator
            # answers only operator routes and serves artifacts to the router.
            artifact_listen = f'127.0.0.1:{take_port()}'
            router_control, router_query = (f'127.0.0.1:{take_port()}' for _ in range(2))
            ingress_control, ingress_listen = (f'127.0.0.1:{take_port()}' for _ in range(2))
            roles = {
                'packing-router': {'role': 'packing-router', 'data_dir': str(work / 'packing-router'),
                                   'artifact_origin': f'http://{artifact_listen}',
                                   'listen': router_query, 'control_listen': router_control},
                'query-ingress': {'role': 'query-ingress', 'data_dir': str(work / 'query-ingress'),
                                  'listen': ingress_listen, 'control_listen': ingress_control},
            }
            for name, settings in roles.items():
                role_config = work / f'{name}.json'
                write_role_config(role_config, f'127.0.0.1:{take_port()}', settings)
                log = (args.out / f'{name}.log').open('w')
                logs.append(log)
                processes.append(subprocess.Popen([str(query_server), '--config', str(role_config)],
                                                   stdout=log, stderr=log))
            routers = work / 'packing-routers.json'
            routers.write_text(json.dumps([{'name': 'packing-router', 'url': f'http://{router_control}',
                                            'query_url': f'http://{router_query}'}]))
            config = work / 'workers.json'
            groups = [{'name': 'group-1', 'replicas': replicas[:2]}]
            config.write_text(json.dumps({'groups': groups}))
            port = take_port()
            operator = f'http://127.0.0.1:{port}'
            origin = f'http://{ingress_listen}'
            log = (args.out / 'coordinator.log').open('w')
            logs.append(log)
            publisher_config = work / 'publisher.json'
            publisher_config.write_text(json.dumps({
                'management_listen': f'127.0.0.1:{take_port()}',
                'enhance': {'enabled': True, 'settings': {
                    'listen': f'127.0.0.1:{port}', 'data_dir': str(work / 'coordinator'),
                    'worker_config': str(config), 'isolated_fixture': True,
                    'fixture_records': args.records, 'fixture_append_records': 1, 'poll_seconds': 5,
                    'packing_router_config': str(routers), 'query_ingress': [f'http://{ingress_control}'],
                    'artifact_listen': artifact_listen, 'frontier_replicas': 2,
                    'zakura_rpc_url': 'http://127.0.0.1:8232', 'prevout_cache_outputs': 250000,
                    'capacity_fallback_rows_per_second': 8 if args.expand_inventory else 1,
                    'capacity_readiness_seconds': 21600, 'capacity_burst_rows': 4096, 'sealed_shards': 6,
                }},
            }))
            processes.append(subprocess.Popen([str(publisher), '--config', str(publisher_config)],
                                               stdout=log, stderr=log))
            deadline = time.monotonic() + 300
            while True:
                if any(p.poll() is not None for p in processes):
                    raise RuntimeError('a process exited during startup')
                try:
                    manifest['initial_manifest'] = fetch(origin + '/v1/enhance/init')
                    break
                except Exception:
                    if time.monotonic() >= deadline:
                        raise RuntimeError('initial publication timed out') from None
                    time.sleep(1)
            if args.expand_inventory:
                manifest['initial_capacity'] = fetch(operator + '/v1/health')['capacity']
                pending = manifest['initial_capacity']['requested']
                if not pending or manifest['initial_capacity']['requests'][pending]['target_groups'] != 2:
                    raise RuntimeError('configured forecast did not request a second replica pair')
                if args.journal_script:
                    policy = work / 'infrastructure-policy.json'
                    policy.write_text(json.dumps({'mode': 'isolated-fixture', 'region': 'ams3',
                                                  'project_id': 'fixture-only', 'vpc_id': 'fixture-only'}))
                    state_dir = work / 'infra-journal'
                    command = [sys.executable, str(args.journal_script.resolve()), '--state-dir', str(state_dir),
                               '--coordinator-url', operator, '--inventory', str(config), '--policy', str(policy)]
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
                    health = fetch(operator + '/v1/health')
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
                command = [str(load),  '--fixture-oracle', '--server', origin,
                    '--parallelism', str(concurrency), '--warmup', '2s', '--duration', f'{args.seconds}s',
                    '--seed', '20260922', '--max-error-rate', '1', '--json-out', str(report)]
                (args.out / f'load-c{concurrency}.command.json').write_text(json.dumps(command, indent=2))
                with (args.out / f'load-c{concurrency}.log').open('w') as output:
                    result = subprocess.run(command, stdout=output, stderr=output, timeout=args.seconds + 180)
                manifest['results'].append({'concurrency': concurrency, 'exit_code': result.returncode,
                                            'report': report.name, 'health': fetch(operator + '/v1/health')})
                print(f'concurrency={concurrency} exit={result.returncode}', flush=True)
                if result.returncode:
                    raise RuntimeError('load driver failed; inspect retained evidence')
            if args.open_loop:
                baseline = json.loads((args.out / 'load-c2.json').read_text())['correct_queries_per_second']
                for fraction in [0.5, 0.75, 1.0, 1.25]:
                    report = args.out / f'open-loop-{fraction}.json'
                    command = [str(load),  '--fixture-oracle', '--server', origin,
                        '--parallelism', '8', '--rate', str(baseline * fraction), '--warmup', '2s',
                        '--duration', f'{args.seconds}s', '--seed', '20260922', '--max-error-rate', '1',
                        '--json-out', str(report)]
                    (args.out / f'open-loop-{fraction}.command.json').write_text(json.dumps(command, indent=2))
                    with (args.out / f'open-loop-{fraction}.log').open('w') as output:
                        result = subprocess.run(command, stdout=output, stderr=output, timeout=args.seconds + 180)
                    manifest['results'].append({'offered_fraction': fraction, 'exit_code': result.returncode,
                                                'report': report.name, 'health': fetch(operator + '/v1/health')})
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

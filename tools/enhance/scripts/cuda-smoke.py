#!/usr/bin/env python3
"""Run a CUDA query server and a native coordinator against fresh public fixture state.

Workers are the CUDA `pir-query-server` in `--release-dir`. The coordinator is
the native `pir-publisher` passed as `--publisher`. GPU hosts do not run
the publisher as the worker. No production state, service units, or inventories
are opened. Both isolated replicas use CUDA, so every successful encrypted
fixture query executes on the GPU.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request

PROTOCOL = 'ironwood-enhance-pir-v9-native-two-mask-m29'
METRIC = 'enhance_worker_matvec_duration_seconds_count'


def validate_report(report):
    if (report.get('protocol') != PROTOCOL or report.get('exact_answer_oracle') is not True
            or report.get('completed', 0) <= 0
            or report.get('succeeded') != report.get('completed')
            or report.get('incorrect_answers') != 0
            or report.get('warmup_incorrect_answers') != 0
            or report.get('unstarted_arrivals') != 0
            or any(report.get('errors', {}).values())
            or any(report.get('warmup_errors', {}).values())):
        raise ValueError('encrypted fixture report failed exact-answer/error requirements')


def counter(text):
    values = [float(line.split()[1]) for line in text.splitlines()
              if line.split() and line.split()[0] == METRIC]
    if len(values) != 1 or not math.isfinite(values[0]) or values[0] < 0:
        raise ValueError('missing or invalid CUDA evaluation counter')
    return values[0]


def get(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.read().decode()


def port():
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        return listener.getsockname()[1]


def worker_config(listen, data_dir, management, device):
    """One Enhance worker role. The matrix backend lives in this file."""
    return {
        'management_listen': management,
        'enhance': {'enabled': True, 'settings': {
            'role': 'worker',
            'listen': listen,
            'data_dir': str(data_dir),
            'matvec': {'matvec_backend': 'cuda', 'cuda_device': device},
        }},
    }


def publisher_config(listen, data_dir, workers, management):
    """One Enhance fixture publisher, with independent loopback management."""
    return {
        'management_listen': management,
        'enhance': {'enabled': True, 'settings': {
            'listen': listen, 'data_dir': str(data_dir), 'worker_config': str(workers),
            'isolated_fixture': True, 'fixture_records': 67, 'fixture_append_records': 0,
            'poll_seconds': 1, 'frontier_replicas': 2, 'sealed_shards': 6,
            'zakura_rpc_url': 'http://127.0.0.1:8232', 'prevout_cache_outputs': 250000,
            'capacity_fallback_rows_per_second': 1.0, 'capacity_readiness_seconds': 21600.0,
            'capacity_burst_rows': 4096,
        }},
    }


def fixture_endpoints():
    ports = []
    while len(ports) < 3:
        candidate = port()
        if candidate not in ports:
            ports.append(candidate)
    workers = [f'http://127.0.0.1:{value}' for value in ports[:2]]
    origin = f'http://127.0.0.1:{ports[2]}'
    inventory = {'groups': [{'name': 'isolated', 'replicas': [
        {'name': f'isolated-cuda-{index}', 'url': url}
        for index, url in enumerate(workers, 1)]}]}
    return workers, origin, inventory


def validate_worker_health(health, device):
    if (health.get('protocol') != PROTOCOL or health.get('matvec') != {
            'matvec_backend': 'cuda', 'cuda_device': device} or not health.get('published')):
        raise ValueError('isolated worker is not published native CUDA on the selected device')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--release-dir', type=Path, required=True)
    parser.add_argument('--publisher', type=Path, required=True,
                        help='native pir-publisher; hosts the isolated Enhance application')
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--cuda-device', type=int, default=0)
    parser.add_argument('--timeout', type=int, default=300)
    args = parser.parse_args()
    if args.timeout < 30 or args.cuda_device < 0:
        parser.error('timeout must be at least 30 seconds and device must be nonnegative')
    release = args.release_dir.resolve()
    query_server = release / 'pir-query-server'
    publisher_bin = args.publisher.resolve()
    client = release / 'enhance-pir-cli'
    load = release / 'enhance-pir-load-test'
    binaries = {}
    for path in (query_server, publisher_bin, client, load):
        if not path.is_file():
            parser.error('missing binary ' + str(path))
        binaries[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    evidence = args.evidence_dir.resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    if any(evidence.iterdir()):
        parser.error('evidence directory must be empty')
    root = Path(tempfile.mkdtemp(prefix='enhance-cuda-smoke-'))
    workers, origin, fixture = fixture_endpoints()
    inventory = root / 'workers.json'
    inventory.write_text(json.dumps(fixture))
    processes, logs = [], []
    deadline = time.monotonic() + args.timeout

    def remaining():
        result = deadline - time.monotonic()
        if result <= 0:
            raise TimeoutError('CUDA smoke deadline exceeded')
        return result

    def launch(name, argv):
        log = (evidence / (name + '.log')).open('wb')
        logs.append(log)
        processes.append(subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT,
                                          env=os.environ.copy()))

    try:
        used = {int(url.rsplit(':', 1)[1]) for url in (*workers, origin)}
        for index, worker in enumerate(workers, 1):
            while True:
                management = port()
                if management not in used:
                    used.add(management)
                    break
            data = root / f'worker{index}'
            config = root / f'worker{index}.json'
            config.write_text(json.dumps(worker_config(
                worker.removeprefix('http://'), data, f'127.0.0.1:{management}', args.cuda_device)))
            launch(f'worker{index}', [str(query_server), '--config', str(config)])
        while True:
            management = port()
            if management not in used:
                break
        config = root / 'publisher.json'
        config.write_text(json.dumps(publisher_config(
            origin.removeprefix('http://'), root / 'coordinator', inventory,
            f'127.0.0.1:{management}')))
        launch('coordinator', [str(publisher_bin), '--config', str(config)])
        while True:
            remaining()
            if any(p.poll() is not None for p in processes):
                raise RuntimeError('isolated server exited; inspect smoke logs')
            try:
                health = {f'worker{index}': json.loads(get(worker + '/internal/health'))
                          for index, worker in enumerate(workers, 1)}
                for document in health.values():
                    validate_worker_health(document, args.cuda_device)
                break
            except (OSError, ValueError):
                pass
            time.sleep(min(1, remaining()))
        (evidence / 'health-before.json').write_text(json.dumps(health, indent=2) + '\n')
        for name, document in health.items():
            (evidence / (name + '-health-before.json')).write_text(json.dumps(document, indent=2) + '\n')
        before_by_worker = {f'worker{index}': counter(get(worker + '/internal/metrics'))
                            for index, worker in enumerate(workers, 1)}
        before = sum(before_by_worker.values())
        metadata = subprocess.check_output([str(client), '--server', origin, 'metadata'],
                                           timeout=remaining()).decode()
        (evidence / 'metadata.json').write_text(metadata)
        result = subprocess.run([str(load), '--fixture-oracle', '--server', origin,
            '--parallelism', '1', '--rate', '1', '--warmup', '2s', '--duration', '5s',
            '--max-error-rate', '0', '--fail-fast', '--seed', '0', '--json-out', str(evidence / 'queries.json')],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=remaining())
        (evidence / 'load.log').write_bytes(result.stdout)
        if result.returncode:
            raise RuntimeError('isolated encrypted query client failed')
        report = json.loads((evidence / 'queries.json').read_text())
        validate_report(report)
        after_by_worker = {f'worker{index}': counter(get(worker + '/internal/metrics'))
                           for index, worker in enumerate(workers, 1)}
        after = sum(after_by_worker.values())
        health = {f'worker{index}': json.loads(get(worker + '/internal/health'))
                  for index, worker in enumerate(workers, 1)}
        for name, document in health.items():
            validate_worker_health(document, args.cuda_device)
            (evidence / (name + '-health-after.json')).write_text(json.dumps(document, indent=2) + '\n')
        if (after <= before or any(p.poll() is not None for p in processes)
                or any(after_by_worker[name] < count for name, count in before_by_worker.items())):
            raise ValueError('CUDA evaluations did not increase or worker lost readiness')
        summary = {'passed': True, 'protocol': PROTOCOL, 'cuda_device': args.cuda_device,
                   'binary_sha256': binaries, 'gpu_evaluations_before': before,
                   'gpu_evaluations_after': after, 'gpu_evaluations_by_worker_before': before_by_worker,
                   'gpu_evaluations_by_worker_after': after_by_worker, 'completed': report['completed'],
                   'fixture_only': True, 'production_qualified': False}
        (evidence / 'health-after.json').write_text(json.dumps(health, indent=2) + '\n')
        (evidence / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(json.dumps(summary))
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
        for process in reversed(processes):
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
        for log in logs:
            log.close()
        shutil.rmtree(root)


if __name__ == '__main__':
    main()

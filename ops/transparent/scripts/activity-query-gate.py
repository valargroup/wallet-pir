#!/usr/bin/env python3
"""Bounded candidate query gate with every attempt retained and fail-closed health."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request


def atomic(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--url', required=True)
    parser.add_argument('--qps', type=int, required=True)
    parser.add_argument('--seconds', type=int, required=True)
    parser.add_argument('--processes', type=int, default=1)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--source-sha', required=True)
    args = parser.parse_args()
    if args.qps <= 0 or args.seconds <= 0 or args.processes <= 0 or args.qps % args.processes:
        parser.error('rate must be positive and divisible by process count')
    args.out.mkdir(parents=True, exist_ok=False)
    permit = args.out / 'permit'
    owner = dict(source_sha=args.source_sha, pid=os.getpid(), started_unix=time.time(),
                 driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                 binary_sha256=hashlib.file_digest(args.binary.open('rb'), 'sha256').hexdigest(),
                 fixture_sha256=hashlib.sha256(args.fixture.read_bytes()).hexdigest(),
                 target_qps=args.qps, duration_seconds=args.seconds, processes=args.processes,
                 url=args.url, result=str(args.out / 'result.json'))
    atomic(args.out / 'owner.json', owner)
    children, files, health_failures = [], [], []
    initial_identity = None
    with (args.out / 'health.jsonl').open('x') as health:
        try:
            permit.write_text('allow\n')
            for index in range(args.processes):
                log = (args.out / f'queries-{index}.jsonl').open('x')
                error = (args.out / f'stderr-{index}.log').open('x')
                files.extend([log, error])
                command = [str(args.binary), '--url', args.url, '--fixture', str(args.fixture),
                           '--qps', str(args.qps // args.processes), '--workers', '8',
                           '--seconds', str(args.seconds), '--permit', str(permit)]
                children.append(subprocess.Popen(command, stdout=log, stderr=error))
            owner['child_pids'] = [child.pid for child in children]
            atomic(args.out / 'owner.json', owner)
            while any(child.poll() is None for child in children):
                observation = dict(unix=time.time())
                try:
                    memory = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
                    ratio = int(memory['MemAvailable'].split()[0]) / int(memory['MemTotal'].split()[0])
                    disk = os.statvfs(args.out)
                    observation.update(available_memory_fraction=ratio, available_disk_fraction=disk.f_bavail / disk.f_blocks)
                    with urllib.request.urlopen(args.url + '/v1/ready', timeout=5) as response:
                        observation['ready'] = json.load(response)
                    identity = (observation['ready'].get('binary_sha256'), observation['ready'].get('incarnation'),
                                observation['ready'].get('map_sha256'))
                    if initial_identity is None:
                        initial_identity = identity
                    elif identity != initial_identity:
                        raise RuntimeError('candidate identity changed during frozen query gate')
                    if ratio < .2 or disk.f_bavail / disk.f_blocks < .2 or not observation['ready'].get('ready'):
                        raise RuntimeError('candidate health floor failed')
                except Exception as error:
                    observation['error'] = str(error)
                    health_failures.append(observation)
                health.write(json.dumps(observation) + '\n')
                health.flush()
                if health_failures:
                    break
                permit.write_text('allow\n')
                time.sleep(5)
        finally:
            permit.write_text('deny\n')
            for child in children:
                if child.poll() is None:
                    child.terminate()
            for child in children:
                try:
                    child.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
            for file in files:
                file.close()
    values = []
    for index in range(len(children)):
        for line in (args.out / f'queries-{index}.jsonl').read_text().splitlines():
            value = json.loads(line)
            value['process'] = index
            values.append(value)
    queries = [v for v in values if v.get('event') == 'query']
    errors = [v for v in values if v.get('event') == 'error']
    timings = sorted(v['http_seconds'] for v in queries)
    def percentile(fraction):
        return timings[min(len(timings) - 1, int((len(timings) - 1) * fraction))] if timings else None
    failed_ids = {(v['process'], v.get('logical_id', v.get('sequence'))) for v in errors}
    succeeded_ids = {(v['process'], v.get('logical_id', v.get('sequence'))) for v in queries}
    logical_failures = len(failed_ids - succeeded_ids)
    failed_attempt_ratio = len(errors) / max(1, len(queries) + len(errors))
    rate = len(succeeded_ids) / args.seconds
    failures = []
    if health_failures: failures.append('health floor failed')
    if any(child.returncode for child in children): failures.append('client process failed')
    if logical_failures or any(not v.get('exact') for v in queries): failures.append('logical correctness failure')
    if rate < .95 * args.qps: failures.append('achieved rate below 95 percent')
    if failed_attempt_ratio >= .01: failures.append('transport attempt failures at or above 1 percent')
    if not timings or percentile(.5) >= .7 or percentile(.99) >= 2: failures.append('latency gate failed')
    result = dict(**owner, ended_unix=time.time(), status='failed' if failures else 'passed',
                  failures=failures, exact=len(queries), logical_failures=logical_failures,
                  failed_attempts=len(errors), failed_attempt_ratio=failed_attempt_ratio,
                  completed_qps=rate, p50_seconds=percentile(.5), p99_seconds=percentile(.99),
                  missed_slots=sum(v.get('event') == 'missed_slot' for v in values),
                  child_exit_codes=[child.returncode for child in children],
                  limitations=['Candidate endpoint only; production background traffic is measured separately',
                               'Row equality does not establish extraction correctness or completed-wallet capacity'])
    atomic(args.out / 'result.json', result)
    print(json.dumps(result))
    return bool(failures)


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
"""Observe public HTTPS PIR correctness and latency without certifying rollout."""
import argparse
import hashlib
import json
import math
import re
import subprocess
import time
from pathlib import Path
from urllib.parse import urlsplit


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def assess(report, rate, seconds):
    """Require every scheduled query to be exact and both p99 measures to fit."""
    failures = []
    def count(name):
        value = report.get(name)
        if type(value) is not int or value < 0:
            raise ValueError('invalid count: ' + name)
        return value

    errors = report.get('errors')
    warmup_errors = report.get('warmup_errors')
    for value in (errors, warmup_errors):
        if not isinstance(value, dict) or any(type(n) is not int or n < 0 for n in value.values()):
            raise ValueError('invalid error counts')
    completed = count('completed')
    succeeded = count('succeeded')
    wrong = count('incorrect_answers')
    unstarted = count('unstarted_arrivals')
    if report.get('protocol') != 'ironwood-enhance-pir-v6' or report.get('exact_answer_oracle') is not True:
        failures.append('protocol_or_oracle_mismatch')
    if report.get('offered_qps') != rate:
        failures.append('offered_rate_mismatch')
    if type(report.get('seconds')) not in (int, float) or not math.isfinite(report['seconds']) or report['seconds'] < seconds:
        failures.append('short_window')
    if completed + unstarted != math.floor(rate * seconds):
        failures.append('arrival_count_mismatch')
    if completed != succeeded + wrong + sum(errors.values()):
        failures.append('inconsistent_counts')
    if wrong or count('warmup_incorrect_answers'):
        failures.append('wrong_answer')
    if errors or warmup_errors or unstarted or succeeded != completed:
        failures.append('availability_failure')
    for name in ('successful_p99_ms', 'successful_scheduled_p99_ms', 'scheduled_p99_ms'):
        value = report.get(name)
        if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= 1000:
            failures.append(name + '_missing_or_exceeds_1000')
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--oracle', type=Path, required=True)
    parser.add_argument('--server', required=True)
    parser.add_argument('--source-revision', required=True)
    parser.add_argument('--server-revision', required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--rate', type=float, default=0.2)
    parser.add_argument('--window-seconds', type=int, default=300)
    parser.add_argument('--windows', type=int, default=288)
    args = parser.parse_args()
    origin = urlsplit(args.server)
    if origin.scheme != 'https' or not origin.hostname or origin.username or origin.password or origin.query or origin.fragment:
        parser.error('use a public HTTPS origin without credentials, query, or fragment')
    if not math.isfinite(args.rate) or args.rate <= 0 or args.window_seconds <= 0 or args.windows <= 0:
        parser.error('use positive rate, window and count')
    if math.floor(args.rate * args.window_seconds) == 0:
        parser.error('each window must schedule at least one query')
    if any(not re.fullmatch('[0-9a-f]{40}', revision)
           for revision in (args.source_revision, args.server_revision)):
        parser.error('use full source and deployed revisions')
    binary, oracle = args.binary.resolve(strict=True), args.oracle.resolve(strict=True)
    args.out.mkdir(parents=True, exist_ok=False, mode=0o700)
    manifest = dict(kind='enhance-public-monitor-v1', qualification='unqualified',
                    server=args.server, source_revision=args.source_revision,
                    server_revision=args.server_revision, binary_sha256=digest(binary),
                    oracle_sha256=digest(oracle), rate=args.rate,
                    window_seconds=args.window_seconds, windows=args.windows,
                    started_ns=time.time_ns(), status='running', runs=[],
                    limitations=['Oracle provenance and deployed binary identity require independent verification',
                                 'Synthetic traffic does not measure every pilot wallet request',
                                 'Worker, freshness, wallet-state and incident gates remain separate'])
    write(args.out / 'manifest.json', manifest)
    try:
        for index in range(args.windows):
            if digest(binary) != manifest['binary_sha256'] or digest(oracle) != manifest['oracle_sha256']:
                raise RuntimeError('binary or oracle changed during observation')
            name = f'window-{index:03d}'
            report_path = args.out / f'{name}.json'
            command = [str(binary), '--server', args.server, '--oracle', str(oracle),
                       '--parallelism', '1', '--duration', f'{args.window_seconds}s',
                       '--warmup', '0s', '--rate', str(args.rate), '--seed', str(index),
                       '--max-error-rate', '0', '--slo-p99-ms', '1000',
                       '--json-out', str(report_path.resolve())]
            write(args.out / f'{name}.command.json', command)
            run = dict(name=name, started_ns=time.time_ns(), status='running')
            manifest['runs'].append(run)
            write(args.out / 'manifest.json', manifest)
            with (args.out / f'{name}.log').open('x') as log:
                result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT,
                                        timeout=args.window_seconds + 180)
            run.update(exit_code=result.returncode, finished_ns=time.time_ns())
            if digest(binary) != manifest['binary_sha256'] or digest(oracle) != manifest['oracle_sha256']:
                raise RuntimeError('binary or oracle changed during observation')
            failures = assess(json.loads(report_path.read_text()), args.rate, args.window_seconds) if report_path.exists() else ['missing_report']
            if result.returncode:
                failures.append('load_process_failed')
            run.update(status='failed' if failures else 'passed', failures=failures,
                       report_sha256=digest(report_path) if report_path.exists() else None)
            write(args.out / 'manifest.json', manifest)
            if failures:
                raise RuntimeError(name + ': ' + ', '.join(failures))
        manifest['status'] = ('observation_complete'
                              if args.window_seconds >= 300
                              and args.windows * args.window_seconds >= 86_400
                              and args.rate == 0.2 else 'short_run_only')
    except (Exception, KeyboardInterrupt) as error:
        manifest['status'] = 'failed'
        if manifest['runs'] and manifest['runs'][-1]['status'] == 'running':
            manifest['runs'][-1].update(status='failed', finished_ns=time.time_ns())
        manifest['failure'] = type(error).__name__ + ': ' + str(error)
        raise
    finally:
        manifest['finished_ns'] = time.time_ns()
        write(args.out / 'manifest.json', manifest)


if __name__ == '__main__':
    main()

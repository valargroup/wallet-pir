#!/usr/bin/env python3
"""Record public HTTPS load gates; this never grants production qualification."""
import argparse
import hashlib
import json
import math
import platform
import re
import subprocess
import time
from pathlib import Path
from urllib.parse import urlsplit


PROTOCOL = 'ironwood-enhance-pir-v6'


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


def assess(report, rate, seconds, burst=False):
    """Keep failed and unstarted attempts in the denominator; reject missing evidence."""
    failures = []
    def require(ok, reason):
        if not ok:
            failures.append(reason)
    def count(key):
        value = report.get(key)
        if type(value) is not int or value < 0:
            raise ValueError('invalid count: ' + key)
        return value
    def finite(value):
        return type(value) in (int, float) and math.isfinite(value)
    require(report.get('protocol') == PROTOCOL, 'protocol_mismatch')
    require(report.get('exact_answer_oracle') is True, 'missing_exact_oracle')
    completed, succeeded = count('completed'), count('succeeded')
    wrong, unsent = count('incorrect_answers'), count('unstarted_arrivals')
    errors = report.get('errors')
    warmup = report.get('warmup_errors')
    for value in (errors, warmup):
        if not isinstance(value, dict) or any(type(n) is not int or n < 0 for n in value.values()):
            raise ValueError('invalid error counts')
    require(completed == succeeded + wrong + sum(errors.values()), 'inconsistent_counts')
    require(succeeded > 0, 'no_correct_queries')
    require(wrong == count('warmup_incorrect_answers') == 0, 'incorrect_answers')
    require(finite(report.get('seconds')) and report['seconds'] >= seconds, 'short_measurement')
    require(report.get('offered_qps') == rate, 'offered_rate_mismatch')
    if rate is not None:
        require(completed + unsent == math.floor(rate * seconds), 'arrival_count_mismatch')
    warmup_correct = count('warmup_correct_answers')
    if not burst:
        total = completed + unsent + warmup_correct + count('warmup_incorrect_answers') + sum(warmup.values())
        failed = wrong + unsent + sum(errors.values()) + count('warmup_incorrect_answers') + sum(warmup.values())
        require(total > 0 and failed / total <= 0.001, 'availability_below_99_9_percent')
        # Require each phase as well, so successful warmup cannot hide bad measurement.
        require(completed + unsent > 0 and (wrong + unsent + sum(errors.values())) / (completed + unsent) <= 0.001,
                'measured_availability_below_99_9_percent')
        for key in ('successful_p99_ms', 'successful_scheduled_p99_ms', 'scheduled_p99_ms'):
            require(finite(report.get(key)) and 0 < report[key] <= 1000, key + '_missing_or_exceeds_1000')
    return failures


def write(path, data):
    path.write_text(json.dumps(data, indent=2, allow_nan=False) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--oracle', type=Path, required=True)
    parser.add_argument('--server', required=True)
    parser.add_argument('--source-revision', required=True, help='Source revision of the load binary')
    parser.add_argument('--server-revision', required=True, help='Independently verified deployed revision')
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--step-seconds', type=int, default=1800)
    parser.add_argument('--soak-seconds', type=int, default=21600)
    parser.add_argument('--burst-seconds', type=int, default=300)
    parser.add_argument('--warmup-seconds', type=int, default=10)
    args = parser.parse_args()
    origin = urlsplit(args.server)
    if origin.scheme != 'https' or not origin.hostname or origin.username or origin.password or origin.query or origin.fragment:
        parser.error('use a public HTTPS origin without credentials, query, or fragment')
    if any(n <= 0 for n in (args.step_seconds, args.soak_seconds, args.burst_seconds, args.warmup_seconds)):
        parser.error('durations must be positive')
    if any(not re.fullmatch('[0-9a-f]{40}', s) for s in (args.source_revision, args.server_revision)):
        parser.error('use full source and deployed revisions')
    binary, oracle = args.binary.resolve(strict=True), args.oracle.resolve(strict=True)
    args.out.mkdir(parents=True, exist_ok=False, mode=0o700)
    manifest = dict(kind='enhance-public-qualification', qualification='unqualified',
                    source_revision=args.source_revision, server_revision=args.server_revision,
                    binary_sha256=digest(binary), oracle_sha256=digest(oracle),
                    server=args.server, client_platform=platform.platform(), started_ns=time.time_ns(),
                    required_step_seconds=1800, required_soak_seconds=21600,
                    limitations=['Oracle provenance and live binary identity require independent verification',
                                 'Worker sampling, wallet lifecycle, noise qualification and recovery are separate gates'],
                    runs=[], status='running')
    write(args.out / 'manifest.json', manifest)
    stages = [(f'rate-{rate}', rate, args.step_seconds, False) for rate in (1, 2, 4)]
    stages += [('soak-4', 4, args.soak_seconds, False), ('burst-8', None, args.burst_seconds, True)]
    try:
        for name, rate, seconds, burst in stages:
            command = [str(binary), '--server', args.server, '--oracle', str(oracle),
                       '--parallelism', '8', '--duration', f'{seconds}s',
                       '--warmup', f'{args.warmup_seconds}s', '--seed', '20260923',
                       '--max-error-rate', '1' if burst else '0.001',
                       '--json-out', str((args.out / f'{name}.json').resolve())]
            if rate is not None:
                command += ['--rate', str(rate), '--slo-p99-ms', '1000']
            write(args.out / f'{name}.command.json', command)
            run = dict(name=name, started_ns=time.time_ns(), status='running')
            manifest['runs'].append(run)
            write(args.out / 'manifest.json', manifest)
            with (args.out / f'{name}.log').open('x') as log:
                result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT,
                                        timeout=seconds + args.warmup_seconds + 180)
            run.update(exit_code=result.returncode, finished_ns=time.time_ns())
            report_path = args.out / f'{name}.json'
            failures = assess(json.loads(report_path.read_text()), rate, seconds, burst) if report_path.exists() else ['missing_report']
            if result.returncode:
                failures.append('load_process_failed')
            run.update(status='failed' if failures else 'passed', failures=failures,
                       report_sha256=digest(report_path) if report_path.exists() else None)
            write(args.out / 'manifest.json', manifest)
            if failures:
                raise RuntimeError(name + ': ' + ', '.join(failures))
        manifest['status'] = ('load_gates_passed' if args.step_seconds >= 1800 and args.soak_seconds >= 21600
                              and args.burst_seconds >= 300 else 'short_run_only')
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

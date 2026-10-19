#!/usr/bin/env python3
"""Compare full-history rsync bootstrap with the production HTTPS manager.

Run `compare` on the isolated publisher host after preparing a fixed publication.
The same script, consumer example and independently pinned frozen client must
already be installed on the isolated query host. `receive` owns and stops its
consumer, checks journal-derived oracle samples, and emits one JSON result.
Raw logs and per-run results stay in the explicitly selected new output roots.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import selectors
import shlex
import shutil
import signal
import statistics
import subprocess
import sys
import time
import urllib.request
import ssl


def digest(path):
    with path.open('rb') as file:
        return hashlib.file_digest(file, 'sha256').hexdigest()


def inventory(origin, ca):
    if not origin.startswith('https://'):
        raise ValueError('publisher must use HTTPS')
    context = ssl.create_default_context(cafile=str(ca))
    def get(path):
        with urllib.request.urlopen(origin.rstrip('/') + path, context=context, timeout=30) as response:
            return response.read(16 * 1024 * 1024 + 1)
    current = json.loads(get('/provider/v1/current'))
    if current['eligibility'] != 'canonical':
        raise ValueError('publisher is not canonical')
    body = get('/provider/v1/inventories/' + current['inventory_sha256'])
    if hashlib.sha256(body).hexdigest() != current['inventory_sha256']:
        raise ValueError('publisher inventory digest mismatch')
    return current, json.loads(body)


def artifact_paths(items):
    paths = []
    for item in items:
        path = item['path']
        if not re.fullmatch(r'[A-Za-z0-9_./-]+', path) or any(p in ('', '.', '..') for p in path.split('/')) or PurePosixPath(path).is_absolute():
            raise ValueError('unsafe artifact path')
        if path in paths:
            raise ValueError('duplicate artifact path')
        paths.append(path)
    return paths


def validate_oracle(samples):
    if {sample['zone'] for sample in samples} != {0, 1, 2}:
        raise ValueError('oracle must cover early, middle and late history')
    for sample in samples:
        if not re.fullmatch(r'[0-9a-f]+', sample['script_hex']):
            raise ValueError('invalid oracle script')
        expected = sample['expected']
        if not expected['history']:
            raise ValueError('empty history is not a successful bootstrap oracle')


def check_answer(actual, expected):
    for key, value in expected.items():
        found = actual.get(key)
        if isinstance(value, list) and isinstance(found, list):
            value = sorted(value, key=lambda v: json.dumps(v, sort_keys=True))
            found = sorted(found, key=lambda v: json.dumps(v, sort_keys=True))
        if found != value:
            raise ValueError(f'client answer differs from journal oracle: {key}')


def process_usage(pid):
    """Report this process only; shared cgroup peaks are not process measurements."""
    try:
        status = Path(f'/proc/{pid}/status').read_text()
        ticks = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        peak = re.search(r'^VmHWM:\s+(\d+) kB$', status, re.M)
        return dict(cpu_seconds=(int(ticks[11]) + int(ticks[12])) / os.sysconf('SC_CLK_TCK'),
                    peak_rss_bytes=int(peak[1]) * 1024 if peak else None,
                    peak_rss_scope='process lifetime')
    except FileNotFoundError:
        return dict(cpu_seconds=None, peak_rss_bytes=None)


def network_bytes():
    path = Path('/proc/net/dev')
    if not path.is_file():
        return None
    received = sent = 0
    for line in path.read_text().splitlines()[2:]:
        interface, values = line.split(':', 1)
        if interface.strip() == 'lo':
            continue
        values = values.split()
        received += int(values[0])
        sent += int(values[8])
    return dict(received=received, sent=sent)


def disk_usage(paths):
    seen = set()
    logical = allocated = 0
    for root in paths:
        for path in root.rglob('*'):
            if not path.is_file(): continue
            stat = path.stat()
            if (stat.st_dev, stat.st_ino) in seen: continue
            seen.add((stat.st_dev, stat.st_ino))
            logical += stat.st_size
            allocated += stat.st_blocks * 512
    return dict(logical_file_bytes=logical, allocated_file_bytes=allocated,
                filesystem_free_bytes=shutil.disk_usage(paths[0]).free)


def stop(process):
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    if process.stdout is not None:
        process.stdout.close()


def wait_for_activation(process, state, timeout, interrupt_bytes):
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    deadline = time.monotonic() + timeout
    with selector:
        while time.monotonic() < deadline:
            if interrupt_bytes:
                held = 0
                for path in state.rglob('*'):
                    try:
                        if path.is_file(): held += path.stat().st_size
                    except FileNotFoundError:
                        pass  # The downloader atomically renamed a completed object.
                if held >= interrupt_bytes:
                    return None, dict(on_disk_bytes=held, network=network_bytes(), usage=process_usage(process.pid))
            if selector.select(min(0.1, max(0, deadline - time.monotonic()))):
                line = process.stdout.readline()
                if not line:
                    raise RuntimeError('consumer exited before warm activation; inspect consumer stderr')
                if interrupt_bytes:
                    raise RuntimeError('missed interruption window; bootstrap completed before the fault')
                return json.loads(line), None
    raise TimeoutError('consumer did not report warm activation')


def receive(args):
    root = args.output.resolve()
    if '.local-pir' not in root.parts:
        raise ValueError('consumer output must be under a test .local-pir directory')
    root.mkdir(parents=True, exist_ok=False)
    samples = json.loads(args.oracle.read_text())
    validate_oracle(samples)
    command = [str(args.consumer), '--method', args.method, '--state', str(root / 'state'),
               '--genesis', args.genesis, '--listen', args.listen,
               '--cache-bytes', str(args.cache_bytes), '--download-bytes', str(args.download_bytes)]
    if args.method == 'https':
        command += ['--publisher', args.publisher, '--ca', str(args.ca)]
    else:
        command += ['--publication', str(args.publication)]
    started = time.monotonic()
    deadline = started + args.timeout
    process = None
    completed = False
    try:
        interrupted = None
        for attempt in range(2 if args.interrupt_bytes else 1):
            with (root / f'consumer-{attempt}.stderr').open('w') as errors:
                process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=errors, text=True)
                ready, fault = wait_for_activation(process, root / 'state', max(0, deadline - time.monotonic()),
                                                   args.interrupt_bytes if attempt == 0 else 0)
                if fault is not None:
                    interrupted = fault
                    stop(process)
                    process = None
                    (root / 'interrupted.json').write_text(json.dumps(fault, indent=2) + '\n')
                    continue
                break
        if ready is not None:
            (root / 'activation.json').write_text(json.dumps(ready, indent=2) + '\n')
            status = ready['report']['status']
            if status['active']['map_sha256'] != args.map_sha256 or not status['warm'] or status['invalidated']:
                raise ValueError('consumer activated a different or invalid publication')
            if status['runtimes']['built'] or status['runtimes']['shipped_fallbacks']:
                raise ValueError('consumer built runtimes locally')
            for index, sample in enumerate(samples):
                actual = json.loads(subprocess.check_output(
                    [str(args.client), 'http://' + args.listen, sample['script_hex'], str(sample['birthday'])],
                    text=True, timeout=max(0.01, min(deadline - time.monotonic(), 60))))
                (root / f'answer-{index}.json').write_text(json.dumps(actual, indent=2) + '\n')
                check_answer(actual, sample['expected'])
            result = dict(method=args.method, seconds_to_verified_queries=time.monotonic() - started,
                          activation=ready, consumer_usage=process_usage(process.pid),
                          consumer_sha256=digest(args.consumer), client_sha256=digest(args.client),
                          interrupted=interrupted, network_end=network_bytes(),
                          disk=disk_usage([root, args.publication]),
                          hostname=os.uname().nodename, cpus=os.cpu_count())
            (root / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
            completed = True
            return result
    finally:
        if process is not None:
            stop(process)
        if completed and args.discard_data:
            state = root / 'state'
            if state.is_dir(): shutil.rmtree(state)
            # Only the sibling created for this exact run belongs to us.
            expected = root.parent / (root.name + '-publication')
            if args.publication.resolve() != expected:
                raise ValueError('refusing to remove a publication outside this test run')
            if expected.is_dir(): shutil.rmtree(expected)


def rsync_transfer(command, timeout, interrupt_bytes):
    """Interrupt rsync's real progress stream, then reuse its completed files."""
    interrupted = None
    prefix = ''
    if interrupt_bytes:
        process = subprocess.Popen(command + ['--info=progress2', '--outbuf=L'],
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        output = bytearray()
        deadline = time.monotonic() + timeout
        try:
            with selector:
                while time.monotonic() < deadline:
                    if not selector.select(0.1):
                        continue
                    chunk = os.read(process.stdout.fileno(), 65536)
                    if not chunk:
                        raise RuntimeError('rsync finished before the interruption threshold')
                    output.extend(chunk)
                    progress = re.findall(rb'([\d,]+)\s+\d+%', output)
                    observed = max((int(p.replace(b',', b'')) for p in progress), default=0)
                    if observed >= interrupt_bytes:
                        interrupted = dict(observed_payload_progress_bytes=observed,
                                           protocol_bytes_before_interruption=None)
                        break
                else:
                    raise TimeoutError('rsync did not reach interruption threshold')
        finally:
            stop(process)
        prefix = output.decode(errors='replace') + '\n--- restarted rsync ---\n'
    body = subprocess.check_output(command, text=True, env={**os.environ, 'LC_ALL': 'C'}, timeout=timeout)
    return prefix + body, interrupted


def remote(host, arguments, *, timeout=300):
    subprocess.run(['ssh-reachable', host], check=True, stdout=subprocess.DEVNULL)
    return subprocess.check_output(['ssh-check', '--check-timeout', str(timeout), host, '--', shlex.join(arguments)], text=True)


def compare(args):
    args.output.mkdir(parents=True, exist_ok=False)
    current, prepared = inventory(args.publisher, args.ca)
    files = artifact_paths(prepared['artifacts'])
    # Validate once before measuring; this also warms publisher input equally.
    for item in prepared['artifacts']:
        path = args.publication / item['path']
        if path.stat().st_size != item['bytes'] or digest(path) != item['sha256']:
            raise ValueError('rsync source differs from HTTPS publication')
    file_list = args.output / 'files.txt'
    file_list.write_text('\n'.join(files) + '\n')
    (args.output / 'inventory.json').write_text(json.dumps(prepared, indent=2) + '\n')
    results = []
    deadline = time.monotonic() + 3600
    for pair in range(args.runs + (1 if args.interruptions else 0)):
        for method in (('rsync', 'https') if pair % 2 == 0 else ('https', 'rsync')):
            if time.monotonic() >= deadline:
                raise TimeoutError('comparison reached its one-hour experiment budget')
            interrupted_case = pair == args.runs
            name = ('interrupted-' if interrupted_case else f'{pair + 1}-') + method
            root = args.query_root / name
            publication = root.parent / (name + '-publication')
            started = time.monotonic()
            transfer_seconds = None
            sent = None
            interruption = None
            source_before = {str(pid): process_usage(pid) for pid in (args.publisher_pid, args.tls_pid) if pid}
            # Non-loopback totals include SSH control and unrelated host
            # traffic; they are usable only with the required host isolation.
            network_before = json.loads(remote(args.query_host, ['python3', str(args.remote_runner), 'network']))
            create = ('from pathlib import Path; '
                      f'root=Path({str(root)!r}); publication=Path({str(publication)!r}); '
                      'assert not root.exists(), "consumer directory already exists"; '
                      'publication.mkdir(parents=True, exist_ok=False)')
            remote(args.query_host, ['python3', '-c', create])
            if method == 'rsync':
                transfer_started = time.monotonic()
                transfer, interruption = rsync_transfer([
                    'rsync', '-aL', '--ignore-existing', '--stats', '--files-from', str(file_list),
                    '-e', 'ssh -o BatchMode=yes -o StrictHostKeyChecking=yes',
                    str(args.publication) + '/', args.query_host + ':' + str(publication) + '/',
                ], args.timeout, sum(a['bytes'] for a in prepared['artifacts']) // 2 if interrupted_case else 0)
                transfer_seconds = time.monotonic() - transfer_started
                (args.output / (name + '-rsync.txt')).write_text(transfer)
                matches = re.findall(r'Total bytes sent: ([\d,]+)', transfer)
                counters = [int(value.replace(',', '')) for value in matches]
                sent = sum(counters) if len(counters) == (2 if interrupted_case else 1) else None
                if interruption is not None:
                    interruption['resume_protocol_bytes_sent'] = counters[-1] if counters else None
            command = ['python3', str(args.remote_runner), 'receive', '--output', str(root),
                       '--method', method, '--publication', str(publication), '--consumer', str(args.consumer),
                       '--client', str(args.client), '--oracle', str(args.oracle), '--publisher', args.publisher,
                       '--ca', str(args.query_ca), '--genesis', prepared['genesis_hash'],
                       '--map-sha256', prepared['map_sha256'], '--listen', args.listen,
                       '--cache-bytes', str(args.cache_bytes), '--download-bytes', str(args.download_bytes),
                       '--timeout', str(args.timeout - 15), '--discard-data']
            if interrupted_case and method == 'https':
                command += ['--interrupt-bytes', str(sum(a['bytes'] for a in prepared['artifacts']) // 2)]
            result = json.loads(remote(args.query_host, command, timeout=args.timeout))
            result.update(pair=pair + 1, end_to_end_seconds=time.monotonic() - started,
                          rsync_transfer_seconds=transfer_seconds, rsync_protocol_bytes_sent=sent,
                          sync_seconds=(transfer_seconds or 0) + result['seconds_to_verified_queries'],
                          interrupted_case=interrupted_case, rsync_interruption=interruption,
                          query_network_before=network_before,
                          publisher_processes_before=source_before,
                          publisher_processes_after={str(pid): process_usage(pid) for pid in (args.publisher_pid, args.tls_pid) if pid})
            # SSH probe/process startup are included in this end-to-end number;
            # the consumer's activation and stage numbers remain separate.
            (args.output / (name + '.json')).write_text(json.dumps(result, indent=2) + '\n')
            results.append(result)
            print(json.dumps(result), flush=True)
            after, _ = inventory(args.publisher, args.ca)
            if after != current:
                raise ValueError('publisher changed during the fixed-publication experiment')
    summary = {}
    for method in ('rsync', 'https'):
        seconds = [r['sync_seconds'] for r in results if r['method'] == method and not r['interrupted_case']]
        summary[method] = dict(runs=len(seconds), median_seconds=statistics.median(seconds),
                               min_seconds=min(seconds), max_seconds=max(seconds))
    (args.output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    return summary


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv == ['network']:
        print(json.dumps(network_bytes()), flush=True)
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('compare', 'receive', 'network'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--publication', type=Path, required=True)
    parser.add_argument('--publisher', required=True)
    parser.add_argument('--ca', type=Path, required=True)
    parser.add_argument('--consumer', type=Path, required=True)
    parser.add_argument('--client', type=Path, required=True)
    parser.add_argument('--oracle', type=Path, required=True)
    parser.add_argument('--listen', default='127.0.0.1:18092')
    parser.add_argument('--cache-bytes', type=int, required=True)
    parser.add_argument('--download-bytes', type=int, required=True)
    parser.add_argument('--timeout', type=int, default=300)
    parser.add_argument('--method', choices=('rsync', 'https'))
    parser.add_argument('--genesis')
    parser.add_argument('--map-sha256')
    parser.add_argument('--query-host')
    parser.add_argument('--query-root', type=Path)
    parser.add_argument('--query-ca', type=Path)
    parser.add_argument('--remote-runner', type=Path)
    parser.add_argument('--runs', type=int, default=3)
    parser.add_argument('--interruptions', action='store_true', help='also interrupt and resume each method once')
    parser.add_argument('--interrupt-bytes', type=int, default=0)
    parser.add_argument('--publisher-pid', type=int)
    parser.add_argument('--tls-pid', type=int)
    parser.add_argument('--discard-data', action='store_true', help='remove successful consumer copies after retaining receipts')
    args = parser.parse_args(argv)
    if not 15 < args.timeout <= 300 or args.runs < 1 or args.interrupt_bytes < 0:
        parser.error('timeout must be 16–300 seconds and runs must be positive')
    needed = ('method', 'genesis', 'map_sha256') if args.action == 'receive' else ('query_host', 'query_root', 'query_ca', 'remote_runner')
    if any(getattr(args, key) is None for key in needed):
        parser.error('missing action arguments: ' + ', '.join(needed))
    if args.action == 'compare' and (not args.query_root.is_absolute() or '.local-pir' not in args.query_root.parts):
        parser.error('query root must be an absolute test .local-pir directory')
    if '.local-pir' not in args.output.resolve().parts:
        parser.error('output must be under a test .local-pir directory')
    if args.action == 'receive':
        def interrupted(signum, _frame):
            raise SystemExit(128 + signum)
        for signum in (signal.SIGHUP, signal.SIGTERM):
            signal.signal(signum, interrupted)
    print(json.dumps(receive(args) if args.action == 'receive' else compare(args)), flush=True)


if __name__ == '__main__':
    main()

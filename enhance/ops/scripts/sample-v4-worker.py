#!/usr/bin/env python3
"""Collect one read-only v4 qualification sample; never issue a passing receipt.

Counters and peaks are cumulative and are not reset. Consumers must retain error
samples, compare boot/process identities, and calculate deltas over their window.
The JSON `error` field, not exit zero, determines whether collection succeeded.
"""
from decimal import Decimal
import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import time
import urllib.request

SERVICE = 'enhance-pir-v4-worker.service'
PROTOCOL = 'ironwood-enhance-pir-v5'


def counters(value):
    result = {}
    for line in value.splitlines():
        name, number = line.split()
        if name in result or not number.isdecimal():
            raise ValueError('invalid kernel counter')
        result[name] = int(number)
    return result


def kilobytes(value):
    result = {}
    for line in value.splitlines():
        match = re.fullmatch(r'([A-Za-z_]+):\s+([0-9]+) kB', line)
        if match:
            if match[1] in result:
                raise ValueError('duplicate proc memory field')
            result[match[1]] = int(match[2]) * 1024
    return result


def start_ticks(path):
    # comm may contain spaces and parentheses; field 3 follows its final ')'.
    tail = path.read_text().rsplit(')', 1)[1].split()
    return int(tail[19])


def service_properties():
    result = subprocess.run(['systemctl', 'show', SERVICE,
        '--property=MainPID,ControlGroup,ActiveState,SubState,NRestarts,MemoryHigh,MemoryMax,MemorySwapMax'],
        capture_output=True, text=True, timeout=10)
    if result.returncode:
        raise RuntimeError('service inspection failed')
    return dict(line.split('=', 1) for line in result.stdout.splitlines())


SAMPLE_VERSION = 2
CORE_METRICS = {'up', 'epoch', 'revision', 'retained_generations', 'candidate_present',
                'preparation_busy', 'queries_in_flight', 'memory_sample_available', 'memory_model_available'}
MEMORY_METRICS = {'live_database_bytes', 'published_database_bytes', 'latest_database_bytes',
                  'candidate_database_bytes', 'query_only_database_bytes', 'source_reclamation_database_bytes'}
MODEL_METRICS = {'memory_model_uses_candidate', 'model_union_database_bytes', 'model_growth_reserved_bytes',
                 'model_transition_reserved_bytes', 'model_overhead_bytes', 'model_total_bytes',
                 'model_limit_bytes', 'model_within_limit'}
FLAG_METRICS = {'up', 'candidate_present', 'preparation_busy', 'memory_sample_available',
                'memory_model_available', 'memory_model_uses_candidate', 'model_within_limit'}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def worker_get(address, path, limit, port=8291):
    if type(port) is not int or not 1 <= port <= 65535:
        raise ValueError("invalid worker port")
    address = str(ipaddress.IPv4Address(address))
    with urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect()).open(
            f'http://{address}:{port}/internal/v4/{path}', timeout=3) as response:
        data = response.read(limit + 1)
    if len(data) > limit:
        raise ValueError('oversized worker response')
    return data


def health(address, port=8291):
    value = json.loads(worker_get(address, 'health', 1024 * 1024, port))
    if not isinstance(value, dict) or value.get('protocol') != PROTOCOL:
        raise ValueError('worker health protocol differs')
    return {key: value[key] for key in ('protocol', 'incarnation', 'epoch', 'revision', 'published', 'resident_database_bytes')} | {
        'candidate_present': value['candidate'] is not None,
        'candidate_sha256': hashlib.sha256(json.dumps(value['candidate'], sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest() if value['candidate'] is not None else None}


def parse_runtime_metrics(data):
    """Parse the worker's bounded scalar contract, not arbitrary Prometheus labels.

    Integer gauges beyond exact float64 integer range cannot support byte evidence.
    Unknown/labeled, duplicate, nonfinite or incomplete samples are rejected.
    """
    if len(data) > 65536:
        raise ValueError('oversized runtime metrics')
    values = {}
    for line in data.decode('utf-8').splitlines():
        if not line or line.startswith('#'):
            continue
        match = re.fullmatch(r'enhance_v4_worker_([a-z_]+) ([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?)', line)
        if not match or match[1] in values or match[1] not in CORE_METRICS | MEMORY_METRICS | MODEL_METRICS:
            raise ValueError('invalid runtime metric sample')
        try:
            number = Decimal(match[2])
            if not 0 <= number <= 2**53 or number != number.to_integral_value():
                raise ValueError('runtime metric is not an exact nonnegative integer')
        except ArithmeticError:
            raise ValueError('invalid runtime numeric representation') from None
        values[match[1]] = int(number)
    if not CORE_METRICS <= values.keys() or any(values[key] not in (0, 1) for key in FLAG_METRICS & values.keys()):
        raise ValueError('missing core metrics or invalid availability flag')
    if values['up'] != 1 or values['queries_in_flight'] > 2 or values['retained_generations'] > 5:
        raise ValueError('runtime state violates worker limits')
    expected = CORE_METRICS.copy()
    if values['memory_sample_available']:
        expected |= MEMORY_METRICS
    if values['memory_model_available']:
        if not values['memory_sample_available']:
            raise ValueError('model has no memory sample')
        expected |= MODEL_METRICS
    if values.keys() != expected:
        raise ValueError('availability and runtime samples differ')
    return values


def runtime_metrics(address, port=8291):
    started = time.monotonic_ns()
    data = worker_get(address, 'metrics', 65536, port)
    return {'values': parse_runtime_metrics(data), 'exposition_sha256': hashlib.sha256(data).hexdigest(),
            'started_monotonic_ns': started, 'finished_monotonic_ns': time.monotonic_ns()}


def runtime_consistent(before, metrics, after):
    identity = ('incarnation', 'epoch', 'revision', 'published', 'candidate_present', 'candidate_sha256')
    return (all(before[key] == after[key] for key in identity)
            and metrics['epoch'] == after['epoch'] and metrics['revision'] == after['revision']
            and metrics['candidate_present'] == int(after['candidate_present'])
            and metrics['retained_generations'] == len(after['published']))


def collect(root=Path('/'), properties=None):
    started = time.monotonic_ns()
    boot = root / 'proc/sys/kernel/random/boot_id'
    boot_id = boot.read_text().strip()
    memory = kilobytes((root / 'proc/meminfo').read_text())
    for field in ('MemTotal', 'MemAvailable', 'SwapTotal', 'SwapFree'):
        if field not in memory:
            raise ValueError('missing host memory field')
    sample = {'kind': 'enhance-v4-hardware-sample', 'version': SAMPLE_VERSION, 'error': None,
              'wall_time_ns': time.time_ns(), 'started_monotonic_ns': started, 'boot_id': boot_id,
              'hostname': socket.gethostname().split('.')[0],
              'host_id_sha256': hashlib.sha256((root / 'etc/machine-id').read_bytes()).hexdigest(),
              'host_memory_bytes': memory, 'page_size_bytes': os.sysconf('SC_PAGE_SIZE')}
    vmstat = counters((root / 'proc/vmstat').read_text())
    sample['host_swap_pages'] = {key: vmstat[key] for key in ('pswpin', 'pswpout')}
    properties = service_properties() if properties is None else properties
    sample['service'] = properties
    if properties.get('ActiveState') != 'active' or int(properties.get('MainPID', '0')) <= 0:
        sample.update(error='service_not_active', finished_monotonic_ns=time.monotonic_ns())
        return sample
    if properties['ControlGroup'] != '/system.slice/' + SERVICE:
        raise ValueError('unexpected service cgroup')
    receipt = json.loads((root / 'srv/enhance-pir-v4/bootstrap.json').read_text())
    if receipt.get('worker_name') != sample['hostname'] or receipt.get('phase') != 'bootstrapped':
        raise ValueError('bootstrap receipt does not describe this host')
    pid = int(properties['MainPID'])
    process = root / 'proc' / str(pid)
    identity = start_ticks(process / 'stat')
    binary = (process / 'exe').resolve()
    relative = str(binary.relative_to(root.resolve()))
    match = re.fullmatch('opt/enhance-pir-v4/releases/([0-9a-f]{40}|[0-9a-f]{64})/enhance-pir-v4', relative)
    binary_sha256 = hashlib.sha256(binary.read_bytes()).hexdigest()
    expected_directory = receipt['revision'] if match and len(match[1]) == 40 else binary_sha256
    if not match or match[1] != expected_directory or binary_sha256 != receipt['binary_sha256']:
        raise ValueError('running binary differs from bootstrap identity')
    port = receipt.get('private_port', 8291)
    if type(port) is not int or not 1 <= port <= 65535:
        raise ValueError('invalid worker port in bootstrap receipt')
    sample.update(binary_sha256=binary_sha256, source_revision=receipt['revision'], worker_private_ipv4=receipt['private_ipv4'], worker_private_port=port,
                  bootstrap_manifest_sha256=receipt['manifest_sha256'], main_pid=pid, main_start_ticks=identity)
    group = root / 'sys/fs/cgroup' / properties['ControlGroup'].lstrip('/')
    pids = sorted(int(p) for p in (group / 'cgroup.procs').read_text().split())
    if pid not in pids or len(pids) != len(set(pids)):
        raise ValueError('main process missing from its cgroup')
    memory_files = ('memory.current', 'memory.peak', 'memory.high', 'memory.max', 'memory.swap.current', 'memory.swap.max')
    sample['cgroup_bytes'] = {name: int((group / name).read_text()) for name in memory_files}
    for name in ('memory.events', 'memory.events.local', 'memory.stat', 'memory.swap.events', 'cpu.stat'):
        sample[name] = counters((group / name).read_text())
    if (not {'high', 'max', 'oom', 'oom_kill'} <= sample['memory.events'].keys()
            or not {'high', 'max', 'oom', 'oom_kill'} <= sample['memory.events.local'].keys()
            or not {'anon', 'file', 'shmem', 'kernel'} <= sample['memory.stat'].keys()):
        raise ValueError('missing required cgroup memory accounting')
    peak = group / 'memory.swap.peak'
    sample['cgroup_bytes']['memory.swap.peak'] = int(peak.read_text()) if peak.exists() else None
    sample['memory.pressure'] = {}
    for line in (group / 'memory.pressure').read_text().splitlines():
        kind, *entries = line.split()
        values = dict(entry.split('=', 1) for entry in entries)
        sample['memory.pressure'][kind] = {key: int(value) if key == 'total' else float(value) for key, value in values.items()}
    if (set(sample['memory.pressure']) != {'some', 'full'}
            or any(set(values) != {'avg10', 'avg60', 'avg300', 'total'}
                   or any(not math.isfinite(v) or v < 0 for v in values.values())
                   for values in sample['memory.pressure'].values())):
        raise ValueError('missing or invalid memory pressure accounting')
    for property_name, kernel, key in [('MemoryHigh', 'memory.high', 'memory_high_bytes'),
                                       ('MemoryMax', 'memory.max', 'memory_max_bytes'),
                                       ('MemorySwapMax', 'memory.swap.max', 'memory_swap_max_bytes')]:
        if int(properties[property_name]) != receipt['limits'][key] or sample['cgroup_bytes'][kernel] != receipt['limits'][key]:
            raise ValueError('effective limits changed since bootstrap')
    sample['process_memory'] = []
    for child_pid in pids:
        child = root / 'proc' / str(child_pid)
        child_start = start_ticks(child / 'stat')
        rollup = kilobytes((child / 'smaps_rollup').read_text())
        if not {'Rss', 'Pss', 'Pss_Anon', 'Pss_File', 'Pss_Shmem', 'Swap'} <= rollup.keys():
            raise ValueError('missing accurate process memory evidence')
        status_memory = kilobytes((child / 'status').read_text())
        if not {'VmHWM', 'VmRSS', 'VmSwap'} <= status_memory.keys():
            raise ValueError('missing process high-water accounting')
        if start_ticks(child / 'stat') != child_start:
            raise ValueError('process identity changed during sampling')
        sample['process_memory'].append({'pid': child_pid, 'start_ticks': child_start, 'bytes': rollup, 'status_bytes': status_memory})
    sample['host_memory_finished_monotonic_ns'] = time.monotonic_ns()
    sample['worker'] = health(receipt['private_ipv4'], port)
    try:
        runtime = runtime_metrics(receipt['private_ipv4'], port)
        after = health(receipt['private_ipv4'], port)
        runtime['state_consistent'] = runtime_consistent(sample['worker'], runtime['values'], after)
        sample['worker_after_metrics'] = after
        sample['runtime_metrics'] = runtime
    except (ValueError, KeyError, TypeError, OSError):
        # Keep the independently collected host evidence when the runtime scrape
        # fails; never turn a missing or invalid memory sample into zero bytes.
        sample['runtime_metrics'] = {'error': 'runtime_metrics_failed'}
        sample['error'] = 'runtime_metrics_failed'
    sample['disk_free_bytes'] = shutil.disk_usage(root / 'srv/enhance-pir-v4/worker').free
    if (start_ticks(process / 'stat') != identity or boot.read_text().strip() != boot_id
            or sorted(int(p) for p in (group / 'cgroup.procs').read_text().split()) != pids
            or any(start_ticks(root / 'proc' / str(p['pid']) / 'stat') != p['start_ticks'] for p in sample['process_memory'])):
        raise ValueError('host or cgroup membership changed during sampling')
    sample['finished_monotonic_ns'] = time.monotonic_ns()
    return sample


def main():
    started = time.monotonic_ns()
    try:
        sample = collect()
    except (ValueError, KeyError, IndexError, TypeError, OSError, RuntimeError, subprocess.SubprocessError):
        # Do not substitute zeroes or omit the failed interval. No exception text:
        # paths and HTTP errors may include operational details.
        sample = {'kind': 'enhance-v4-hardware-sample', 'version': SAMPLE_VERSION, 'error': 'collection_failed',
                  'wall_time_ns': time.time_ns(), 'started_monotonic_ns': started,
                  'finished_monotonic_ns': time.monotonic_ns()}
    print(json.dumps(sample, sort_keys=True, allow_nan=False))


if __name__ == '__main__':
    main()

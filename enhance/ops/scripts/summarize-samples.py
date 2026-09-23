#!/usr/bin/env python3
"""Summarize an immutable worker-local trace snapshot; never qualify hardware.

Copy samples.jsonl before invoking this command. Running captures have no final
manifest hash; the output hashes exactly the bytes consumed. A requested wall
window checks endpoint coverage but does not establish cross-host clock alignment.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path

GUARD = 7 * 1024**3 - 512 * 1024**2


def pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError('duplicate JSON key')
        result[key] = value
    return result


def reject_constant(value):
    raise ValueError('nonfinite JSON value')


def integer(value):
    if type(value) is not int or value < 0:
        raise ValueError('expected nonnegative integer')
    return value


def summarize(path, max_gap_seconds=3, window=None):
    if not math.isfinite(max_gap_seconds) or max_gap_seconds <= 0:
        raise ValueError('invalid maximum gap')
    if window is not None and not 0 <= window[0] < window[1]:
        raise ValueError('invalid measurement window')
    result = dict(kind='enhance-local-sample-summary', qualification='unqualified',
                  records=0, valid_samples=0, error_samples=0, findings=[],
                  first_wall_ns=None, last_wall_ns=None, max_gap_seconds=0,
                  max_sampled_rss_plus_kernel_bytes=0, resident_guard_bytes=GUARD,
                  max_cgroup_current_bytes=0, max_cgroup_peak_bytes=0,
                  max_swap_current_bytes=0, min_disk_free_bytes=None,
                  runtime_unavailable_samples=0, runtime_inconsistent_samples=0,
                  max_model_total_bytes=None, max_live_database_bytes=None,
                  max_pressure_avg10={}, pressure_total_delta_us={})
    identities = set()
    first_counters, previous_counters = {}, {}
    previous_wall = None
    digest = hashlib.sha256()
    first_pressure, last_pressure = {}, {}

    def finding(name):
        if name not in result['findings']:
            result['findings'].append(name)

    with path.open('rb') as handle:
        while line := handle.readline(1024 * 1024 + 1):
            if len(line) > 1024 * 1024 or not line.endswith(b'\n'):
                raise ValueError('oversized or incomplete trace record')
            digest.update(line)
            entry = json.loads(line, object_pairs_hook=pairs, parse_constant=reject_constant)
            sample = entry['sample']
            wall = integer(sample['wall_time_ns'])
            if previous_wall is not None:
                if wall <= previous_wall:
                    finding('nonmonotonic_wall_clock')
                gap = (wall - previous_wall) / 1e9
                result['max_gap_seconds'] = max(result['max_gap_seconds'], gap)
                if gap > max_gap_seconds:
                    finding('sampling_gap')
            previous_wall = wall
            result['records'] += 1
            if result['first_wall_ns'] is None:
                result['first_wall_ns'] = wall
            result['last_wall_ns'] = wall
            if sample['error'] is not None:
                result['error_samples'] += 1
                finding('sample_errors')
                continue
            if sample['version'] != 2 or sample['kind'] != 'enhance-hardware-sample':
                raise ValueError('unsupported sample schema')
            identities.add(tuple(sample[k] for k in ('host_id_sha256', 'boot_id',
                'main_pid', 'main_start_ticks', 'source_revision', 'binary_sha256')))
            if len(identities) > 1:
                finding('host_process_or_binary_changed')
            memory = sample['process_memory']
            if not memory or not any(p['pid'] == sample['main_pid'] for p in memory):
                raise ValueError('missing main process memory')
            resident = sum(integer(p['bytes']['Rss']) for p in memory) + integer(sample['memory.stat']['kernel'])
            result['max_sampled_rss_plus_kernel_bytes'] = max(result['max_sampled_rss_plus_kernel_bytes'], resident)
            if resident > GUARD:
                finding('observed_resident_guard_violation')
            group = sample['cgroup_bytes']
            for out, key in [('max_cgroup_current_bytes', 'memory.current'),
                             ('max_cgroup_peak_bytes', 'memory.peak'),
                             ('max_swap_current_bytes', 'memory.swap.current')]:
                result[out] = max(result[out], integer(group[key]))
            if integer(group['memory.peak']) > integer(group['memory.max']):
                finding('cgroup_peak_exceeds_hard_limit')
            if group['memory.swap.current']:
                finding('worker_swap_observed')
            free = integer(sample['disk_free_bytes'])
            result['min_disk_free_bytes'] = free if result['min_disk_free_bytes'] is None else min(free, result['min_disk_free_bytes'])
            for kind, pressure in sample.get('memory.pressure', {}).items():
                average = pressure['avg10']
                if type(average) not in (int, float) or not math.isfinite(average) or not 0 <= average <= 100:
                    raise ValueError('invalid memory pressure')
                total = integer(pressure['total'])
                first_pressure.setdefault(kind, total)
                if kind in last_pressure and total < last_pressure[kind]:
                    finding('pressure_counter_reset')
                last_pressure[kind] = total
                result['max_pressure_avg10'][kind] = max(result['max_pressure_avg10'].get(kind, 0), average)
            for category in ('memory.events', 'host_swap_pages'):
                counters = {k: integer(v) for k, v in sample[category].items()}
                if category in previous_counters:
                    before = previous_counters[category]
                    if counters.keys() != before.keys() or any(counters[k] < before[k] for k in before if k in counters):
                        finding('counters_reset_or_changed')
                    if category == 'memory.events' and any(counters[k] > before[k] for k in ('oom', 'oom_kill')):
                        finding('worker_oom_event')
                first_counters.setdefault(category, counters)
                previous_counters[category] = counters
            runtime = sample['runtime_metrics']
            values = runtime['values']
            result['runtime_unavailable_samples'] += values['memory_sample_available'] == 0
            result['runtime_inconsistent_samples'] += not runtime['state_consistent']
            for out, key in [('max_model_total_bytes', 'model_total_bytes'), ('max_live_database_bytes', 'live_database_bytes')]:
                if key in values:
                    value = integer(values[key])
                    result[out] = value if result[out] is None else max(value, result[out])
            result['valid_samples'] += 1
    if not result['valid_samples']:
        finding('no_valid_samples')
    if window is not None and (result['first_wall_ns'] is None or result['first_wall_ns'] > window[0] or result['last_wall_ns'] < window[1]):
        finding('measurement_window_not_covered')
    result['trace_sha256'] = digest.hexdigest()
    result['pressure_total_delta_us'] = {k: total - first_pressure[k] for k, total in last_pressure.items()}
    result['measurement_window_wall_ns'] = window
    result['maximum_allowed_gap_seconds'] = max_gap_seconds
    result['identities'] = [dict(zip(('host_id_sha256', 'boot_id', 'main_pid', 'main_start_ticks', 'source_revision', 'binary_sha256'), identity)) for identity in sorted(identities)]
    result['counter_deltas'] = {category: {k: value - first_counters[category].get(k, value) for k, value in counters.items()} for category, counters in previous_counters.items()}
    result['sampled_guard_headroom_bytes'] = GUARD - result['max_sampled_rss_plus_kernel_bytes'] if result['valid_samples'] else None
    result['limitations'] = ['Sampled peaks do not bound between-sample resident memory.',
        'Cgroup peaks and counters may include time before this capture.',
        'This report does not validate workload placement, duration, replication, latency or host provenance.',
        'A window uses wall clocks; clock alignment requires separate evidence.']
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('trace', type=Path)
    parser.add_argument('--max-gap-seconds', type=float, default=3)
    parser.add_argument('--window', type=int, nargs=2, metavar=('START_NS', 'END_NS'))
    args = parser.parse_args()
    print(json.dumps(summarize(args.trace, args.max_gap_seconds, args.window), indent=2, allow_nan=False))


if __name__ == '__main__':
    main()

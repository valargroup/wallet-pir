#!/usr/bin/env python3
"""Summarizes bench.sh output into summary.json.

    analyze.py OUT_DIR > summary.json

Cycles are the controller's `cycle` timeline events after bootstrap; the first
replay cycle is reported apart, because the worker starts with nothing active
and its first prepare builds the bootstrap candidate too. Worker CPU per cycle
is the worker's utime + stime over the steady cycles' span, divided by their
count; the worker answers no queries, so all of it is preparation.
"""
import json
import statistics
import sys
from pathlib import Path

MIB = 1 << 20


def pct(values, q):
    values = sorted(values)
    if not values:
        return None
    return values[min(len(values) - 1, max(0, round(q * (len(values) - 1))))]


def dist(values):
    values = [v for v in values if v is not None]
    if not values:
        return None
    return {'n': len(values), 'min': min(values), 'p50': pct(values, 0.5), 'p95': pct(values, 0.95),
            'max': max(values), 'mean': round(statistics.fmean(values), 3)}


def lines(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def cpu_seconds(samples, start_ms, end_ms):
    inside = [s for s in samples if start_ms <= s[0] <= end_ms]
    if len(inside) < 2:
        return None
    return (inside[-1][1] - inside[0][1]) / inside[0][2]


def run(out, name):
    cycles = [e for e in lines(out / ('%s-timeline.jsonl' % name)) if e.get('kind') == 'cycle']
    replay = [c for c in cycles if c.get('workers')]
    steady = replay[1:]
    samples = []
    for line in (out / ('%s-worker-cpu.tsv' % name)).read_text().splitlines()[1:]:
        ms, ticks, hz = (int(x) for x in line.split('\t'))
        samples.append((ms, ticks, hz))
    start = min(c['activated_ms'] - c['cycle_ms'] for c in steady)
    end = max(c['activated_ms'] for c in steady) + 500
    cpu = cpu_seconds(samples, start, end)
    workers = [c['workers'][0] for c in steady]
    files = [f for c in steady for f in (c.get('shipped') or [])]
    summary = {
        'cycles': len(steady),
        'first_cycle': {k: replay[0].get(k) for k in ('prepare_ms', 'prebuild_ms', 'cycle_ms')},
        'recent_records': dist([c['recent']['records'] for c in steady]),
        'prebuild_ms': dist([c.get('prebuild_ms') for c in steady]),
        'prebuild_failures': max(c.get('prebuild_failures', 0) for c in steady),
        'shipped_mib_per_cycle': dist([c['shipped_bytes'] / MIB for c in steady if c.get('shipped_bytes')]),
        'shipped_files_per_cycle': dist([len(c['shipped']) for c in steady if c.get('shipped')]),
        'shipped_file_mib': {table: dist([f['bytes'] / MIB for f in files if f['table'] == table])
                             for table in sorted({f['table'] for f in files})},
        'controller_build_s_per_file': dist([f['build_seconds'] for f in files]),
        'controller_write_s_per_file': dist([f['write_seconds'] for f in files]),
        'candidate_ms': dist([c['candidate_ms'] for c in steady]),
        'prepare_ms': dist([c['prepare_ms'] for c in steady]),
        'worker_prepare_s': dist([w.get('seconds') for w in workers]),
        'worker_built': dist([w.get('built') for w in workers]),
        'worker_shipped': dist([w.get('shipped') for w in workers]),
        'worker_shipped_fallbacks_total': sum(w.get('shipped_fallbacks') or 0 for w in workers),
        'worker_self_check_ms': dist([w.get('self_check_ms') for w in workers]),
        'cycle_ms': dist([c['cycle_ms'] for c in steady]),
        'worker_cpu_seconds_total': cpu,
        'worker_cpu_seconds_per_cycle': None if cpu is None else round(cpu / len(steady), 3),
    }
    return summary


def main():
    out = Path(sys.argv[1])
    rsync = lines(out / 'rsync.jsonl')
    stats = {}
    for path in sorted(out.glob('rsync-*-*.stats')):
        exclude = path.stem.split('-')[1]
        for line in path.read_text().splitlines():
            if line.startswith('Total transferred file size:'):
                stats.setdefault(exclude, set()).add(int(line.split(':')[1].split()[0].replace(',', '')))
    result = {
        'baseline': run(out, 'baseline'),
        'shipped': run(out, 'shipped'),
        'rsync_one_block_delta': {
            exclude: {'seconds': dist([r['seconds'] for r in rsync if r['exclude'] == exclude]),
                      'transferred_mib': sorted(round(b / MIB, 3) for b in stats.get(exclude, []))}
            for exclude in ('none', 'runtimes')
        },
    }
    json.dump(result, sys.stdout, indent=1, sort_keys=True)
    print()


if __name__ == '__main__':
    main()

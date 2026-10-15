#!/usr/bin/env python3
"""Summarizes each arm's raw records and evaluates the paired acceptance.

    analyze.py WORK [--ticks-per-second 100] > summary.json

Per arm, over the measured window (from the first steady block's schedule to
the arm's `steady_end`):

- CPU seconds of the publisher, the worker and the load generator (each
  process tree), and of the worker's runtime build pool threads, where
  builds and shipped self-checks run;
- the CPU each measured CPU group spent outside the arm's processes, which
  is how contamination by other work on the host shows;
- peak RSS per role, the lowest host available memory, worker readiness;
- query outcomes, unstarted queries and latency quantiles, overall and per
  class;
- activation delay per steady height: from the block's append to the
  activation of the first publication covering it;
- per cycle: the publisher's preparation report, the bytes staging sent and
  how the worker produced its runtimes (shipped, refused, restored, built);
- the worker metrics deltas over the window, and the restart.

The acceptance follows the plan: worker CPU and worker build-pool CPU lower
with shipping in every pair, every answer exact, no failure and no
unstarted query, no refused shipped runtime and no worker build in a
shipping arm's window, the freshness, readiness and memory gates in every
arm, and each worst latency and activation-delay quantile with shipping no
higher than the worst without. An arm whose measured groups saw more than
the contamination bound of foreign CPU is reported contaminated, and a
pair with a contaminated arm cannot qualify.
"""
import argparse
import json
import math
import re
from pathlib import Path

FRESHNESS_SECONDS = 30.0
MEMORY_HEADROOM = 0.20
CONTAMINATION_FRACTION = 0.02
GROUPS = {'publisher': [0, 1], 'worker': [2, 3], 'load': [4, 5]}
PAIRS = [('pair1-off', 'pair1-on'), ('pair2-off', 'pair2-on'), ('pair3-off', 'pair3-on')]


def lines(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def quantile(values, q):
    values = sorted(values)
    if not values:
        return None
    rank = min(len(values), max(1, math.ceil(q * len(values))))
    return values[rank - 1]


def quantiles(values):
    return {'n': len(values), 'p50': quantile(values, 0.5), 'p95': quantile(values, 0.95),
            'p99': quantile(values, 0.99), 'max': max(values) if values else None}


def window(samples, start, end):
    inside = [s for s in samples if start <= s['unix'] <= end]
    return inside


def delta(first, last, pick):
    a, b = pick(first), pick(last)
    if a is None or b is None:
        return None
    return b - a


def arm(directory, tps):
    raw = directory / 'raw'
    events = {e['event']: e for e in lines(raw / 'arm-events.jsonl')}
    if 'steady_end' not in events:
        return {'name': directory.name, 'complete': False,
                'failure': events.get('arm_failed', {}).get('error')}
    start, end = events['steady_start']['unix'], events['steady_end']['unix']
    samples = [s for s in window(lines(raw / 'samples.jsonl'), start, end) if 'error' not in s]
    if len(samples) < 2:
        return {'name': directory.name, 'complete': False, 'failure': 'no process samples in the window'}
    first, last = samples[0], samples[-1]
    seconds = last['unix'] - first['unix']
    cpu = {}
    for role in ('publisher', 'worker', 'load', 'chain'):
        cpu[role] = delta(first, last, lambda s: (s['roles'].get(role) or {}).get('ticks'))
        cpu[role] = cpu[role] / tps if cpu[role] is not None else None

    def build_ticks(sample):
        return sum(v for k, v in (sample.get('worker_threads') or {}).items() if k.startswith('runtime-build'))

    cpu['worker_build_pool'] = (build_ticks(last) - build_ticks(first)) / tps
    # Foreign CPU on each measured group: busy ticks of its CPUs less what
    # the processes assigned to it used.
    foreign = {}
    for group, ids in GROUPS.items():
        busy = sum(last['cpu_busy'][str(c)] - first['cpu_busy'][str(c)] for c in ids) / tps
        own = {'publisher': cpu['publisher'], 'worker': cpu['worker'], 'load': cpu['load']}[group] or 0
        foreign[group] = {'busy_seconds': busy, 'own_seconds': own, 'foreign_seconds': max(0.0, busy - own),
                          'foreign_fraction': max(0.0, busy - own) / (seconds * len(ids))}
    contaminated = any(f['foreign_fraction'] > CONTAMINATION_FRACTION for f in foreign.values())
    rss = {role: max(((s['roles'].get(role) or {}).get('rss') or 0) for s in samples)
           for role in ('publisher', 'worker', 'load')}
    available = min(s['MemAvailable'] for s in samples)
    total = samples[0]['MemTotal']
    ready = [s['ready'] for s in samples]
    readiness = sum(1 for r in ready if r == 200) / len(ready)

    queries = lines(raw / 'queries.jsonl')
    outcomes = {}
    for q in queries:
        outcomes[q.get('outcome', 'missing')] = outcomes.get(q.get('outcome', 'missing'), 0) + 1
    exact = [q for q in queries if q.get('outcome') == 'exact']
    latency = quantiles([q['latency_ms'] for q in exact])
    by_class = {}
    for q in exact:
        by_class.setdefault(q['class'], []).append(q['latency_ms'])
    by_class = {k: quantiles(v) for k, v in sorted(by_class.items())}
    populated = sum(1 for q in exact if q.get('populated'))

    blocks = {b['height']: b for b in lines(raw / 'blocks.jsonl')}
    steady = sorted(h for h, b in blocks.items() if b['appended_unix'] >= start - 1)
    cycles = []
    text = re.sub(r'\x1b\[[0-9;]*m', '', (raw / 'controller.log').read_text(errors='replace'))
    for line in text.splitlines():
        if 'publication cycle' in line and 'cycle=' in line:
            text = line[line.index('cycle=') + 6:]
            try:
                cycles.append(json.loads(text))
            except json.JSONDecodeError:
                pass
    activations = sorted((c['activated_unix'], c['height']) for c in cycles)
    delays = []
    for height in steady:
        covering = next((t for t, h in activations if h >= height), None)
        if covering is not None:
            delays.append(covering - blocks[height]['appended_unix'])
    missing = len(steady) - len(delays)
    in_window = [c for c in cycles if steady and c['height'] >= steady[0]]
    stagings = [s for s in lines(raw / 'fleet.jsonl') if s.get('operation') == 'prepare' and 'error' not in s]
    steady_stagings = [s for s in stagings if s['unix'] >= start]
    produced = {}
    for staging in steady_stagings:
        for key, value in (staging.get('runtimes') or {}).items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                produced[key] = produced.get(key, 0) + value
    preparation = {}
    for c in in_window:
        for key, value in (c.get('runtime_preparation') or {}).items():
            if isinstance(value, (int, float)):
                preparation[key] = preparation.get(key, 0) + value
    after = events['steady_end']
    warm_metrics = events.get('warmup_active', {}).get('metrics', {})
    metrics = {k: after['metrics'].get(k, 0) - warm_metrics.get(k, 0) for k in after['metrics']}
    fleet_failures = [s for s in lines(raw / 'fleet.jsonl') if 'error' in s]
    return {
        'name': directory.name,
        'complete': True,
        'ship': json.loads((directory / 'controller.json').read_text())['ship_runtimes'],
        'window': {'start_unix': start, 'end_unix': end, 'seconds': seconds,
                   'steady_heights': [steady[0], steady[-1]] if steady else None},
        'cpu_seconds': cpu,
        'foreign_cpu': foreign,
        'contaminated': contaminated,
        'rss_peak_bytes': rss,
        'host_memory': {'total': total, 'min_available': available,
                        'min_available_fraction': available / total},
        'readiness_fraction': readiness,
        'queries': {'scheduled': len(queries), 'outcomes': outcomes, 'exact_populated': populated,
                    'latency_ms': latency, 'by_class': by_class},
        'activation_delay_seconds': quantiles(delays),
        'heights_never_activated': missing,
        'cycles': len(in_window),
        'transferred_bytes': sum(s.get('transferred_bytes') or 0 for s in steady_stagings),
        'publisher_preparation': preparation,
        'worker_runtimes': produced,
        'worker_metrics_delta': metrics,
        'fleet_failures': len(fleet_failures),
        'restart': [e for e in lines(raw / 'arm-events.jsonl')
                    if e['event'] == 'worker_ready' and e.get('label') == 'restart'],
        'cold': [e for e in lines(raw / 'arm-events.jsonl') if e['event'] == 'worker_ready' and e.get('label') == 'cold'],
    }


def gates(a):
    failures = []
    outcomes = a['queries']['outcomes']
    if set(outcomes) - {'exact'}:
        failures.append(f"queries not exact: {outcomes}")
    if a['heights_never_activated']:
        failures.append(f"{a['heights_never_activated']} heights never activated")
    if (a['activation_delay_seconds']['max'] or 0) > FRESHNESS_SECONDS:
        failures.append(f"activation delay max {a['activation_delay_seconds']['max']:.1f}s")
    if a['readiness_fraction'] < 1.0:
        failures.append(f"ready {a['readiness_fraction']:.4f} of probes")
    if a['host_memory']['min_available_fraction'] < MEMORY_HEADROOM:
        failures.append(f"host memory available fell to {a['host_memory']['min_available_fraction']:.3f}")
    if a['fleet_failures']:
        failures.append(f"{a['fleet_failures']} fleet operations failed")
    if a['ship']:
        if a['worker_runtimes'].get('shipped_fallbacks', 0) or a['worker_metrics_delta'].get(
                'transparent_shard_shipped_fallbacks_total', 0):
            failures.append('shipped runtimes were refused')
        if a['worker_runtimes'].get('built', 0) or a['worker_metrics_delta'].get('transparent_shard_builds_total', 0):
            failures.append('the worker built runtimes while shipping')
    return failures


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('work')
    parser.add_argument('--ticks-per-second', type=int, default=100)
    args = parser.parse_args()
    arms = {}
    for directory in sorted((Path(args.work) / 'arms').iterdir()):
        arms[directory.name] = arm(directory, args.ticks_per_second)
    for a in arms.values():
        if a['complete']:
            a['gate_failures'] = gates(a)
    acceptance = {'pairs': [], 'accepted': False}
    complete = all(arms.get(n, {}).get('complete') for pair in PAIRS for n in pair)
    if complete:
        ok = True
        for off, on in PAIRS:
            a, b = arms[off], arms[on]
            pair = {'off': off, 'on': on,
                    'worker_cpu_seconds': [a['cpu_seconds']['worker'], b['cpu_seconds']['worker']],
                    'worker_build_pool_cpu_seconds': [a['cpu_seconds']['worker_build_pool'],
                                                      b['cpu_seconds']['worker_build_pool']],
                    'contaminated': a['contaminated'] or b['contaminated']}
            pair['worker_cpu_decreased'] = (b['cpu_seconds']['worker'] < a['cpu_seconds']['worker']
                                            and b['cpu_seconds']['worker_build_pool']
                                            < a['cpu_seconds']['worker_build_pool'])
            ok &= pair['worker_cpu_decreased'] and not pair['contaminated']
            acceptance['pairs'].append(pair)
        worst = {}
        for ship in (False, True):
            names = [n for pair in PAIRS for n in pair if arms[n]['ship'] == ship]
            worst[ship] = {
                f'query_{q}': max(arms[n]['queries']['latency_ms'][q] for n in names) for q in ('p50', 'p95', 'p99')}
            worst[ship].update({
                f'activation_{q}': max(arms[n]['activation_delay_seconds'][q] for n in names)
                for q in ('p50', 'p95', 'p99')})
        acceptance['worst_off'] = worst[False]
        acceptance['worst_on'] = worst[True]
        acceptance['quantiles_not_worse'] = all(worst[True][k] <= worst[False][k] for k in worst[False])
        acceptance['gate_failures'] = {n: arms[n]['gate_failures'] for pair in PAIRS for n in pair
                                       if arms[n]['gate_failures']}
        acceptance['accepted'] = ok and acceptance['quantiles_not_worse'] and not acceptance['gate_failures']
    print(json.dumps({'arms': arms, 'acceptance': acceptance}, indent=1))


if __name__ == '__main__':
    main()

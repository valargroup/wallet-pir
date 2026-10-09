#!/usr/bin/env python3
"""Lookup latency inside vs outside the recent worker's prepare (and ship) intervals.

  prepare_pairing.py OUT_DIR WINDOW_NAME
Reads runs/<name>/rate-*.jsonl and timeline/<name>.jsonl. A prepare interval is
[activated - activate_ms - prepare_ms, activated - activate_ms]; the ship interval
precedes it by ship_ms. Prints JSON.
"""
import glob, json, sys

out, name = sys.argv[1], sys.argv[2]
lines = open(f'{out}/runs/{name}/window.json').read().splitlines()
start = json.loads(lines[0])['start_unix']; end = json.loads(lines[1])['end_unix']
cycles = []
for line in open(f'{out}/timeline/{name}.jsonl'):
    e = json.loads(line)
    if e.get('kind') != 'cycle' or not any(w.get('role') == 'recent-replica' for w in e.get('workers', [])):
        continue
    act = e['activated_ms'] / 1000
    if not (start - 30 <= act <= end + 30):
        continue
    p_end = act - e['activate_ms'] / 1000
    p_start = p_end - e['prepare_ms'] / 1000
    cycles.append({'cycle': e['cycle'], 'ship': (p_start - e['ship_ms'] / 1000, p_start), 'prepare': (p_start, p_end), 'tail': (p_end, p_end + 2.0)})


def pct(xs, p):
    xs = sorted(xs)
    return xs[max(0, min(len(xs) - 1, int(round(p / 100 * len(xs) + 0.5)) - 1))] if xs else None


def stats(xs):
    return {'n': len(xs), 'p50_ms': round(pct(xs, 50) * 1e3, 1) if xs else None, 'p95_ms': round(pct(xs, 95) * 1e3, 1) if xs else None,
            'p99_ms': round(pct(xs, 99) * 1e3, 1) if xs else None, 'max_ms': round(max(xs) * 1e3, 1) if xs else None}


def phase(t):
    for c in cycles:
        for k in ('ship', 'prepare', 'tail'):
            a, b = c[k]
            if a <= t <= b:
                return k
    return 'outside'


by = {'lookup_total': {}, 'lookup_recent_total': {}, 'query_http_recent': {}}
for path in glob.glob(f'{out}/runs/{name}/rate-*.jsonl'):
    for line in open(path):
        e = json.loads(line)
        t = e.get('unix')
        if t is None or not (start <= t <= end):
            continue
        ph = phase(t)
        if e.get('event') == 'lookup':
            by['lookup_total'].setdefault(ph, []).append(e['total_seconds'])
            if not e.get('sealed', e.get('tier') == 'archive'):
                by['lookup_recent_total'].setdefault(ph, []).append(e['total_seconds'])
        elif e.get('event') == 'query' and not e.get('sealed'):
            by['query_http_recent'].setdefault(ph, []).append(e['http_seconds'])
res = {'window': name, 'cycles': [c['cycle'] for c in cycles],
       'prepare_seconds_total': round(sum(c['prepare'][1] - c['prepare'][0] for c in cycles), 1),
       'ship_seconds_total': round(sum(c['ship'][1] - c['ship'][0] for c in cycles), 1)}
for metric, groups in by.items():
    res[metric] = {ph: stats(xs) for ph, xs in sorted(groups.items())}
    loads = groups.get('prepare', []) + groups.get('ship', [])
    outside = groups.get('outside', [])
    if loads and outside:
        res[metric]['loads_vs_outside_p99_ratio'] = round(pct(loads, 99) / pct(outside, 99), 3)
print(json.dumps(res, indent=1))

#!/usr/bin/env python3
"""Summarize one comparison run: client latency per class, exactness, rate, retries,
and per-worker server evaluation, slot use, memory and restarts from the sampler.

Usage: cmp-analyze.py RUN_DIR SAMPLES_JSONL
RUN_DIR holds c*/queries.jsonl from cmp-multi.sh. Writes RUN_DIR/analysis.json.
"""
import collections, json, sys
from pathlib import Path

root, samples_path = Path(sys.argv[1]), Path(sys.argv[2])

def pct(values, p):
    values = sorted(values)
    return values[min(len(values) - 1, int((len(values) - 1) * p))] if values else None

def summary(qs):
    lat = [q['http_seconds'] for q in qs]
    return {'queries': len(qs), 'exact': sum(q.get('exact') is True for q in qs),
            'wrong': sum(q.get('exact') is False for q in qs),
            'p50': pct(lat, .5), 'p95': pct(lat, .95), 'p99': pct(lat, .99), 'max': max(lat, default=None)}

queries, errors, missed, failed = [], [], 0, 0
for client in sorted(root.glob('c*/queries.jsonl')):
    events = []
    for line in client.read_text().splitlines():
        try:
            events.append(json.loads(line))
        except ValueError:
            pass
    answered = {e['logical_id'] for e in events if e.get('event') == 'query' and e.get('exact') is True and 'logical_id' in e}
    queries += [e for e in events if e.get('event') == 'query']
    errs = [e for e in events if e.get('event') == 'error']
    errors += errs
    failed += len({e.get('logical_id') for e in errs} - answered)
    missed += sum(e.get('event') == 'missed_slot' for e in events)

starts = sorted(q['unix'] for q in queries if 'unix' in q)
start, end = starts[0], starts[-1]
by_class = collections.defaultdict(list)
for q in queries:
    by_class[q['geometry'] + '/' + q['table']].append(q)

import re
def normalize(metrics):
    # Labels carry the active publication and assignment, which change every block while
    # the counters stay process-cumulative; key by metric name and histogram bound only.
    out = {}
    for key, value in metrics.items():
        name = key.split('{')[0]
        le = re.search(r'[{,]le="([^"]+)"', key)
        out[name + (f'{{le="{le.group(1)}"}}' if le else '')] = value
    return out
samples = [json.loads(l) for l in samples_path.read_text().splitlines() if l.strip()]
for s in samples:
    for w in s['workers'].values():
        if 'metrics' in w:
            w['metrics'] = normalize(w['metrics'])
window = [s for s in samples if start - 20 <= s['unix'] <= end + 20]
workers = {}
for wid in sorted({w for s in window for w in s['workers']}):
    rows = [(s['unix'], s['workers'][wid]) for s in window if wid in s['workers'] and 'metrics' in s['workers'][wid]]
    if len(rows) < 2:
        continue
    (t0, a), (t1, b) = rows[0], rows[-1]
    m0, m1 = a['metrics'], b['metrics']
    d = lambda k: m1.get(k, 0) - m0.get(k, 0)
    seconds = t1 - t0
    buckets = sorted((float(k.split('le="')[1].split('"')[0].replace('+Inf', 'inf')), m1[k] - m0.get(k, 0))
                     for k in m1 if k.startswith('evaluation_seconds_bucket'))
    count = d('evaluation_seconds_count')
    def hq(p):
        for le, c in buckets:
            if count and c >= p * count:
                return le
    workers[wid] = {
        'role': a['role'], 'queries': d('queries_total'),
        'server_errors': sum(d(k) for k in ('query_errors_total', 'queue_rejections_total', 'overloads_total', 'deadline_exceeded_total')),
        'evaluation_mean_seconds': d('evaluation_seconds_sum') / count if count else None,
        'evaluation_p50_le': hq(.5), 'evaluation_p99_le': hq(.99),
        'queue_wait_mean_seconds': d('queue_wait_seconds_sum') / d('queue_wait_seconds_count') if d('queue_wait_seconds_count') else None,
        'slot_utilization': d('query_slot_busy_microseconds_total') / 1e6 / (seconds * m1['query_slots']) if m1.get('query_slots') else None,
        'cpu_cores': d('process_cpu_seconds_total') / seconds,
        'rss_max_gib': max(r['metrics'].get('process_rss_bytes', 0) for _, r in rows) / 2**30,
        'cgroup_max_gib': max(r['metrics'].get('cgroup_memory_current_bytes', 0) for _, r in rows) / 2**30,
        'mem_available_min': min((r.get('mem_available_fraction', 1) for _, r in rows), default=None),
        'loadavg1_max': max((r.get('loadavg1', 0) for _, r in rows), default=None),
        'restarted': m1.get('process_start_time_seconds') != m0.get('process_start_time_seconds'),
        'builds': d('builds_total'),
    }
fresh = [s['controller']['freshness_seconds'] for s in window if 'controller' in s]
desired = [s['scaler'].get('desired_recent') for s in window if isinstance(s.get('scaler'), dict)]
overall = summary(queries)
out = {'window': [start, end], 'overall': overall, 'achieved_qps': (len(starts) - 1) / (end - start),
       'missed_slots': missed, 'transport_errors': len(errors), 'failed_queries': failed,
       'error_samples': errors[:3], 'classes': {k: summary(v) for k, v in sorted(by_class.items())},
       'workers': workers, 'freshness_max_seconds': max(fresh, default=None),
       'freshness_p50_seconds': pct(fresh, .5), 'scaler_desired_seen': sorted({x for x in desired if x is not None})}
archive_mem = [w['mem_available_min'] for w in workers.values() if w['role'] == 'archive-owner' and w['mem_available_min'] is not None]
out['gate'] = {'p99_below_2s': overall['p99'] < 2.0, 'p50_below_700ms': overall['p50'] < 0.7,
               'all_exact': overall['wrong'] == 0 and overall['exact'] == overall['queries'],
               'no_failed_queries': failed == 0, 'not_client_bound': missed <= 0.01 * overall['queries'],
               'no_restarts': not any(w['restarted'] for w in workers.values()),
               'archive_memory_25pct': all(m >= 0.25 for m in archive_mem)}
out['gate']['passed'] = all(out['gate'].values())
(root / 'analysis.json').write_text(json.dumps(out, indent=1) + '\n')
print(json.dumps({k: out[k] for k in ('overall', 'achieved_qps', 'missed_slots', 'transport_errors', 'failed_queries', 'freshness_max_seconds', 'scaler_desired_seen', 'gate')}, indent=1))
for wid, w in workers.items():
    print(wid, json.dumps({k: (round(v, 4) if isinstance(v, float) else v) for k, v in w.items()}))

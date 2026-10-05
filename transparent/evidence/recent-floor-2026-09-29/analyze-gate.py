#!/usr/bin/env python3
"""Summarize a rate-query gate run: latency, exactness, rate and per-worker slot use."""
import collections, json, sys
from pathlib import Path
root = Path(sys.argv[1])
events = []
for line in (root/'queries.jsonl').read_text().splitlines():
    try:
        events.append(json.loads(line))
    except ValueError:
        pass
queries = [e for e in events if e.get('event') == 'query']
errors = [e for e in events if e.get('event') == 'error']
missed = [e for e in events if e.get('event') == 'missed_slot']
def pct(values, p):
    values = sorted(values)
    return values[min(len(values) - 1, int((len(values) - 1) * p))] if values else None
def summary(qs):
    lat = [q['http_seconds'] for q in qs]
    return {'queries': len(qs), 'exact': sum(q.get('exact') is True for q in qs),
            'p50': pct(lat, .5), 'p95': pct(lat, .95), 'p99': pct(lat, .99), 'max': max(lat) if lat else None}
def metrics(name):
    out, host = {}, None
    for line in (root/name).read_text().splitlines():
        if line.startswith('== '):
            host = line[3:]; out[host] = {}
        elif line.strip():
            key, value = line.split()
            out[host][key] = float(value)
    return out
start = float((root/'start_unix').read_text())
end = float((root/'end_unix').read_text()) if (root/'end_unix').exists() else None
before, after = metrics('metrics-before.txt'), metrics('metrics-after.txt') if (root/'metrics-after.txt').exists() else {}
workers = {}
for host, values in after.items():
    b = before.get(host, {})
    seconds = (end - start) if end else None
    busy = values.get('transparent_shard_query_slot_busy_microseconds_total', 0) - b.get('transparent_shard_query_slot_busy_microseconds_total', 0)
    slots = values.get('transparent_shard_query_slots')
    workers[host] = {'queries': values.get('transparent_shard_queries_total', 0) - b.get('transparent_shard_queries_total', 0),
                     'errors': sum(values.get(k, 0) - b.get(k, 0) for k in ('transparent_shard_queue_rejections_total', 'transparent_shard_overloads_total', 'transparent_shard_deadline_exceeded_total')),
                     'slot_utilization': busy / 1e6 / (seconds * slots) if seconds and slots else None}
by_class = collections.defaultdict(list)
for q in queries:
    by_class[q['geometry'] + '/' + q['table']].append(q)
starts = sorted(q['unix'] for q in queries if 'unix' in q)
out = {'overall': summary(queries), 'errors': len(errors), 'error_samples': errors[:5], 'missed_slots': len(missed),
       'achieved_qps': (len(starts) - 1) / (starts[-1] - starts[0]) if len(starts) > 1 else None,
       'classes': {k: summary(v) for k, v in sorted(by_class.items())}, 'workers': workers,
       'gate': {'p99_below_2s': (summary(queries)['p99'] or 99) < 2.0, 'p50_below_700ms': (summary(queries)['p50'] or 99) < 0.7,
                'no_errors': not errors, 'all_exact': summary(queries)['exact'] == len(queries)}}
out['gate']['passed'] = all(out['gate'].values())
(root/'analysis.json').write_text(json.dumps(out, indent=1) + '\n')
print(json.dumps({k: out[k] for k in ('overall', 'errors', 'missed_slots', 'achieved_qps', 'gate')}, indent=1))
print(json.dumps(out['workers'], indent=1))

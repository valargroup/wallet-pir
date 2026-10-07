#!/usr/bin/env python3
"""Summarise one txid-rate window plus the history and controller samples around it.

  analyze.py OUT_DIR WINDOW_NAME [PRE_SECONDS]

Reads OUT_DIR/runs/<name>/{window.json,rate-*.jsonl}, OUT_DIR/history-samples.jsonl,
OUT_DIR/recent-cpu.jsonl and OUT_DIR/timeline/<name>.jsonl (controller cycles), and
prints and writes OUT_DIR/runs/<name>/summary.json.
"""
import glob, json, os, sys
from collections import Counter, defaultdict


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    k = max(0, min(len(xs) - 1, int(round(p / 100 * len(xs) + 0.5)) - 1))
    return xs[k]


def stats(xs):
    return {'n': len(xs), 'p50': pct(xs, 50), 'p95': pct(xs, 95), 'p99': pct(xs, 99),
            'max': max(xs) if xs else None, 'mean': sum(xs) / len(xs) if xs else None}


def jl(path):
    out = []
    if os.path.exists(path):
        for line in open(path):
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return out


def main():
    out, name = sys.argv[1], sys.argv[2]
    pre = float(sys.argv[3]) if len(sys.argv) > 3 else 600
    rd = os.path.join(out, 'runs', name)
    lines = open(os.path.join(rd, 'window.json')).read().splitlines()
    win = json.loads(lines[0])
    end = json.loads(lines[-1]).get('end_unix') if len(lines) > 1 else None
    start = win['start_unix']
    events = []
    for f in sorted(glob.glob(os.path.join(rd, 'rate-*.jsonl'))):
        events += jl(f)
    kinds = Counter(e.get('event') for e in events)
    lookups = [e for e in events if e.get('event') == 'lookup']
    errors = [e for e in events if e.get('event') == 'error']
    missed = [e for e in events if e.get('event') == 'missed_slot']
    paused = [e for e in events if e.get('event') == 'paused']
    queries = [e for e in events if e.get('event') == 'query']
    s = {'window': win, 'end_unix': end, 'events': dict(kinds)}
    s['missed_slots'] = sum(e.get('slots', 1) for e in missed)
    s['missed_reasons'] = dict(Counter(e.get('reason') for e in missed))
    s['paused'] = len(paused)
    s['errors'] = len(errors)
    s['error_kinds'] = dict(Counter((e.get('kind'), e.get('status'), (e.get('error') or '')[:120]) .__repr__() for e in errors))
    s['units'] = len(lookups)
    s['exact'] = sum(1 for e in lookups if e.get('exact'))
    s['not_exact'] = [{k: e.get(k) for k in ('sequence', 'outcome', 'expected', 'class', 'fixture_class', 'sealed', 'shard_id')}
                      for e in lookups if not e.get('exact')][:20]
    s['outcomes'] = dict(Counter(e.get('outcome') for e in lookups))
    dur = (end or max(e['unix'] for e in lookups)) - start if lookups else None
    s['duration_s'] = dur
    s['achieved_units_per_s'] = len(lookups) / dur if dur else None
    s['stale_retries'] = sum(e.get('stale_retries') or 0 for e in lookups)
    s['schedule_lag'] = stats([e.get('schedule_lag_seconds') or 0 for e in lookups])
    tot = [e['total_seconds'] for e in lookups]
    s['total_seconds'] = stats(tot)
    s['by_cold'] = {str(c): stats([e['total_seconds'] for e in lookups if bool(e.get('cold')) == c]) for c in (False, True)}
    s['by_tier'] = {t: stats([e['total_seconds'] for e in lookups if bool(e.get('sealed')) == (t == 'archive')]) for t in ('recent', 'archive')}
    by_class = defaultdict(list)
    for e in lookups:
        by_class[e.get('class') or e.get('unit') or 'query'].append(e['total_seconds'])
    s['by_class'] = {k: stats(v) for k, v in sorted(by_class.items())}
    s['queries_per_unit'] = dict(Counter(e.get('queries') for e in lookups))
    s['per_query_http'] = {t: stats([q['http_seconds'] for q in queries if q.get('table') == t]) for t in sorted({q.get('table') for q in queries})}
    s['per_query_status'] = dict(Counter(q.get('status') for q in queries))
    # 60 s buckets of p99 so a single bad minute is visible.
    buckets = defaultdict(list)
    for e in lookups:
        buckets[int((e['unix'] - start) // 60)].append(e['total_seconds'])
    s['per_minute_p99'] = {k: pct(v, 99) for k, v in sorted(buckets.items())}
    # History samples.
    hs = [h for h in jl(os.path.join(out, 'history-samples.jsonl')) if h.get('ok')]
    stop = end or start + win['seconds']

    def hsum(rows):
        return {'samples': len(rows), 'p99_max': max((r['p99'] for r in rows), default=None),
                'p99_median': pct([r['p99'] for r in rows], 50), 'p50_median': pct([r['p50'] for r in rows], 50),
                'errors_max': max((r['errors'] for r in rows), default=None),
                'exact_min': min((r['exact'] for r in rows), default=None),
                'freshness_max': max((r['freshness'] for r in rows), default=None),
                'freshness_median': pct([r['freshness'] for r in rows], 50),
                'ready_replicas_min': min((r['ready_replicas'] for r in rows), default=None)}
    # trailing_60s lags: the first 60 s of a window's samples still hold pre-load data,
    # and samples up to 60 s after the end still hold load data.
    s['history_pre'] = hsum([h for h in hs if start - pre <= h['unix'] < start])
    s['history_during'] = hsum([h for h in hs if start + 60 <= h['unix'] <= stop])
    s['history_during_incl_tail'] = hsum([h for h in hs if start <= h['unix'] <= stop + 60])
    # Controller cycles from the timeline.
    tl = [c for c in jl(os.path.join(out, 'timeline', name + '.jsonl')) if c.get('kind') == 'cycle']

    def csum(rows):
        fr = [f for c in rows for f in (c.get('freshness_ms') or [])]
        return {'cycles': len(rows), 'freshness_ms': stats(fr), 'prepare_ms': stats([c['prepare_ms'] for c in rows if 'prepare_ms' in c]),
                'ship_ms': stats([c['ship_ms'] for c in rows if 'ship_ms' in c]),
                'cycle_ms': stats([c['cycle_ms'] for c in rows if 'cycle_ms' in c]),
                'freshness_over_20s': sum(1 for f in fr if f > 20000), 'freshness_samples': len(fr),
                'seals': sum(len(c.get('sealed_published') or []) for c in rows),
                'drops': sum(len(c.get('dropped') or []) for c in rows)}
    s['controller_pre'] = csum([c for c in tl if (start - pre) * 1000 <= c['unix_ms'] < start * 1000])
    s['controller_during'] = csum([c for c in tl if start * 1000 <= c['unix_ms'] <= stop * 1000])
    # recent-01 CPU over the window and the pre window.
    cpu = [c for c in jl(os.path.join(out, 'recent-cpu.jsonl')) if 'proc_stat_cpu' in c]

    def cpu_between(a, b):
        rows = [c for c in cpu if a <= c['unix'] <= b]
        if len(rows) < 2:
            return None
        f, l = rows[0], rows[-1]
        tf, tl_ = sum(f['proc_stat_cpu']), sum(l['proc_stat_cpu'])
        idle = (l['proc_stat_cpu'][3] + l['proc_stat_cpu'][4]) - (f['proc_stat_cpu'][3] + f['proc_stat_cpu'][4])
        wall = l['remote_unix'] - f['remote_unix']
        return {'seconds': wall, 'host_busy_pct_of_4cpu': 100 * (1 - idle / (tl_ - tf)) if tl_ > tf else None,
                'txid_worker_cores': (l['txid_worker_cpu_ns'] - f['txid_worker_cpu_ns']) / 1e9 / wall,
                'history_shard_cores': (l['history_shard_cpu_ns'] - f['history_shard_cpu_ns']) / 1e9 / wall,
                'samples': len(rows)}
    s['recent_cpu_pre'] = cpu_between(start - pre, start)
    s['recent_cpu_during'] = cpu_between(start, stop)
    json.dump(s, open(os.path.join(rd, 'summary.json'), 'w'), indent=1, default=str)
    print(json.dumps(s, indent=1, default=str))


if __name__ == '__main__':
    main()

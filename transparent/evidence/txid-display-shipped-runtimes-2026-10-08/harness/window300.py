#!/usr/bin/env python3
"""Block-to-serving and CPU per block since the shipped-runtimes controller restart.

  window300.py OUT_DIR TIMELINE_JSONL [FIRST_CYCLE] [MAX_CYCLES]
Cycle records come from the controller timeline (read-only tail); CPU from
OUT_DIR/recent-cpu.jsonl (30 s unit counters) outside the load windows in
OUT_DIR/events.jsonl, and OUT_DIR/coordinator-cpu-samples.jsonl. Prints JSON.
"""
import json, sys, time

out, timeline = sys.argv[1], sys.argv[2]
first = int(sys.argv[3]) if len(sys.argv) > 3 else 2233
maxn = int(sys.argv[4]) if len(sys.argv) > 4 else 300


def pct(xs, p):
    xs = sorted(xs)
    return xs[max(0, min(len(xs) - 1, int(round(p / 100 * len(xs) + 0.5)) - 1))] if xs else None


def dist(xs, scale=1.0, nd=2):
    if not xs:
        return None
    return {'n': len(xs), 'p50': round(pct(xs, 50) * scale, nd), 'p95': round(pct(xs, 95) * scale, nd),
            'p99': round(pct(xs, 99) * scale, nd), 'max': round(max(xs) * scale, nd), 'mean': round(sum(xs) / len(xs) * scale, nd)}


cyc = {}
for line in open(timeline):
    e = json.loads(line)
    if e.get('kind') == 'cycle' and e['cycle'] >= first and e['cycle'] < first + maxn:
        cyc[e['cycle']] = e
cycles = [cyc[k] for k in sorted(cyc)]
rec = [c for c in cycles if any(w.get('role') == 'recent-replica' for w in c.get('workers', []))]
fresh = [x / 1e3 for c in rec for x in c['freshness_ms']]
res = {
    'cycles': len(cycles), 'first_cycle': cycles[0]['cycle'] if cycles else None, 'last_cycle': cycles[-1]['cycle'] if cycles else None,
    'span_utc': [time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(cycles[0]['activated_ms'] / 1e3)), time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(cycles[-1]['activated_ms'] / 1e3))] if cycles else None,
    'block_to_serving_s': {**dist(fresh), 'over_20s': sum(x > 20 for x in fresh), 'over_8s': sum(x > 8 for x in fresh)},
    'cycle_s': dist([c['cycle_ms'] / 1e3 for c in rec]),
    'prebuild_s': dist([c['prebuild_ms'] / 1e3 for c in rec if c.get('prebuild_ms') is not None]),
    'ship_s': {**dist([c['ship_ms'] / 1e3 for c in rec]), 'over_2s': sum(c['ship_ms'] > 2000 for c in rec)},
    'prepare_s': dist([c['prepare_ms'] / 1e3 for c in rec]),
    'activate_s': dist([c['activate_ms'] / 1e3 for c in rec], nd=3),
    'shipped_mib': dist([c['shipped_bytes'] / 2**20 for c in rec if c.get('shipped_bytes')]),
    'prebuild_failures_last': cycles[-1].get('prebuild_failures') if cycles else None,
    'sealed_published': sum(len(c.get('sealed_published') or []) for c in cycles),
}
w = [x for c in rec for x in c.get('workers', []) if x.get('role') == 'recent-replica']
res['recent_worker'] = {
    'cycles': len(w),
    'shipped_total': sum(x.get('shipped') or 0 for x in w), 'fallbacks_total': sum(x.get('shipped_fallbacks') or 0 for x in w),
    'cycles_without_2_shipped': sum((x.get('shipped') or 0) != 2 for x in w),
    'self_check_s': dist([x['self_check_ms'] / 1e3 for x in w if x.get('self_check_ms') is not None], nd=3),
    'prepare_seconds': dist([x['seconds'] for x in w if x.get('seconds') is not None]),
}

# CPU per block outside load windows.
events = [json.loads(l) for l in open(f'{out}/events.jsonl')]
windows = []
for e in events:
    if e['event'] == 'window_start':
        windows.append([e['unix'] - 30, None, e['name']])
    elif e['event'] == 'window_end' and windows and windows[-1][1] is None:
        windows[-1][1] = e['unix'] + 90
windows = [w for w in windows if w[1]]
cpu = [json.loads(l) for l in open(f'{out}/recent-cpu.jsonl') if '"error"' not in l]
act = sorted(c['activated_ms'] / 1e3 for c in rec)


def in_window(t):
    return any(a <= t <= b for a, b, _ in windows)


quiet = []  # (t0, t1, worker_ns, history_ns)
seg = None
for s in cpu:
    t = s['remote_unix']
    if in_window(t):
        if seg and len(seg) > 1:
            quiet.append(seg)
        seg = None
        continue
    if seg is None:
        seg = [s]
    else:
        seg.append(s)
if seg and len(seg) > 1:
    quiet.append(seg)
wsum = hsum = tsum = 0.0
blocks = 0
for seg in quiet:
    a, b = seg[0], seg[-1]
    wsum += (b['txid_worker_cpu_ns'] - a['txid_worker_cpu_ns']) / 1e9
    hsum += (b['history_shard_cpu_ns'] - a['history_shard_cpu_ns']) / 1e9
    tsum += b['remote_unix'] - a['remote_unix']
    blocks += sum(a['remote_unix'] <= t <= b['remote_unix'] for t in act)
res['recent_cpu_quiet'] = {'segments': len(quiet), 'seconds': round(tsum), 'blocks': blocks,
                           'txid_worker_cpu_s_per_block': round(wsum / blocks, 2) if blocks else None,
                           'txid_worker_cores': round(wsum / tsum, 3) if tsum else None,
                           'history_shard_cores': round(hsum / tsum, 3) if tsum else None,
                           'load_windows_excluded': [n for _, _, n in windows]}
try:
    cs = [json.loads(l) for l in open(f'{out}/coordinator-cpu-samples.jsonl')]
    if len(cs) >= 2:
        a, b = cs[0], cs[-1]
        res['coordinator_controller_cpu'] = {'from_cycle': a['cycle'], 'to_cycle': b['cycle'],
                                             'cpu_s_per_cycle': round((b['controller_cpu_ns'] - a['controller_cpu_ns']) / 1e9 / max(1, b['cycle'] - a['cycle']), 2)}
    else:
        res['coordinator_controller_cpu'] = {'note': 'one sample so far', 'cpu_s_since_start_per_cycle': round(cs[0]['controller_cpu_ns'] / 1e9 / max(1, cs[0]['cycle'] - first + 1), 2)}
except FileNotFoundError:
    pass
print(json.dumps(res, indent=1))

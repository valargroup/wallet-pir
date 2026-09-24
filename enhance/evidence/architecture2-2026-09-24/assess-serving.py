#!/usr/bin/env python3
"""Assess the recorded production serving topology; never qualify larger assignments."""
import argparse
import datetime
import gzip
import json
from pathlib import Path

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--directory', type=Path, default=Path(__file__).parent)
a = p.parse_args()
root = a.directory
failures = []

def read(name):
    return json.loads((root / name).read_text())

def instant(name):
    return int(datetime.datetime.fromisoformat((root / name).read_text().strip().replace('Z', '+00:00')).timestamp() * 1e9)

run = read('sustained-load.json')
start = instant('sustained-started.txt')
end = instant('sustained-completed.txt')
if end - start < 21600 * 10**9 or run['seconds'] < 21599:
    failures.append('six-hour duration not completed')
if run['offered_qps'] != 8 or run['concurrency'] != 4:
    failures.append('unexpected offered load or concurrency')
if run['completed'] != 172800 or run['unstarted_arrivals']:
    failures.append('not all 172800 offered arrivals completed')
if run['incorrect_answers'] or run['warmup_incorrect_answers'] or not run['exact_answer_oracle']:
    failures.append('exact-answer validation failed')
if sum(run['errors'].values()) / max(1, run['completed']) > 0.001:
    failures.append('query error rate exceeds 0.1 percent')
if run['p99_ms'] > 1000 or run['scheduled_p99_ms'] > 1000:
    failures.append('p99 exceeds one second')
results = []
for role in ('coordinator', 'ingress', 'router', 'worker-01', 'worker-02'):
    path = root / f'{role}-sustained.jsonl.gz'
    with gzip.open(path, 'rt') as stream:
        samples = [json.loads(line) for line in stream]
    samples = [s for s in samples if start <= s['wall_ns'] <= end]
    good = [s for s in samples if s.get('error') is None]
    result = {'role': role, 'samples': len(samples), 'errors': len(samples) - len(good)}
    if not good:
        failures.append(f'{role}: no valid samples')
        results.append(result)
        continue
    gaps = [good[0]['wall_ns'] - start, end - good[-1]['wall_ns']]
    gaps += [b['wall_ns'] - c['wall_ns'] for c, b in zip(good, good[1:])]
    result['max_gap_seconds'] = max(gaps) / 1e9
    result['peak_cgroup_bytes'] = max(int(s['cgroup']['memory.peak']) for s in good)
    result['peak_current_bytes'] = max(int(s['cgroup']['memory.current']) for s in good)
    result['max_swap_bytes'] = max(int(s['cgroup']['memory.swap.current']) for s in good)
    result['event_deltas'] = {k: good[-1]['events'][k] - good[0]['events'][k] for k in ('high', 'max', 'oom', 'oom_kill')}
    result['host_swap_deltas'] = {k: good[-1]['host_swap'][k] - good[0]['host_swap'][k] for k in ('pswpin', 'pswpout')}
    if result['errors'] or result['max_gap_seconds'] > 6:
        failures.append(f'{role}: sampling gap or error')
    if len({tuple(s['identity']) for s in good}) != 1 or any(int(s['service']['NRestarts']) for s in good):
        failures.append(f'{role}: serving process restarted')
    if result['max_swap_bytes'] or any(result['event_deltas'].values()) or any(result['host_swap_deltas'].values()):
        failures.append(f'{role}: memory pressure or swap use')
    if role == 'router' and result['peak_cgroup_bytes'] >= 7 * 2**30:
        failures.append('router reached 7-GiB process cap')
    if role.startswith('worker') and max(s['process']['VmRSS'] + s['memory_stat'].get('kernel', 0) for s in good) > 6.5 * 2**30:
        failures.append(f'{role}: exceeded 6.5-GiB resident guard')
    if role == 'coordinator':
        result['generation_first'] = good[0]['health']['generation']
        result['generation_last'] = good[-1]['health']['generation']
        if result['generation_last'] <= result['generation_first']:
            failures.append('no production publication progress')
        if any(s['health']['resident_packing_objects'] != 0 or not s['health']['remote_packing'] for s in good):
            failures.append('coordinator retained serving packing material')
    results.append(result)
report = {'kind': 'architecture2-six-hour-production-serving', 'passed': not failures,
          'qualified_scope': 'one growing domain, five retained sessions, four router admissions, two evaluation replicas, offered 8 QPS',
          'failures': failures, 'load': run, 'hosts': results}
(root / 'sustained-assessment.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report, indent=2))
raise SystemExit(bool(failures))

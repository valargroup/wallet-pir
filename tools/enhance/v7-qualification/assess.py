#!/usr/bin/env python3
"""Assess the focused v7 workload; never issue six-hour hardware qualification."""
import argparse
import json
from pathlib import Path

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('exercise', type=Path)
p.add_argument('--worker', action='append', required=True, type=Path)
p.add_argument('--coordinator', required=True, type=Path)
p.add_argument('--out', required=True, type=Path)
a = p.parse_args()
run = json.loads(a.exercise.read_text())
start = run['measurement_started_wall_ns']
end = start + int(run['measurement_seconds'] * 1e9)
failures = []
if run.get('status') != 'recorded':
    failures.append('workload did not finish recording')
for field, minimum in [('measurement_seconds', 1800), ('publications', 30), ('background_correct_answers', 1), ('exact_boundary_and_retained_probes', 1)]:
    if run.get(field, 0) < minimum:
        failures.append(f'{field} below {minimum}')
if run.get('background_errors') != 0:
    failures.append('background query errors')
results = []
for role, path in [('worker', x) for x in a.worker] + [('coordinator', a.coordinator)]:
    samples = [json.loads(line) for line in path.read_text().splitlines()]
    samples = [s for s in samples if start <= s['wall_ns'] <= end]
    errors = sum(s.get('error') is not None for s in samples)
    good = [s for s in samples if s.get('error') is None]
    gaps = [good[i + 1]['wall_ns'] - good[i]['wall_ns'] for i in range(len(good) - 1)]
    max_gap = max([good[0]['wall_ns'] - start, end - good[-1]['wall_ns'], *gaps], default=end-start) / 1e9 if good else run['measurement_seconds']
    report = {'role': role, 'file': path.name, 'samples': len(samples), 'errors': errors, 'max_gap_seconds': max_gap}
    if not good or errors or max_gap > 6:
        failures.append(f'{path.name}: incomplete observation')
    if good:
        report.update(max_rss_bytes=max(s['process']['VmRSS'] for s in good),
                      max_rss_plus_kernel_bytes=max(s['process']['VmRSS'] + s.get('memory_stat', {}).get('kernel', 0) for s in good),
                      process_lifetime_hwm_bytes=max(s['process'].get('VmHWM', s['process']['VmRSS']) for s in good),
                      max_cgroup_bytes=max(int(s['cgroup']['memory.current']) for s in good),
                      max_process_swap_bytes=max(s['process'].get('VmSwap', 0) for s in good),
                      max_cgroup_swap_bytes=max(int(s['cgroup']['memory.swap.current']) for s in good))
        report['event_deltas'] = {k: good[-1]['events'][k] - good[0]['events'][k] for k in ('high', 'max', 'oom', 'oom_kill')}
        report['host_swap_deltas'] = {k: good[-1]['host_swap'][k] - good[0]['host_swap'][k] for k in ('pswpin', 'pswpout')}
        identities = {tuple(s['identity']) for s in good}
        if len(identities) != 1 or any(int(s['service']['NRestarts']) for s in good):
            failures.append(f'{path.name}: process restarted')
        if any(report['event_deltas'].values()) or any(report['host_swap_deltas'].values()):
            failures.append(f'{path.name}: memory pressure or host swap activity')
        if max(int(s['cgroup']['memory.swap.current']) for s in good) > int(good[0]['cgroup']['memory.swap.current']):
            failures.append(f'{path.name}: cgroup swap growth')
        if role == 'worker' and max(report['max_rss_plus_kernel_bytes'], report['process_lifetime_hwm_bytes']) > 6.5 * 1024**3:
            failures.append(f'{path.name}: worker resident usage exceeds 6.5 GiB guard')
    results.append(report)
result = {'kind': 'focused-v7-assessment', 'passed': not failures, 'failures': failures,
          'qualification': 'unqualified', 'six_hour_hardware_qualification': False,
          'binary_sha256': run['binary_sha256'], 'measurement_seconds': run['measurement_seconds'],
          'hosts': results}
a.out.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
raise SystemExit(bool(failures))

#!/usr/bin/env python3
"""Summarize transparent replica membership for a journal window (read-only)."""
import collections, glob, json, os, subprocess, sys
since, until = sys.argv[1], sys.argv[2]
out = subprocess.run(['journalctl', '-u', 'transparent-replica-reconciler', '--since', since, '--until', until,
                      '--no-pager', '-o', 'cat'], capture_output=True, text=True).stdout
events = collections.Counter()
for line in out.splitlines():
    try:
        e = json.loads(line)
    except ValueError:
        continue
    k = e.get('event')
    if k == 'worker_membership':
        k += ':' + e['worker'] + ':' + str(e['eligible'])
    elif k in ('membership_status_failed', 'worker_control_transport_failed'):
        k += ':' + e['worker'] + ':' + str(e.get('operation'))
    elif k != 'worker_private_progress':
        continue
    events[k] += 1
state = '/opt/transparent-publisher/state'
lo = float(subprocess.run(['date', '-d', since, '+%s'], capture_output=True, text=True).stdout)
hi = float(subprocess.run(['date', '-d', until, '+%s'], capture_output=True, text=True).stdout)
prepared = []
for f in glob.glob(state + '/*.prepared.json'):
    t = os.path.getmtime(f)
    if lo <= t < hi:
        prepared.append((t, sorted(json.load(open(f)))))
prepared.sort()
recent_counts = collections.Counter(sum('recent' in w for w in ws) for _, ws in prepared)
per_worker = collections.Counter(w for _, ws in prepared for w in ws)
print(json.dumps({'since': since, 'until': until, 'publications': len(prepared),
                  'prepared_recent_replicas_histogram': dict(sorted(recent_counts.items())),
                  'prepared_publications_per_worker': dict(sorted(per_worker.items())),
                  'reconciler_events': dict(sorted(events.items()))}, indent=1))

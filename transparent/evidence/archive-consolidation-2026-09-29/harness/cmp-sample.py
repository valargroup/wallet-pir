#!/usr/bin/env python3
"""Sample every enrolled worker, the controller and the scaler every 15 s (read-only).

Usage: cmp-sample.py OUT_DIR SECONDS
Writes OUT_DIR/samples.jsonl: worker counters, RSS, cgroup memory, host MemAvailable,
publication freshness and the scaler's latest status.
"""
import json, subprocess, sys, time, urllib.request
from pathlib import Path

out, seconds = Path(sys.argv[1]), float(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)
FLEET = json.loads(Path('/opt/transparent-publisher/fleet.json').read_text())
KEEP = ('queries_total', 'query_errors_total', 'queue_rejections_total', 'overloads_total',
        'deadline_exceeded_total', 'query_slot_busy_microseconds_total', 'query_slots',
        'evaluation_seconds_sum', 'evaluation_seconds_count', 'evaluation_seconds_bucket',
        'query_seconds_sum', 'query_seconds_count', 'queue_wait_seconds_sum', 'queue_wait_seconds_count',
        'process_rss_bytes', 'process_cpu_seconds_total', 'cgroup_memory_current_bytes',
        'cgroup_memory_max_bytes', 'cache_resident_bytes', 'builds_total', 'process_start_time_seconds',
        'query_queue_depth', 'assigned_shards')

def get(url, timeout=3):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read().decode()

def metrics(upstream):
    values = {}
    for line in get(f'http://{upstream}/metrics').splitlines():
        if not line.startswith('transparent_shard_'):
            continue
        key, value = line.rsplit(' ', 1)
        name = key.split('{')[0][len('transparent_shard_'):]
        if name in KEEP:
            values[key[len('transparent_shard_'):]] = float(value)
    return values

def mem_available(host):
    args = ['ssh', '-i', FLEET['ssh_key'], '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes',
            '-o', 'StrictHostKeyChecking=yes', '-o', 'UserKnownHostsFile=' + FLEET['known_hosts'],
            '-o', 'ConnectTimeout=4', 'root@' + host,
            "awk '/MemTotal|MemAvailable/{print $2}' /proc/meminfo; cat /proc/loadavg"]
    lines = subprocess.run(args, capture_output=True, text=True, timeout=10).stdout.split('\n')
    total, avail = float(lines[0]), float(lines[1])
    return {'mem_available_fraction': avail / total, 'loadavg1': float(lines[2].split()[0])}

deadline = time.time() + seconds
with open(out / 'samples.jsonl', 'a') as f:
    while time.time() < deadline:
        roster = json.loads(Path('/opt/transparent-publisher/roster.json').read_text())
        sample = {'unix': time.time(), 'workers': {}}
        for w in roster:
            row = {'role': w['role']}
            try:
                row['metrics'] = metrics(w['upstream'])
            except Exception as e:
                row['metrics_error'] = str(e)
            try:
                row.update(mem_available(w['ssh_host']))
            except Exception as e:
                row['host_error'] = str(e)
            sample['workers'][w['id']] = row
        try:
            sample['controller'] = json.loads(get('http://127.0.0.1:8094/v1/status'))
        except Exception as e:
            sample['controller_error'] = str(e)
        try:
            sample['scaler'] = json.loads(Path('/opt/transparent-publisher/scaler/status.json').read_text())
        except Exception as e:
            sample['scaler_error'] = str(e)
        f.write(json.dumps(sample) + '\n'); f.flush()
        time.sleep(max(0, 15 - (time.time() - sample['unix'])))

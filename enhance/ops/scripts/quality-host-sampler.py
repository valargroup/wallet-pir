#!/usr/bin/env python3
"""Read-only host and Caddy snapshots, isolated from request and APM scrape paths.

The root-owned config lists explicit hosts, service units, filesystems, and SSH
credential paths. An optional `roster` block instead derives one target per
enrolled or draining transparent worker from the fleet roster on every cycle,
so elastic members and replaced owners are sampled under their own names. Only numeric resource observations and aggregate Caddy metrics
are written. Failed hosts disappear from the next snapshot, never retain a fresh
timestamp. The HTTP admin API is accessed on router loopback only.
"""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import time

REMOTE = r'''
import json, os, pathlib, subprocess, sys, time, urllib.request
unit, directory = sys.argv[1:3]
out = {'at': int(time.time())}
memory = {}
for line in pathlib.Path('/proc/meminfo').read_text().splitlines():
    key, value = line.split(':', 1)
    memory[key] = int(value.split()[0]) * 1024
out['memory_available_bytes'] = memory['MemAvailable']
out['memory_total_bytes'] = memory['MemTotal']
v = os.statvfs(directory)
out['disk_available_bytes'] = v.f_bavail * v.f_frsize
out['disk_total_bytes'] = v.f_blocks * v.f_frsize
raw = subprocess.check_output(['systemctl','show',unit,'-p','NRestarts','-p','ControlGroup','-p','CPUUsageNSec','-p','MemoryCurrent'], text=True, timeout=3)
props = dict(line.split('=',1) for line in raw.splitlines() if '=' in line)
for field, source in [('restarts','NRestarts'),('cgroup_memory_bytes','MemoryCurrent')]:
    if props.get(source,'').isdigit(): out[field] = int(props[source])
if props.get('CPUUsageNSec','').isdigit(): out['cpu_seconds'] = int(props['CPUUsageNSec']) // 1000000000
group = props.get('ControlGroup','')
if group.startswith('/system.slice/') and '..' not in group:
    p = pathlib.Path('/sys/fs/cgroup') / group.lstrip('/') / 'memory.events'
    if p.exists():
        events = dict(line.split() for line in p.read_text().splitlines())
        out['oom_kills'] = int(events['oom_kill'])
if len(sys.argv)>3 and sys.argv[3]=='edge':
    text = urllib.request.urlopen('http://127.0.0.1:2019/metrics',timeout=3).read(2097152).decode()
    lines = [line for line in text.splitlines() if line.startswith(('caddy_http_','caddy_config_last_reload_','process_start_time_seconds'))]
    out['edge_metrics'] = '\n'.join(lines)+'\npir_observation_timestamp_seconds '+str(out['at'])+'\n'
print(json.dumps(out))
'''


def sample(target, ssh):
    if not re.fullmatch(r'[A-Za-z0-9_.@-]+', target['unit']):
        raise ValueError('invalid service unit')
    args = [target['unit'], target['data_dir']]
    if target.get('edge'):
        args.append('edge')
    if target.get('host'):
        command = ['ssh', '-i', ssh['key'], '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes',
                   '-o', 'StrictHostKeyChecking=yes', '-o', 'UserKnownHostsFile='+ssh['known_hosts'],
                   '-o', 'ConnectTimeout=4', target['host'],
                   'python3 - '+' '.join(shlex.quote(a) for a in args)]
    else:
        command = ['python3', '-', *args]
    run = subprocess.run(command, input=REMOTE, capture_output=True, text=True, timeout=10, check=True)
    if len(run.stdout)>2097152:
        raise ValueError('snapshot exceeds limit')
    return json.loads(run.stdout)


def atomic(path, text):
    path = Path(path)
    tmp = path.with_suffix(path.suffix+'.next')
    tmp.write_text(text)
    os.chmod(tmp, 0o600)
    tmp.replace(path)


def roster_targets(spec):
    """One target per enrolled or draining roster member, read fresh each cycle."""
    roster = json.loads(Path(spec['path']).read_text())
    return [{'sources': [w['id']], 'unit': spec['unit'], 'data_dir': spec.get('data_dir', '/'),
             'host': 'root@' + w['ssh_host']}
            for w in roster if w.get('intent', 'enrolled') in ('enrolled', 'draining')]


def collect(config):
    result = {}
    targets = list(config['targets'])
    if config.get('roster'):
        # An unreadable roster keeps the explicit targets rather than stopping sampling.
        try:
            targets = roster_targets(config['roster']) + targets
        except (OSError, ValueError, KeyError, TypeError):
            print(json.dumps({'event': 'roster_unavailable'}), flush=True)
    if len(targets)>64:
        raise ValueError('too many hosts')
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = [(target,pool.submit(sample,target,config['ssh'])) for target in targets]
        for target, future in futures:
            try:
                value = future.result()
                edge = value.pop('edge_metrics', None)
                if edge is not None:
                    atomic(config['edge_output'],edge)
                for source in target['sources']:
                    result[source] = value
            except Exception:
                for source in target['sources']:
                    result[source] = {'at': 0}
                # No raw SSH errors, command lines, credentials, or server bodies.
                print(json.dumps({'event':'host_sample_unavailable','sources':target['sources']}),flush=True)
    atomic(config['output'],json.dumps(result))


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--config',required=True)
    parser.add_argument('--once',action='store_true')
    args=parser.parse_args()
    while True:
        started=time.monotonic()
        collect(json.loads(Path(args.config).read_text()))
        if args.once:
            return
        time.sleep(max(0,15-(time.monotonic()-started)))


if __name__=='__main__':
    main()

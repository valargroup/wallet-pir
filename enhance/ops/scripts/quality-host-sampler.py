#!/usr/bin/env python3
"""Read-only host and Caddy snapshots, isolated from request and APM scrape paths.

The root-owned config lists explicit hosts, service units, filesystems, and SSH
credential paths. An optional `roster` block instead derives one target per
enrolled or draining transparent worker from the fleet roster on every cycle,
so elastic members and replaced owners are sampled under their own names. Only numeric resource observations and aggregate Caddy metrics
are written. Failed hosts disappear from the next snapshot, never retain a fresh
timestamp. The HTTP admin API is accessed on router loopback only.

Optional keys on an explicit target:

- `host_id`: the machine's name (default: the target's first source);
- `size`: its DigitalOcean size slug, reported unchanged;
- `units`: up to 8 more systemd unit names to report beside `unit`.

The `roster` block may also carry `size`, applied to every derived target, and
`role_units`, a map from a roster member's `role` to the unit to sample
instead of `unit`. A derived target's `host_id` is its worker id.

Each output entry keeps the original fields and adds `unit` (the main unit,
whose OOM kills are the entry's `oom_kills`), `host_id`, `size` (when
configured), `swap_total_bytes`, `swap_free_bytes` and `units`:
`{"<unit>": {"memory_current_bytes", "memory_anon_bytes", "memory_peak_bytes",
"memory_max_bytes" (null when unlimited), "oom_kills"}}` for `unit` and every
name in `units`. `memory_anon_bytes` is the cgroup's `anon` plus `shmem` from
`memory.stat`: memory the unit cannot hand back, unlike the page cache counted
in its current use and peak. A property the host does not report, or a unit
systemd has not loaded, is left out, never filled in.
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
unit, directory, mode = sys.argv[1:4]
extra = sys.argv[4:]
out = {'at': int(time.time()), 'unit': unit}
memory = {}
for line in pathlib.Path('/proc/meminfo').read_text().splitlines():
    key, value = line.split(':', 1)
    memory[key] = int(value.split()[0]) * 1024
out['memory_available_bytes'] = memory['MemAvailable']
out['memory_total_bytes'] = memory['MemTotal']
for field, source in [('swap_total_bytes','SwapTotal'),('swap_free_bytes','SwapFree')]:
    if source in memory: out[field] = memory[source]
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
def cgroup_file(group, name):
    if group.startswith('/system.slice/') and '..' not in group:
        p = pathlib.Path('/sys/fs/cgroup') / group.lstrip('/') / name
        if p.exists(): return p.read_text()
    return None
units = {}
for name in [unit] + [u for u in extra if u != unit]:
    try:
        raw = subprocess.check_output(['systemctl','show',name,'-p','LoadState','-p','MemoryCurrent','-p','MemoryPeak','-p','MemoryMax','-p','ControlGroup'], text=True, timeout=3)
    except Exception:
        continue
    props = dict(line.split('=',1) for line in raw.splitlines() if '=' in line)
    if props.get('LoadState') != 'loaded': continue
    group = props.get('ControlGroup','')
    entry = {}
    for field, source, cgroup_name in [('memory_current_bytes','MemoryCurrent',None),('memory_peak_bytes','MemoryPeak','memory.peak'),('memory_max_bytes','MemoryMax',None)]:
        value = props.get(source,'')
        if value.isdigit(): entry[field] = int(value)
        elif field == 'memory_max_bytes' and value == 'infinity': entry[field] = None
        elif cgroup_name:
            text = cgroup_file(group, cgroup_name)
            if text is not None and text.strip().isdigit(): entry[field] = int(text.strip())
    stat = cgroup_file(group, 'memory.stat')
    if stat is not None:
        stat = dict(line.split() for line in stat.splitlines() if len(line.split()) == 2)
        if stat.get('anon', '').isdigit():
            shmem = stat.get('shmem', '')
            entry['memory_anon_bytes'] = int(stat['anon']) + (int(shmem) if shmem.isdigit() else 0)
    events = cgroup_file(group, 'memory.events')
    if events is not None:
        events = dict(line.split() for line in events.splitlines())
        if 'oom_kill' in events: entry['oom_kills'] = int(events['oom_kill'])
    units[name] = entry
out['units'] = units
if mode=='edge':
    text = urllib.request.urlopen('http://127.0.0.1:2019/metrics',timeout=3).read(2097152).decode()
    lines = [line for line in text.splitlines() if line.startswith(('caddy_http_','caddy_config_last_reload_','process_start_time_seconds'))]
    out['edge_metrics'] = '\n'.join(lines)+'\npir_observation_timestamp_seconds '+str(out['at'])+'\n'
print(json.dumps(out))
'''


# No leading `-`, so a name can never be read as a systemctl option.
UNIT = re.compile(r'[A-Za-z0-9_.@][A-Za-z0-9_.@-]*')
MAX_UNITS = 8


def extra_units(target):
    """The target's extra unit names, validated: a list of at most MAX_UNITS unit names."""
    units = target.get('units', [])
    if (not isinstance(units, list) or len(units) > MAX_UNITS
            or not all(isinstance(u, str) and UNIT.fullmatch(u) for u in units)):
        raise ValueError('invalid extra units')
    return units


def sample(target, ssh):
    if not UNIT.fullmatch(target['unit']):
        raise ValueError('invalid service unit')
    args = [target['unit'], target['data_dir'], 'edge' if target.get('edge') else '-', *extra_units(target)]
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
    role_units = spec.get('role_units', {})
    targets = []
    for w in roster:
        if w.get('intent', 'enrolled') not in ('enrolled', 'draining'):
            continue
        target = {'sources': [w['id']], 'host_id': w['id'], 'unit': role_units.get(w.get('role'), spec['unit']),
                  'data_dir': spec.get('data_dir', '/'), 'host': 'root@' + w['ssh_host']}
        if spec.get('size'):
            target['size'] = spec['size']
        targets.append(target)
    return targets


# The APM's limit for host and unit names; a longer or other name would be dropped there.
HOST_ID = re.compile(r'[A-Za-z0-9_.@-]{1,80}')
SIZE = re.compile(r'[a-z0-9-]{1,64}')


def identity(target):
    """The machine fields an output entry carries beside its observations.

    A malformed `host_id` falls back to the first source and a malformed
    `size` is left out, so a bad config never puts free text in the snapshot.
    """
    host_id = target.get('host_id')
    found = {'host_id': host_id if isinstance(host_id, str) and HOST_ID.fullmatch(host_id)
             else target['sources'][0]}
    size = target.get('size')
    if isinstance(size, str) and SIZE.fullmatch(size):
        found['size'] = size
    return found


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
                value.update(identity(target))
                for source in target['sources']:
                    result[source] = value
            except Exception:
                for source in target['sources']:
                    result[source] = {'at': 0, **identity(target)}
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

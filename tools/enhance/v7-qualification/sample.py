#!/usr/bin/env python3
"""Record deployment-specific Linux process/cgroup evidence without resetting counters."""
import argparse
import hashlib
import json
import pathlib
import subprocess
import time
import urllib.request

p = argparse.ArgumentParser()
p.add_argument('--service', required=True)
p.add_argument('--health-url', required=True)
p.add_argument('--binary-sha256', required=True)
p.add_argument('--seconds', type=int, required=True)
p.add_argument('--out', type=pathlib.Path, required=True)
a = p.parse_args()
a.out.parent.mkdir(parents=True, exist_ok=True)
started = time.monotonic()
identity = None

def counters(path):
    return dict((k, int(v)) for k, v in (line.split() for line in path.read_text().splitlines()))

with a.out.open('x') as output:
    while time.monotonic() - started < a.seconds:
        before = time.monotonic()
        sample = {'wall_ns': time.time_ns(), 'elapsed': before - started, 'error': None}
        try:
            state = subprocess.check_output(['systemctl', 'show', a.service,
                '-p', 'MainPID', '-p', 'ControlGroup', '-p', 'NRestarts', '-p', 'ActiveState'], text=True, timeout=5)
            state = dict(line.split('=', 1) for line in state.splitlines())
            assert state['ActiveState'] == 'active' and int(state['MainPID']) > 0
            proc = pathlib.Path('/proc') / state['MainPID']
            ticks = proc.joinpath('stat').read_text().rsplit(')', 1)[1].split()[19]
            current = (state['MainPID'], ticks, str(proc.joinpath('exe').resolve()))
            if identity is None:
                assert hashlib.sha256(proc.joinpath('exe').read_bytes()).hexdigest() == a.binary_sha256
                identity = current
            assert current == identity, 'process identity changed'
            group = pathlib.Path('/sys/fs/cgroup') / state['ControlGroup'].lstrip('/')
            sample['service'] = state
            sample['identity'] = current
            sample['cgroup'] = {name: group.joinpath(name).read_text().strip() for name in
                ['memory.current', 'memory.peak', 'memory.swap.current', 'memory.max', 'memory.high']}
            sample['events'] = counters(group / 'memory.events')
            sample['memory_stat'] = counters(group / 'memory.stat')
            sample['process'] = {line.split(':')[0]: int(line.split()[1]) * 1024
                for line in proc.joinpath('status').read_text().splitlines() if line.startswith(('VmRSS:', 'VmHWM:', 'VmSwap:'))}
            sample['host_swap'] = {k: v for k, v in counters(pathlib.Path('/proc/vmstat')).items() if k in ('pswpin', 'pswpout')}
            with urllib.request.urlopen(a.health_url, timeout=2) as response:
                sample['health'] = json.load(response)
            assert sample['health']['protocol'] == 'ironwood-enhance-pir-v7'
            assert proc.joinpath('stat').read_text().rsplit(')', 1)[1].split()[19] == ticks
        except Exception as exc:
            sample['error'] = type(exc).__name__
        sample['collection_seconds'] = time.monotonic() - before
        output.write(json.dumps(sample, sort_keys=True) + '\n')
        output.flush()
        time.sleep(max(0, min(2 - (time.monotonic() - before), a.seconds - (time.monotonic() - started))))

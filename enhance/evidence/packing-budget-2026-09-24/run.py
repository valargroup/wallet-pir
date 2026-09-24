#!/usr/bin/env python3
"""Run only the isolated benchmark in a fresh, bounded systemd cgroup per trial."""
import argparse
import json
import pathlib
import subprocess
import time

p = argparse.ArgumentParser()
p.add_argument('--binary', required=True)
p.add_argument('--out', required=True)
p.add_argument('--name', required=True)
p.add_argument('--limit', default='4G')
p.add_argument('--cpus', default='0,1')
p.add_argument('args', nargs=argparse.REMAINDER)
a = p.parse_args()
out = pathlib.Path(a.out).resolve()
out.mkdir(parents=True, exist_ok=True)
unit = 'packing-budget-' + a.name
log = out / (a.name + '.jsonl')
if log.exists():
    raise SystemExit('refusing to overwrite evidence')
command = ['systemd-run', '--unit=' + unit, '-p', 'MemoryMax=' + a.limit,
           '-p', 'MemorySwapMax=0', '-p', 'MemoryAccounting=yes',
           '-p', 'AllowedCPUs=' + a.cpus, '-p', 'RemainAfterExit=yes',
           '-p', 'RuntimeMaxSec=900', '-p', 'StandardOutput=file:' + str(log),
           '-p', 'StandardError=file:' + str(out / (a.name + '.stderr')),
           a.binary] + a.args
started = time.time()
subprocess.run(command, check=True)
measurements = []
props = {}
while True:
    raw = subprocess.check_output(['systemctl', 'show', unit, '-p', 'ActiveState',
        '-p', 'SubState', '-p', 'Result', '-p', 'ExecMainStatus', '-p', 'ExecMainCode',
        '-p', 'ControlGroup', '-p', 'MemoryPeak', '-p', 'CPUUsageNSec'], text=True)
    props = dict(line.split('=', 1) for line in raw.splitlines() if '=' in line)
    group = props.get('ControlGroup', '')
    cg = pathlib.Path('/sys/fs/cgroup' + group) if group else None
    sample = {'elapsed': time.time() - started, 'control_group': group}
    for f in ['memory.current', 'memory.peak', 'memory.events', 'memory.stat', 'cpu.stat']:
        if cg is None:
            break
        try:
            sample[f] = (cg / f).read_text().strip()
        except FileNotFoundError:
            pass
    measurements.append(sample)
    if props.get('SubState') in ['exited', 'failed', 'dead']:
        break
    time.sleep(0.1)
summary = {'command': command, 'started_unix': started, 'elapsed': time.time() - started,
           'properties': props, 'samples': measurements}
(out / (a.name + '.cgroup.json')).write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps({'name': a.name, 'properties': props, 'elapsed': summary['elapsed']}), flush=True)
subprocess.run(['systemctl', 'stop', unit], check=False)
subprocess.run(['systemctl', 'reset-failed', unit], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

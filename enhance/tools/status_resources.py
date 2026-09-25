#!/usr/bin/env python3
"""Capture host and cgroup resource evidence for a private Status campaign."""
import argparse
import json
import subprocess
import time
from pathlib import Path


def sample(units):
    memory = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    swap = (int(memory['SwapTotal'].split()[0]) - int(memory['SwapFree'].split()[0])) * 1024
    services = {}
    for unit in units:
        properties = dict(line.split('=', 1) for line in subprocess.check_output(
            ['systemctl', 'show', unit, '-p', 'ControlGroup', '-p', 'NRestarts',
             '-p', 'ActiveState', '-p', 'MemoryCurrent', '-p', 'MemoryPeak'], text=True).splitlines())
        group = Path('/sys/fs/cgroup') / properties['ControlGroup'].lstrip('/')
        events = dict(line.split() for line in (group / 'memory.events').read_text().splitlines())
        services[unit] = dict(properties, memory_events={k: int(v) for k, v in events.items()})
    oom = sum(s['memory_events']['oom'] + s['memory_events']['oom_kill'] for s in services.values())
    result = dict(sampled_ms=time.time_ns() // 1_000_000, oom_events=oom, swap_bytes=swap,
                  memory_available_bytes=int(memory['MemAvailable'].split()[0]) * 1024, services=services)
    if Path('/usr/bin/nvidia-smi').exists():
        result['gpu_memory_mib'] = subprocess.check_output(
            ['nvidia-smi', '--query-gpu=memory.used,memory.total', '--format=csv,noheader,nounits'],
            text=True).strip()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=int, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--unit', action='append', required=True)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 22_000:
        parser.error('seconds must be 1..22000')
    end = time.monotonic() + args.seconds
    with args.output.open('x') as stream:
        while True:
            stream.write(json.dumps(sample(args.unit), sort_keys=True) + '\n')
            stream.flush()
            remaining = end - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(5, remaining))


if __name__ == '__main__':
    main()

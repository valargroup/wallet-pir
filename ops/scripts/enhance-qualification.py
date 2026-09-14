#!/usr/bin/env python3
"""Supervise an isolated pair's fixture and retain hardware/failover evidence.

Run on the coordinator against NEW workers only. This never grants the combined
deployment receipt: online expansion and production publication lag need their
own evidence. Credentials and synthetic worker data are managed by the operator.
"""
import argparse
import concurrent.futures
import json
from pathlib import Path
import shlex
import subprocess
import time

SAMPLE = r'''
import json,subprocess
from pathlib import Path
p=subprocess.run(['systemctl','show','enhance-pir-worker','--property=ControlGroup,ActiveState,NRestarts'],capture_output=True,text=True,check=True)
v=dict(line.split('=',1) for line in p.stdout.splitlines() if '=' in line)
root=Path('/sys/fs/cgroup')/v['ControlGroup'].lstrip('/')
out={'active':v['ActiveState'],'restarts':int(v['NRestarts'])}
if v['ActiveState']=='active':
 for name in ['memory.current','memory.peak','memory.swap.current']:
  out[name]=int((root/name).read_text())
 out['memory.events']={k:int(n) for k,n in (line.split() for line in (root/'memory.events').read_text().splitlines())}
out['swap_io']={k:int(n) for k,n in (line.split() for line in Path('/proc/vmstat').read_text().splitlines()) if k in ['pswpin','pswpout']}
print(json.dumps(out))
'''


def assess(samples, fixture, failover_completed, seconds):
    reasons = []
    if not fixture.get('passed') or not fixture.get('retained_session_queries'):
        reasons.append('exact-answer/retained-session fixture did not pass')
    if fixture.get('serving_seconds', 0) < seconds or fixture.get('publications', 0) < 300:
        reasons.append('duration/publication requirement not met')
    if not failover_completed:
        reasons.append('controlled replica outage was not completed')
    for host, records in samples.items():
        if any('error' in s for s in records):
            reasons.append(f'{host}: missing hardware evidence')
        active = [s for s in records if s.get('active') == 'active']
        if len(active) < seconds / 15:
            reasons.append(f'{host}: insufficient active samples')
            continue
        memory = sorted(s['memory.current'] for s in active)
        if memory[int((len(memory) - 1) * .95)] >= 6 * 1024**3:
            reasons.append(f'{host}: steady memory exceeds 6 GiB')
        if max(s['memory.peak'] for s in active) >= 7 * 1024**3:
            reasons.append(f'{host}: peak memory reaches 7 GiB')
        if any(s['memory.events'].get('oom_kill', 0) or s['restarts'] for s in active):
            reasons.append(f'{host}: memory kill or unplanned service restart')
        if any(s.get('active') != 'active' and not s.get('planned_outage') for s in records):
            reasons.append(f'{host}: unexpected service outage')
        if any(b['time'] - a['time'] > 30 for a, b in zip(records, records[1:])):
            reasons.append(f'{host}: hardware sampling gap exceeds 30 seconds')
        swapping_since = None
        for a, b in zip(active, active[1:]):
            swapping = sum(b['swap_io'].values()) > sum(a['swap_io'].values())
            swapping_since = (swapping_since or a['time']) if swapping else None
            if swapping_since and b['time'] - swapping_since >= 300:
                reasons.append(f'{host}: sustained swap I/O')
                break
    return reasons


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--isolated-workers', action='store_true', required=True)
    parser.add_argument('--worker', action='append', required=True)
    parser.add_argument('--key', required=True)
    parser.add_argument('--known-hosts', required=True)
    parser.add_argument('--artifact-dir', type=Path, required=True)
    parser.add_argument('--run-dir', type=Path, required=True)
    parser.add_argument('--seconds', type=int, default=21600)
    args = parser.parse_args()
    if len(set(args.worker)) != 2 or args.seconds < 21600:
        parser.error('requires two distinct new workers and at least six hours')
    args.run_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    (args.run_dir / 'revision').write_text((args.artifact_dir / 'revision').read_text())

    def ssh(host, command):
        result = subprocess.run(['ssh', '-i', args.key, '-o', 'BatchMode=yes',
            '-o', 'IdentitiesOnly=yes', '-o', 'ForwardAgent=no', '-o', 'ConnectTimeout=10',
            '-o', 'StrictHostKeyChecking=yes', '-o', f'UserKnownHostsFile={args.known_hosts}',
            f'root@{host}', command], capture_output=True, text=True, timeout=20)
        if result.returncode:
            raise RuntimeError('worker SSH operation failed')
        return result.stdout

    report = args.run_dir / 'fixture.json'
    command = [str(args.artifact_dir / 'enhance-pir-qualify'), '--isolated-workers',
        '--shards', '16', '--seconds', str(args.seconds), '--min-publications', '300',
        '--work-dir', str(args.run_dir / 'fixture'), '--output', str(report)]
    for host in args.worker:
        command.extend(['--worker-url', f'http://{host}:8091'])
    started = time.time()
    stopped = False
    restarted = False
    stop_time = None
    samples = {host: [] for host in args.worker}
    with (args.run_dir / 'fixture.log').open('w') as log, \
            (args.run_dir / 'hardware.jsonl').open('w') as trace, \
            concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        fixture = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        try:
            while fixture.poll() is None:
                now = time.time()
                if not stopped and now - started >= 1800:
                    ssh(args.worker[0], 'systemctl stop enhance-pir-worker')
                    stopped, stop_time = True, time.time()
                if stopped and not restarted and now - stop_time >= 90:
                    ssh(args.worker[0], 'systemctl start enhance-pir-worker')
                    restarted = True
                futures = {host: pool.submit(ssh, host, 'python3 -c ' + shlex.quote(SAMPLE)) for host in args.worker}
                for host, future in futures.items():
                    try:
                        sample = json.loads(future.result())
                    except Exception:
                        sample = {'error': 'hardware probe failed'}
                    sample.update(time=time.time(), host=host,
                        planned_outage=host == args.worker[0] and stopped and not restarted)
                    samples[host].append(sample)
                    trace.write(json.dumps(sample) + '\n')
                trace.flush()
                time.sleep(5)
        finally:
            if fixture.poll() is None:
                fixture.terminate()
                fixture.wait(timeout=30)
            if stopped and not restarted:
                ssh(args.worker[0], 'systemctl start enhance-pir-worker')
    result = json.loads(report.read_text()) if report.exists() else {}
    reasons = assess(samples, result, stopped and restarted, args.seconds)
    if fixture.returncode:
        reasons.append('fixture process failed')
    summary = {'passed': not reasons, 'reasons': reasons, 'fixture': result,
        'failover_completed': stopped and restarted,
        'scope': 'isolated c-4 pair; online expansion and production lag require separate evidence'}
    (args.run_dir / 'summary.json').write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary))
    return bool(reasons)


if __name__ == '__main__':
    raise SystemExit(main())

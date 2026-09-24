#!/usr/bin/env python3
"""Fetch and assess the completed SSH campaign without changing production services."""
import argparse
import pathlib
import subprocess
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--wait', action='store_true')
p.add_argument('--directory', type=pathlib.Path, default=pathlib.Path(__file__).parent)
a = p.parse_args()
root = a.directory.resolve()
coordinator = 'root@167.99.42.60'
ssh = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10']
while True:
    state = subprocess.run(ssh + [coordinator, 'systemctl show architecture2-production-qualification.service -p ActiveState -p Result -p ExecMainStatus'], capture_output=True, text=True, check=True).stdout
    if 'ActiveState=active' not in state and 'ActiveState=activating' not in state:
        break
    if not a.wait:
        raise SystemExit('Campaign is still active; no completion claimed.')
    time.sleep(30)
(root / 'sustained-service.txt').write_text(state)
for filename in ['sustained-started.txt', 'sustained-completed.txt', 'sustained-load.json', 'sustained-load.log', 'health-before-sustained.json', 'health-after-sustained.json', 'rate-4.json', 'rate-8.json', 'rate-16.json']:
    result = subprocess.run(ssh + [coordinator, f'cat /root/architecture2-deploy/{filename}'], capture_output=True)
    if result.returncode == 0:
        (root / filename).write_bytes(result.stdout)
for role, host, path in [
    ('coordinator', coordinator, '/root/architecture2-deploy/coordinator-samples.jsonl'),
    ('ingress', coordinator, '/root/architecture2-deploy/ingress-samples.jsonl'),
    ('router', 'root@10.142.0.14', '/root/architecture2-production-samples.jsonl'),
    ('worker-01', 'root@10.142.0.15', '/root/architecture2-production-samples.jsonl'),
    ('worker-02', 'root@10.142.0.16', '/root/architecture2-production-samples.jsonl'),
]:
    command = ssh + ([] if host == coordinator else ['-J', coordinator]) + [host, f'gzip -c {path}']
    with (root / f'{role}-sustained.jsonl.gz').open('wb') as target:
        subprocess.run(command, stdout=target, check=True)
if not (root / 'sustained-completed.txt').exists():
    raise SystemExit('Campaign failed before completion; collected evidence requires inspection.')
subprocess.run(['python3', str(root / 'assess-serving.py'), '--directory', str(root)], check=True)

#!/usr/bin/env python3
"""Remove the one retained v5 rollback copy after the v6 cutover window."""
import datetime, fcntl, hashlib, json, os, shutil, subprocess, sys, urllib.request
from pathlib import Path

DEADLINE = datetime.datetime(2026, 9, 24, 17, 5, tzinfo=datetime.timezone.utc)
REVISION = '2b76bcdfadebd564ffdcad0e460901ac0cd91bcc'
CURRENT = 'afdb4b6d70d6947fa60688a3cf4df21fe5eeae69'
BINARY_SHA256 = '2a421799ff260dc3696bf7ccc34cd2bcf6bb8c86f3fe3782a681912adedad42c'
HOST = subprocess.check_output(['hostname'], text=True).strip()
ROLE = 'coordinator' if HOST == 'enhance-pir-coordinator-01' else 'worker'
assert HOST in {'enhance-pir-coordinator-01', 'enhance-pir-worker-01', 'enhance-pir-worker-02'}
CURRENT_UNIT = 'enhance-pir-coordinator.service' if ROLE == 'coordinator' else 'enhance-pir-worker.service'
OLD_UNIT = 'enhance-pir-v4-coordinator.service' if ROLE == 'coordinator' else 'enhance-pir-v4-worker.service'

def status(unit, action):
    return subprocess.run(['systemctl', action, unit], capture_output=True, text=True).stdout.strip()

def fetch(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)

lock = open('/run/lock/enhance-production.lock', 'a+')
fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
assert status(CURRENT_UNIT, 'is-active') == 'active'
assert status(CURRENT_UNIT, 'is-enabled') == 'enabled'
assert status(OLD_UNIT, 'is-active') != 'active'
assert status(OLD_UNIT, 'is-enabled') != 'enabled'
binary = Path('/opt/enhance-pir-v6/releases') / CURRENT / 'enhance-pir-server'
assert hashlib.sha256(binary.read_bytes()).hexdigest() == BINARY_SHA256
if ROLE == 'coordinator':
    health = fetch('http://127.0.0.1:8080/v1/health')
    assert health['protocol'] == 'ironwood-enhance-pir-v6'
    assert health['published_replica_counts'].get('shard-group-01') == 2
    manifest = fetch('http://127.0.0.1:8080/v1/enhance/init')
    assert manifest['protocol_revision'] == 'ironwood-enhance-pir-v6'
    assert manifest['schema_version'] == 11
    old_data = Path('/srv/enhance-pir-v11/canonical')
else:
    ip = '10.142.0.15' if HOST.endswith('01') else '10.142.0.16'
    health = fetch(f'http://{ip}:8091/internal/health')
    assert health['protocol'] == 'ironwood-enhance-pir-v6' and health['published']
    old_data = Path('/srv/enhance-pir-v11/worker')
old_release = Path('/opt/enhance-pir-v4/releases') / REVISION
old_unit_file = Path('/etc/systemd/system') / OLD_UNIT
assert old_data.is_dir() and old_release.is_dir() and old_unit_file.is_file()
now = datetime.datetime.now(datetime.timezone.utc)
if '--check' in sys.argv:
    print(json.dumps({'status': 'ready_for_deadline', 'host': HOST, 'deadline': DEADLINE.isoformat(), 'time': now.isoformat()}))
    sys.exit(0)
assert now >= DEADLINE, 'rollback retention window has not elapsed'
shutil.rmtree(old_data)
shutil.rmtree(old_release)
old_unit_file.unlink()
subprocess.run(['systemctl', 'daemon-reload'], check=True)
print(json.dumps({'status': 'rollback_copy_removed', 'host': HOST, 'time': now.isoformat()}))

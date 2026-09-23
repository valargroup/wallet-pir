#!/usr/bin/env python3
"""Install an unqualified candidate on a dedicated Linux worker.

Run `inspect` before selecting host limits. Installation never registers capacity
or constitutes hardware qualification. The bundle checksum must come from the
trusted release verification step, independently of the downloaded directory.
"""
import argparse
import fcntl
import grp
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import platform
import pwd
import re
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request

GIB = 1024 ** 3
MIB = 1024 ** 2
# Per-worker candidate floor, separate from the multi-worker test host budget.
# Full-size transition measurements and the native campaign must accompany it.
MIN_FREE_DISK_BYTES = 32 * GIB
SERVICE = 'enhance-pir-worker.service'
USER = 'enhance-pir'
PROTOCOL = 'ironwood-enhance-pir-v6'
BUNDLE_FILES = {'enhance-pir-server', 'enhance-pir-cli', 'enhance-pir-load-test',
                'test-local.py', 'bootstrap-worker.py', 'sample-worker.py', 'summarize-samples.py',
                'assess-campaign.py', 'workers.example.json', 'candidate.json', 'revision', 'SHA256SUMS'}


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def atomic(path, data, mode=0o600):
    descriptor, temporary = tempfile.mkstemp(prefix='.bootstrap-', dir=path.parent)
    try:
        with os.fdopen(descriptor, 'wb') as handle:
            os.fchmod(handle.fileno(), mode)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def save(path, value):
    atomic(path, (json.dumps(value, sort_keys=True, indent=2) + '\n').encode())


def verify_bundle(bundle, revision, manifest_sha256):
    if not re.fullmatch('[0-9a-f]{40}', revision) or not re.fullmatch('[0-9a-f]{64}', manifest_sha256):
        raise ValueError('pin the exact revision and trusted checksum-manifest digest')
    paths = list(bundle.iterdir())
    if {p.name for p in paths} != BUNDLE_FILES or any(p.is_symlink() or not p.is_file() for p in paths):
        raise ValueError('candidate bundle must contain exactly the regular release files')
    manifest = (bundle / 'SHA256SUMS').read_bytes()
    if sha256(manifest) != manifest_sha256:
        raise ValueError('candidate checksum manifest differs from the trusted digest')
    checksums = {}
    for line in manifest.decode().splitlines():
        match = re.fullmatch('([0-9a-f]{64})  ([A-Za-z0-9_.-]+)', line)
        if not match or match[2] in checksums:
            raise ValueError('invalid candidate checksum manifest')
        checksums[match[2]] = match[1]
    if set(checksums) != BUNDLE_FILES - {'SHA256SUMS'}:
        raise ValueError('candidate checksum inventory differs')
    for name, checksum in checksums.items():
        if sha256((bundle / name).read_bytes()) != checksum:
            raise ValueError('candidate file checksum mismatch')
    metadata = json.loads((bundle / 'candidate.json').read_text())
    if ((bundle / 'revision').read_text().strip() != revision or metadata.get('source_revision') != revision
            or metadata.get('kind') != 'enhance-pir' or metadata.get('schema_version') != 11
            or metadata.get('protocol_revision') != PROTOCOL or metadata.get('qualification') != 'unqualified'
            or metadata.get('source_dirty') is not False):
        raise ValueError('bootstrap requires a clean exact-revision unqualified candidate')
    header = (bundle / 'enhance-pir-server').read_bytes()[:20]
    if len(header) != 20 or header[:6] != b'\x7fELF\x02\x01' or int.from_bytes(header[18:20], 'little') != 62:
        raise ValueError('worker binary must be a Linux x86-64 ELF executable')
    return {'revision': revision, 'manifest_sha256': manifest_sha256, 'binary_sha256': checksums['enhance-pir-server']}


def storage_filesystem(path=Path('/srv/enhance-pir/worker')):
    # Before initial installation the worker directory may not exist. Measure
    # its closest existing ancestor so a separate data mount is respected.
    while not path.exists():
        parent = path.parent
        if parent == path:
            raise ValueError('worker storage has no existing parent')
        path = parent
    return path


def inspect_host():
    if platform.system() != 'Linux':
        raise ValueError('worker bootstrap requires Linux')
    values = {}
    for line in Path('/proc/meminfo').read_text().splitlines():
        key, value = line.split(':', 1)
        if key in ('MemTotal', 'MemAvailable', 'SwapTotal'):
            values[key] = int(value.split()[0]) * 1024
    return {'hostname': socket.gethostname().split('.')[0], 'machine': platform.machine(),
            'cpus': os.cpu_count(), 'memory_total_bytes': values['MemTotal'],
            'memory_available_bytes': values['MemAvailable'], 'swap_total_bytes': values['SwapTotal'],
            'disk_free_bytes': shutil.disk_usage(storage_filesystem()).free,
            'memory_controller': 'memory' in Path('/sys/fs/cgroup/cgroup.controllers').read_text().split(),
            'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}


def validate_limits(facts, limits):
    keys = {'memory_high_bytes', 'memory_max_bytes', 'memory_swap_max_bytes', 'host_reserve_bytes'}
    if set(limits) != keys or any(type(limits[k]) is not int for k in keys):
        raise ValueError('supply explicit integer byte limits and host reserve')
    high, maximum, swap, reserve = (limits[k] for k in ('memory_high_bytes', 'memory_max_bytes', 'memory_swap_max_bytes', 'host_reserve_bytes'))
    baseline = facts['memory_total_bytes'] - facts['memory_available_bytes']
    if (facts['machine'] != 'x86_64' or facts['cpus'] != 4 or facts['memory_controller'] is not True
            or not 8_000_000_000 <= facts['memory_total_bytes'] <= 8 * GIB
            or facts['disk_free_bytes'] < MIN_FREE_DISK_BYTES):
        raise ValueError('host does not match the isolated four-CPU nominal 8 GB worker profile')
    # The selected x86-64 worker kernel exposes 4 KiB cgroup pages. A value
    # rounded by the kernel cannot pass the installer's exact limit check.
    if (high != 7 * GIB or maximum % 4096 or swap % 4096
            or not high < maximum <= 15 * GIB // 2 or reserve < 512 * MIB
            or maximum + max(reserve, baseline + 128 * MIB) > facts['memory_total_bytes']
            or not 0 <= swap <= min(2 * GIB, facts['swap_total_bytes'])):
        raise ValueError('candidate limits do not fit measured RAM, host overhead or swap')


def placement_policy(sealed_shards=6):
    if type(sealed_shards) is not int or sealed_shards not in (6, 7):
        raise ValueError('sealed shard limit must be six or seven')
    return {'sealed_shards': sealed_shards}


def unit(binary_sha256, private_ipv4, limits, sealed_shards=6):
    placement_policy(sealed_shards)
    if not re.fullmatch('[0-9a-f]{64}', binary_sha256) or str(ipaddress.IPv4Address(private_ipv4)) != private_ipv4:
        raise ValueError('invalid binary identity or private address')
    return f'''[Unit]
Description=Enhance PIR isolated worker (unqualified candidate)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User={USER}
Group={USER}
ExecStart=/opt/enhance-pir/releases/{binary_sha256}/enhance-pir-server worker --sealed-shards {sealed_shards} --listen {private_ipv4}:8291 --data-dir /srv/enhance-pir/worker
Restart=on-failure
RestartSec=3
LimitNOFILE=1048576
MemoryAccounting=yes
MemoryHigh={limits['memory_high_bytes']}
MemoryMax={limits['memory_max_bytes']}
MemorySwapMax={limits['memory_swap_max_bytes']}
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ReadWritePaths=/srv/enhance-pir/worker
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
'''.encode()


def run(arguments):
    result = subprocess.run(arguments, capture_output=True, timeout=90)
    if result.returncode:
        raise RuntimeError('worker bootstrap command failed')
    return result.stdout.decode()


def service_user():
    try:
        account = pwd.getpwnam(USER)
    except KeyError:
        run(['useradd', '--system', '--user-group', '--home-dir', '/srv/enhance-pir/worker', '--no-create-home',
             '--shell', '/usr/sbin/nologin', USER])
        account = pwd.getpwnam(USER)
    if account.pw_uid == 0 or grp.getgrnam(USER).gr_gid != account.pw_gid or account.pw_dir != '/srv/enhance-pir/worker' or account.pw_shell != '/usr/sbin/nologin':
        raise ValueError('existing service user differs from the dedicated identity')
    return account


def idle_health(address):
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(f'http://{address}:8291/internal/health', timeout=3) as response:
        data = response.read(65537)
    if len(data) > 65536:
        raise ValueError('oversized worker health')
    health = json.loads(data)
    if (health.get('protocol') != PROTOCOL or health.get('epoch') != 0 or health.get('revision') != 0
            or health.get('published') != [] or health.get('candidate') is not None
            or not isinstance(health.get('incarnation'), str) or not health['incarnation']):
        raise ValueError('bootstrap requires a fresh idle worker')
    return health


def verify_service(binary, limits):
    properties = dict(line.split('=', 1) for line in run(['systemctl', 'show', SERVICE,
        '--property=MainPID,ControlGroup,MemoryHigh,MemoryMax,MemorySwapMax']).splitlines())
    if int(properties['MainPID']) <= 0 or Path('/proc', properties['MainPID'], 'exe').resolve() != binary:
        raise ValueError('service is not running the pinned binary')
    group = properties['ControlGroup']
    if not group.startswith('/system.slice/') or '..' in Path(group).parts:
        raise ValueError('unexpected worker cgroup')
    cgroup = Path('/sys/fs/cgroup') / group.lstrip('/')
    for setting, kernel, key in [('MemoryHigh', 'memory.high', 'memory_high_bytes'),
                                  ('MemoryMax', 'memory.max', 'memory_max_bytes'),
                                  ('MemorySwapMax', 'memory.swap.max', 'memory_swap_max_bytes')]:
        if int(properties[setting]) != limits[key] or int((cgroup / kernel).read_text()) != limits[key]:
            raise ValueError('effective service memory limits differ from the candidate limits')
    return {'main_pid': int(properties['MainPID']), 'cgroup': group}


def install(bundle, revision, manifest_sha256, worker_name, private_ipv4, limits, facts, root=Path('/'), sealed_shards=6):
    identity = verify_bundle(bundle, revision, manifest_sha256)
    validate_limits(facts, limits)
    if not re.fullmatch('enhance-pir-v4-g0[1-4]-r[12]', worker_name) or facts['hostname'] != worker_name:
        raise ValueError('host identity differs from the requested replica')
    address = ipaddress.IPv4Address(private_ipv4)
    if not any(address in ipaddress.IPv4Network(cidr) for cidr in ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16')):
        raise ValueError('worker must bind its private VPC address')
    content = unit(identity['binary_sha256'], private_ipv4, limits, sealed_shards)
    base = root / 'srv/enhance-pir'
    unit_path = root / 'etc/systemd/system' / SERVICE
    receipt = base / 'bootstrap.json'
    data = base / 'worker'
    binary = root / 'opt/enhance-pir/releases' / identity['binary_sha256'] / 'enhance-pir-server'
    previous = None
    expected = {**identity, 'worker_name': worker_name, 'private_ipv4': private_ipv4, 'limits': limits,
                'placement_policy': placement_policy(sealed_shards), 'unit_sha256': sha256(content), 'qualification': 'unqualified'}
    if receipt.exists():
        previous = json.loads(receipt.read_text())
        if any(previous.get(k) != v for k, v in expected.items()):
            raise ValueError('bootstrap identity changed; upgrades require a separate procedure')
    elif unit_path.exists() or (data.exists() and any(data.iterdir())):
        raise ValueError('refuse to adopt an existing service or worker state')
    if unit_path.is_symlink() or (unit_path.exists() and unit_path.read_bytes() != content):
        raise ValueError('existing service configuration differs')
    disk_state = data / 'worker.json'
    if disk_state.exists():
        state = json.loads(disk_state.read_text())
        if state.get('epoch') != 0 or state.get('revision') != 0 or state.get('published') or state.get('candidate') is not None:
            raise ValueError('refuse to bootstrap an assigned worker')
    if previous is not None and previous.get('phase') == 'bootstrapped':
        # A repeat after successful bootstrap is read-only with respect to the
        # service. The coordinator may since have registered this worker; never
        # stop it if a readiness check now fails or assignment races this check.
        if not unit_path.exists() or not binary.is_file() or binary.is_symlink() or sha256(binary.read_bytes()) != identity['binary_sha256']:
            raise ValueError('previously bootstrapped installation needs explicit repair')
        health = idle_health(private_ipv4)
        if health.get('placement_policy') != placement_policy(sealed_shards):
            raise ValueError('worker placement policy differs from bootstrap policy')
        runtime = verify_service(binary, limits)
        result = {**expected, 'phase': 'bootstrapped', 'host': facts, 'health': health, 'runtime': runtime}
        save(receipt, result)
        return result
    base.mkdir(parents=True, exist_ok=True)
    base.chmod(0o711)
    save(receipt, {**expected, 'phase': 'installing', 'host': facts})
    account = service_user()
    data.mkdir(mode=0o700, exist_ok=True)
    os.chown(data, account.pw_uid, account.pw_gid)
    data.chmod(0o700)
    binary.parent.mkdir(parents=True, exist_ok=True)
    # Root-owned executable directories must remain traversable by the service user.
    for parent in (root / 'opt/enhance-pir', binary.parent.parent, binary.parent):
        parent.chmod(0o755)
    if binary.exists() and (binary.is_symlink() or sha256(binary.read_bytes()) != identity['binary_sha256']):
        raise ValueError('immutable installed binary differs')
    if not binary.exists():
        payload = (bundle / 'enhance-pir-server').read_bytes()
        if sha256(payload) != identity['binary_sha256']:
            raise ValueError('candidate binary changed after verification')
        atomic(binary, payload, 0o755)
    atomic(unit_path, content, 0o644)
    started = False
    try:
        run(['systemctl', 'daemon-reload'])
        started = True
        run(['systemctl', 'enable', '--now', SERVICE])
        deadline = time.monotonic() + 45
        while True:
            try:
                health = idle_health(private_ipv4)
                if health.get('placement_policy') != placement_policy(sealed_shards):
                    raise ValueError('worker placement policy differs from bootstrap policy')
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise RuntimeError('worker health startup deadline exceeded') from None
                time.sleep(0.5)
        runtime = verify_service(binary, limits)
        result = {**expected, 'phase': 'bootstrapped', 'host': facts, 'health': health, 'runtime': runtime}
        save(receipt, result)
        return result
    except BaseException:
        stopped = not started
        if started:
            try:
                run(['systemctl', 'stop', SERVICE])
                stopped = True
            except (OSError, RuntimeError, subprocess.SubprocessError):
                pass
        save(receipt, {**expected, 'phase': 'failed', 'host': facts, 'stop_confirmed': stopped})
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['inspect', 'install'])
    parser.add_argument('--bundle', type=Path)
    parser.add_argument('--revision')
    parser.add_argument('--manifest-sha256')
    parser.add_argument('--worker-name')
    parser.add_argument('--private-ipv4')
    parser.add_argument('--limits', type=Path)
    parser.add_argument('--sealed-shards', type=int, choices=(6, 7), default=6)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        facts = inspect_host()
        if args.command == 'inspect':
            print(json.dumps(facts, sort_keys=True))
            return
        if os.geteuid() != 0 or any(getattr(args, name) is None for name in ('bundle', 'revision', 'manifest_sha256', 'worker_name', 'private_ipv4', 'limits')):
            raise ValueError('root and all explicit installation inputs are required')
        with open('/run/enhance-pir-bootstrap.lock', 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            receipt = install(args.bundle, args.revision, args.manifest_sha256, args.worker_name,
                              args.private_ipv4, json.loads(args.limits.read_text()), facts, sealed_shards=args.sealed_shards)
        print(json.dumps(receipt, sort_keys=True))
    except (ValueError, KeyError, TypeError, OSError, RuntimeError, subprocess.SubprocessError):
        raise SystemExit('worker bootstrap stopped; inspect the dedicated host and bootstrap receipt') from None


if __name__ == '__main__':
    main()

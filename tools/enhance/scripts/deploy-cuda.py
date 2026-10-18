#!/usr/bin/env python3
"""Deploy a verified CI archive to the one optional native CUDA worker."""
import argparse
import contextlib
import hashlib
import importlib.util
import json
import os
import re
from pathlib import Path
import shutil
import stat
import sys
import uuid

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'tools/lib'))
from wallet_pir_ops import durable
from wallet_pir_ops.deploy import descriptors, units
from wallet_pir_ops.deploy.engine import Deployer, DeployError, unit_files
from wallet_pir_ops.deploy.remote import SSHExecutor, production_lock
from wallet_pir_ops.deploy.transaction import Journal, FINAL

PROTOCOL = 'ironwood-enhance-pir-v9-native-two-mask-m29'
EXPECTED = {'protocol': PROTOCOL, 'matvec.matvec_backend': 'cuda', 'matvec.cuda_device': 0}


def validate_inventory(inventory, service):
    chosen = descriptors.targets(service, inventory)
    if len(chosen) != 1 or chosen[0].key != 'worker@enhance-pir-gpu-01':
        raise ValueError('CUDA deploy inventory must contain only worker@enhance-pir-gpu-01')
    target = chosen[0]
    if target.unit != 'enhance-pir-gpu-worker.service' or target.health_equals != EXPECTED:
        raise ValueError('CUDA worker unit and native/CUDA health expectations are required')
    if inventory.lock['type'] != 'pinned_host' or inventory.hosts[target.host].get('jump') != 'coordinator':
        raise ValueError('CUDA deployment requires the pinned coordinator runner and coordinator jump')
    if inventory.ssh['mode'] != 'pinned':
        raise ValueError('CI CUDA deployment requires pinned SSH')
    if not inventory.hosts[target.host].get('jump'):
        raise ValueError('GPU SSH must use the coordinator jump host')
    config = inventory.services['enhance'].get('cuda_validation', {})
    if (not config.get('origin', '').startswith('https://')
            or not Path(config.get('oracle', '')).is_absolute()
            or not config.get('router_url', '').startswith('http://')
            or not config.get('coordinator_url', '').startswith('http://')):
        raise ValueError('CUDA validation requires public HTTPS origin, absolute oracle and private health URLs')
    return target


def preflight_inputs(args, *, require_private_state=True):
    # Check coordinator prerequisites before extracting/staging any artifact.
    for path, private in [(args.ssh_key, True), (args.known_hosts, False), (args.inventory, False)]:
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != 0:
            raise ValueError('deployment input must be a root-owned regular file')
        if info.st_mode & (0o077 if private else 0o022):
            raise ValueError('deployment input permissions are too broad')
    if args.state_dir.exists() or args.state_dir.is_symlink():
        info = args.state_dir.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & (0o077 if require_private_state else 0o022):
            raise ValueError('deployment state directory must be private and root-owned')
    if not shutil.which('ssh') or not shutil.which('scp'):
        raise ValueError('deployment runner requires ssh and scp')


def prepare_state_directory(path, held):
    """Protect the existing root-owned ledger under the production lock.

    Older provisioning created it with ordinary directory permissions. Tighten
    only that directory; never replace it or change any retained transaction.
    Symlinks, foreign ownership and writable shared directories still refuse.
    """
    held.verify()
    path.mkdir(mode=0o700, exist_ok=True)
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        info = os.fstat(descriptor)
        if info.st_uid != 0 or info.st_mode & 0o022:
            raise ValueError('deployment state directory must be root-owned and not shared-writable')
        if info.st_mode & 0o077:
            os.fchmod(descriptor, 0o700)
    finally:
        os.close(descriptor)


def adopt_reviewed_baseline(deployer, current, expected, held, evidence):
    """Adopt an explicitly reviewed executable change, preserving all other unit bytes."""
    if expected is None:
        return None
    if current['exe_sha256'] != expected:
        raise DeployError('running GPU executable differs from --expected-current-sha256')
    if not deployer.baseline_path.exists():
        return None
    raw = deployer.baseline_path.read_bytes()
    baseline = json.loads(raw)
    live = deployer.snapshot({deployer.targets[0].key: current})
    if baseline.get('service') != live['service'] or set(baseline.get('targets', {})) != set(live['targets']):
        raise DeployError('reviewed baseline adoption requires exactly the existing GPU target')
    key = deployer.targets[0].key
    old, new = baseline['targets'][key], live['targets'][key]
    for field in ['host', 'role', 'unit', 'load_state', 'active_state']:
        if old.get(field) != new.get(field):
            raise DeployError('reviewed baseline adoption refused changed ' + field)
    before, after = list(unit_files(old)), list(unit_files(new))
    old_config = units.effective([text for _, text in before])
    new_config = units.effective([text for _, text in after])
    old_binary = units.split_exec(units.exec_start(old_config))[1]
    new_binary = units.split_exec(units.exec_start(new_config))[1]
    if new_binary != deployer.service.release_binary(expected):
        raise DeployError('reviewed GPU executable must use its immutable release path')
    if [path for path, _ in before] != [path for path, _ in after]:
        raise DeployError('reviewed baseline adoption refused changed unit file paths')
    for (_, old_text), (_, new_text) in zip(before, after):
        rewritten = ''.join(line.replace(old_binary, new_binary, 1) if line.startswith('ExecStart=') else line
                            for line in old_text.splitlines(keepends=True))
        if rewritten != new_text:
            raise DeployError('reviewed baseline adoption permits only the executable path to change')
    if old['exe_sha256'] == expected and before == after:
        return None
    held.verify()
    # Retain exact prior bytes before replacing the baseline. The private CI
    # evidence file is uploaded with the deployment receipt, including failure.
    with os.fdopen(os.open(evidence/'baseline-before.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') as saved:
        saved.write(raw)
        saved.flush()
        os.fsync(saved.fileno())
    held.verify()
    durable.atomic_json(deployer.baseline_path, live, mode=0o600)
    after_raw = deployer.baseline_path.read_bytes()
    with os.fdopen(os.open(evidence/'baseline-after.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') as saved:
        saved.write(after_raw)
    return {'expected_current_sha256': expected, 'previous_executable_sha256': old['exe_sha256'],
            'previous_baseline_sha256': hashlib.sha256(raw).hexdigest(),
            'current_baseline_sha256': hashlib.sha256(after_raw).hexdigest()}


def rollback_verified_gpu(deployer, transaction, held):
    """Restore this GPU transaction, then verify public CUDA answers with runtime credentials."""
    if not re.fullmatch(r'enhance-[A-Za-z0-9-]{1,160}', transaction):
        raise DeployError('rollback requires a canonical GPU transaction identifier')
    journal = Journal.load(deployer.state_dir, 'enhance', transaction)
    target = deployer.targets[0]
    if (journal is None or len(journal.hosts) != 1
            or (journal.hosts[0]['host'], journal.hosts[0]['unit']) != (target.host, target.unit)):
        raise DeployError('CUDA rollback requires a transaction for exactly the selected GPU worker')
    expected = journal.hosts[0]['previous']['exe_sha256']
    if not isinstance(expected, str) or not re.fullmatch(r'[0-9a-f]{64}', expected):
        raise DeployError('GPU rollback requires a retained previous executable')
    restored = deployer.rollback(transaction)
    held.verify()
    check = dict(descriptors.exact_check(deployer.service, deployer.inventory))
    check['argv'] = list(check['argv'])
    check['argv'][check['argv'].index('--sha256') + 1] = expected
    deployer.run_exact_check(check, restored, expected)
    return restored, expected


def repair_reviewed_fee_oracle(path, replacement, expected, held, evidence, write):
    """Fill missing fee metadata from a pinned independent oracle, preserving every case."""
    if replacement is None:
        return None
    for file in [path, replacement]:
        info = file.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise DeployError('oracle repair requires root-owned regular inputs without shared write access')
    old_raw, new_raw = path.read_bytes(), replacement.read_bytes()
    if hashlib.sha256(new_raw).hexdigest() != expected:
        raise DeployError('oracle replacement differs from the reviewed SHA-256')
    old, new = json.loads(old_raw), json.loads(new_raw)
    for records in [old, new]:
        if not isinstance(records, list) or any(
                not isinstance(r, dict) or type(r.get('position')) is not int
                or not 0 <= r['position'] < 2**64 or not isinstance(r.get('record_hex'), str) for r in records):
            raise DeployError('oracle repair requires native record positions and hexadecimal bytes')
    if (not isinstance(old, list) or not old or not isinstance(new, list)
            or [r['position'] for r in old] != [r['position'] for r in new]):
        raise DeployError('oracle repair must preserve every position in order')
    if len({r['position'] for r in old}) != len(old):
        raise DeployError('oracle repair requires distinct existing positions')
    changed = 0
    for before, after in zip(old, new):
        a, b = bytes.fromhex(before['record_hex']), bytes.fromhex(after['record_hex'])
        # Native v9's existing fee-only repair policy: flag byte 640 bit2,
        # fee bytes645..653; ciphertext, transparent flags and expiry stay exact.
        if len(a) != 653 or len(b) != 653:
            raise DeployError('oracle repair requires existing native v9 record widths')
        if a == b:
            continue
        if a[640] & 4 or b[640] != (a[640] | 4) or a[:640] != b[:640] or a[641:645] != b[641:645]:
            raise DeployError('oracle repair may only fill previously missing fee metadata')
        changed += 1
    if old_raw == new_raw:
        return None
    held.verify()
    with os.fdopen(os.open(evidence/'oracle-before.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') as saved:
        saved.write(old_raw)
        saved.flush()
        os.fsync(saved.fileno())
    held.verify()
    write(str(path), new_raw, stat.S_IMODE(path.lstat().st_mode))
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise DeployError('installed public oracle differs from its reviewed digest')
    with os.fdopen(os.open(evidence/'oracle-after.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') as saved:
        saved.write(new_raw)
    return {'previous_sha256':hashlib.sha256(old_raw).hexdigest(), 'sha256':expected,
            'positions':len(old), 'fee_metadata_updates':changed}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['preflight', 'deploy', 'rollback'])
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--sha', required=True)
    parser.add_argument('--state-dir', type=Path, required=True)
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--ssh-key', type=Path, required=True)
    parser.add_argument('--known-hosts', type=Path, required=True)
    parser.add_argument('--oracle-replacement', type=Path, help='independent canonical fee-corrected oracle at the same positions')
    parser.add_argument('--oracle-sha256', help='reviewed SHA-256 of --oracle-replacement')
    parser.add_argument('--transaction', help='exact GPU transaction to restore in rollback mode')
    parser.add_argument('--expected-current-sha256', help='reviewed running GPU hash for adoption of a stale baseline')
    args = parser.parse_args()
    if args.expected_current_sha256 is not None and not re.fullmatch(r"[0-9a-f]{64}", args.expected_current_sha256):
        parser.error("--expected-current-sha256 must be a full lowercase SHA-256")
    if bool(args.oracle_replacement) != bool(args.oracle_sha256):
        parser.error('--oracle-replacement and --oracle-sha256 must be supplied together')
    if args.oracle_replacement is not None and not args.oracle_replacement.is_absolute():
        parser.error('--oracle-replacement must be an absolute coordinator path')
    if args.oracle_sha256 and not re.fullmatch(r'[0-9a-f]{64}', args.oracle_sha256):
        parser.error('--oracle-sha256 must be a full lowercase SHA-256')
    if args.mode == 'rollback' and (not args.transaction or args.expected_current_sha256 is not None):
        parser.error('rollback requires --transaction and permits no baseline adoption')
    if args.mode != 'rollback' and args.transaction:
        parser.error('--transaction belongs only to rollback')
    if os.geteuid() != 0:
        parser.error('run on the root coordinator deployment runner')
    try:
        preflight_inputs(args, require_private_state=False)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    args.evidence_dir = args.evidence_dir.resolve()
    args.evidence_dir.mkdir(parents=True, exist_ok=False)
    spec = importlib.util.spec_from_file_location('release', ROOT / 'tools/ci/release.py')
    release = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(release)
    bundle = args.evidence_dir / 'bundle'
    release.extract(args.archive, bundle, args.sha, 'enhance-pir-native-cuda')
    for name in ['candidate.json', 'build.json', 'SHA256SUMS', 'revision']:
        shutil.copy2(bundle / name, args.evidence_dir / name)
    digest = hashlib.sha256((bundle / 'enhance-pir-server').read_bytes()).hexdigest()
    document = json.loads(args.inventory.read_text())
    # Addresses, users and expected health come from the operator inventory;
    # ephemeral CI identity paths are supplied at runtime, never persisted there.
    document['ssh'].update({'key': str(args.ssh_key.resolve()),
                            'known_hosts': str(args.known_hosts.resolve())})
    if hashlib.sha256(args.known_hosts.read_bytes()).hexdigest() != document['ssh']['known_hosts_sha256']:
        raise ValueError('CI host-key inventory differs from the operator pin')
    effective = args.evidence_dir / 'inventory.json'
    effective.write_text(json.dumps(document))
    effective.chmod(0o600)
    inventory = descriptors.load_inventory(effective)
    service = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')['enhance']
    target = validate_inventory(inventory, service)
    executor = SSHExecutor(inventory)
    verification_dir = args.evidence_dir / 'public-check'
    document['services']['enhance']['exact_check'] = {
        'host': document['hosts'][target.host]['jump'],
        'argv': [sys.executable, str(ROOT / 'tools/enhance/scripts/verify-cuda-deployment.py'),
                 '--inventory', str(effective), '--client', str(bundle / 'enhance-pir-load-test'),
                 '--sha256', digest, '--out', str(verification_dir)], 'timeout': 480}
    effective.write_text(json.dumps(document))
    inventory = descriptors.load_inventory(effective)
    deployer = Deployer(service, inventory, executor, args.state_dir)
    result = {'passed': False, 'mode': args.mode, 'revision': args.sha, 'binary_sha256': digest}
    try:
        with production_lock(inventory, executor) as held:
            prepare_state_directory(args.state_dir, held)
            preflight_inputs(args)
            deployer.lock_factory = lambda: contextlib.nullcontext(held)
            deployer.lock = held
            deployer.check_identities([inventory.lock.get('host')])
            latest = Journal.load(args.state_dir, 'enhance')
            if args.mode != 'rollback' and latest and latest.status not in FINAL:
                raise DeployError('unfinished transaction; resume rollback before CUDA deployment')
            result['oracle_repair'] = repair_reviewed_fee_oracle(
                Path(inventory.services['enhance']['cuda_validation']['oracle']),
                args.oracle_replacement, args.oracle_sha256, held, args.evidence_dir,
                lambda path, data, mode: executor.write('coordinator', path, data, mode))
            if args.mode == 'rollback':
                restored, expected = rollback_verified_gpu(deployer, args.transaction, held)
                result.update(passed=True, binary_sha256=expected, transaction=restored.id,
                              transaction_status=restored.status)
                return
            current = executor.probe_unit(target.host, target.unit)
            result['previous_binary_sha256'] = current['exe_sha256']
            ok, reason = deployer.check({'host': target.host, 'unit': target.unit, 'verify': {
                'health': target.url(target.role.health), 'ready': target.role.ready,
                'health_equals': target.health_equals}}, current['exe_sha256'])
            if not ok:
                raise DeployError('previous worker must be healthy native CUDA before staging: ' + reason)
            result['baseline_adoption'] = adopt_reviewed_baseline(
                deployer, current, args.expected_current_sha256, held, args.evidence_dir)
            if not deployer.baseline_path.exists():
                deployer.capture_baseline()
            plans, problems = deployer.assess(digest, bundle / service.binary, retire_historical=True)
            deployer.describe(plans, digest)
            if problems:
                raise DeployError('preflight refused: ' + '; '.join(problems))
            staged = service.root + '/cuda-validation/' + digest
            held.verify()
            executor.mkdir(target.host, staged, 0o755)
            for name in ['enhance-pir-server', 'enhance-pir-cli', 'enhance-pir-load-test', 'cuda-smoke.py']:
                held.verify()
                path = staged + '/' + name
                expected = hashlib.sha256((bundle / name).read_bytes()).hexdigest()
                if executor.sha256(target.host, path) != expected:
                    executor.upload(target.host, bundle / name, path, 0o755)
                if executor.sha256(target.host, path) != expected:
                    raise DeployError('staged validation payload checksum mismatch')
            user = inventory.hosts[target.host]['user']
            smoke_path = '/tmp/wallet-pir-cuda-smoke-' + uuid.uuid4().hex
            code, output = executor.run(target.host, ['sudo', '-n', '-u', user, '--', 'env',
                'LD_LIBRARY_PATH=/usr/local/cuda-12.2/targets/x86_64-linux/lib',
                'python3', staged + '/cuda-smoke.py', '--release-dir', staged,
                '--evidence-dir', smoke_path], 360)
            result['smoke_exit_code'] = code
            result['smoke_output'] = output[-4000:]
            for name in ['summary.json', 'queries.json', 'metadata.json', 'health-before.json', 'health-after.json',
                         'worker1.log', 'worker2.log', 'worker1-health-before.json', 'worker2-health-before.json',
                         'worker1-health-after.json', 'worker2-health-after.json', 'coordinator.log', 'load.log']:
                data = executor.read(target.host, smoke_path + '/' + name)
                if data is not None:
                    (args.evidence_dir / ('smoke-' + name)).write_text(data)
            held.verify()
            executor.run(target.host, ['rm', '-rf', '--', smoke_path], 30)
            if code:
                raise DeployError('isolated CUDA encrypted smoke failed')
            if args.mode == 'deploy':
                journal = deployer.deploy(digest, bundle / service.binary,
                    {'revision': args.sha, 'kind': 'enhance-pir-native-cuda'},
                    retire_historical=True, verify_noop=True)
                result.update({'transaction': journal.id, 'transaction_status': journal.status})
            result['passed'] = True
    except BaseException as error:
        result['error'] = str(error)
        latest = Journal.load(args.state_dir, 'enhance')
        if latest:
            result.update({'transaction': latest.id, 'transaction_status': latest.status})
        # Record post-failure identity/readiness independently of rollback logs.
        try:
            result['worker_after_failure'] = executor.probe_unit(target.host, target.unit)
            result['worker_after_failure'].pop('fragment_text', None)
            result['worker_after_failure'].pop('drop_ins', None)
            result['health_after_failure'] = json.loads(executor.http_get(target.host, target.url(target.role.health))[1])
        except Exception:
            result['rollback_state_unverified'] = True
        raise
    finally:
        latest = Journal.load(args.state_dir, 'enhance')
        if latest:
            sanitized = {key: latest.data.get(key) for key in
                         ['id', 'service', 'binary_sha256', 'source', 'status']}
            sanitized['hosts'] = [{key: record.get(key) for key in
                ['key', 'role', 'host', 'unit', 'action', 'phase', 'restart_issued']}
                for record in latest.hosts]
            sanitized['events'] = [{key: event.get(key) for key in
                ['unix', 'message', 'target', 'returncode']} for event in latest.data.get('events', [])]
            (args.evidence_dir / 'transaction.json').write_text(json.dumps(sanitized, indent=2) + '\n')
        (args.evidence_dir / 'deployment.json').write_text(json.dumps(result, indent=2) + '\n')
        effective.unlink(missing_ok=True)
        # Downloaded binaries are not duplicated in the evidence artifact.
        shutil.rmtree(bundle)


if __name__ == '__main__':
    main()

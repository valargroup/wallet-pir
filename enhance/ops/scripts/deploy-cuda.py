#!/usr/bin/env python3
"""Deploy a verified CI archive to the one optional native CUDA worker."""
import argparse
import contextlib
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
import uuid

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'ops/lib'))
from wallet_pir_ops.deploy import descriptors
from wallet_pir_ops.deploy.engine import Deployer, DeployError
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['preflight', 'deploy'])
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--sha', required=True)
    parser.add_argument('--state-dir', type=Path, required=True)
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--ssh-key', type=Path, required=True)
    parser.add_argument('--known-hosts', type=Path, required=True)
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error('run on the root coordinator deployment runner')
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
    service = descriptors.load_descriptors(ROOT / 'enhance/ops/deploy/deploy.toml')['enhance']
    target = validate_inventory(inventory, service)
    executor = SSHExecutor(inventory)
    verification_dir = args.evidence_dir / 'public-check'
    document['services']['enhance']['exact_check'] = {
        'host': document['hosts'][target.host]['jump'],
        'argv': [sys.executable, str(ROOT / 'enhance/ops/scripts/verify-cuda-deployment.py'),
                 '--inventory', str(effective), '--client', str(bundle / 'enhance-pir-load-test'),
                 '--sha256', digest, '--out', str(verification_dir)], 'timeout': 480}
    effective.write_text(json.dumps(document))
    inventory = descriptors.load_inventory(effective)
    deployer = Deployer(service, inventory, executor, args.state_dir)
    result = {'passed': False, 'mode': args.mode, 'revision': args.sha, 'binary_sha256': digest}
    try:
        with production_lock(inventory, executor) as held:
            deployer.lock_factory = lambda: contextlib.nullcontext(held)
            deployer.lock = held
            deployer.check_identities([inventory.lock.get('host')])
            latest = Journal.load(args.state_dir, 'enhance')
            if latest and latest.status not in FINAL:
                raise DeployError('unfinished transaction; resume rollback before CUDA deployment')
            current = executor.probe_unit(target.host, target.unit)
            result['previous_binary_sha256'] = current['exe_sha256']
            ok, reason = deployer.check({'host': target.host, 'unit': target.unit, 'verify': {
                'health': target.url(target.role.health), 'ready': target.role.ready,
                'health_equals': target.health_equals}}, current['exe_sha256'])
            if not ok:
                raise DeployError('previous worker must be healthy native CUDA before staging: ' + reason)
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
                         'worker.log', 'coordinator.log', 'load.log']:
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

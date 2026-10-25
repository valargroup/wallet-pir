"""The transactional Enhance, Status and Receiver deploy CLI against an in-memory fleet.

Uses current descriptors and service templates; only hosts are fake.
"""
import dataclasses
import hashlib
import http.server
import io
import json
import os
import re
import shutil
import shlex
import subprocess
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'tools/lib'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from fake_fleet import FakeFleet, sha256  # noqa: E402
from wallet_pir_ops.deploy import cli, descriptors, units  # noqa: E402
from wallet_pir_ops.deploy.engine import Deployer, DeployError  # noqa: E402
from wallet_pir_ops.deploy.remote import HELPER, LockHeld, SSHExecutor  # noqa: E402
from wallet_pir_ops.deploy.transaction import Journal  # noqa: E402

class ImmutableWrapperSource(unittest.TestCase):
    def test_plain_python_invocation_does_not_create_bytecode(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / 'tools/lib', root / 'tools/lib',
                            ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
            shutil.copytree(ROOT / 'tools/transparent/lib', root / 'tools/transparent/lib',
                            ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
            (root / 'tools/scripts').mkdir(parents=True)
            wrapper = root / 'tools/scripts/wallet-pir-deploy.py'
            shutil.copyfile(ROOT / 'tools/scripts/wallet-pir-deploy.py', wrapper)
            env = dict(os.environ)
            env.pop('PYTHONDONTWRITEBYTECODE', None)
            result = subprocess.run([sys.executable, str(wrapper), '--help'],
                                    env=env, capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(list(root.rglob('*.pyc')), [])
            self.assertEqual(list(root.rglob('__pycache__')), [])


DEPLOY = ROOT / 'tools/enhance/deploy'
STATUS_DEPLOY = ROOT / 'tools/status/deploy'
SERVICES = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')
LOCK = ('coordinator', descriptors.LOCK_PATH)

OLD = b'pir-query-server previous build'
OLD_SHA = sha256(OLD)
NEW = b'pir-query-server later build'
NEW_SHA = sha256(NEW)
TOKEN = '/etc/enhance-pir/internal-token.env'
# One Enhance transaction runs one binary. These four share pir-query-server.
ENHANCE = [('worker-01', 'enhance-pir-worker.service'), ('worker-02', 'enhance-pir-worker.service'),
           ('router-01', 'enhance-pir-packing-router.service'), ('coordinator', 'enhance-pir-query-ingress.service')]
SERVING_SELECTORS = ('worker', 'packing-router', 'query-ingress')
STATUS_OLD = b'status-pir built from c5a6486'
STATUS_NEW = b'status-pir built from a later revision'
STATUS_QUERY_OLD = b'pir-query-server built from c5a6486'
STATUS_QUERY_NEW = b'pir-query-server built from a later revision'
STATUS_LEGACY = '/opt/wallet-pir/releases/c5a6486/native'
RECEIVER_TEMPLATE = ROOT / 'tools/receiver/digitalocean/receiver-pir.service.in'
RECEIVER_OLD = b'pir-publisher built from c7f6d296'
RECEIVER_NEW = b'pir-publisher built from a later revision'
RECEIVER_CURRENT = '/opt/receiver-pir/current/pir-publisher'
NEAR_KEY = '2026-10-01'
NEAR_KEY_FILE = '/etc/receiver-pir/near-%s.env'
from tools.receiver.lib.roles import publisher as receiver_publisher
RECEIVER_SETTINGS = {
    'config_path': '/etc/receiver-pir/publisher.json', 'management_listen': '127.0.0.1:18381',
    'near_key_id': NEAR_KEY, 'chain': {'rpc_url': 'http://node.example', 'rpc_cookie': None, 'poll_ms': 1000},
    'receiver': {'data_dir': '/srv/receiver-pir/index', 'bind': '127.0.0.1:18380'},
}
def receiver_unit(release, key=NEAR_KEY):
    return receiver_publisher({**RECEIVER_SETTINGS, 'near_key_id': key}, release=release).unit
RECEIVER_LIVE = receiver_unit('/opt/receiver-pir/current')
# The unit installed by hand before the tool, with the optional unversioned key file.
RECEIVER_HAND_INSTALLED = RECEIVER_LIVE.replace('EnvironmentFile=-' + NEAR_KEY_FILE % NEAR_KEY,
                                                'EnvironmentFile=-/etc/receiver-pir/near.env')
RECEIVER_PROBE = ['{release_dir}/receiver-probe', '--origin', 'https://receiver.example', '--no-auth']


def receiver_release(sha):
    return '/opt/receiver-pir/releases/' + sha


def probe_argv(sha):
    """The receiver's exact check as it runs for release `sha`."""
    return [argument.replace('{release_dir}', receiver_release(sha)) for argument in RECEIVER_PROBE]

def worker_settings(listen):
    return {'listen': listen, 'data_dir': '/srv/enhance-pir/worker', 'sealed_shards': 6,
            'matvec_backend': 'cpu', 'credentials': [TOKEN],
            'synchronization': synchronization(shards=[0])}


def synchronization(**responsibilities):
    return {'publisher_origin': 'https://publisher.example', 'download_budget_bytes': 4 << 30,
            'cache_limit_bytes': 4 << 30, **responsibilities}


def status_settings(role):
    return dict(network_hex='08' * 32, state_dir='/var/lib/status-pir-' + role + '-native',
                listen='127.0.0.1:' + ('8481' if role == 'worker' else '8482'),
                publisher='https://publisher.example',
                worker_origin='http://127.0.0.1:8481',
                **({'query_listen': '127.0.0.1:8484'} if role == 'router' else {}))


INVENTORY = {
    'lock': {'type': 'remote', 'host': 'coordinator'},
    'ssh': {'mode': 'config'},
    'hosts': {name: {} for name in ('coordinator', 'worker-01', 'worker-02', 'router-01', 'status-01', 'receiver-01')},
    'services': {
        'enhance': {
            'roles': {
                'worker': [{'host': 'worker-01', 'vars': {'listen': '10.0.0.15:8091'},
                            'settings': worker_settings('10.0.0.15:8091')},
                           {'host': 'worker-02', 'vars': {'listen': '10.0.0.16:8091'},
                            'settings': worker_settings('10.0.0.16:8091')}],
                'packing-router': [{'host': 'router-01', 'vars': {'control_listen': '10.0.0.14:8093'},
                                    'settings': {'listen': '10.0.0.14:8092', 'control_listen': '10.0.0.14:8093',
                                                 'data_dir': '/srv/enhance-pir-router',
                                                 'synchronization': synchronization(workers={
                                                     '0': ['http://10.0.0.15:8091', 'http://10.0.0.16:8091']}),
                                                 'credentials': [TOKEN]}}],
                'query-ingress': [{'host': 'coordinator', 'vars': {'control_listen': '127.0.0.1:8083'},
                                   'settings': {'listen': '127.0.0.1:8082', 'control_listen': '127.0.0.1:8083',
                                                'data_dir': '/srv/enhance-pir-ingress', 'credentials': [TOKEN],
                                                'synchronization': synchronization(routers={'0': [{
                                                    'control_origin': 'http://10.0.0.14:8093',
                                                    'query_origin': 'http://10.0.0.14:8092'}]})}}],
            },
            'exact_check': {'host': 'coordinator', 'argv': ['/usr/local/bin/exact', '{release_dir}', '{transaction}']},
        },
        'status': {
            'roles': {role: [{'host': 'status-01', 'settings': status_settings(role)}] for role in ('worker', 'router')},
            'exact_check': {'host': 'coordinator', 'argv': ['/usr/local/bin/exact', '{release_dir}']},
        },
        'receiver': {
                        'roles': {'server': [{'host': 'receiver-01', 'vars': {'management_listen': '127.0.0.1:18381'}, 'settings': RECEIVER_SETTINGS}]},
            'exact_check': {'host': 'receiver-01', 'argv': RECEIVER_PROBE, 'timeout': 120},
        },
    },
}


def dropin(unit, name):
    return '/etc/systemd/system/%s.d/%s' % (unit, name)


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class Fleet(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.state = self.dir / 'state'
        self.inventory_path = self.dir / 'inventory.json'
        self.inventory_path.write_text(json.dumps(INVENTORY))
        self.inventory = descriptors.load_inventory(self.inventory_path)
        self.fake = FakeFleet()
        self.lines = []
        self.clock = Clock()
        self.binary = self.dir / 'pir-query-server'
        self.binary.write_bytes(NEW)
        self.status_binary = self.dir / 'status-pir'
        self.status_binary.write_bytes(STATUS_NEW)
        self.status_query_binary = self.dir / 'status-pir-query-server'
        self.status_query_binary.write_bytes(STATUS_QUERY_NEW)
        self.receiver_binary = self.dir / 'pir-publisher'
        self.receiver_binary.write_bytes(RECEIVER_NEW)

    def deployer(self, name='enhance', only=None, service=None):
        deployer = Deployer(service or SERVICES[name], self.inventory, self.fake, self.state, out=self.lines.append,
                            sleep=self.clock.sleep, clock=self.clock, poll_seconds=5, only=only)
        for target in deployer.targets:
            for url in (target.url(target.role.health), target.url(target.role.ready_url)):
                if url:
                    self.fake.endpoints[(target.host, url)] = target.unit
        return deployer

    def enhance_fleet(self):
        """Serving roles run the rendered pir-query-server unit."""
        service = SERVICES['enhance']
        release = '/opt/enhance-pir/releases/' + OLD_SHA
        for target in descriptors.targets(service, self.inventory):
            rendered = target.role.render(target.settings, release)
            files = {release + '/pir-query-server': OLD, TOKEN: 'ENHANCE_INTERNAL_TOKEN=test\n', **rendered.files}
            self.fake.install(target.host, target.unit, rendered.unit, (), files)
        deployer = self.deployer(only=list(SERVING_SELECTORS))
        deployer.capture_baseline()
        self.fake.log.clear()
        return deployer

    def status_fleet(self, live_edits=None, only=('worker', 'router')):
        """Status units as rendered from the templates, then edited like the live drift.

        Worker and router run pir-query-server; publication belongs to the unified publisher.
        """
        service = SERVICES['status']
        placements = [('status-01', 'worker'), ('status-01', 'router')]
        for host, role in placements:
            rendered = service.roles[role].render(status_settings(role), STATUS_LEGACY)
            text = rendered.unit
            for old, new in (live_edits or {}).get(role, []):
                self.assertIn(old, text)
                text = text.replace(old, new)
            files = {STATUS_LEGACY + '/pir-query-server': STATUS_QUERY_OLD, **rendered.files}
            self.fake.install(host, service.roles[role].unit, text, (), files)
        deployer = self.deployer('status', only=list(only))
        deployer.capture_baseline()
        self.fake.log.clear()
        return deployer

    def receiver_fleet(self, live=RECEIVER_LIVE):
        """The receiver unit `live`, by default the template starting the binary through `current`.

        Key files for `NEAR_KEY` and the legacy `near.env` are installed.
        """
        files = {**receiver_publisher(RECEIVER_SETTINGS, release='/opt/receiver-pir/current').files, RECEIVER_CURRENT: RECEIVER_OLD, NEAR_KEY_FILE % NEAR_KEY: 'NEAR_INTENTS_EXPLORER=old\n',
                 '/etc/receiver-pir/near.env': 'NEAR_INTENTS_EXPLORER=old\n'}
        self.fake.product_check = lambda host, argv: (0, 'valid offline configuration')
        self.fake.install('receiver-01', 'receiver-pir.service', live, (), files)
        # Identity is nested, and `serving` stays null until the first publication.
        self.fake.health_shapes[('receiver-01', 'receiver-pir.service')] = lambda ready, exe: {
            'identity': {'binary_sha256': exe}, 'ready': bool(ready)}
        deployer = self.deployer('receiver')
        deployer.capture_baseline()
        self.fake.log.clear()
        return deployer

    def provisioned_receiver(self):
        """`(deployer, sha)` for a host provisioned as the runbook says: release `sha` running the
        template's unit with ` (bootstrap)` on its Description."""
        sha = sha256(RECEIVER_NEW)
        self.fake.put('receiver-01', receiver_release(sha) + '/pir-publisher', RECEIVER_NEW)
        managed = receiver_unit(receiver_release(sha))
        return self.receiver_fleet(re.sub(r'(?m)^Description=.*$', r'\g<0> (bootstrap)', managed, count=1)), sha

    def restarts(self):
        return [(host, detail[1]) for host, op, detail in self.fake.log if op == 'systemctl' and detail[0] == 'restart']

    def running(self, host, unit):
        return self.fake.host(host).units[unit]['exe']


class ParallelStagingTests(Fleet):
    def test_hosts_upload_together_before_any_service_restarts(self):
        deployer = self.enhance_fleet()
        uploads = threading.Barrier(2)
        upload = self.fake.upload

        def overlapping_upload(host, local_path, path, mode):
            if host in ('worker-01', 'worker-02'):
                uploads.wait(timeout=3)
            self.assertFalse(any(op == 'systemctl' for _, op, _ in self.fake.log))
            upload(host, local_path, path, mode)

        with mock.patch.object(self.fake, 'upload', side_effect=overlapping_upload):
            journal = deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual(journal.status, 'committed')
        restarted = [(host, args[-1]) for host, op, args in self.fake.log
                     if op == 'systemctl' and args[0] == 'restart']
        self.assertEqual(restarted, ENHANCE)

    def test_failed_upload_waits_for_other_hosts_before_releasing_the_lock(self):
        deployer = self.enhance_fleet()
        uploads = threading.Barrier(2)
        upload = self.fake.upload
        completed = threading.Event()

        def failing_upload(host, local_path, path, mode):
            if host in ('worker-01', 'worker-02'):
                uploads.wait(timeout=3)
            if host == 'worker-01':
                raise OSError('upload interrupted')
            upload(host, local_path, path, mode)
            if host == 'worker-02':
                self.assertIn(LOCK, self.fake.held)
                completed.set()

        with mock.patch.object(self.fake, 'upload', side_effect=failing_upload):
            with self.assertRaisesRegex(OSError, 'upload interrupted'):
                deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertTrue(completed.is_set())
        self.assertNotIn(LOCK, self.fake.held)
        self.assertFalse(any(op == 'systemctl' for _, op, _ in self.fake.log))
        self.assertEqual(Journal.load(self.state, 'enhance').status, 'failed')


class DeployTests(Fleet):
    def test_router_launch_overlays_retire_and_rollback_restores_their_bytes(self):
        self.enhance_fleet()
        unit = 'enhance-pir-packing-router.service'
        path = dropin(unit, 'zzzzzzzz-native2m-1dae185.conf')
        old_command = '/opt/enhance-pir/releases/' + OLD_SHA + '/pir-query-server'
        overlay = '[Service]\nExecStart=\nExecStart=' + old_command + ' --config /etc/enhance-pir/packing-router.json\nReadWritePaths=/srv/old-router\n'
        self.fake.edit('router-01', path, overlay)
        deployer = self.deployer(only=['packing-router'])
        deployer.capture_baseline()
        _, problems = deployer.assess(NEW_SHA, str(self.binary), allow_drift=True)
        self.assertTrue(problems, 'retirement requires an explicit operator choice')
        journal = deployer.deploy(NEW_SHA, str(self.binary), allow_drift=True, retire_historical=True)
        self.assertEqual(journal.status, 'committed')
        self.assertNotIn(path, self.fake.host('router-01').files)
        deployer.rollback(journal.id)
        self.assertEqual(self.fake.read('router-01', path), overlay)

    def test_router_overlay_cannot_hide_credentials_or_limits_during_retirement(self):
        self.enhance_fleet()
        unit = 'enhance-pir-packing-router.service'
        path = dropin(unit, 'zzzzzzzz-native2m-1dae185.conf')
        old_command = '/opt/enhance-pir/releases/' + OLD_SHA + '/pir-query-server'
        for extra in ('EnvironmentFile=/etc/other.env', 'MemoryMax=1G'):
            with self.subTest(extra=extra):
                overlay = '[Service]\nExecStart=\nExecStart=' + old_command + '\n' + extra + '\n'
                self.fake.edit('router-01', path, overlay)
                deployer = self.deployer(only=['packing-router'])
                deployer.capture_baseline()
                _, problems = deployer.assess(NEW_SHA, str(self.binary), allow_drift=True, retire_historical=True)
                self.assertTrue(any('more than ExecStart' in p for p in problems), problems)

    def test_cuda_driver_failure_refuses_before_restarting_the_working_worker(self):
        deployer = self.enhance_fleet()
        deployer.targets = [dataclasses.replace(deployer.targets[0],
                            artifact_kind='enhance-pir-native-cuda',
                            health_equals={'matvec.cuda_device': 2})]
        self.fake.product_check = lambda host, argv: (1, 'CUDA unavailable')
        with self.assertRaisesRegex(DeployError, 'staged CUDA evaluator check failed'):
            deployer.deploy(NEW_SHA, self.binary)
        self.assertEqual(self.restarts(), [])
        self.assertEqual(self.running('worker-01', 'enhance-pir-worker.service'), OLD_SHA)
        self.assertIn(('worker-01', [deployer.release_binary(NEW_SHA), 'check-backend',
                                   '--matvec-backend', 'cuda', '--cuda-device', '2']), self.fake.runs)

    def test_cuda_evaluator_is_checked_before_activation(self):
        deployer = self.enhance_fleet()
        deployer.targets = [dataclasses.replace(deployer.targets[0],
                            artifact_kind='enhance-pir-native-cuda')]
        journal = deployer.deploy(NEW_SHA, self.binary)
        self.assertEqual(journal.status, 'committed')
        self.assertIn(('worker-01', [deployer.release_binary(NEW_SHA), 'check-backend',
                                   '--matvec-backend', 'cuda', '--cuda-device', '0']), self.fake.runs)

    def test_cuda_probe_preserves_only_the_units_explicit_library_path(self):
        deployer = self.enhance_fleet()
        deployer.targets = [dataclasses.replace(deployer.targets[0],
                            artifact_kind='enhance-pir-native-cuda')]
        state = self.fake.probe_unit('worker-01', 'enhance-pir-worker.service')
        self.fake.host('worker-01').files[state['fragment_path']] += (
            b'\n[Service]\nEnvironment=LD_LIBRARY_PATH=/opt/cuda/lib PRIVATE_TOKEN=fixture-only\n')
        self.fake.systemctl('worker-01', 'daemon-reload')
        deployer.capture_baseline()
        deployer.deploy(NEW_SHA, self.binary, allow_drift=True)
        command = ['env', 'LD_LIBRARY_PATH=/opt/cuda/lib', deployer.release_binary(NEW_SHA),
                   'check-backend', '--matvec-backend', 'cuda', '--cuda-device', '0']
        self.assertIn(('worker-01', command), self.fake.runs)
        self.assertFalse(any('PRIVATE_TOKEN' in argument for _, argv in self.fake.runs for argument in argv))

    def test_only_the_selected_targets_roll(self):
        self.enhance_fleet()
        deployer = self.deployer(only=['worker@worker-02', 'worker@worker-01'])
        self.assertEqual([t.key for t in deployer.targets], ['worker@worker-01', 'worker@worker-02'])
        self.deployer().capture_baseline()
        self.fake.log.clear()
        self.assertEqual(deployer.deploy(NEW_SHA, self.binary).status, 'committed')
        self.assertEqual(self.restarts(), [('worker-01', 'enhance-pir-worker.service'),
                                           ('worker-02', 'enhance-pir-worker.service')])
        for host, unit in ENHANCE:
            self.assertEqual(self.running(host, unit), NEW_SHA if unit == 'enhance-pir-worker.service' else OLD_SHA)
        # The refreshed baseline still records the targets left out.
        recorded = json.loads(self.deployer().baseline_path.read_text())['targets']
        self.assertCountEqual(recorded, [t.key for t in self.deployer().targets])
        self.assertEqual(self.deployer().baseline_problems(self.deployer().probe(), required=True), [])
        with self.assertRaisesRegex(Exception, 'no target matches'):
            self.deployer(only=['worker@nowhere'])

    def test_noop_deploy_with_the_running_binary_changes_nothing(self):
        deployer = self.enhance_fleet()
        self.assertIsNone(deployer.deploy(OLD_SHA))
        self.assertEqual(self.fake.log, [])
        self.assertEqual(self.fake.runs, [])
        self.assertFalse((self.state / 'latest-enhance.json').exists())
        self.assertIn('no-op', self.lines[-1])

    def test_deploy_restarts_in_role_order_and_replaces_the_serving_unit(self):
        deployer = self.enhance_fleet()
        journal = deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual(journal.status, 'committed')
        self.assertEqual(self.restarts(), ENHANCE)
        release = '/opt/enhance-pir/releases/%s/pir-query-server' % NEW_SHA
        configs = {'enhance-pir-worker.service': '/etc/enhance-pir/worker.json',
                   'enhance-pir-packing-router.service': '/etc/enhance-pir/packing-router.json',
                   'enhance-pir-query-ingress.service': '/etc/enhance-pir/query-ingress.json'}
        for host, unit in ENHANCE:
            self.assertEqual(self.running(host, unit), NEW_SHA)
            text = self.fake.read(host, '/etc/systemd/system/' + unit)
            prefix, binary, tail = units.split_exec(units.exec_start(units.effective([text])))
            self.assertEqual((prefix, binary, tail), ('', release, '--config ' + configs[unit]))
            self.assertIn('User=enhance-pir', text)
            self.assertIn('EnvironmentFile=' + TOKEN, text)
            self.assertIn('ReadWritePaths=', text)
            self.assertNotIn(dropin(unit, units.MANAGED_DROP_IN), self.fake.host(host).files)
        worker = self.fake.read('worker-01', '/etc/systemd/system/enhance-pir-worker.service')
        self.assertIn('MemoryMax=7680M', worker)
        self.assertIn('MemoryHigh=7G', worker)
        router = self.fake.read('router-01', '/etc/systemd/system/enhance-pir-packing-router.service')
        self.assertIn('MemoryMax=7G', router)
        ingress = self.fake.read('coordinator', '/etc/systemd/system/enhance-pir-query-ingress.service')
        self.assertIn('MemoryMax=256M', ingress)
        # One upload per host, into an immutable release directory named by the file.
        uploads = [(host, detail) for host, op, detail in self.fake.log if op == 'upload']
        self.assertCountEqual([host for host, _ in uploads], ['worker-01', 'worker-02', 'router-01', 'coordinator'])
        self.assertTrue(all(self.fake.host(host).files[release] == NEW for host, _ in uploads))
        self.assertIn(('coordinator', ['/usr/local/bin/exact', '/opt/enhance-pir/releases/' + NEW_SHA, journal.id]),
                      self.fake.runs)
        # The baseline now describes the committed state, so a repeat is a no-op.
        self.fake.log.clear()
        self.assertIsNone(deployer.deploy(NEW_SHA, self.binary))
        self.assertEqual(self.fake.log, [])

    def test_failure_rolls_back_only_touched_targets_in_reverse(self):
        deployer = self.enhance_fleet()
        self.fake.unhealthy.add(('router-01', NEW_SHA))
        with self.assertRaisesRegex(DeployError, 'packing-router@router-01: not verified'):
            deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        touched = ENHANCE[:3]
        self.assertEqual(self.restarts(), touched + list(reversed(touched)))
        for host, unit in ENHANCE:
            self.assertEqual(self.running(host, unit), OLD_SHA)
            self.assertNotIn(dropin(unit, units.MANAGED_DROP_IN), self.fake.host(host).files)
        previous = '/opt/enhance-pir/releases/' + OLD_SHA
        for target in deployer.targets:
            rendered = target.role.render(target.settings, previous)
            self.assertEqual(self.fake.read(target.host, '/etc/systemd/system/' + target.unit), rendered.unit)
        # The ingress shares the lock host and was staged without being restarted.
        self.assertEqual({op for _, op, _ in self.fake.mutations('coordinator')}, {'mkdir', 'upload', 'rename'})
        journal = Journal.load(self.state, 'enhance')
        self.assertEqual(journal.status, 'rolled-back')
        self.assertEqual([r['phase'] for r in journal.hosts], ['restored'] * 3 + ['pending'])
        # Nothing new is known, so the baseline still matches and allows a later deploy.
        self.assertEqual(deployer.baseline_problems(deployer.probe(), required=True), [])

    def test_role_mode_refuses_a_drop_in_the_rendered_unit_does_not_own(self):
        cases = [
            ('90-v7.conf', '[Service]\nExecStart=\nExecStart=/bin/old worker\nReadWritePaths=/srv/old\nMemoryHigh=6G\n',
             'cannot be superseded'),
            ('50-local.conf', '[Service]\nExecStart=\nExecStart=/bin/old worker\n', 'overrides ExecStart'),
            ('50-memory.conf', '[Service]\nMemoryHigh=6G\n', 'owns its whole unit'),
        ]
        for name, text, message in cases:
            with self.subTest(drop_in=name):
                self.setUp()
                self.enhance_fleet()
                unit = 'enhance-pir-worker.service'
                self.fake.edit('worker-01', dropin(unit, name), text)
                deployer = self.deployer(only=['worker@worker-01'])
                deployer.capture_baseline()
                self.fake.log.clear()
                with self.assertRaisesRegex(DeployError, message):
                    deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
                self.assertEqual(self.fake.log, [])

    def test_baseline_is_required_and_live_changes_since_it_are_refused(self):
        deployer = self.enhance_fleet()
        unit = 'enhance-pir-worker.service'
        path = '/etc/systemd/system/' + unit
        self.fake.edit('worker-01', path, self.fake.read('worker-01', path).replace('RestartSec=3', 'RestartSec=4'))
        with self.assertRaisesRegex(DeployError, 'unit files changed since the baseline'):
            deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        deployer.baseline_path.unlink()
        with self.assertRaisesRegex(DeployError, 'run capture-baseline'):
            deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual(self.fake.log, [])

    def test_unreloaded_unit_edit_is_refused(self):
        deployer = self.enhance_fleet()
        unit = 'enhance-pir-worker.service'
        self.fake.edit('worker-01', dropin(unit, '60-memory.conf'), '[Service]\nMemoryMax=5G\n', reload=False)
        with self.assertRaisesRegex(DeployError, 'without a daemon-reload'):
            deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual(self.fake.log, [])

    def test_production_lock_held_elsewhere_refuses_before_any_change(self):
        deployer = self.enhance_fleet()
        self.fake.held.add(LOCK)
        with self.assertRaises(LockHeld):
            deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual(self.fake.log, [])
        self.assertFalse((self.state / 'latest-enhance.json').exists())

    def test_losing_the_lock_stops_before_the_next_side_effect(self):
        deployer = self.enhance_fleet()

        def lose(host, op, detail):
            if op == 'upload':
                self.fake.lock.lost = True
        self.fake.observer = lose
        with self.assertRaises(LockHeld):
            deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual([op for _, op, _ in self.fake.log if op == 'upload'], ['upload'])
        self.assertEqual(self.restarts(), [])
        self.assertEqual(Journal.load(self.state, 'enhance').status, 'failed')

    def test_every_mutation_happens_under_the_lock(self):
        deployer = self.enhance_fleet()
        self.fake.observer = lambda host, op, detail: self.assertIn(LOCK, self.fake.held)
        deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        deployer.rollback()
        self.assertFalse(self.fake.held)

    def test_identity_refused_by_one_host_changes_nothing(self):
        deployer = self.enhance_fleet()
        self.fake.refuse_identity.add('router-01')
        with self.assertRaisesRegex(DeployError, 'not accepted by router-01; nothing was changed'):
            deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual(self.fake.log, [])

    def test_failed_self_check_touches_no_unit(self):
        deployer = self.enhance_fleet()
        self.fake.self_check_fails.add(NEW_SHA)
        with self.assertRaisesRegex(DeployError, 'self-check'):
            deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual(self.restarts(), [])
        self.assertFalse(any(detail[0].startswith('/etc/') for _, op, detail in self.fake.log if op == 'write'))
        self.assertEqual(Journal.load(self.state, 'enhance').status, 'failed')

    def report(self, **overrides):
        document = {'schema': 'wallet-pir-contract-info/1', 'executable': 'pir-query-server',
                    'contract_version': '1.4.2', 'contract_major': 1,
                    'products': ['enhance'], 'backends': ['cuda'], **overrides}
        return json.dumps(document)

    def test_staging_records_the_executables_contract_identity_and_elapsed_time(self):
        deployer = self.enhance_fleet()
        self.fake.contract_reports[NEW_SHA] = self.report()
        deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        journal = Journal.load(self.state, 'enhance')
        staged = [event for event in journal.data['events'] if event['message'] == 'staged']
        self.assertTrue(staged)
        for event in staged:
            self.assertEqual(event['contract'], {'executable': 'pir-query-server', 'sha256': NEW_SHA,
                                                 'contract_version': '1.4.2', 'contract_major': 1,
                                                 'products': ['enhance'], 'backends': ['cuda']})
        elapsed = deployer.status()['timing']
        self.assertIsNotNone(elapsed['clocks']['total'])
        self.assertIsNotNone(elapsed['clocks']['interruption'])
        self.assertIsNone(elapsed['clocks']['source_to_accepted'])
        self.assertTrue(elapsed['targets'])

    def test_a_release_without_contract_info_stays_deployable_as_unknown(self):
        deployer = self.enhance_fleet()
        deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        staged = [event for event in Journal.load(self.state, 'enhance').data['events']
                  if event['message'] == 'staged']
        self.assertTrue(staged)
        self.assertTrue(all(event['contract'] == 'unknown' for event in staged))

    def test_malformed_or_foreign_contract_info_refuses_before_any_restart(self):
        for report in (self.report(executable='pir-publisher'), self.report(contract_major=2), 'not json'):
            with self.subTest(report=report):
                self.setUp()
                deployer = self.enhance_fleet()
                self.fake.contract_reports[NEW_SHA] = report
                with self.assertRaisesRegex(DeployError, 'contract'):
                    deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
                self.assertEqual(self.restarts(), [])
                self.assertEqual(Journal.load(self.state, 'enhance').status, 'failed')

    def test_journal_phase_is_written_before_each_side_effect(self):
        deployer = self.enhance_fleet()
        seen = []

        def observe(host, op, detail):
            journal = Journal.load(self.state, 'enhance')
            seen.append((host, op, detail, journal.status, {r['unit']: r['phase'] for r in journal.hosts if r['host'] == host}))
        self.fake.observer = observe
        deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.fake.unhealthy.add(('coordinator', OLD_SHA))  # irrelevant to the checks below
        for host, op, detail, status, phases in seen:
            if op == 'upload':
                self.assertEqual(status, 'staging')
            if op == 'write' and detail.startswith('/opt/enhance-pir/transactions/'):
                unit = detail.split('/')[5]
                self.assertEqual(phases[unit], 'backing-up')
            if op in ('write', 'rename') and str(detail).startswith(('/etc/systemd', "('/etc/systemd")):
                path = detail if op == 'write' else detail[0]
                unit = next(u for u in phases if '/%s.d/' % u in path or path.endswith('/' + u))
                self.assertEqual(phases[unit], 'installing')
            if op == 'systemctl' and detail[0] == 'restart':
                self.assertEqual(phases[detail[1]], 'restarting')
        self.assertTrue(any(op == 'systemctl' for _, op, *_ in seen))

    def test_health_reporting_another_binary_fails_verification(self):
        deployer = self.enhance_fleet()
        self.fake.identity_override[('worker-01', 'enhance-pir-worker.service')] = OLD_SHA
        with self.assertRaisesRegex(DeployError, 'health reports binary_sha256'):
            deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual(self.running('worker-01', 'enhance-pir-worker.service'), OLD_SHA)
        self.assertEqual(Journal.load(self.state, 'enhance').status, 'rolled-back')

    def test_exact_check_failure_rolls_everything_back(self):
        deployer = self.enhance_fleet()
        self.fake.exact_result = (1, 'answer mismatch at row 17')
        self.fake.product_check = lambda host, argv: (0, 'configuration accepted')
        with self.assertRaisesRegex(DeployError, 'exact-answer check failed'):
            deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual(self.restarts(), ENHANCE + list(reversed(ENHANCE)))
        self.assertTrue(all(self.running(h, u) == OLD_SHA for h, u in ENHANCE))

    def test_missing_exact_check_must_be_skipped_explicitly(self):
        document = json.loads(self.inventory_path.read_text())
        del document['services']['enhance']['exact_check']
        self.inventory_path.write_text(json.dumps(document))
        self.inventory = descriptors.load_inventory(self.inventory_path)
        deployer = self.enhance_fleet()
        with self.assertRaisesRegex(DeployError, 'skip-exact-check'):
            deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual(deployer.deploy(NEW_SHA, self.binary, retire_historical=True, skip_exact_check=True).status,
                         'committed')

    def test_requested_hold_keeps_failed_candidate_for_operator_repair(self):
        deployer = self.enhance_fleet()
        deployer.hold_on_failure = True
        self.fake.exact_result = (1, 'answer mismatch at row 17')
        self.fake.product_check = lambda host, argv: (0, 'configuration accepted')
        with self.assertRaisesRegex(DeployError, 'exact-answer check failed'):
            deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual(Journal.load(self.state, 'enhance').status, 'held')
        self.assertEqual(self.restarts(), ENHANCE)
        self.assertTrue(all(self.running(h, u) == NEW_SHA for h, u in ENHANCE))


class RollbackTests(Fleet):
    def test_rollback_restores_every_touched_target_and_is_idempotent(self):
        deployer = self.enhance_fleet()
        before = {host: dict(self.fake.host(host).files) for host, _ in ENHANCE}
        deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.fake.log.clear()
        deployer.rollback()
        self.assertEqual(self.restarts(), list(reversed(ENHANCE)))
        for host, unit in ENHANCE:
            self.assertEqual(self.running(host, unit), OLD_SHA)
            for path in self.fake.unit_paths(host, unit):
                self.assertEqual(self.fake.host(host).files[path], before[host][path])
            self.assertNotIn(dropin(unit, units.MANAGED_DROP_IN), self.fake.host(host).files)
        self.fake.log.clear()
        deployer.rollback()
        self.assertEqual(self.fake.log, [])
        self.assertIn('already rolled back', self.lines[-1])

    def test_interrupted_rollback_resumes_only_unfinished_targets(self):
        deployer = self.enhance_fleet()
        deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.fake.failing_restart.add(('worker-01', 'enhance-pir-worker.service'))
        with self.assertRaisesRegex(DeployError, 'rollback incomplete'):
            deployer.rollback()
        self.assertEqual(Journal.load(self.state, 'enhance').status, 'rollback-failed')
        self.fake.failing_restart.clear()
        self.fake.log.clear()
        deployer.rollback()
        self.assertEqual(self.restarts(), [('worker-01', 'enhance-pir-worker.service')])
        self.assertEqual(self.running('worker-01', 'enhance-pir-worker.service'), OLD_SHA)

    def test_rollback_retries_readiness_without_restarting_the_restored_process(self):
        self.enhance_fleet()
        deployer = self.deployer(only=['worker@worker-01'])
        deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.fake.unhealthy.add(('worker-01', OLD_SHA))
        with self.assertRaisesRegex(DeployError, 'rollback incomplete'):
            deployer.rollback()
        self.fake.log.clear()
        # The authority may remain unavailable across more than one retry.
        with self.assertRaisesRegex(DeployError, 'rollback incomplete'):
            deployer.rollback()
        self.assertEqual(self.fake.log, [], 'retry must leave the preparing process running')
        self.assertEqual(Journal.load(self.state, 'enhance').status, 'rollback-failed')
        with self.assertRaisesRegex(DeployError, 'finish it with rollback'):
            deployer.deploy(NEW_SHA, self.binary)
        self.fake.unhealthy.clear()
        self.assertEqual(deployer.rollback().status, 'rolled-back')
        self.assertEqual(self.fake.log, [], 'successful retry only observes readiness')

    def test_rollback_restart_receipt_cannot_cover_changed_state(self):
        for changed in ('files', 'reload', 'inactive', 'executable', 'invocation'):
            with self.subTest(changed=changed):
                self.setUp()
                self.enhance_fleet()
                deployer = self.deployer(only=['worker@worker-01'])
                journal = deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
                self.fake.unhealthy.add(('worker-01', OLD_SHA))
                with self.assertRaises(DeployError):
                    deployer.rollback()
                record = journal.hosts[0]
                state = self.fake.host('worker-01').units[record['unit']]
                if changed == 'files':
                    change = next(c for c in record['changes'] if c['new'] is not None)
                    self.fake.put('worker-01', change['path'], change['new'])
                    self.fake._reload('worker-01')
                elif changed == 'reload':
                    # The unit file is the journaled change. An unreloaded drop-in
                    # makes the loaded unit stale without a third version of that file.
                    self.fake.put('worker-01', dropin(record['unit'], '60-unloaded.conf'),
                                  '[Service]\n# reload required\n')
                elif changed == 'inactive':
                    state['active'] = 'inactive'
                elif changed == 'executable':
                    state['exe'] = NEW_SHA
                else:
                    state['invocation_id'] = 'f' * 32
                self.fake.unhealthy.clear()
                self.fake.log.clear()
                self.assertEqual(deployer.rollback().status, 'rolled-back')
                self.assertEqual(self.restarts(), [('worker-01', record['unit'])])

    def test_a_lost_rollback_restart_reply_does_not_create_a_receipt(self):
        self.enhance_fleet()
        deployer = self.deployer(only=['worker@worker-01'])
        deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        original = self.fake.systemctl

        def lost(host, *args):
            original(host, *args)
            if args[0] == 'restart':
                raise OSError('restart reply lost')

        with mock.patch.object(self.fake, 'systemctl', side_effect=lost):
            with self.assertRaises(DeployError):
                deployer.rollback()
        self.fake.log.clear()
        self.assertEqual(deployer.rollback().status, 'rolled-back')
        self.assertEqual(self.restarts(), [('worker-01', 'enhance-pir-worker.service')])

    def test_runner_crash_mid_deploy_is_recovered_by_rollback(self):
        deployer = self.enhance_fleet()

        class Crash(BaseException):
            pass

        def crash(host, op, detail):
            if host == 'worker-02' and op == 'systemctl' and detail[0] == 'restart':
                raise Crash()
        self.fake.observer = crash
        deployer.rollback_journal = lambda journal, force=False: None  # the runner dies instead
        with self.assertRaises(Crash):
            deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        journal = Journal.load(self.state, 'enhance')
        self.assertEqual([r['phase'] for r in journal.hosts[:2]], ['verified', 'restarting'])
        self.fake.observer = None
        self.fake.log.clear()
        self.deployer().rollback()
        self.assertEqual(self.restarts(), [('worker-02', 'enhance-pir-worker.service'),
                                           ('worker-01', 'enhance-pir-worker.service')])
        self.assertEqual(Journal.load(self.state, 'enhance').status, 'rolled-back')
        self.assertTrue(all(self.running(h, u) == OLD_SHA for h, u in ENHANCE))

    def test_rollback_refuses_files_changed_after_the_transaction(self):
        deployer = self.enhance_fleet()
        deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        unit = 'enhance-pir-worker.service'
        self.fake.edit('worker-02', '/etc/systemd/system/' + unit,
                       self.fake.read('worker-02', '/etc/systemd/system/' + unit) + '\n# changed after the transaction\n')
        self.fake.log.clear()
        with self.assertRaisesRegex(DeployError, 'changed since the transaction'):
            deployer.rollback()
        self.assertEqual(self.fake.log, [])

    def test_unfinished_transaction_blocks_a_new_deploy(self):
        deployer = self.enhance_fleet()
        self.fake.failing_restart.add(('worker-01', 'enhance-pir-worker.service'))
        self.fake.unhealthy.add(('worker-02', NEW_SHA))
        with self.assertRaisesRegex(DeployError, 'rollback did not complete'):
            deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual(Journal.load(self.state, 'enhance').status, 'rollback-failed')
        self.fake.log.clear()
        with self.assertRaisesRegex(DeployError, 'finish it with rollback'):
            deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual(self.fake.log, [])
        self.fake.failing_restart.clear()
        deployer.rollback()
        self.assertTrue(all(self.running(h, u) == OLD_SHA for h, u in ENHANCE))

    def test_only_the_latest_live_transaction_can_be_rolled_back(self):
        deployer = self.enhance_fleet()
        first = deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        third = self.dir / 'third'
        third.write_bytes(b'third build')
        deployer.deploy(sha256(b'third build'), third)
        with self.assertRaisesRegex(DeployError, 'roll it back first'):
            deployer.rollback(first.id)


class StatusTests(Fleet):
    LIVE = {'worker': [('MemoryMax=12G', 'MemoryMax=6G')],
            'router': [('RAYON_NUM_THREADS=4', 'RAYON_NUM_THREADS=2')]}

    def test_template_drift_from_the_live_unit_is_refused(self):
        deployer = self.status_fleet(self.LIVE)
        with self.assertRaisesRegex(DeployError, 'allow-unit-drift'):
            deployer.deploy(sha256(STATUS_QUERY_NEW), self.status_query_binary)
        self.assertIn("Service.MemoryMax: '6G' -> '12G'", '\n'.join(self.lines))
        self.assertEqual(self.fake.log, [])

    def test_running_binary_with_reconciled_templates_is_a_noop(self):
        deployer = self.status_fleet()
        self.assertIsNone(deployer.deploy(sha256(STATUS_QUERY_OLD)))
        self.assertEqual(self.fake.log, [])

    def test_running_binary_with_drift_still_restarts_and_rolls_back_by_restart(self):
        deployer = self.status_fleet(self.LIVE)
        # The restart moves the unit to the release layout, so the bytes must be supplied.
        with self.assertRaisesRegex(DeployError, 'not staged and no binary'):
            deployer.deploy(sha256(STATUS_QUERY_OLD), allow_drift=True)
        running = self.dir / 'running-pir-query-server'
        running.write_bytes(STATUS_QUERY_OLD)
        journal = deployer.deploy(sha256(STATUS_QUERY_OLD), running, allow_drift=True)
        self.assertEqual([r['action'] for r in journal.hosts], ['restart', 'restart'])
        self.fake.log.clear()
        deployer.rollback()
        # Same executable before and after, but the process must pick up the restored unit.
        self.assertEqual(self.restarts(), [('status-01', 'status-router.service'), ('status-01', 'status-worker.service')])

    def test_accepted_drift_renders_the_whole_unit_from_the_template(self):
        deployer = self.status_fleet(self.LIVE)
        sha = sha256(STATUS_QUERY_NEW)
        journal = deployer.deploy(sha, self.status_query_binary, allow_drift=True)
        self.assertEqual(journal.status, 'committed')
        expected = SERVICES['status'].roles['worker'].render(
            status_settings('worker'), '/opt/status-pir/releases/' + sha).unit
        self.assertEqual(self.fake.host('status-01').files['/etc/systemd/system/status-worker.service'].decode(), expected)
        self.assertIn('/opt/status-pir/releases/%s/pir-query-server --config /etc/status-pir/worker.json' % sha, expected)
        self.assertEqual(self.restarts(), [('status-01', 'status-worker.service'), ('status-01', 'status-router.service')])
        deployer.rollback()
        text = self.fake.host('status-01').files['/etc/systemd/system/status-worker.service'].decode()
        self.assertIn('MemoryMax=6G', text)
        self.assertIn(STATUS_LEGACY, text)

    def test_set_property_drop_in_counts_as_live_configuration(self):
        self.status_fleet()
        self.fake.edit('status-01', '/etc/systemd/system.control/status-worker.service.d/50-MemoryMax.conf',
                       '[Service]\nMemoryMax=6G\n')
        deployer = self.deployer('status', only=['worker', 'router'])
        deployer.capture_baseline()
        plans, problems = deployer.assess(sha256(STATUS_QUERY_NEW), self.status_query_binary)
        # A complete rendered role refuses an overlay that would silently change its budget.
        self.assertTrue(any('owns its whole unit' in problem for problem in problems))
        self.assertEqual(self.fake.log, [])

    def test_upgrade_restores_absent_configuration_files_on_rollback(self):
        deployer = self.status_fleet()
        for role in ('worker', 'router'):
            self.fake.host('status-01').files.pop('/etc/status-pir/' + role + '.json')
        deployer.capture_baseline()
        self.fake.log.clear()
        journal = deployer.deploy(sha256(STATUS_QUERY_NEW), self.status_query_binary)
        self.assertEqual(journal.status, 'committed')
        self.assertTrue(all('/etc/status-pir/' + role + '.json' in self.fake.host('status-01').files
                            for role in ('worker', 'router')))
        deployer.rollback()
        self.assertTrue(all('/etc/status-pir/' + role + '.json' not in self.fake.host('status-01').files
                            for role in ('worker', 'router')))

    def test_status_roles_deploy_together_without_a_standalone_controller(self):
        deployer = self.status_fleet()
        journal = deployer.deploy(sha256(STATUS_QUERY_NEW), self.status_query_binary)
        self.assertEqual(journal.status, 'committed')
        self.assertEqual(self.restarts(), [('status-01', 'status-worker.service'),
                                         ('status-01', 'status-router.service')])
        with self.assertRaises(descriptors.DescriptorError):
            self.deployer('status', only=['controller'])


class ReceiverTests(Fleet):
    def unit(self):
        return self.fake.read('receiver-01', '/etc/systemd/system/receiver-pir.service')

    def command(self):
        return self.fake.host('receiver-01').units['receiver-pir.service']['exec_start']

    def test_rollback_to_the_retired_runtime_uses_its_captured_wallet_listener(self):
        legacy = '/opt/receiver-pir/releases/legacy/receiver-directory'
        self.fake.put('receiver-01', legacy, RECEIVER_OLD)
        unit = RECEIVER_LIVE.replace(RECEIVER_CURRENT + ' --config /etc/receiver-pir/publisher.json',
                                     legacy + ' --serve --bind 10.70.0.11:18380 --data-dir /srv/receiver-pir/index')
        deployer = self.receiver_fleet(unit)
        original = self.fake.http_get
        observed = []

        def installed_listener(host, url, timeout=5):
            if self.running(host, 'receiver-pir.service') == sha256(RECEIVER_OLD):
                observed.append(url)
                if url == 'http://10.70.0.11:18380/v1/receiver/inventory':
                    return 200, '{"inventory":"retained"}'
                return 0, 'replacement management listener is absent'
            return original(host, url, timeout)

        self.fake.http_get = installed_listener
        journal = deployer.deploy(sha256(RECEIVER_NEW), self.receiver_binary, allow_drift=True)
        deployer.rollback(journal.id)
        self.assertEqual(Journal.load(self.state, 'receiver').status, 'rolled-back')
        self.assertEqual(self.running('receiver-01', 'receiver-pir.service'), sha256(RECEIVER_OLD))
        self.assertEqual(self.unit(), unit)
        self.assertEqual(observed, ['http://10.70.0.11:18380/v1/receiver/inventory'])

    def test_first_deploy_adopts_the_hand_installed_unit_and_rollback_returns_to_current(self):
        deployer = self.receiver_fleet(RECEIVER_HAND_INSTALLED)
        sha = sha256(RECEIVER_NEW)
        before = dict(self.fake.host('receiver-01').files)
        # Besides the binary path, only the key file differs, and is reviewed drift.
        drift = ["Service.EnvironmentFile: ['-/etc/receiver-pir/near.env'] -> ['-%s']" % (NEAR_KEY_FILE % NEAR_KEY)]
        with self.assertRaisesRegex(DeployError, 'allow-unit-drift'):
            deployer.deploy(sha, self.receiver_binary)
        self.assertEqual(self.fake.log, [])
        journal = deployer.deploy(sha, self.receiver_binary, allow_drift=True)
        self.assertEqual(journal.status, 'committed')
        self.assertEqual(journal.hosts[0]['drift'], drift)
        self.assertEqual(self.restarts(), [('receiver-01', 'receiver-pir.service')])
        self.assertEqual(self.running('receiver-01', 'receiver-pir.service'), sha)
        self.assertEqual(self.unit(), receiver_unit(receiver_release(sha)))
        self.assertEqual(self.fake.unit_paths('receiver-01', 'receiver-pir.service'),
                         ['/etc/systemd/system/receiver-pir.service'])
        self.assertIn('--config /etc/receiver-pir/publisher.json', self.command())
        self.assertEqual(self.fake.read('receiver-01', RECEIVER_CURRENT), RECEIVER_OLD.decode())
        self.fake.log.clear()
        deployer.rollback()
        self.assertEqual(self.restarts(), [('receiver-01', 'receiver-pir.service')])
        self.assertEqual(self.running('receiver-01', 'receiver-pir.service'), sha256(RECEIVER_OLD))
        self.assertEqual(self.unit(), RECEIVER_HAND_INSTALLED)
        self.assertTrue(self.command().startswith(RECEIVER_CURRENT + ' '))
        for path in self.fake.unit_paths('receiver-01', 'receiver-pir.service'):
            self.assertEqual(self.fake.host('receiver-01').files[path], before[path])

    def test_the_first_deploy_restarts_a_bootstrap_unit_and_runs_the_check(self):
        """The binary already runs, but the Description drift restarts it into the managed unit."""
        deployer, sha = self.provisioned_receiver()
        plans, problems = deployer.assess(sha, self.receiver_binary, allow_drift=True)
        self.assertEqual(([plan.action for plan in plans], problems), (['restart'], []))
        (description,) = re.findall(r'(?m)^Description=(.*)$', RECEIVER_TEMPLATE.read_text())
        self.assertEqual(plans[0].drift, ["Unit.Description: '%s (bootstrap)' -> '%s'" % (description, description)])
        with self.assertRaisesRegex(DeployError, 'allow-unit-drift'):
            deployer.deploy(sha, self.receiver_binary)
        journal = deployer.deploy(sha, self.receiver_binary, allow_drift=True)
        self.assertEqual(journal.status, 'committed')
        self.assertEqual(self.restarts(), [('receiver-01', 'receiver-pir.service')])
        self.assertEqual(self.fake.runs[-1], ('receiver-01', probe_argv(sha)))
        self.assertEqual(self.unit(), receiver_unit(receiver_release(sha)))
        # The managed unit again is a no-op: no transaction and no check.
        self.fake.log.clear()
        runs = len(self.fake.runs)
        self.assertIsNone(deployer.deploy(sha, self.receiver_binary))
        self.assertEqual((self.fake.log, len(self.fake.runs)), ([], runs))
        self.assertEqual(Journal.load(self.state, 'receiver').id, journal.id)

    def test_a_bootstrap_unit_failing_its_check_is_restored(self):
        deployer, sha = self.provisioned_receiver()
        bootstrap = self.unit()
        self.fake.exact_result = (1, '{"passed":false,"category":"answer_mismatch"}')
        with self.assertRaisesRegex(DeployError, 'exact-answer check failed'):
            deployer.deploy(sha, self.receiver_binary, allow_drift=True)
        self.assertEqual(Journal.load(self.state, 'receiver').status, 'rolled-back')
        self.assertEqual(self.unit(), bootstrap)
        self.assertEqual(self.restarts(), [('receiver-01', 'receiver-pir.service')] * 2)
        self.assertEqual(self.running('receiver-01', 'receiver-pir.service'), sha)

    def test_template_argument_change_takes_effect_and_rolls_back(self):
        sha = sha256(RECEIVER_NEW)
        self.receiver_fleet().deploy(sha, self.receiver_binary)
        deployed = self.unit()
        template = self.dir / 'receiver-pir.service.in'
        template.write_text(RECEIVER_TEMPLATE.read_text().replace('RestartSec=15', 'RestartSec=20'))
        service = SERVICES['receiver']
        changed = dataclasses.replace(service, roles={**service.roles, 'server': dataclasses.replace(
            service.roles['server'], template=template)})
        deployer = self.deployer('receiver', service=changed)
        self.fake.log.clear()
        # The same, already staged binary: only the arguments change, and the
        # plan shows them as drift for review.
        with self.assertRaisesRegex(DeployError, 'allow-unit-drift'):
            deployer.deploy(sha)
        self.assertIn('20', '\n'.join(line for line in self.lines if 'drift Service.RestartSec' in line))
        self.assertEqual(self.fake.log, [])
        journal = deployer.deploy(sha, allow_drift=True)
        self.assertEqual(journal.status, 'committed')
        self.assertEqual(self.restarts(), [('receiver-01', 'receiver-pir.service')])
        self.assertIn('RestartSec=20', self.unit())
        self.assertIn('--config /etc/receiver-pir/publisher.json', self.command())
        self.assertEqual(self.running('receiver-01', 'receiver-pir.service'), sha)
        self.fake.log.clear()
        deployer.rollback()
        # Same executable either way; the restart picks up the restored unit.
        self.assertEqual(self.restarts(), [('receiver-01', 'receiver-pir.service')])
        self.assertEqual(self.unit(), deployed)
        self.assertIn('RestartSec=15', self.unit())

    def test_a_server_that_never_publishes_is_rolled_back(self):
        deployer = self.receiver_fleet()
        sha = sha256(RECEIVER_NEW)
        self.fake.unhealthy.add(('receiver-01', sha))
        with self.assertRaisesRegex(DeployError, 'not verified within 300s: health ready is False'):
            deployer.deploy(sha, self.receiver_binary)
        self.assertEqual(self.running('receiver-01', 'receiver-pir.service'), sha256(RECEIVER_OLD))
        self.assertEqual(self.unit(), RECEIVER_LIVE)
        self.assertEqual(Journal.load(self.state, 'receiver').status, 'rolled-back')
        self.assertNotIn(probe_argv(sha), [argv for _, argv in self.fake.runs])

    def test_the_probe_runs_on_the_receiver_under_the_lock_before_commit(self):
        deployer = self.receiver_fleet()
        sha = sha256(RECEIVER_NEW)
        seen, run = [], self.fake.run

        def observe(host, argv, timeout):
            if argv == probe_argv(sha):
                seen.append((host, timeout, LOCK in self.fake.held, Journal.load(self.state, 'receiver').status,
                             self.running('receiver-01', 'receiver-pir.service')))
            return run(host, argv, timeout)
        self.fake.run = observe
        self.assertEqual(deployer.deploy(sha, self.receiver_binary).status, 'committed')
        self.assertEqual(seen, [('receiver-01', 120, True, 'verifying', sha)])

    def test_audit_file_ids_cannot_add_unit_lines_or_path_segments(self):
        sha = sha256(RECEIVER_NEW)
        deployer = self.receiver_fleet()
        document = json.loads(self.inventory_path.read_text())
        for value in ['ab\nExecStartPre=/bin/sh -c id', 'ab cd', '../ab', 'ab/cd', 'a=b', '"ab"', '']:
            with self.subTest(value=value):
                document['services']['receiver']['roles']['server'][0]['settings']['near_key_id'] = value
                self.inventory_path.write_text(json.dumps(document))
                original = self.inventory
                self.inventory = descriptors.load_inventory(self.inventory_path)
                deployer = self.deployer('receiver')
                self.inventory = original
                with self.assertRaisesRegex(DeployError, 'refused before any change'):
                    deployer.deploy(sha, self.receiver_binary, allow_drift=True)
                self.assertEqual(self.fake.log, [])

    def rotated(self, key):
        """A receiver deployer whose inventory names NEAR key file `key`, as a rotation sets it."""
        document = json.loads(self.inventory_path.read_text())
        document['services']['receiver']['roles']['server'][0]['settings']['near_key_id'] = key
        self.inventory_path.write_text(json.dumps(document))
        self.inventory = descriptors.load_inventory(self.inventory_path)
        return self.deployer('receiver')

    def test_a_key_rotation_is_a_reviewed_unit_change_of_the_same_release(self):
        sha = sha256(RECEIVER_NEW)
        self.receiver_fleet().deploy(sha, self.receiver_binary)
        previous = self.unit()
        self.fake.put('receiver-01', NEAR_KEY_FILE % 'next', 'NEAR_INTENTS_EXPLORER=new\n')
        deployer = self.rotated('next')
        self.fake.log.clear()
        plans, problems = deployer.assess(sha)
        self.assertEqual(plans[0].action, 'restart')
        self.assertEqual(plans[0].drift, ["Service.EnvironmentFile: ['-%s'] -> ['-%s']"
                                          % (NEAR_KEY_FILE % NEAR_KEY, NEAR_KEY_FILE % 'next')])
        self.assertTrue(any('allow-unit-drift' in problem for problem in problems), problems)
        with self.assertRaisesRegex(DeployError, 'allow-unit-drift'):
            deployer.deploy(sha)
        self.assertEqual(self.fake.log, [])
        journal = deployer.deploy(sha, allow_drift=True)
        self.assertEqual(journal.status, 'committed')
        self.assertEqual(self.restarts(), [('receiver-01', 'receiver-pir.service')])
        self.assertEqual(self.running('receiver-01', 'receiver-pir.service'), sha)
        self.assertEqual(self.unit(), previous.replace(NEAR_KEY_FILE % NEAR_KEY, NEAR_KEY_FILE % 'next'))
        # Rolling the rotation back restores the unit naming the previous key file.
        self.fake.log.clear()
        deployer.rollback()
        self.assertEqual(self.restarts(), [('receiver-01', 'receiver-pir.service')])
        self.assertEqual(self.unit(), previous)
        self.assertEqual(self.running('receiver-01', 'receiver-pir.service'), sha)

    def test_a_drop_in_cannot_add_settings_to_the_unit_it_owns(self):
        sha = sha256(RECEIVER_NEW)
        for path in [dropin('receiver-pir.service', '50-key.conf'),
                     '/etc/systemd/system.control/receiver-pir.service.d/50-MemoryMax.conf']:
            with self.subTest(path=path):
                self.fake, self.state = FakeFleet(), self.dir / ('state-' + os.path.basename(path))
                self.receiver_fleet()
                self.fake.edit('receiver-01', path, '[Service]\nEnvironmentFile=/etc/receiver-pir/near.env\n')
                deployer = self.deployer('receiver')
                deployer.capture_baseline()
                self.fake.log.clear()
                refusal = 'server@receiver-01: role server owns its whole unit; remove or fold in ' + path
                plans, problems = deployer.assess(sha, self.receiver_binary, allow_drift=True)
                self.assertEqual(plans[0].drop_ins, [(path, 'refuse')])
                self.assertIn(refusal, problems)
                with self.assertRaisesRegex(DeployError, 'refused before any change:(.|\n)*' + refusal):
                    deployer.deploy(sha, self.receiver_binary, allow_drift=True)
                self.assertEqual(self.fake.log, [])
                self.assertEqual(Journal.load(self.state, 'receiver'), None)

    def test_a_rotation_failing_its_check_restores_the_previous_key(self):
        sha = sha256(RECEIVER_NEW)
        self.receiver_fleet().deploy(sha, self.receiver_binary)
        previous = self.unit()
        self.fake.put('receiver-01', NEAR_KEY_FILE % 'next', 'NEAR_INTENTS_EXPLORER=new\n')
        deployer = self.rotated('next')
        self.fake.exact_result = (1, '{"passed":false,"category":"answer_mismatch"}')
        self.fake.log.clear()
        with self.assertRaises(DeployError):
            deployer.deploy(sha, allow_drift=True)
        self.assertEqual(Journal.load(self.state, 'receiver').status, 'rolled-back')
        self.assertEqual(self.unit(), previous)
        self.assertEqual(self.restarts(), [('receiver-01', 'receiver-pir.service')] * 2)
        state = self.fake.host('receiver-01').units['receiver-pir.service']
        self.assertEqual((state['active'], state['exe']), ('active', sha))
        self.assertIn('-' + NEAR_KEY_FILE % NEAR_KEY, units.effective([previous])['Service']['EnvironmentFile'])

    def test_an_uninstalled_key_starts_the_service_without_the_audit(self):
        sha = sha256(RECEIVER_NEW)
        self.receiver_fleet().deploy(sha, self.receiver_binary)
        journal = self.rotated('uninstalled').deploy(sha, allow_drift=True)
        self.assertEqual(journal.status, 'committed')
        state = self.fake.host('receiver-01').units['receiver-pir.service']
        self.assertEqual((state['active'], state['exe']), ('active', sha))

    def test_a_failed_or_timed_out_probe_rolls_back(self):
        sha = sha256(RECEIVER_NEW)
        for i, result in enumerate([(1, '{"passed":false,"category":"answer_mismatch"}'),
                                    (124, 'timed out after 120s'),
                                    subprocess.TimeoutExpired(['ssh', 'receiver-01'], 150)]):
            with self.subTest(result=result):
                self.fake, self.state = FakeFleet(), self.dir / ('state-%d' % i)
                deployer = self.receiver_fleet()
                self.fake.exact_result = result
                with self.assertRaises((DeployError, subprocess.TimeoutExpired)):
                    deployer.deploy(sha, self.receiver_binary)
                self.assertEqual([argv for _, argv in self.fake.runs][-1], probe_argv(sha))
                self.assertEqual(self.running('receiver-01', 'receiver-pir.service'), sha256(RECEIVER_OLD))
                self.assertEqual(self.unit(), RECEIVER_LIVE)
                self.assertEqual(Journal.load(self.state, 'receiver').status, 'rolled-back')
                # The verified release stays staged for a retry; only units roll back.
                self.assertEqual(self.fake.read('receiver-01', receiver_release(sha) + '/pir-publisher'),
                                 RECEIVER_NEW.decode())


class UnitTests(unittest.TestCase):
    def test_effective_configuration_follows_systemd_merging(self):
        config = units.effective([
            '[Service]\n# comment\nExecStart=/a/b --x \\\n  --y\nEnvironment=A=1 "B=two words"\nMemoryMax=4G\nAfter=x.service y.service\n',
            '[Service]\nExecStart=\nExecStart=/c/d --z\nEnvironment=A=3\nMemoryMax=6G\nAfter=z.service\n'])
        self.assertEqual(config['Service']['ExecStart'], ['/c/d --z'])
        self.assertEqual(config['Service']['Environment'], {'A': '3', 'B': 'two words'})
        self.assertEqual(config['Service']['MemoryMax'], '6G')
        self.assertEqual(config['Service']['After'], ['x.service', 'y.service', 'z.service'])
        self.assertEqual(units.effective(['[Service]\nExecStart=/a/b --x \\\n  --y\n'])['Service']['ExecStart'], ['/a/b --x --y'])

    def test_normalized_form_ignores_comments_layout_and_binary_path(self):
        left = units.effective(['[Service]\nExecStart=-/opt/a/bin  --flag ${X}\n# note\n'])
        right = units.effective(['[Service]\nExecStart=/old\n', '[Service]\nExecStart=\nExecStart=-/opt/b/bin --flag ${X}\n'])
        self.assertEqual(units.normalized(left), units.normalized(right))
        self.assertEqual(units.split_exec('-/opt/a/bin  --flag ${X}'), ('-', '/opt/a/bin', '--flag ${X}'))

    def test_drop_in_classification(self):
        self.assertTrue(units.exec_start_only('[Service]\nExecStart=\nExecStart=/x\n'))
        self.assertFalse(units.exec_start_only('[Service]\nExecStart=/x\nReadWritePaths=/y\n'))
        self.assertTrue(units.sets_exec_start('[Service]\nExecStart=/x\nReadWritePaths=/y\n'))
        self.assertFalse(units.sets_exec_start('[Service]\nMemoryMax=1G\n'))
        self.assertTrue(units.adoptable('/etc/systemd/system/u.service.d/zz-cleanup-71be21f.conf', ['zz-cleanup-*.conf']))
        self.assertLess('90-v7.conf', units.MANAGED_DROP_IN)
        self.assertLess('zz-cleanup-71be21f.conf', units.MANAGED_DROP_IN)

    def test_render_refuses_unset_placeholders(self):
        with self.assertRaisesRegex(units.UnitError, '@NEAR_KEY@'):
            units.render(RECEIVER_TEMPLATE.read_text(), {'RELEASE': '/opt/receiver-pir/releases/x'})

    def test_repository_descriptors_and_example_inventory_load(self):
        SERVICES = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')
        inventory = descriptors.load_inventory(DEPLOY / 'deploy-inventory.example.json')
        for name, service in SERVICES.items():
            if name not in inventory.services:
                self.assertTrue(all(role.optional for role in service.roles.values()))
                continue
            keys = [t.key for t in descriptors.targets(service, inventory)]
            self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(SERVICES['enhance'].order,
                         ('worker', 'external-worker', 'packing-router', 'query-ingress', 'combined'))
        self.assertEqual(SERVICES['status'].order, ('worker', 'external-worker', 'router', 'combined'))
        self.assertEqual(SERVICES['receiver'].order, ('server', 'query'))
        publisher = SERVICES['publisher']
        self.assertEqual(publisher.binary, 'pir-publisher')
        self.assertEqual(publisher.order, ('server',))
        self.assertTrue(publisher.roles['server'].owns_unit)
        # Optional roles are rendered whole. Required Enhance serving roles are role mode too.
        self.assertTrue(all(not role.optional or role.mode == 'role'
                            for service in SERVICES.values() for role in service.roles.values()))
        enhance = SERVICES['enhance']
        self.assertEqual({name for name, role in enhance.roles.items() if role.binary == 'pir-query-server'},
                         {'worker', 'external-worker', 'packing-router', 'query-ingress', 'combined'})
        self.assertNotIn('coordinator', enhance.roles)
        self.assertNotIn('controller', SERVICES['status'].roles)
        self.assertEqual(SERVICES['receiver'].artifact_kinds, ('receiver-pir',))
        self.assertIn('pir-publisher', cli.load_release().BINARIES['receiver-pir'])
        (server,) = descriptors.targets(SERVICES['receiver'], inventory)
        self.assertEqual(descriptors.exact_check(SERVICES['receiver'], inventory)['host'], server.host)

    def test_only_a_template_role_can_own_its_unit(self):
        self.assertTrue(SERVICES['receiver'].roles['server'].owns_unit)
        self.assertFalse(any(role.owns_unit for name in ('enhance', 'status') for role in SERVICES[name].roles.values()
                             if role.mode != 'role'))
        text = (DEPLOY / 'deploy.toml').read_text().replace('template = "', 'template = "%s/' % DEPLOY)
        text += (ROOT / 'tools/receiver/digitalocean/service.toml').read_text().replace(
            'template = "receiver-pir.service.in"', 'template = "'+str(RECEIVER_TEMPLATE)+'"')
        text = text.replace('template = "../deploy/receiver-query.service"',
                            'template = "' + str(ROOT / 'tools/receiver/deploy/receiver-query.service') + '"')
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'deploy.toml'
            text += '\n[services.template-test]\nroot="/opt/test"\nbinary="test"\nself_check=["--help"]\norder=["server"]\n[services.template-test.roles.server]\nunit="test.service"\nmode="template"\ntemplate="' + str(RECEIVER_TEMPLATE) + '"\nhealth="http://127.0.0.1/health"\nowns_unit = true\n'
            for old, new in [('\nowns_unit = true\n', '\nowns_unit = "yes"\n'),
                             ('mode = "role"\n', 'mode = "exec-drop-in"\nowns_unit = true\n')]:
                with self.subTest(new=new):
                    self.assertIn(old, text)
                    path.write_text(text.replace(old, new, 1))
                    with self.assertRaisesRegex(descriptors.DescriptorError, 'owns_unit'):
                        descriptors.load_descriptors(path)


class LocalExecutor(SSHExecutor):
    """The real helper protocol, run locally instead of over SSH."""

    def transport(self, host):
        return ['sh', '-c']


class MultiplexTests(unittest.TestCase):
    def test_helper_calls_share_one_connection_and_the_lock_has_its_own(self):
        executor = SSHExecutor(None)
        ssh = ['ssh', '-F', '/dev/null', '-o', 'BatchMode=yes', 'root@10.0.0.1']
        argv = executor.multiplexed(ssh)
        self.assertEqual(argv[0], 'ssh')
        self.assertEqual(argv[-1], 'root@10.0.0.1')
        options = dict(argv[i + 1].split('=', 1) for i, a in enumerate(argv[:-1]) if a == '-o')
        self.assertEqual(options['ControlMaster'], 'auto')
        self.assertTrue(options['ControlPath'].startswith(executor.control_dir + '/'))
        self.assertLess(len(options['ControlPath'].replace('%C', 'c' * 40)), 100)
        self.assertEqual(options['ServerAliveInterval'], '15')
        self.assertEqual(os.stat(executor.control_dir).st_mode & 0o777, 0o700)
        # One private directory per executor, reused by every call.
        self.assertEqual(executor.multiplexed(ssh), argv)
        # A lock keeper in front of ssh still gets the options before the host.
        kept = executor.multiplexed(['python3', 'keeper.py', *ssh])
        self.assertEqual(kept[:2], ['python3', 'keeper.py'])
        self.assertEqual(kept[-1], 'root@10.0.0.1')
        self.assertIn('ControlMaster=auto', kept)
        # Anything that is not SSH runs unchanged.
        self.assertEqual(executor.multiplexed(['sh', '-c']), ['sh', '-c'])

        class Recording(SSHExecutor):
            def transport(self, host):
                return list(ssh)
        lock = Recording(None)
        started = []

        class Process:
            def __init__(self, argv, **_):
                started.append(argv)
                self.stdout, self.stdin = io.BytesIO(b'locked\n'), io.BytesIO()

            def poll(self):
                return None

            def wait(self, timeout=None):
                return 0
        with mock.patch.object(subprocess, 'Popen', Process):
            with lock.hold_lock('h', '/run/lock/x') as held:
                held.verify()
        argv = started[0]
        self.assertIn('ControlMaster=no', argv)
        self.assertIn('ControlPath=none', argv)
        self.assertIn('ServerAliveInterval=15', argv)
        self.assertEqual(argv[argv.index('root@10.0.0.1') + 1:][0].split()[0], 'flock')


class HelperTests(unittest.TestCase):
    def test_configuration_check_fits_in_a_multiplexed_command(self):
        request = json.dumps({'op': 'run', 'argv': ['binary', '--config', 'x' * 8192], 'timeout': 60})
        command = shlex.join(['python3', '-c', HELPER, request])
        self.assertLess(len(command.encode()), 32768)

    def test_helper_file_operations_round_trip(self):
        executor = LocalExecutor(None)
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'a', 'unit.conf')
            executor.mkdir('local', os.path.dirname(path), 0o755)
            executor.write('local', path, b'[Service]\nExecStart=/x\n', 0o644)
            self.assertEqual(executor.read('local', path), '[Service]\nExecStart=/x\n')
            self.assertEqual(executor.sha256('local', path), hashlib.sha256(b'[Service]\nExecStart=/x\n').hexdigest())
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o644)
            source = Path(tmp) / 'binary'
            source.write_bytes(os.urandom(3 << 20))
            executor.upload('local', source, os.path.join(tmp, 'a', 'bin'), 0o755)
            self.assertEqual(executor.sha256('local', os.path.join(tmp, 'a', 'bin')),
                             hashlib.sha256(source.read_bytes()).hexdigest())
            executor.rename('local', os.path.join(tmp, 'a'), os.path.join(tmp, 'b'))
            with self.assertRaisesRegex(Exception, 'FileExistsError'):
                executor.rename('local', os.path.join(tmp, 'b', 'unit.conf'), os.path.join(tmp, 'b', 'bin'))
            executor.remove('local', os.path.join(tmp, 'b', 'unit.conf'))
            self.assertIsNone(executor.read('local', os.path.join(tmp, 'b', 'unit.conf')))
            self.assertGreater(executor.free_bytes('local', os.path.join(tmp, 'missing', 'dir')), 0)
            self.assertEqual(executor.run('local', ['sh', '-c', 'echo hi; exit 3'], 10), (3, 'hi\n'))
            # An exact check past its timeout is killed and fails like any other.
            self.assertEqual(executor.run('local', ['sleep', '10'], 1), (124, 'timed out after 1s'))

    def test_helper_http_get(self):
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                body = b'{"ready": true}' if self.path == '/ok' else b'no'
                self.send_response(200 if self.path == '/ok' else 503)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass
        server = http.server.HTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        base = 'http://127.0.0.1:%d' % server.server_port
        executor = LocalExecutor(None)
        self.assertEqual(executor.http_get('local', base + '/ok'), (200, '{"ready": true}'))
        self.assertEqual(executor.http_get('local', base + '/no')[0], 503)
        server.shutdown()
        server.server_close()
        self.assertEqual(executor.http_get('local', base + '/ok', timeout=2)[0], 0)


class CommandLineTests(Fleet):
    def run_cli(self, *argv):
        self.lines.clear()
        return cli.main(['--inventory', str(self.inventory_path), '--state-dir', str(self.state), *argv],
                        executor=self.fake, out=self.lines.append, sleep=self.clock.sleep, clock=self.clock)

    def serving_cli(self, *argv):
        selected = [item for role in SERVING_SELECTORS for item in ('--only', role)]
        return self.run_cli(*argv, *selected)

    def test_plan_is_read_only_and_preflight_needs_a_baseline(self):
        self.enhance_fleet()
        self.deployer()  # registers health endpoints
        (self.state / 'baselines/enhance.json').unlink()
        self.assertEqual(self.serving_cli('plan', 'enhance', '--binary', str(self.binary)), 0)
        self.assertIn('worker@worker-01 enhance-pir-worker.service: restart', self.lines)
        self.assertEqual(self.serving_cli('preflight', 'enhance', '--binary', str(self.binary), '--retire-historical'), 1)
        self.assertTrue(any('run capture-baseline' in line for line in self.lines))
        self.assertEqual(self.serving_cli('capture-baseline', 'enhance'), 0)
        self.assertEqual(self.serving_cli('preflight', 'enhance', '--binary', str(self.binary), '--retire-historical'), 0)
        self.assertEqual(self.fake.log, [])

    def test_deploy_status_and_rollback_commands(self):
        self.enhance_fleet()
        self.deployer()
        self.assertEqual(self.serving_cli('deploy', 'enhance', '--sha256', NEW_SHA, '--retire-historical'), 1)
        self.assertTrue(any('not staged and no binary' in line for line in self.lines))
        self.assertEqual(self.serving_cli('deploy', 'enhance', '--binary', str(self.binary), '--retire-historical'), 0)
        self.assertEqual(self.run_cli('status', 'enhance'), 0)
        self.assertIn('committed', self.lines[0])
        self.assertEqual(self.run_cli('rollback', 'enhance'), 0)
        self.assertTrue(all(self.running(h, u) == OLD_SHA for h, u in ENHANCE))

    def test_plan_and_preflight_name_a_drop_in_on_a_unit_its_role_owns(self):
        self.receiver_fleet()
        path = dropin('receiver-pir.service', '50-key.conf')
        self.fake.edit('receiver-01', path, '[Service]\nEnvironmentFile=/etc/receiver-pir/near.env\n')
        self.assertEqual(self.run_cli('capture-baseline', 'receiver'), 0)
        problem = 'problem: server@receiver-01: role server owns its whole unit; remove or fold in ' + path
        self.assertEqual(self.run_cli('plan', 'receiver', '--binary', str(self.receiver_binary)), 0)
        self.assertIn(problem, self.lines)
        self.assertIn('  drop-in %s: REFUSED' % path, self.lines)
        self.assertEqual(self.run_cli('preflight', 'receiver', '--binary', str(self.receiver_binary),
                                      '--allow-unit-drift'), 1)
        self.assertIn(problem, self.lines)
        self.assertEqual(self.fake.log, [])


if __name__ == '__main__':
    unittest.main()

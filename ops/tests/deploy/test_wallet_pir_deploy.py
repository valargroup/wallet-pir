"""The transactional Enhance, Status and Receiver deploy CLI against an in-memory fleet.

Uses the repository's own descriptors and unit templates; only hosts are fake.
"""
import dataclasses
import hashlib
import http.server
import json
import os
import shutil
import subprocess
from pathlib import Path
import sys
import tarfile
import tempfile
import threading
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'ops/lib'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from fake_fleet import FakeFleet, sha256  # noqa: E402
from wallet_pir_ops.deploy import cli, descriptors, units  # noqa: E402
from wallet_pir_ops.deploy.engine import Artifact, Deployer, DeployError  # noqa: E402
from wallet_pir_ops.deploy.remote import LockHeld, SSHExecutor  # noqa: E402
from wallet_pir_ops.deploy.transaction import Journal  # noqa: E402

class ImmutableWrapperSource(unittest.TestCase):
    def test_plain_python_invocation_does_not_create_bytecode(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / 'ops/lib', root / 'ops/lib',
                            ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
            (root / 'ops/scripts').mkdir(parents=True)
            wrapper = root / 'ops/scripts/wallet-pir-deploy.py'
            shutil.copyfile(ROOT / 'ops/scripts/wallet-pir-deploy.py', wrapper)
            env = dict(os.environ)
            env.pop('PYTHONDONTWRITEBYTECODE', None)
            result = subprocess.run([sys.executable, str(wrapper), '--help'],
                                    env=env, capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(list(root.rglob('*.pyc')), [])
            self.assertEqual(list(root.rglob('__pycache__')), [])


DEPLOY = ROOT / 'enhance/ops/deploy'
SERVICES = descriptors.load_descriptors(DEPLOY / 'deploy.toml')
LOCK = ('coordinator', descriptors.LOCK_PATH)

OLD = b'enhance-pir-server built from 71be21f'
OLD_SHA = sha256(OLD)
NEW = b'enhance-pir-server built from a later revision'
NEW_SHA = sha256(NEW)
LEGACY = '/opt/enhance-pir/releases/cleanup-71be21f-ba576af27d7f/enhance-pir-server'
ENHANCE = [('worker-01', 'enhance-pir-worker.service'), ('worker-02', 'enhance-pir-worker.service'),
           ('router-01', 'enhance-pir-packing-router.service'), ('coordinator', 'enhance-pir-query-ingress.service'),
           ('coordinator', 'enhance-pir-server.service')]
NETWORK = 'ab' * 32
STATUS_OLD = b'status-pir built from c5a6486'
STATUS_NEW = b'status-pir built from a later revision'
STATUS_LEGACY = '/opt/wallet-pir/releases/c5a6486/native'
RECEIVER_TEMPLATE = ROOT / 'receiver/ops/digitalocean/receiver-pir.service.in'
RECEIVER_OLD = b'receiver-directory built from c7f6d296'
RECEIVER_NEW = b'receiver-directory built from a later revision'
RECEIVER_CURRENT = '/opt/receiver-pir/current/receiver-directory'
NEAR_KEY = '2026-10-01'
NEAR_KEY_FILE = '/etc/receiver-pir/near-%s.env'
RECEIVER_LIVE = units.render(RECEIVER_TEMPLATE.read_text(), {'RELEASE': '/opt/receiver-pir/current', 'NEAR_KEY': NEAR_KEY})
# The unit installed by hand before the tool, with the optional unversioned key file.
RECEIVER_HAND_INSTALLED = RECEIVER_LIVE.replace('EnvironmentFile=' + NEAR_KEY_FILE % NEAR_KEY,
                                                'EnvironmentFile=-/etc/receiver-pir/near.env')
RECEIVER_PROBE = ['{release_dir}/receiver-probe', '--origin', 'https://receiver.example',
                  '--fixture', '{release_dir}/probe-fixture.json', '--no-auth']
PROBE = b'receiver-probe built from a later revision'
FIXTURE = (ROOT / 'receiver/ops/digitalocean/probe-fixture.json').read_bytes()


def receiver_release(sha):
    return '/opt/receiver-pir/releases/' + sha


def probe_argv(sha):
    """The receiver's exact check as it runs for release `sha`."""
    return [argument.replace('{release_dir}', receiver_release(sha)) for argument in RECEIVER_PROBE]

INVENTORY = {
    'lock': {'type': 'remote', 'host': 'coordinator'},
    'ssh': {'mode': 'config'},
    'hosts': {name: {} for name in ('coordinator', 'worker-01', 'worker-02', 'router-01', 'status-01', 'receiver-01')},
    'services': {
        'enhance': {
            'roles': {
                'worker': [{'host': 'worker-01', 'vars': {'listen': '10.0.0.15:8091'}},
                           {'host': 'worker-02', 'vars': {'listen': '10.0.0.16:8091'}}],
                'packing-router': [{'host': 'router-01', 'vars': {'control_listen': '10.0.0.14:8093'}}],
                'query-ingress': [{'host': 'coordinator'}],
                'coordinator': [{'host': 'coordinator'}],
            },
            'exact_check': {'host': 'coordinator', 'argv': ['/usr/local/bin/exact', '{release_dir}', '{transaction}']},
        },
        'status': {
            'template_vars': {'NETWORK': NETWORK},
            'roles': {'worker': [{'host': 'status-01'}], 'router': [{'host': 'status-01'}],
                      'controller': [{'host': 'coordinator'}]},
            'exact_check': {'host': 'coordinator', 'argv': ['/usr/local/bin/exact', '{release_dir}']},
        },
        'receiver': {
            'template_vars': {'NEAR_KEY': NEAR_KEY},
            'roles': {'server': [{'host': 'receiver-01', 'vars': {'listen': '10.0.0.11:18380'}}]},
            'exact_check': {'host': 'receiver-01', 'argv': RECEIVER_PROBE, 'timeout': 120},
        },
    },
}


def dropin(unit, name):
    return '/etc/systemd/system/%s.d/%s' % (unit, name)


def cleanup_drop_in(fragment):
    """The shape of the September 24 `zz-cleanup-71be21f.conf`: ExecStart only, same argument tail."""
    prefix, _, tail = units.split_exec(units.exec_start(units.effective([fragment])))
    return '[Service]\nExecStart=\nExecStart=%s%s %s\n' % (prefix, LEGACY, tail)


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
        self.binary = self.dir / 'enhance-pir-server'
        self.binary.write_bytes(NEW)
        self.status_binary = self.dir / 'status-pir'
        self.status_binary.write_bytes(STATUS_NEW)
        self.receiver_binary = self.dir / 'receiver-directory'
        self.receiver_binary.write_bytes(RECEIVER_NEW)
        self.companions = {}
        for name, data in (('receiver-probe', PROBE), ('probe-fixture.json', FIXTURE)):
            (self.dir / name).write_bytes(data)
            self.companions[name] = Artifact(self.dir / name, sha256(data))
        # The same digests with no local copy: usable only once staged.
        self.staged_companions = {name: Artifact(None, a.sha256) for name, a in self.companions.items()}

    def deployer(self, name='enhance', only=None, service=None):
        deployer = Deployer(service or SERVICES[name], self.inventory, self.fake, self.state, out=self.lines.append,
                            sleep=self.clock.sleep, clock=self.clock, poll_seconds=5, only=only)
        for target in deployer.targets:
            for url in (target.url(target.role.health), target.url(target.role.ready_url)):
                if url:
                    self.fake.endpoints[(target.host, url)] = target.unit
        return deployer

    def enhance_fleet(self):
        for host, unit in ENHANCE:
            fragment = (DEPLOY / unit).read_text()
            drop_ins = [(dropin(unit, 'zz-cleanup-71be21f.conf'), cleanup_drop_in(fragment))]
            if unit in ('enhance-pir-worker.service', 'enhance-pir-server.service'):
                # The v7 override also carries non-ExecStart settings.
                drop_ins.append((dropin(unit, '90-v7.conf'), cleanup_drop_in(fragment) + 'ReadWritePaths=/srv/enhance-pir-v7\n'))
            environment = {path: '' for path in units.effective([fragment])['Service'].get('EnvironmentFile', [])}
            self.fake.install(host, unit, fragment, drop_ins, {LEGACY: OLD, **environment})
        deployer = self.deployer()
        deployer.capture_baseline()
        self.fake.log.clear()
        return deployer

    def status_fleet(self, live_edits=None):
        """Status units as rendered from the templates, then edited like the live drift."""
        service = SERVICES['status']
        placements = [('status-01', 'worker'), ('status-01', 'router'), ('coordinator', 'controller')]
        for host, role in placements:
            text = units.render(service.roles[role].template.read_text(), {'RELEASE': STATUS_LEGACY, 'NETWORK': NETWORK})
            for old, new in (live_edits or {}).get(role, []):
                self.assertIn(old, text)
                text = text.replace(old, new)
            self.fake.install(host, service.roles[role].unit, text, (), {STATUS_LEGACY + '/status-pir': STATUS_OLD})
        deployer = self.deployer('status')
        deployer.capture_baseline()
        self.fake.log.clear()
        return deployer

    def receiver_fleet(self, live=RECEIVER_LIVE):
        """The receiver unit `live`, by default the template starting the binary through `current`.

        Key files for `NEAR_KEY` and the legacy `near.env` are installed.
        """
        files = {RECEIVER_CURRENT: RECEIVER_OLD, NEAR_KEY_FILE % NEAR_KEY: 'NEAR_INTENTS_EXPLORER=old\n',
                 '/etc/receiver-pir/near.env': 'NEAR_INTENTS_EXPLORER=old\n'}
        self.fake.install('receiver-01', 'receiver-pir.service', live, (), files)
        # Identity is nested, and `serving` stays null until the first publication.
        self.fake.health_shapes[('receiver-01', 'receiver-pir.service')] = lambda ready, exe: {
            'identity': {'binary_sha256': exe}, 'serving': 'cd' * 32 if ready else None, 'epoch': 0}
        deployer = self.deployer('receiver')
        deployer.capture_baseline()
        self.fake.log.clear()
        return deployer

    def provisioned_receiver(self):
        """`(deployer, sha)` for a host provisioned by hand: the unit rendered for release `sha`, which holds only the binary."""
        sha = sha256(RECEIVER_NEW)
        self.fake.put('receiver-01', receiver_release(sha) + '/receiver-directory', RECEIVER_NEW)
        live = units.render(RECEIVER_TEMPLATE.read_text(), {'RELEASE': receiver_release(sha), 'NEAR_KEY': NEAR_KEY})
        return self.receiver_fleet(live), sha

    def check_on_coordinator(self):
        """Move the receiver's exact check to a separate host, `coordinator`."""
        document = json.loads(self.inventory_path.read_text())
        document['services']['receiver']['exact_check']['host'] = 'coordinator'
        self.inventory_path.write_text(json.dumps(document))
        self.inventory = descriptors.load_inventory(self.inventory_path)

    def restarts(self):
        return [(host, detail[1]) for host, op, detail in self.fake.log if op == 'systemctl' and detail[0] == 'restart']

    def running(self, host, unit):
        return self.fake.host(host).units[unit]['exe']


class DeployTests(Fleet):
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
        self.assertEqual(len(json.loads(deployer.baseline_path.read_text())['targets']), len(ENHANCE))
        self.assertEqual(self.deployer().baseline_problems(self.deployer().probe(), required=True), [])
        with self.assertRaisesRegex(Exception, 'no target matches'):
            self.deployer(only=['worker@nowhere'])

    def test_noop_deploy_with_the_running_binary_changes_nothing(self):
        deployer = self.enhance_fleet()
        # A service without companions verifies only when asked.
        self.assertFalse(deployer.verifies())
        self.assertIsNone(deployer.deploy(OLD_SHA))
        self.assertEqual(self.fake.log, [])
        self.assertEqual(self.fake.runs, [])
        self.assertFalse((self.state / 'latest-enhance.json').exists())
        self.assertIn('no-op', self.lines[-1])

    def test_deploy_restarts_in_role_order_and_installs_the_managed_drop_in(self):
        deployer = self.enhance_fleet()
        journal = deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual(journal.status, 'committed')
        self.assertEqual(self.restarts(), ENHANCE)
        release = '/opt/enhance-pir/releases/%s/enhance-pir-server' % NEW_SHA
        for host, unit in ENHANCE:
            self.assertEqual(self.running(host, unit), NEW_SHA)
            files = self.fake.host(host).files
            managed = files[dropin(unit, units.MANAGED_DROP_IN)].decode()
            self.assertIn('ExecStart=\nExecStart=' + release + ' ', managed)
            # The argument tail, including environment references, is preserved.
            self.assertEqual(units.split_exec(managed.splitlines()[-1][len('ExecStart='):])[2],
                             units.split_exec(units.exec_start(units.effective([(DEPLOY / unit).read_text()])))[2])
            # The ExecStart-only historical drop-in is retired, never deleted.
            self.assertNotIn(dropin(unit, 'zz-cleanup-71be21f.conf'), files)
            retired = '/opt/enhance-pir/transactions/%s/%s/retired/zz-cleanup-71be21f.conf' % (journal.id, unit)
            self.assertEqual(files[retired].decode(), cleanup_drop_in((DEPLOY / unit).read_text()))
        # The v7 drop-in with other settings stays; its ExecStart is overridden.
        self.assertIn(dropin('enhance-pir-worker.service', '90-v7.conf'), self.fake.host('worker-01').files)
        # One upload per host, into an immutable release directory.
        uploads = [(host, detail) for host, op, detail in self.fake.log if op == 'upload']
        self.assertEqual([host for host, _ in uploads], ['worker-01', 'worker-02', 'router-01', 'coordinator'])
        self.assertTrue(all(self.fake.host(host).files[release] == NEW for host, _ in uploads))
        self.assertIn(('coordinator', ['/usr/local/bin/exact', '/opt/enhance-pir/releases/' + NEW_SHA, journal.id]),
                      self.fake.runs)
        # The baseline now describes the committed state, so a repeat is a no-op.
        self.fake.log.clear()
        self.assertIsNone(deployer.deploy(NEW_SHA, self.binary))
        self.assertEqual(self.fake.log, [])

    def test_historical_drop_ins_are_superseded_unless_retirement_is_asked_for(self):
        deployer = self.enhance_fleet()
        self.assertEqual(deployer.deploy(NEW_SHA, self.binary).status, 'committed')
        for host, unit in ENHANCE:
            self.assertEqual(self.running(host, unit), NEW_SHA)
            # Left in place; the managed drop-in sorts after it and wins.
            self.assertIn(dropin(unit, 'zz-cleanup-71be21f.conf'), self.fake.host(host).files)

    def test_failure_rolls_back_only_touched_targets_in_reverse(self):
        deployer = self.enhance_fleet()
        self.fake.unhealthy.add(('router-01', NEW_SHA))
        with self.assertRaisesRegex(DeployError, 'packing-router@router-01: not verified'):
            deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        touched = ENHANCE[:3]
        self.assertEqual(self.restarts(), touched + list(reversed(touched)))
        for host, unit in ENHANCE:
            self.assertEqual(self.running(host, unit), OLD_SHA)
            files = self.fake.host(host).files
            self.assertNotIn(dropin(unit, units.MANAGED_DROP_IN), files)
            self.assertEqual(files[dropin(unit, 'zz-cleanup-71be21f.conf')].decode(),
                             cleanup_drop_in((DEPLOY / unit).read_text()))
        # The coordinator only received the staged release; its units were never touched.
        self.assertEqual({op for _, op, _ in self.fake.mutations('coordinator')}, {'mkdir', 'upload', 'rename'})
        journal = Journal.load(self.state, 'enhance')
        self.assertEqual(journal.status, 'rolled-back')
        self.assertEqual([r['phase'] for r in journal.hosts], ['restored'] * 3 + ['pending'] * 2)
        # Nothing new is known, so the baseline still matches and allows a later deploy.
        self.assertEqual(deployer.baseline_problems(deployer.probe(), required=True), [])

    def test_only_an_exec_start_drop_in_sorting_after_the_managed_one_is_refused(self):
        self.enhance_fleet()
        unit = 'enhance-pir-worker.service'
        self.fake.edit('worker-02', dropin(unit, '50-local.conf'), '[Service]\nExecStart=\nExecStart=%s worker\n' % LEGACY)
        deployer = self.deployer()
        deployer.capture_baseline()
        self.fake.log.clear()
        # An earlier ExecStart layer is superseded: it stays and the managed drop-in wins.
        self.assertEqual(deployer.deploy(NEW_SHA, self.binary, retire_historical=True).status, 'committed')
        self.assertIn(dropin(unit, '50-local.conf'), self.fake.host('worker-02').files)
        self.assertEqual(self.running('worker-02', unit), NEW_SHA)
        deployer.rollback()
        # One sorting after the managed drop-in would still win, so it is refused.
        late = 'z' * 40 + '-local.conf'
        self.fake.edit('worker-02', dropin(unit, late), '[Service]\nExecStart=\nExecStart=%s worker\n' % LEGACY)
        deployer.capture_baseline()
        self.fake.log.clear()
        with self.assertRaisesRegex(DeployError, 'unmanaged drop-in .*-local.conf overrides ExecStart'):
            deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual(self.fake.log, [])
        del self.fake.host('worker-02').files[dropin(unit, late)]
        # A drop-in that does not touch ExecStart is part of the unit, not a refusal.
        self.fake.edit('worker-02', dropin(unit, '50-local.conf'), '[Service]\nMemoryHigh=6G\n')
        deployer.capture_baseline()
        self.assertEqual(deployer.deploy(NEW_SHA, self.binary, retire_historical=True).status, 'committed')
        self.assertIn(dropin(unit, '50-local.conf'), self.fake.host('worker-02').files)

    def test_baseline_is_required_and_live_changes_since_it_are_refused(self):
        deployer = self.enhance_fleet()
        unit = 'enhance-pir-worker.service'
        self.fake.edit('worker-01', '/etc/systemd/system/' + unit,
                       (DEPLOY / unit).read_text().replace('RestartSec=3', 'RestartSec=4'))
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
        self.fake.edit('worker-02', dropin(unit, units.MANAGED_DROP_IN), '[Service]\nExecStart=\nExecStart=/bin/other\n')
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
            'router': [('STATUS_PREPARATION_THREADS=6', 'STATUS_PREPARATION_THREADS=4')]}

    def test_template_drift_from_the_live_unit_is_refused(self):
        deployer = self.status_fleet(self.LIVE)
        with self.assertRaisesRegex(DeployError, 'allow-unit-drift') as refusal:
            deployer.deploy(sha256(STATUS_NEW), self.status_binary)
        self.assertIn("Service.MemoryMax: '6G' -> '12G'", '\n'.join(self.lines))
        self.assertNotIn('controller@coordinator: the new unit differs', str(refusal.exception))
        self.assertEqual(self.fake.log, [])

    def test_running_binary_with_reconciled_templates_is_a_noop(self):
        deployer = self.status_fleet()
        self.assertIsNone(deployer.deploy(sha256(STATUS_OLD)))
        self.assertEqual(self.fake.log, [])

    def test_running_binary_with_drift_still_restarts_and_rolls_back_by_restart(self):
        deployer = self.status_fleet(self.LIVE)
        # The restart moves the unit to the release layout, so the bytes must be supplied.
        with self.assertRaisesRegex(DeployError, 'not staged and no binary'):
            deployer.deploy(sha256(STATUS_OLD), allow_drift=True)
        running = self.dir / 'running-status-pir'
        running.write_bytes(STATUS_OLD)
        journal = deployer.deploy(sha256(STATUS_OLD), running, allow_drift=True)
        self.assertEqual([r['action'] for r in journal.hosts], ['restart', 'restart', 'skip'])
        self.fake.log.clear()
        deployer.rollback()
        # Same executable before and after, but the process must pick up the restored unit.
        self.assertEqual(self.restarts(), [('status-01', 'status-router.service'), ('status-01', 'status-worker.service')])

    def test_accepted_drift_renders_the_whole_unit_from_the_template(self):
        deployer = self.status_fleet(self.LIVE)
        sha = sha256(STATUS_NEW)
        journal = deployer.deploy(sha, self.status_binary, allow_drift=True)
        self.assertEqual(journal.status, 'committed')
        expected = units.render((DEPLOY / 'status-worker.service.in').read_text(),
                                {'RELEASE': '/opt/status-pir/releases/' + sha, 'NETWORK': NETWORK})
        self.assertEqual(self.fake.host('status-01').files['/etc/systemd/system/status-worker.service'].decode(), expected)
        self.assertEqual(self.restarts(), [('status-01', 'status-worker.service'), ('status-01', 'status-router.service'),
                                           ('coordinator', 'status-controller-qualification.service')])
        deployer.rollback()
        text = self.fake.host('status-01').files['/etc/systemd/system/status-worker.service'].decode()
        self.assertIn('MemoryMax=6G', text)
        self.assertIn(STATUS_LEGACY, text)

    def test_template_values_cannot_add_unit_lines_or_path_segments(self):
        deployer = self.status_fleet()
        sha = sha256(STATUS_NEW)
        document = json.loads(self.inventory_path.read_text())
        for value in ['ab\nExecStartPre=/bin/sh -c id', 'ab cd', '../ab', 'ab/cd', 'a=b', '"ab"', '']:
            with self.subTest(value=value):
                document['services']['status']['template_vars']['NETWORK'] = value
                self.inventory_path.write_text(json.dumps(document))
                inventory = descriptors.load_inventory(self.inventory_path)
                with self.assertRaisesRegex(descriptors.DescriptorError, 'template_vars.NETWORK'):
                    descriptors.template_values(SERVICES['status'], inventory, sha)
                deployer.inventory = inventory
                with self.assertRaisesRegex(DeployError, 'refused before any change'):
                    deployer.deploy(sha, self.status_binary, allow_drift=True)
                self.assertEqual(self.fake.log, [])
        self.assertEqual(descriptors.template_values(SERVICES['status'], self.inventory, sha)['NETWORK'], NETWORK)

    def test_set_property_drop_in_counts_as_live_configuration(self):
        self.status_fleet()
        self.fake.edit('status-01', '/etc/systemd/system.control/status-worker.service.d/50-MemoryMax.conf',
                       '[Service]\nMemoryMax=6G\n')
        deployer = self.deployer('status')
        deployer.capture_baseline()
        plans, problems = deployer.assess(sha256(STATUS_NEW), self.status_binary)
        # The drop-in stays and still applies over the rendered unit: no drift.
        self.assertEqual(plans[0].drift, [])
        self.assertEqual(problems, [])


class ReceiverTests(Fleet):
    def unit(self):
        return self.fake.read('receiver-01', '/etc/systemd/system/receiver-pir.service')

    def command(self):
        return self.fake.host('receiver-01').units['receiver-pir.service']['exec_start']

    def release(self, host, sha):
        """The files of release `sha` on `host`, by name."""
        prefix = receiver_release(sha) + '/'
        return {path[len(prefix):]: data for path, data in self.fake.host(host).files.items() if path.startswith(prefix)}

    def staged(self):
        return {'receiver-directory': RECEIVER_NEW, 'receiver-probe': PROBE, 'probe-fixture.json': FIXTURE}

    def test_first_deploy_adopts_the_hand_installed_unit_and_rollback_returns_to_current(self):
        deployer = self.receiver_fleet(RECEIVER_HAND_INSTALLED)
        sha = sha256(RECEIVER_NEW)
        before = dict(self.fake.host('receiver-01').files)
        # Besides the binary path, only the key file differs, and is reviewed drift.
        drift = ["Service.EnvironmentFile: ['-/etc/receiver-pir/near.env'] -> ['%s']" % (NEAR_KEY_FILE % NEAR_KEY)]
        with self.assertRaisesRegex(DeployError, 'allow-unit-drift'):
            deployer.deploy(sha, self.receiver_binary, companions=self.companions)
        self.assertEqual(self.fake.log, [])
        journal = deployer.deploy(sha, self.receiver_binary, allow_drift=True, companions=self.companions)
        self.assertEqual(journal.status, 'committed')
        self.assertEqual(journal.hosts[0]['drift'], drift)
        self.assertEqual(self.restarts(), [('receiver-01', 'receiver-pir.service')])
        self.assertEqual(self.running('receiver-01', 'receiver-pir.service'), sha)
        self.assertEqual(self.unit(), units.render(RECEIVER_TEMPLATE.read_text(),
                                                   {'RELEASE': receiver_release(sha), 'NEAR_KEY': NEAR_KEY}))
        self.assertEqual(self.fake.unit_paths('receiver-01', 'receiver-pir.service'),
                         ['/etc/systemd/system/receiver-pir.service'])
        self.assertIn('--bind 10.70.0.11:18380', self.command())
        self.assertEqual(self.fake.read('receiver-01', RECEIVER_CURRENT), RECEIVER_OLD.decode())
        self.fake.log.clear()
        deployer.rollback()
        self.assertEqual(self.restarts(), [('receiver-01', 'receiver-pir.service')])
        self.assertEqual(self.running('receiver-01', 'receiver-pir.service'), sha256(RECEIVER_OLD))
        self.assertEqual(self.unit(), RECEIVER_HAND_INSTALLED)
        self.assertTrue(self.command().startswith(RECEIVER_CURRENT + ' '))
        for path in self.fake.unit_paths('receiver-01', 'receiver-pir.service'):
            self.assertEqual(self.fake.host('receiver-01').files[path], before[path])

    def test_the_bundle_probe_and_fixture_are_staged_with_the_binary_and_gate_the_deploy(self):
        deployer = self.receiver_fleet()
        sha = sha256(RECEIVER_NEW)
        uploads, upload = [], self.fake.upload
        self.fake.upload = lambda host, local, path, mode: (uploads.append((host, path, mode)),
                                                            upload(host, local, path, mode))
        seen, run = [], self.fake.run

        def observe(host, argv, timeout):
            if argv == probe_argv(sha):
                seen.append((host, self.release(host, sha)))
            return run(host, argv, timeout)
        self.fake.run = observe
        journal = deployer.deploy(sha, self.receiver_binary, companions=self.companions)
        self.assertEqual(journal.status, 'committed')
        partial = '/opt/receiver-pir/releases/.partial-' + journal.id
        self.assertEqual(uploads, [('receiver-01', partial + '/receiver-directory', 0o755),
                                   ('receiver-01', partial + '/receiver-probe', 0o755),
                                   ('receiver-01', partial + '/probe-fixture.json', 0o644)])
        # One rename makes the whole verified release visible at once.
        self.assertEqual([d for _, op, d in self.fake.log if op == 'rename'], [(partial, receiver_release(sha))])
        self.assertEqual(self.release('receiver-01', sha), self.staged())
        # The probe and fixture that ran are this bundle's, from the release directory.
        self.assertEqual(seen, [('receiver-01', self.staged())])
        self.assertIn('--fixture', probe_argv(sha))
        self.assertIn(receiver_release(sha) + '/probe-fixture.json', probe_argv(sha))
        # A repeat with the same bundle changes nothing but runs the check again.
        self.fake.log.clear()
        seen.clear()
        journal = deployer.deploy(sha, self.receiver_binary, companions=self.companions)
        self.assertEqual(journal.status, 'committed')
        self.assertEqual(self.fake.log, [])
        self.assertEqual(seen, [('receiver-01', self.staged())])

    def test_missing_or_corrupt_companions_stage_nothing(self):
        deployer = self.receiver_fleet()
        sha = sha256(RECEIVER_NEW)
        for companions, error in [
                (None, r"stages companions \['receiver-probe', 'probe-fixture.json'\]"),
                ({'receiver-probe': self.companions['receiver-probe']}, 'stages companions'),
                ({**self.companions, 'extra': self.companions['receiver-probe']}, 'stages companions'),
                ({**self.companions, 'probe-fixture.json': Artifact(None, sha256(FIXTURE))},
                 r"lacks \['probe-fixture.json'\] and no bundle was given")]:
            with self.subTest(error=error):
                with self.assertRaisesRegex(DeployError, error):
                    deployer.deploy(sha, self.receiver_binary, companions=companions)
                self.assertEqual(self.fake.log, [])
        # A local file that is not the bytes the bundle's checksum names.
        corrupt = {**self.companions, 'probe-fixture.json': Artifact(self.dir / 'receiver-probe', sha256(FIXTURE))}
        with self.assertRaisesRegex(DeployError, 'uploaded probe-fixture.json does not match'):
            deployer.deploy(sha, self.receiver_binary, companions=corrupt)
        self.assertEqual(self.release('receiver-01', sha), {})
        self.assertEqual(self.restarts(), [])
        self.assertEqual(Journal.load(self.state, 'receiver').status, 'failed')

    def test_interrupted_staging_leaves_no_release_and_a_retry_completes_it(self):
        deployer = self.receiver_fleet()
        sha = sha256(RECEIVER_NEW)

        class Crash(BaseException):
            pass

        def crash(host, op, detail):
            if op == 'upload' and detail.endswith('/probe-fixture.json'):
                raise Crash()
        self.fake.observer = crash
        with self.assertRaises(Crash):
            deployer.deploy(sha, self.receiver_binary, companions=self.companions)
        self.assertEqual(self.release('receiver-01', sha), {})
        self.assertNotIn(receiver_release(sha), self.fake.host('receiver-01').dirs)
        self.assertEqual(self.restarts(), [])
        self.assertEqual(Journal.load(self.state, 'receiver').status, 'failed')
        self.fake.observer = None
        self.assertEqual(deployer.deploy(sha, self.receiver_binary, companions=self.companions).status, 'committed')
        self.assertEqual(self.release('receiver-01', sha), self.staged())

    def test_a_reused_release_is_checked_file_by_file(self):
        sha = sha256(RECEIVER_NEW)
        release = receiver_release(sha)
        with self.subTest('matching'):
            deployer = self.receiver_fleet()
            for name, data in self.staged().items():
                self.fake.put('receiver-01', '%s/%s' % (release, name), data)
            self.assertEqual(deployer.deploy(sha, companions=self.staged_companions).status, 'committed')
            self.assertEqual([op for _, op, _ in self.fake.log if op in ('upload', 'rename')], [])
        with self.subTest('staged before companions'):
            self.fake, self.state = FakeFleet(), self.dir / 'state-older'
            deployer = self.receiver_fleet()
            self.fake.put('receiver-01', release + '/receiver-directory', RECEIVER_NEW)
            journal = deployer.deploy(sha, self.receiver_binary, companions=self.companions)
            self.assertEqual(journal.status, 'committed')
            partial = '/opt/receiver-pir/releases/.partial-' + journal.id
            # Only the missing files, each renamed in without replacing anything.
            self.assertEqual([d for _, op, d in self.fake.log if op == 'upload'],
                             [partial + '/receiver-probe', partial + '/probe-fixture.json'])
            self.assertEqual([d for _, op, d in self.fake.log if op == 'rename'],
                             [(partial + '/receiver-probe', release + '/receiver-probe'),
                              (partial + '/probe-fixture.json', release + '/probe-fixture.json')])
            self.assertEqual(self.release('receiver-01', sha), self.staged())
        with self.subTest('conflicting'):
            # Same server bytes, another bundle's fixture: refused before any
            # change, even though every target already runs this binary.
            other = self.dir / 'other-fixture.json'
            other.write_bytes(FIXTURE + b'\n')
            companions = {**self.companions, 'probe-fixture.json': Artifact(other, sha256(FIXTURE + b'\n'))}
            self.fake.log.clear()
            with self.assertRaisesRegex(DeployError, 'immutable release file %s/probe-fixture.json holds different '
                                        'bytes' % release):
                deployer.deploy(sha, self.receiver_binary, companions=companions)
            self.assertEqual(self.fake.log, [])
            self.assertEqual(self.release('receiver-01', sha), self.staged())
            self.assertEqual(Journal.load(self.state, 'receiver').id, journal.id)

    def test_the_release_is_staged_on_a_separate_check_host(self):
        document = json.loads(self.inventory_path.read_text())
        document['services']['receiver']['exact_check']['host'] = 'coordinator'
        self.inventory_path.write_text(json.dumps(document))
        self.inventory = descriptors.load_inventory(self.inventory_path)
        deployer = self.receiver_fleet()
        sha = sha256(RECEIVER_NEW)
        journal = deployer.deploy(sha, self.receiver_binary, companions=self.companions)
        self.assertEqual(journal.status, 'committed')
        self.assertEqual(self.fake.runs[-1], ('coordinator', probe_argv(sha)))
        for host in ('receiver-01', 'coordinator'):
            self.assertEqual(self.release(host, sha), self.staged(), host)
        # The check host only received the release; it runs no unit.
        self.assertEqual({op for _, op, _ in self.fake.mutations('coordinator')}, {'mkdir', 'upload', 'rename'})
        # Its copy is checked too: a conflict there refuses a deploy that
        # would restart nothing before it stages or checks anything.
        self.fake.put('coordinator', receiver_release(sha) + '/receiver-probe', b'an older probe')
        self.fake.log.clear()
        runs = len(self.fake.runs)
        with self.assertRaisesRegex(DeployError, 'coordinator: immutable release file'):
            deployer.deploy(sha, self.receiver_binary, companions=self.companions)
        self.assertEqual(self.fake.log, [])
        self.assertEqual(len(self.fake.runs), runs)
        self.assertEqual(Journal.load(self.state, 'receiver').id, journal.id)

    def test_a_hand_provisioned_host_is_verified_without_restart(self):
        """The unit already runs the bundle's binary, from a release that lacks its companions."""
        deployer, sha = self.provisioned_receiver()
        self.assertEqual(self.release('receiver-01', sha), {'receiver-directory': RECEIVER_NEW})
        unit = self.unit()
        seen, run = [], self.fake.run

        def observe(host, argv, timeout):
            if argv == probe_argv(sha):
                seen.append((host, LOCK in self.fake.held, self.release(host, sha)))
            return run(host, argv, timeout)
        self.fake.run = observe
        plans, problems = deployer.assess(sha, self.receiver_binary, companions=self.companions)
        self.assertEqual(([plan.action for plan in plans], problems), (['skip'], []))
        self.assertEqual(deployer.release_hosts(plans), ['receiver-01'])
        journal = deployer.deploy(sha, self.receiver_binary, companions=self.companions)
        self.assertEqual(journal.status, 'committed')
        self.assertIn('verification without restart', [event['message'] for event in journal.data['events']])
        self.assertIn('committed %s, verified without restart' % journal.id, self.lines)
        # Only the missing companions are added; the unit and process stay as they were.
        self.assertEqual([d for _, op, d in self.fake.log if op == 'upload'],
                         ['/opt/receiver-pir/releases/.partial-%s/%s' % (journal.id, name)
                          for name in ('receiver-probe', 'probe-fixture.json')])
        self.assertEqual({op for _, op, _ in self.fake.log}, {'mkdir', 'upload', 'rename'})
        self.assertEqual((self.restarts(), self.unit()), ([], unit))
        self.assertEqual(seen, [('receiver-01', True, self.staged())])
        self.assertEqual([record['phase'] for record in journal.hosts], ['pending'])
        self.assertEqual(journal.touched(), [])

    def test_an_unchanged_target_that_is_not_ready_fails_its_verification(self):
        deployer, sha = self.provisioned_receiver()
        self.fake.unhealthy.add(('receiver-01', sha))
        with self.assertRaisesRegex(DeployError, 'not verified within 300s: health serving is None'):
            deployer.deploy(sha, self.receiver_binary, companions=self.companions)
        self.assertEqual(Journal.load(self.state, 'receiver').status, 'failed')
        self.assertNotIn(probe_argv(sha), [argv for _, argv in self.fake.runs])
        self.assertEqual([op for _, op, _ in self.fake.log if op in ('write', 'systemctl')], [])

    def test_a_failed_verification_is_retried_in_full(self):
        """A failed check leaves the release complete; the retry runs the check again."""
        deployer, sha = self.provisioned_receiver()
        self.fake.exact_result = (1, '{"passed":false,"category":"answer_mismatch"}')
        with self.assertRaisesRegex(DeployError, 'exact-answer check failed'):
            deployer.deploy(sha, self.receiver_binary, companions=self.companions)
        self.assertEqual(Journal.load(self.state, 'receiver').status, 'failed')
        self.assertEqual(self.release('receiver-01', sha), self.staged())
        self.fake.exact_result = (0, 'exact answers ok')
        self.fake.log.clear()
        journal = deployer.deploy(sha, self.receiver_binary, companions=self.companions)
        self.assertEqual(journal.status, 'committed')
        self.assertEqual(self.fake.log, [])
        self.assertEqual([argv for _, argv in self.fake.runs].count(probe_argv(sha)), 2)
        self.assertEqual(self.restarts(), [])

    def test_a_hand_provisioned_host_with_a_separate_check_host(self):
        self.check_on_coordinator()
        with self.subTest('conflict'):
            # The check host holds another bundle's probe: nothing is uploaded anywhere.
            deployer, sha = self.provisioned_receiver()
            self.fake.put('coordinator', receiver_release(sha) + '/receiver-probe', b'an older probe')
            with self.assertRaisesRegex(DeployError, 'coordinator: immutable release file'):
                deployer.deploy(sha, self.receiver_binary, companions=self.companions)
            self.assertEqual(self.fake.log, [])
            self.assertEqual(self.release('receiver-01', sha), {'receiver-directory': RECEIVER_NEW})
        with self.subTest('verified'):
            self.fake, self.state = FakeFleet(), self.dir / 'state-verified'
            deployer, sha = self.provisioned_receiver()
            plans, problems = deployer.assess(sha, self.receiver_binary, companions=self.companions)
            self.assertEqual((deployer.release_hosts(plans), problems), (['receiver-01', 'coordinator'], []))
            journal = deployer.deploy(sha, self.receiver_binary, companions=self.companions)
            self.assertEqual(journal.status, 'committed')
            for host in ('receiver-01', 'coordinator'):
                self.assertEqual(self.release(host, sha), self.staged(), host)
            self.assertEqual(self.fake.runs[-1], ('coordinator', probe_argv(sha)))
            self.assertEqual(self.restarts(), [])
            self.assertEqual([op for _, op, _ in self.fake.log if op in ('write', 'systemctl')], [])

    def test_a_conflict_on_a_later_host_stages_nothing_anywhere(self):
        """The check host holds bundle A; bundle B shares its server but not its probe."""
        document = json.loads(self.inventory_path.read_text())
        document['services']['receiver']['exact_check']['host'] = 'coordinator'
        self.inventory_path.write_text(json.dumps(document))
        self.inventory = descriptors.load_inventory(self.inventory_path)
        deployer = self.receiver_fleet()
        sha = sha256(RECEIVER_NEW)
        for name, data in self.staged().items():
            self.fake.put('coordinator', '%s/%s' % (receiver_release(sha), name), data)
        other = self.dir / 'other-probe'
        other.write_bytes(b'another probe')
        bundle_b = {**self.companions, 'receiver-probe': Artifact(other, sha256(b'another probe'))}
        # Stage directly, as `preflight --stage` does, with the server host first.
        with self.fake.hold_lock(*LOCK) as lock:
            deployer.lock = lock
            with self.assertRaisesRegex(DeployError, 'nothing was staged:\n  coordinator: immutable release file'):
                deployer.stage(['receiver-01', 'coordinator'], sha, self.receiver_binary, companions=bundle_b)
        with self.assertRaisesRegex(DeployError, 'coordinator: immutable release file'):
            deployer.deploy(sha, self.receiver_binary, companions=bundle_b)
        self.assertEqual(self.fake.log, [])
        self.assertEqual(self.release('receiver-01', sha), {})

    def test_template_argument_change_takes_effect_and_rolls_back(self):
        sha = sha256(RECEIVER_NEW)
        self.receiver_fleet().deploy(sha, self.receiver_binary, companions=self.companions)
        deployed = self.unit()
        template = self.dir / 'receiver-pir.service.in'
        template.write_text(RECEIVER_TEMPLATE.read_text().replace('--concurrency 12', '--concurrency 16'))
        service = SERVICES['receiver']
        changed = dataclasses.replace(service, roles={'server': dataclasses.replace(service.roles['server'],
                                                                                    template=template)})
        deployer = self.deployer('receiver', service=changed)
        self.fake.log.clear()
        # The same, already staged binary: only the arguments change, and the
        # plan shows them as drift for review.
        with self.assertRaisesRegex(DeployError, 'allow-unit-drift'):
            deployer.deploy(sha, companions=self.staged_companions)
        self.assertIn('--concurrency 16', '\n'.join(line for line in self.lines if 'drift Service.ExecStart' in line))
        self.assertEqual(self.fake.log, [])
        journal = deployer.deploy(sha, allow_drift=True, companions=self.staged_companions)
        self.assertEqual(journal.status, 'committed')
        self.assertEqual(self.restarts(), [('receiver-01', 'receiver-pir.service')])
        self.assertIn('--concurrency 16', self.unit())
        self.assertIn(' --concurrency 16 ', self.command())
        self.assertEqual(self.running('receiver-01', 'receiver-pir.service'), sha)
        self.fake.log.clear()
        deployer.rollback()
        # Same executable either way; the restart picks up the restored unit.
        self.assertEqual(self.restarts(), [('receiver-01', 'receiver-pir.service')])
        self.assertEqual(self.unit(), deployed)
        self.assertIn(' --concurrency 12 ', self.command())

    def test_a_server_that_never_publishes_is_rolled_back(self):
        deployer = self.receiver_fleet()
        sha = sha256(RECEIVER_NEW)
        self.fake.unhealthy.add(('receiver-01', sha))
        with self.assertRaisesRegex(DeployError, 'not verified within 300s: health serving is None'):
            deployer.deploy(sha, self.receiver_binary, companions=self.companions)
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
        self.assertEqual(deployer.deploy(sha, self.receiver_binary, companions=self.companions).status, 'committed')
        self.assertEqual(seen, [('receiver-01', 120, True, 'verifying', sha)])

    def rotated(self, key):
        """A receiver deployer whose inventory names NEAR key file `key`, as a rotation sets it."""
        document = json.loads(self.inventory_path.read_text())
        document['services']['receiver']['template_vars']['NEAR_KEY'] = key
        self.inventory_path.write_text(json.dumps(document))
        self.inventory = descriptors.load_inventory(self.inventory_path)
        return self.deployer('receiver')

    def test_a_key_rotation_is_a_reviewed_unit_change_of_the_same_release(self):
        sha = sha256(RECEIVER_NEW)
        self.receiver_fleet().deploy(sha, self.receiver_binary, companions=self.companions)
        previous = self.unit()
        self.fake.put('receiver-01', NEAR_KEY_FILE % 'next', 'NEAR_INTENTS_EXPLORER=new\n')
        deployer = self.rotated('next')
        self.fake.log.clear()
        plans, problems = deployer.assess(sha, companions=self.staged_companions)
        self.assertEqual(plans[0].action, 'restart')
        self.assertEqual(plans[0].drift, ["Service.EnvironmentFile: ['%s'] -> ['%s']"
                                          % (NEAR_KEY_FILE % NEAR_KEY, NEAR_KEY_FILE % 'next')])
        self.assertTrue(any('allow-unit-drift' in problem for problem in problems), problems)
        with self.assertRaisesRegex(DeployError, 'allow-unit-drift'):
            deployer.deploy(sha, companions=self.staged_companions)
        self.assertEqual(self.fake.log, [])
        journal = deployer.deploy(sha, allow_drift=True, companions=self.staged_companions)
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
                plans, problems = deployer.assess(sha, self.receiver_binary, allow_drift=True,
                                                  companions=self.companions)
                self.assertEqual(plans[0].drop_ins, [(path, 'refuse')])
                self.assertIn(refusal, problems)
                with self.assertRaisesRegex(DeployError, 'refused before any change:(.|\n)*' + refusal):
                    deployer.deploy(sha, self.receiver_binary, allow_drift=True, companions=self.companions)
                self.assertEqual(self.fake.log, [])
                self.assertEqual(Journal.load(self.state, 'receiver'), None)

    def test_a_rotation_failing_its_check_or_start_restores_the_previous_key(self):
        sha = sha256(RECEIVER_NEW)
        for i, (key, exact) in enumerate([('next', (1, '{"passed":false,"category":"feeds_not_read"}')),
                                          ('uninstalled', (0, 'exact answers ok'))]):
            with self.subTest(key=key):
                self.fake, self.state = FakeFleet(), self.dir / ('state-%d' % i)
                self.inventory_path.write_text(json.dumps(INVENTORY))
                self.inventory = descriptors.load_inventory(self.inventory_path)
                self.receiver_fleet().deploy(sha, self.receiver_binary, companions=self.companions)
                previous = self.unit()
                self.fake.put('receiver-01', NEAR_KEY_FILE % 'next', 'NEAR_INTENTS_EXPLORER=new\n')
                deployer = self.rotated(key)
                self.fake.exact_result = exact
                self.fake.log.clear()
                with self.assertRaises(DeployError):
                    deployer.deploy(sha, allow_drift=True, companions=self.staged_companions)
                self.assertEqual(Journal.load(self.state, 'receiver').status, 'rolled-back')
                self.assertEqual(self.unit(), previous)
                self.assertEqual(self.restarts(), [('receiver-01', 'receiver-pir.service')] * 2)
                state = self.fake.host('receiver-01').units['receiver-pir.service']
                self.assertEqual((state['active'], state['exe']), ('active', sha))
                self.assertIn(NEAR_KEY_FILE % NEAR_KEY, units.effective([previous])['Service']['EnvironmentFile'])

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
                    deployer.deploy(sha, self.receiver_binary, companions=self.companions)
                self.assertEqual([argv for _, argv in self.fake.runs][-1], probe_argv(sha))
                self.assertEqual(self.running('receiver-01', 'receiver-pir.service'), sha256(RECEIVER_OLD))
                self.assertEqual(self.unit(), RECEIVER_LIVE)
                self.assertEqual(Journal.load(self.state, 'receiver').status, 'rolled-back')
                # The verified release stays staged for a retry; only units roll back.
                self.assertEqual(self.release('receiver-01', sha), self.staged())


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
        with self.assertRaisesRegex(units.UnitError, '@NETWORK@'):
            units.render((DEPLOY / 'status-worker.service.in').read_text(), {'RELEASE': '/opt/status-pir/releases/x'})

    def test_repository_descriptors_and_example_inventory_load(self):
        inventory = descriptors.load_inventory(DEPLOY / 'deploy-inventory.example.json')
        for name, service in SERVICES.items():
            keys = [t.key for t in descriptors.targets(service, inventory)]
            self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(SERVICES['enhance'].order, ('worker', 'packing-router', 'query-ingress', 'coordinator'))
        self.assertEqual(SERVICES['status'].order, ('worker', 'router', 'controller'))
        self.assertEqual(SERVICES['receiver'].order, ('server',))
        self.assertEqual(SERVICES['receiver'].artifact_kinds, ('receiver-pir',))
        self.assertIn('receiver-directory', cli.load_release().BINARIES['receiver-pir'])
        (server,) = descriptors.targets(SERVICES['receiver'], inventory)
        self.assertEqual(descriptors.exact_check(SERVICES['receiver'], inventory)['host'], server.host)
        self.assertEqual(SERVICES['receiver'].companions, (descriptors.Companion('receiver-probe', 0o755),
                                                           descriptors.Companion('probe-fixture.json', 0o644)))
        self.assertEqual([service.companions for name, service in SERVICES.items() if name != 'receiver'], [(), ()])

    def test_only_a_template_role_can_own_its_unit(self):
        self.assertTrue(SERVICES['receiver'].roles['server'].owns_unit)
        self.assertFalse(any(role.owns_unit for name in ('enhance', 'status') for role in SERVICES[name].roles.values()))
        text = (DEPLOY / 'deploy.toml').read_text().replace('template = "', 'template = "%s/' % DEPLOY)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'deploy.toml'
            for old, new in [('\nowns_unit = true\n', '\nowns_unit = "yes"\n'),
                             ('adoptable_drop_ins = ["90-v7.conf", "zz-cleanup-*.conf"]\n',
                              'adoptable_drop_ins = ["90-v7.conf", "zz-cleanup-*.conf"]\nowns_unit = true\n')]:
                with self.subTest(new=new):
                    self.assertIn(old, text)
                    path.write_text(text.replace(old, new, 1))
                    with self.assertRaisesRegex(descriptors.DescriptorError, 'owns_unit'):
                        descriptors.load_descriptors(path)

    def test_companions_need_distinct_plain_names_and_explicit_modes(self):
        text = (DEPLOY / 'deploy.toml').read_text()
        declared = '{ name = "probe-fixture.json", mode = 0o644 }'
        self.assertIn(declared, text)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'deploy.toml'
            path.write_text(text.replace('template = "', 'template = "%s/' % DEPLOY))
            self.assertEqual(descriptors.load_descriptors(path)['receiver'].companions, SERVICES['receiver'].companions)
            for companion in ['{ name = "probe-fixture.json", mode = 0o600 }', '{ name = "probe-fixture.json" }',
                              '{ name = "receiver-directory", mode = 0o755 }', '{ name = "receiver-probe", mode = 0o755 }',
                              '{ name = "../probe-fixture.json", mode = 0o644 }', '"probe-fixture.json"',
                              '{ name = "probe-fixture.json", mode = "0644" }']:
                with self.subTest(companion=companion):
                    # Templates resolve beside the descriptors, so anchor them to the repository.
                    path.write_text(text.replace(declared, companion).replace('template = "', 'template = "%s/' % DEPLOY))
                    with self.assertRaisesRegex(descriptors.DescriptorError, 'companions'):
                        descriptors.load_descriptors(path)


class LocalExecutor(SSHExecutor):
    """The real helper protocol, run locally instead of over SSH."""

    def transport(self, host):
        return ['sh', '-c']


class HelperTests(unittest.TestCase):
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

    def test_plan_is_read_only_and_preflight_needs_a_baseline(self):
        self.enhance_fleet()
        self.deployer()  # registers health endpoints
        (self.state / 'baselines/enhance.json').unlink()
        self.assertEqual(self.run_cli('plan', 'enhance', '--binary', str(self.binary)), 0)
        self.assertIn('worker@worker-01 enhance-pir-worker.service: restart', self.lines)
        self.assertEqual(self.run_cli('preflight', 'enhance', '--binary', str(self.binary), '--retire-historical'), 1)
        self.assertTrue(any('run capture-baseline' in line for line in self.lines))
        self.assertEqual(self.run_cli('capture-baseline', 'enhance'), 0)
        self.assertEqual(self.run_cli('preflight', 'enhance', '--binary', str(self.binary), '--retire-historical'), 0)
        self.assertEqual(self.fake.log, [])

    def test_deploy_status_and_rollback_commands(self):
        self.enhance_fleet()
        self.deployer()
        self.assertEqual(self.run_cli('deploy', 'enhance', '--sha256', NEW_SHA, '--retire-historical'), 1)
        self.assertTrue(any('not staged and no binary' in line for line in self.lines))
        self.assertEqual(self.run_cli('deploy', 'enhance', '--binary', str(self.binary), '--retire-historical'), 0)
        self.assertEqual(self.run_cli('status', 'enhance'), 0)
        self.assertIn('committed', self.lines[0])
        self.assertEqual(self.run_cli('rollback', 'enhance'), 0)
        self.assertTrue(all(self.running(h, u) == OLD_SHA for h, u in ENHANCE))


    def receiver_bundle(self, revision, probe=PROBE):
        """A `receiver-pir` release archive in the shape `tools/ci/release.py` assembles."""
        directory = self.dir / ('bundle-' + revision[:8])
        directory.mkdir()
        files = {'receiver-directory': RECEIVER_NEW, 'receiver-probe': probe, 'revision': (revision + '\n').encode()}
        for source in cli.load_release().FILES['receiver-pir']:
            files[Path(source).name] = (ROOT / source).read_bytes()
        files['SHA256SUMS'] = ''.join('%s  %s\n' % (sha256(data), name) for name, data in sorted(files.items())).encode()
        for name, data in files.items():
            (directory / name).write_bytes(data)
        archive = self.dir / ('receiver-pir-%s.tar.gz' % revision[:8])
        with tarfile.open(archive, 'w:gz') as handle:
            for name in sorted(files):
                handle.add(directory / name, arcname=name)
        return archive

    def test_receiver_deploys_only_from_a_verified_bundle_with_its_companions(self):
        self.receiver_fleet()
        sha = sha256(RECEIVER_NEW)
        for flags in (['--binary', str(self.receiver_binary)], ['--sha256', sha]):
            self.assertEqual(self.run_cli('deploy', 'receiver', *flags), 1)
            self.assertTrue(any('give --archive with --sha' in line for line in self.lines), self.lines)
        self.assertEqual(self.fake.log, [])
        revision = '1' * 40
        archive = self.receiver_bundle(revision)
        self.assertEqual(self.run_cli('deploy', 'receiver', '--archive', str(archive), '--sha', revision), 0, self.lines)
        journal = Journal.load(self.state, 'receiver')
        self.assertEqual(journal.status, 'committed')
        self.assertEqual(journal.data['source']['companions'],
                         {'receiver-probe': sha256(PROBE), 'probe-fixture.json': sha256(FIXTURE)})
        self.assertEqual(self.fake.read('receiver-01', receiver_release(sha) + '/receiver-probe'), PROBE.decode())
        self.assertEqual(self.fake.runs[-1], ('receiver-01', probe_argv(sha)))
        # A later bundle with the same server but another probe cannot reuse the release.
        self.fake.log.clear()
        other = '2' * 40
        archive = self.receiver_bundle(other, b'another probe')
        self.assertEqual(self.run_cli('plan', 'receiver', '--archive', str(archive), '--sha', other), 0)
        self.assertTrue(any('receiver-probe holds different bytes' in line for line in self.lines), self.lines)
        self.assertEqual(self.run_cli('deploy', 'receiver', '--archive', str(archive), '--sha', other), 1)
        self.assertTrue(any('receiver-probe holds different bytes' in line for line in self.lines), self.lines)
        self.assertEqual(self.fake.log, [])

    def test_plan_and_preflight_name_a_drop_in_on_a_unit_its_role_owns(self):
        self.receiver_fleet()
        path = dropin('receiver-pir.service', '50-key.conf')
        self.fake.edit('receiver-01', path, '[Service]\nEnvironmentFile=/etc/receiver-pir/near.env\n')
        self.assertEqual(self.run_cli('capture-baseline', 'receiver'), 0)
        archive = self.receiver_bundle('4' * 40)
        problem = 'problem: server@receiver-01: role server owns its whole unit; remove or fold in ' + path
        self.assertEqual(self.run_cli('plan', 'receiver', '--archive', str(archive), '--sha', '4' * 40), 0)
        self.assertIn(problem, self.lines)
        self.assertIn('  drop-in %s: REFUSED' % path, self.lines)
        self.assertEqual(self.run_cli('preflight', 'receiver', '--archive', str(archive), '--sha', '4' * 40,
                                      '--allow-unit-drift'), 1)
        self.assertIn(problem, self.lines)
        self.assertEqual(self.fake.log, [])

    def test_plan_preflight_and_deploy_agree_on_verifying_a_provisioned_host(self):
        """`preflight --stage` completes the release; the deploy still runs the exact check."""
        _, sha = self.provisioned_receiver()
        revision = '5' * 40
        archive = self.receiver_bundle(revision)
        source = ['--archive', str(archive), '--sha', revision]
        verifies = 'deploy verifies without restart: stages missing release files, checks readiness and exact answers'
        self.assertEqual(self.run_cli('plan', 'receiver', *source), 0, self.lines)
        self.assertIn('server@receiver-01 receiver-pir.service: skip', self.lines)
        self.assertIn(verifies, self.lines)
        self.assertEqual(self.fake.log, [])
        self.assertEqual(self.run_cli('preflight', 'receiver', *source, '--stage'), 0, self.lines)
        self.assertIn(verifies, self.lines)
        self.assertEqual([d.rsplit('/', 1)[1] for _, op, d in self.fake.log if op == 'upload'],
                         ['receiver-probe', 'probe-fixture.json'])
        self.assertNotIn(probe_argv(sha), [argv for _, argv in self.fake.runs])
        self.fake.log.clear()
        self.assertEqual(self.run_cli('deploy', 'receiver', *source), 0, self.lines)
        self.assertEqual(Journal.load(self.state, 'receiver').status, 'committed')
        self.assertEqual(self.fake.runs[-1], ('receiver-01', probe_argv(sha)))
        self.assertEqual((self.fake.log, self.restarts()), ([], []))
        self.assertTrue(any(line.endswith(', verified without restart') for line in self.lines), self.lines)

    def test_preflight_stage_uploads_nothing_when_a_later_host_conflicts(self):
        """The check host holds bundle A; preflight --stage of bundle B (same server, other probe)."""
        document = json.loads(self.inventory_path.read_text())
        document['services']['receiver']['exact_check']['host'] = 'coordinator'
        self.inventory_path.write_text(json.dumps(document))
        self.inventory = descriptors.load_inventory(self.inventory_path)
        self.receiver_fleet()
        sha = sha256(RECEIVER_NEW)
        for name, data in (('receiver-directory', RECEIVER_NEW), ('receiver-probe', PROBE),
                           ('probe-fixture.json', FIXTURE)):
            self.fake.put('coordinator', '%s/%s' % (receiver_release(sha), name), data)
        revision = '3' * 40
        archive = self.receiver_bundle(revision, b'another probe')
        self.assertEqual(self.run_cli('preflight', 'receiver', '--archive', str(archive), '--sha', revision,
                                      '--stage'), 1)
        self.assertTrue(any('nothing was staged' in line and 'coordinator' in line for line in self.lines), self.lines)
        self.assertEqual([entry for entry in self.fake.log if entry[1] in ('mkdir', 'upload', 'rename')], [])
        self.assertFalse(any(path.startswith(receiver_release(sha)) for path in self.fake.host('receiver-01').files))


if __name__ == '__main__':
    unittest.main()

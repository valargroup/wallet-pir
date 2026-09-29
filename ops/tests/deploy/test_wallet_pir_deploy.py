"""The transactional Enhance and Status deploy CLI against an in-memory fleet.

Uses the repository's own descriptors and unit templates; only hosts are fake.
"""
import hashlib
import http.server
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'ops/lib'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from fake_fleet import FakeFleet, sha256  # noqa: E402
from wallet_pir_ops.deploy import cli, descriptors, units  # noqa: E402
from wallet_pir_ops.deploy.engine import Deployer, DeployError  # noqa: E402
from wallet_pir_ops.deploy.remote import LockHeld, SSHExecutor  # noqa: E402
from wallet_pir_ops.deploy.transaction import Journal  # noqa: E402

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

INVENTORY = {
    'lock': {'type': 'remote', 'host': 'coordinator'},
    'ssh': {'mode': 'config'},
    'hosts': {name: {} for name in ('coordinator', 'worker-01', 'worker-02', 'router-01', 'status-01')},
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

    def deployer(self, name='enhance'):
        deployer = Deployer(SERVICES[name], self.inventory, self.fake, self.state, out=self.lines.append,
                            sleep=self.clock.sleep, clock=self.clock, poll_seconds=5)
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
            self.fake.install(host, unit, fragment, drop_ins, {LEGACY: OLD})
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

    def restarts(self):
        return [(host, detail[1]) for host, op, detail in self.fake.log if op == 'systemctl' and detail[0] == 'restart']

    def running(self, host, unit):
        return self.fake.host(host).units[unit]['exe']


class DeployTests(Fleet):
    def test_noop_deploy_with_the_running_binary_changes_nothing(self):
        deployer = self.enhance_fleet()
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

    def test_retiring_historical_drop_ins_needs_the_flag(self):
        deployer = self.enhance_fleet()
        with self.assertRaisesRegex(DeployError, 'retire-historical'):
            deployer.deploy(NEW_SHA, self.binary)
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

    def test_unmanaged_exec_start_drop_in_is_refused_before_any_change(self):
        self.enhance_fleet()
        unit = 'enhance-pir-worker.service'
        self.fake.edit('worker-02', dropin(unit, '50-local.conf'), '[Service]\nExecStart=\nExecStart=%s worker\n' % LEGACY)
        deployer = self.deployer()
        deployer.capture_baseline()
        self.fake.log.clear()
        with self.assertRaisesRegex(DeployError, 'unmanaged drop-in .*50-local.conf overrides ExecStart'):
            deployer.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual(self.fake.log, [])
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


if __name__ == '__main__':
    unittest.main()

"""Observer inventory, coverage, monitoring certificates and a fresh monitoring rebuild.

The rebuild rehearsal runs the real `pir-apm-sidecar`, `pir-apm` and
`pir-monitor` binaries when `PIR_MONITORING_BIN` (or the release-fast target)
holds them, with ephemeral test certificates, rendered configuration and
units, and no SSH.
"""
import base64
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'tools/lib'))
sys.path.insert(0, str(ROOT / 'tools/tests/operations/deploy'))
from fake_fleet import FakeFleet  # noqa: E402
from wallet_pir_ops import monitoring, monitoring_certs, monitoring_install, monitoring_roles  # noqa: E402
from wallet_pir_ops.deploy import descriptors, remote, transaction, units  # noqa: E402

SERVICES = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')

COORDINATOR = {'host_id': 'coordinator', 'address': '10.0.0.2', 'units': ['pir-apm.service'],
               'required_volumes': [{'path': p, 'uuid': '0000-000%d' % i}
                                    for i, p in enumerate(monitoring.COORDINATOR_VOLUMES)]}
WORKER = {'host_id': 'recent-07', 'address': '10.0.0.17'}


def inventory(*observers, retired=()):
    return {'hosts': {o['host_id']: {} for o in observers},
            'retired_hosts': {name: {'retired': 'test'} for name in retired},
            'services': {'apm': {'roles': {'server': [{'host': 'coordinator'}]}},
                         'apm-sidecar': {'roles': {'observer': [
                             {'host': o['host_id'], 'install': True, 'settings': o} for o in observers]}}}}


class ObserverInventory(unittest.TestCase):
    def test_coverage_accepts_dynamic_enrollment_and_refuses_missing_or_malformed_records(self):
        record = monitoring.enrollment_record({'host_id': 'recent-09', 'address': '10.0.0.19'})
        member = {'id': 'recent-09', 'intent': 'enrolled', 'monitoring': record}
        workers = inventory(WORKER)
        workers['services'].pop('apm')
        empty = {'hosts': {}, 'services': {}}
        result = monitoring.coverage(workers, terraform_outputs={
            'worker_ipv4': {'value': '10.0.0.17'}}, roster=[member])
        self.assertEqual(result['enrolled'], ['recent-07', 'recent-09'])
        self.assertEqual(result['missing_declarations'], [])
        self.assertEqual(result['not_provisioned'], [])
        for invalid in (None, dict(record, config_digest='bad'), dict(record, address='bad'),
                        dict(record, checks=['bad name']), dict(record, address='[10.0.0.19]:9443'),
                        dict(record, address='[fc00::19]]:9443')):
            with self.subTest(record=invalid):
                result = monitoring.coverage(empty, terraform_outputs={}, roster=[dict(member, monitoring=invalid)])
                self.assertEqual(result['missing_declarations'], ['recent-09'])
                self.assertEqual(result['enrolled'], [])
        retired = dict(member, intent='retired')
        result = monitoring.coverage(empty, terraform_outputs={}, roster=[retired])
        self.assertEqual((result['missing_declarations'], result['enrolled']), ([], []))
        ipv6 = dict(member, monitoring=dict(record, address='[fc00::19]:9443', checks=['volume:/srv/cache']))
        result = monitoring.coverage(empty, terraform_outputs={}, roster=[ipv6],
                                     expected_volumes={'recent-09': ['/srv/cache']})
        self.assertEqual((result['missing_declarations'], result['missing_volumes']), ([], {}))
        result = monitoring.coverage(empty, terraform_outputs={}, roster=[ipv6],
                                     expected_volumes={'recent-09': [{'path': '/srv/cache', 'uuid': '1234'}]})
        self.assertEqual(result['missing_volumes'], {'recent-09': ['/srv/cache']},
                         'an enrollment digest cannot prove an unknown volume UUID')

    def test_cli_renders_dynamic_capacity_mapping_and_binds_it_to_revision(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, mapping = root / 'inventory.json', root / 'mapping.json'
            source.write_text(json.dumps(inventory(WORKER)))
            value = {'unit': 'transparent-recent.service', 'role_units': {'recent': 'transparent-recent.service'},
                     'data_path': '/srv/cache', 'size': 'm-4vcpu-32gb'}
            mapping.write_text(json.dumps(value))
            result = subprocess.run([sys.executable, str(ROOT / 'tools/scripts/wallet-pir-monitoring.py'),
                                     'render-observers', '--inventory', str(source), '--mode', 'shadow',
                                     '--roster', '/etc/pir/roster.json', '--host-snapshot', '/var/lib/pir/hosts.json',
                                     '--roster-snapshot', str(mapping)], capture_output=True, text=True, check=True)
            rendered = json.loads(result.stdout)
            self.assertEqual(rendered['roster_snapshot'], value)
            self.assertNotEqual(rendered['revision'], monitoring.observers(inventory(WORKER), mode='shadow',
                                                                          roster='/etc/pir/roster.json')['revision'])
            for invalid in (dict(value, unit='bad'), dict(value, data_path='../cache'),
                            dict(value, role_units={'recent': 'bad'})):
                with self.assertRaises(ValueError):
                    monitoring.observers(inventory(WORKER), mode='shadow', roster='/r', roster_snapshot=invalid)

    def test_sidecar_unit_keeps_host_mounts_visible_and_holds_no_secret_bundle(self):
        rendered = monitoring.sidecar(COORDINATOR, release='/opt/pir-apm-sidecar/releases/x')
        service = units.effective([rendered.unit])['Service']
        for hiding in ('InaccessiblePaths', 'TemporaryFileSystem', 'PrivateDevices', 'BindPaths',
                       'BindReadOnlyPaths', 'EnvironmentFile'):
            self.assertNotIn(hiding, service)
        self.assertEqual(sorted(c.split(':')[0] for c in service['LoadCredential']),
                         ['monitoring-ca.pem', 'server.key', 'server.pem'])
        # Only pir-apm and pir-monitor hold notification credentials.
        for path in ('tools/deploy/monitoring/pir-apm-sidecar.service.in',
                     'tools/enhance/deploy/pir-apm-observers.conf'):
            directives = units.effective([(ROOT / path).read_text()])['Service']
            self.assertFalse([v for values in directives.values() for v in values
                              if 'slack' in v.lower() or 'webhook' in v.lower()], path)

    def test_observer_inventory_carries_digest_checks_and_decommissioning(self):
        document = monitoring.observers(inventory(COORDINATOR, WORKER, retired=['old-worker']), mode='shadow')
        coordinator = document['hosts'][0]
        rendered = monitoring.sidecar(COORDINATOR, release='/r')
        self.assertEqual(coordinator['config_digest'], monitoring.config_digest(json.loads(
            rendered.files[monitoring.CONFIG])))
        self.assertEqual(coordinator['tls_name'], 'coordinator.pir-monitoring')
        self.assertIn('volume:/srv/txid-display-genesis', coordinator['checks'])
        self.assertIn('unit:pir-apm.service:oom', coordinator['checks'])
        self.assertEqual(document['decommissioned'], ['old-worker'])
        wrong = inventory(dict(WORKER, host_id='other'))
        wrong['hosts'] = {'recent-07': {}}
        wrong['services']['apm-sidecar']['roles']['observer'][0]['host'] = 'recent-07'
        with self.assertRaisesRegex(ValueError, 'host_id must equal'):
            monitoring.observers(wrong, mode='shadow')
        with self.assertRaisesRegex(ValueError, 'private address'):
            monitoring.sidecar_document(dict(WORKER, address='203.0.113.9'))

    def test_external_hosts_are_dialed_at_their_declared_global_address(self):
        gpu = {'host_id': 'gpu-01', 'address': '198.51.100.7', 'external': True, 'bind_address': '10.8.0.4'}
        document = monitoring.sidecar_document(gpu)
        self.assertEqual(document['listen'], '10.8.0.4:9443', 'binds behind the provider NAT')
        self.assertEqual(document['tls']['collector_identity'], monitoring.COLLECTOR_IDENTITY)
        record = monitoring.enrollment_record(gpu)
        self.assertEqual(record['address'], '198.51.100.7:9443')
        self.assertEqual(record['config_digest'], monitoring.config_digest(document))
        with self.assertRaisesRegex(ValueError, 'unless declared external'):
            monitoring.sidecar_document(dict(gpu, external=False, bind_address='198.51.100.7'))
        with self.assertRaisesRegex(ValueError, 'global address'):
            monitoring.sidecar_document(dict(gpu, address='10.0.0.9'))
        with self.assertRaisesRegex(ValueError, 'only for external hosts'):
            monitoring.sidecar_document(dict(WORKER, bind_address='10.0.0.99'))
        with self.assertRaisesRegex(ValueError, 'specific address'):
            monitoring.sidecar_document(dict(gpu, bind_address='0.0.0.0'))
        report = monitoring.coverage(inventory(COORDINATOR, gpu), terraform_outputs={}, roster=[],
                                     external_hosts={'gpu-01': ['198.51.100.7']})
        self.assertEqual(report['missing_declarations'], [])

    def test_enrollment_is_generated_from_terraform_outputs_and_external_hosts(self):
        outputs = {
            'wallet_pir_coordinator_public_ipv4': {'value': '203.0.113.2'},
            'wallet_pir_coordinator_private_ipv4': {'value': '10.0.0.2'},
            'enhance_worker_groups': {'value': [{'name': 'g0', 'replicas': [
                {'name': 'enhance-01', 'public_ipv4': '203.0.113.3', 'private_ipv4': '10.0.0.3'}]}]},
            'transparent_router_public_ipv4': {'value': '203.0.113.9'},
        }
        document = inventory(COORDINATOR)
        document['hosts'] = {'coordinator': {'address': '203.0.113.2'}, 'enhance-01': {'address': '203.0.113.3'},
                             'gpu-01': {'address': '198.51.100.7'}}
        document['services']['enhance'] = {'roles': {'worker': [{'host': 'enhance-01'}]}}
        units = {('enhance', 'worker'): 'enhance-pir-worker.service', ('apm', 'server'): 'pir-apm.service'}
        enrolled, report = monitoring.enroll(document, terraform_outputs=outputs, role_units=units,
                                             external_hosts={'gpu-01': ['198.51.100.7']})
        self.assertEqual(report['added'], ['enhance-01', 'gpu-01'])
        self.assertEqual(report['undeclared'], ['transparent_router'], 'a provisioned host the inventory lacks')
        observers = {e['host']: e['settings'] for e in enrolled['services']['apm-sidecar']['roles']['observer']}
        self.assertEqual(observers['coordinator'], COORDINATOR, 'existing observers are kept as they are')
        self.assertEqual(observers['enhance-01'], {'host_id': 'enhance-01', 'address': '10.0.0.3',
                                                   'units': ['enhance-pir-worker.service']})
        self.assertTrue(observers['gpu-01']['external'])
        rendered = monitoring.observers(enrolled, mode='shadow')
        self.assertEqual(sorted(h['host_id'] for h in rendered['hosts']), ['coordinator', 'enhance-01', 'gpu-01'])
        again, report = monitoring.enroll(enrolled, terraform_outputs=outputs, role_units=units,
                                          external_hosts={'gpu-01': ['198.51.100.7']})
        self.assertEqual((again, report['added']), (enrolled, []), 'idempotent')
        bare = inventory(dict(COORDINATOR, required_volumes=[]))
        bare['hosts'] = {'coordinator': {'address': '10.0.0.2'}}
        _, report = monitoring.enroll(bare, terraform_outputs=outputs, role_units=units)
        self.assertEqual(report['volumes_to_bind'], {'coordinator': sorted(monitoring.COORDINATOR_VOLUMES)})

    def test_the_example_inventory_renders(self):
        example = json.loads((ROOT / 'tools/deploy/monitoring/monitoring-inventory.example.json').read_text())
        example['hosts'] = {entry['host']: {} for entry in example['services']['apm-sidecar']['roles']['observer']}
        rendered = monitoring.observers(example, mode='shadow')
        self.assertEqual([h['host_id'] for h in rendered['hosts']], ['coordinator', 'transparent-pir-recent-01'])
        self.assertIn('edge', rendered['hosts'][0]['checks'])

    def test_coverage_lists_missing_hosts_volumes_and_dynamic_workers(self):
        outputs = {
            'wallet_pir_coordinator_public_ipv4': {'value': '203.0.113.2'},
            'wallet_pir_coordinator_private_ipv4': {'value': '10.0.0.2'},
            'enhance_worker_groups': {'value': [{'name': 'g0', 'replicas': [
                {'name': 'enhance-01', 'public_ipv4': '203.0.113.3', 'private_ipv4': '10.0.0.3'}]}]},
            'transparent_fleet_json': {'value': json.dumps([{'id': 'recent-07', 'ssh_host': '10.0.0.17'}])},
            'pir_monitor_ipv4': {'value': None},
        }
        partial = monitoring.coverage(inventory(COORDINATOR))
        self.assertTrue(partial['partial'], 'no cloud claim without its sources')
        roster = [{'id': 'recent-07', 'ssh_host': '10.0.0.17'}, {'id': 'recent-09', 'ssh_host': '10.0.0.19'},
                  {'id': 'gone', 'ssh_host': '10.0.0.30', 'intent': 'retired'}]
        trimmed = dict(COORDINATOR, required_volumes=COORDINATOR['required_volumes'][:1])
        report = monitoring.coverage(inventory(trimmed, WORKER), terraform_outputs=outputs, roster=roster,
                                     external_hosts={'gpu-01': ['192.0.2.5']})
        self.assertFalse(report['partial'])
        self.assertEqual(report['missing_declarations'], ['enhance-01', 'gpu-01', 'recent-09'])
        self.assertEqual(report['missing_volumes'],
                         {'coordinator': ['/srv/transparent-activity', '/srv/txid-display-genesis']})
        full = monitoring.coverage(
            inventory(COORDINATOR, WORKER, dict(WORKER, host_id='enhance-01', address='10.0.0.3'),
                      dict(WORKER, host_id='recent-09', address='10.0.0.19'),
                      dict(WORKER, host_id='gpu-01', address='192.168.5.5')),
            terraform_outputs=outputs, roster=roster, external_hosts={'gpu-01': ['192.168.5.5']})
        self.assertEqual((full['missing_declarations'], full['missing_volumes'], full['not_provisioned']),
                         ([], {}, []))


class Certificates(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.root = Path(self.dir.name)
        monitoring_certs.issue_ca(self.root / 'ca')

    def tearDown(self):
        self.dir.cleanup()

    def test_issue_commands_accept_relative_ca_and_output_paths(self):
        for command, output in (('issue-host', 'host-relative'), ('issue-collector', 'collector-relative')):
            argv = [sys.executable, str(ROOT / 'tools/scripts/wallet-pir-monitoring.py'), command,
                    '--ca', 'ca', '--output', output]
            if command == 'issue-host':
                argv += ['--host', 'recent-09']
            subprocess.run(argv, cwd=self.root, capture_output=True, text=True, check=True)
            stem = 'server' if command == 'issue-host' else 'collector'
            self.assertTrue((self.root / output / (stem + '.pem')).is_file())
            self.assertEqual((self.root / output / (stem + '.key')).stat().st_mode & 0o777, 0o600)

    def test_expiry_thresholds_and_fresh_keys_on_rotation(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        for days, expected in ((90, 'healthy'), (20, 'warning'), (3, 'critical')):
            cert = monitoring_certs.issue_leaf(self.root / 'ca', self.root / str(days), 'h.pir-monitoring',
                                               'server', days=days)
            self.assertEqual(monitoring_certs.expiry_status(cert.read_text(), now), expected)
        self.assertEqual(os.stat(self.root / '90' / 'server.key').st_mode & 0o777, 0o600)
        self.assertNotEqual((self.root / '90' / 'server.key').read_bytes(),
                            (self.root / '20' / 'server.key').read_bytes())

    def sidecar_host(self):
        fake = FakeFleet()
        fake.host('w1')
        fake.endpoints[('w1', monitoring_certs.SIDECAR_HEALTH)] = monitoring_certs.SIDECAR_UNIT
        fake.host('w1').units[monitoring_certs.SIDECAR_UNIT] = {
            'loaded': [('/etc/systemd/system/' + monitoring_certs.SIDECAR_UNIT, '')], 'active': 'inactive',
            'pid': 0, 'exe': None}
        fake._reload = lambda host: None
        # The unit runs whatever server.key holds; health follows that process.
        fake._start = lambda host, unit: fake.host(host).units[unit].update(
            active='active', exe=fake.read(host, monitoring_certs.SIDECAR_CREDENTIALS + '/server.key'))
        return fake

    def install(self, fake, files, **options):
        lock = fake.hold_lock('w1', '/run/lock/wallet-pir-production.lock')
        return monitoring_certs.install(fake, lock, 'w1', files, state_dir=self.root / 'journal',
                                        sleep=lambda _: None, **options)

    def installed(self, fake):
        return {name: fake.read('w1', '%s/%s' % (monitoring_certs.SIDECAR_CREDENTIALS, name))
                for name in monitoring_certs.SIDECAR_FILES}

    OLD = {name: ('old ' + name).encode() for name in monitoring_certs.SIDECAR_FILES}
    NEW = dict(OLD, **{'server.pem': b'new cert', 'server.key': b'new key'})

    def test_first_install_rotation_idempotency_and_explicit_rollback(self):
        fake = self.sidecar_host()
        self.assertEqual(self.install(fake, self.OLD), sorted(self.OLD))
        self.assertEqual(self.install(fake, self.NEW), ['server.key', 'server.pem'])
        self.assertEqual(self.installed(fake)['server.key'], 'new key')
        self.assertEqual(self.install(fake, self.NEW), [], 'idempotent')
        journals = list((self.root / 'journal').glob('monitoring-files-*.json'))
        self.assertFalse([p for p in journals if b'new key' in p.read_bytes()], 'journals hold digests only')
        lock = fake.hold_lock('w1', '/run/lock/wallet-pir-production.lock')
        monitoring_certs.rollback(fake, lock, 'w1', state_dir=self.root / 'journal', sleep=lambda _: None)
        self.assertEqual(self.installed(fake), {k: v.decode() for k, v in self.OLD.items()})
        self.assertEqual(fake.host('w1').units[monitoring_certs.SIDECAR_UNIT]['active'], 'active')

    def test_files_for_a_unit_not_yet_installed_are_staged_without_a_restart(self):
        fake = FakeFleet()
        fake.host('c1')
        lock = fake.hold_lock('c1', '/run/lock/wallet-pir-production.lock')
        files = {'slack-webhook': b'https://hooks.slack.com/services/T0/B0/test\n'}
        options = dict(state_dir=self.root / 'journal', directory='/etc/pir-apm/alerting',
                       unit=monitoring_certs.COLLECTOR_UNIT, sleep=lambda _: None)
        self.assertEqual(monitoring_certs.install(fake, lock, 'c1', files, **options), ['slack-webhook'])
        self.assertFalse([entry for entry in fake.log if entry[1] == 'systemctl'], 'no unit to restart')
        self.assertEqual(fake.read('c1', '/etc/pir-apm/alerting/slack-webhook'), files['slack-webhook'].decode())
        journal = json.loads(next((self.root / 'journal').glob('monitoring-files-c1-*.json')).read_text())
        self.assertTrue(journal['hosts'][0]['staged'])
        self.assertNotIn('hooks.slack.com', json.dumps(journal), 'journals hold digests only')
        lock = fake.hold_lock('c1', '/run/lock/wallet-pir-production.lock')
        monitoring_certs.rollback(fake, lock, 'c1', state_dir=self.root / 'journal',
                                  directory='/etc/pir-apm/alerting', sleep=lambda _: None)
        self.assertIsNone(fake.read('c1', '/etc/pir-apm/alerting/slack-webhook'))

    def test_every_failing_effect_restores_the_previous_set(self):
        old = {k: v.decode() for k, v in self.OLD.items()}
        effects = [('write', monitoring_certs.SIDECAR_CREDENTIALS + '/server.key'),
                   ('write', monitoring_certs.SIDECAR_CREDENTIALS + '/server.pem'),
                   ('systemctl', ('restart', monitoring_certs.SIDECAR_UNIT))]
        for op, detail in effects:
            with self.subTest(effect=(op, detail)):
                fake = self.sidecar_host()
                self.install(fake, self.OLD)
                failed = []

                def observe(host, kind, what, op=op, detail=detail):
                    if (kind, what) == (op, detail) and not failed:
                        failed.append(what)
                        raise OSError('injected transport failure')
                fake.observer = observe
                with self.assertRaisesRegex(RuntimeError, 'previous files restored'):
                    self.install(fake, self.NEW)
                self.assertEqual(self.installed(fake), old)
        # A readiness probe whose transport fails is "not ready", then restored.
        fake = self.sidecar_host()
        self.install(fake, self.OLD)

        def broken(executor, host):
            if fake.read(host, monitoring_certs.SIDECAR_CREDENTIALS + '/server.key') == 'new key':
                raise OSError('connection reset')
            return True
        with self.assertRaisesRegex(RuntimeError, 'previous files restored'):
            self.install(fake, self.NEW, ready=broken, ready_timeout=0)
        self.assertEqual(self.installed(fake), old)
        # An unready new process too.
        fake.unhealthy.add(('w1', 'new key'))
        with self.assertRaisesRegex(RuntimeError, 'previous files restored'):
            self.install(fake, self.NEW, ready_timeout=0)
        self.assertEqual(self.installed(fake), old)

    def test_an_applied_but_unacknowledged_write_is_restored(self):
        fake = self.sidecar_host()
        self.install(fake, self.OLD)
        original = fake.write

        def write(host, path, data, mode):
            original(host, path, data, mode)
            if path.endswith('/server.pem') and data == b'new cert':
                raise TimeoutError('reply lost')
        fake.write = write
        with self.assertRaisesRegex(RuntimeError, 'previous files restored'):
            self.install(fake, self.NEW)
        self.assertEqual(self.installed(fake)['server.pem'], 'old server.pem')

    def test_lock_loss_leaves_the_journal_open_and_the_next_run_restores_first(self):
        fake = self.sidecar_host()
        self.install(fake, self.OLD)

        def lose(host, kind, what):
            if kind == 'systemctl':
                fake.lock.lost = True
        fake.observer = lose
        with self.assertRaises(remote.LockHeld):
            self.install(fake, self.NEW)
        fake.observer = None
        self.assertEqual(self.installed(fake)['server.key'], 'new key', 'nothing restored without the lock')
        lock = fake.hold_lock('w1', '/run/lock/wallet-pir-production.lock')
        self.assertIsNotNone(monitoring_certs.reconcile(fake, lock, 'w1', state_dir=self.root / 'journal',
                                                        sleep=lambda _: None))
        self.assertEqual(self.installed(fake)['server.key'], 'old server.key')
        with fake.hold_lock('w1', '/run/lock/wallet-pir-production.lock'):
            with self.assertRaises(remote.LockHeld):
                self.install(fake, self.NEW)

    OBSERVERS = monitoring_roles.APM_OBSERVERS

    def collector_host(self, active=True):
        """An APM whose readiness reports the revision of the inventory it was started with."""
        fake = FakeFleet()
        fake.host('c1').units[monitoring_certs.COLLECTOR_UNIT] = {
            'loaded': [('/etc/systemd/system/' + monitoring_certs.COLLECTOR_UNIT, '')],
            'active': 'active' if active else 'inactive', 'pid': 0, 'exe': None}
        fake.endpoints[('c1', monitoring_certs.COLLECTOR_READY)] = monitoring_certs.COLLECTOR_UNIT
        fake.health_shapes[('c1', monitoring_certs.COLLECTOR_UNIT)] = lambda ready, exe: {
            'ready': ready, 'revision': exe and json.loads(exe)['revision'],
            'expected_hosts': 1, 'valid_hosts': int(ready), 'evaluated_at': time.time()}
        fake._reload = lambda host: None
        fake._start = lambda host, unit: fake.host(host).units[unit].update(
            active='active', exe=fake.read(host, self.OBSERVERS))
        return fake

    def install_observers(self, fake, revision, ready_timeout=0):
        installed = fake.read('c1', self.OBSERVERS)
        lock = fake.hold_lock('c1', '/run/lock/wallet-pir-production.lock')
        return monitoring_certs.install(
            fake, lock, 'c1', {'observers.json': json.dumps({'revision': revision}).encode()},
            state_dir=self.root / 'journal', directory='/etc/pir-apm', unit=monitoring_certs.COLLECTOR_UNIT,
            ready=monitoring_certs.collector_ready(revision), restore_ready=monitoring_certs.collector_restore_ready,
            identity={'previous_revision': installed and json.loads(installed)['revision'], 'revision': revision},
            ready_timeout=ready_timeout, sleep=lambda _: None, mode=0o644)

    def collector_journal(self):
        return transaction.Journal.load(self.root / 'journal', 'monitoring-files-c1-etc-pir-apm')

    def test_observer_rollback_verifies_the_previous_revision_not_the_new_one(self):
        fake = self.collector_host()
        self.install_observers(fake, 'sha256:a')
        self.install_observers(fake, 'sha256:b')
        lock = fake.hold_lock('c1', '/run/lock/wallet-pir-production.lock')
        monitoring_certs.rollback(fake, lock, 'c1', state_dir=self.root / 'journal', directory='/etc/pir-apm',
                                  restore_ready=monitoring_certs.collector_restore_ready, ready_timeout=0,
                                  sleep=lambda _: None)
        self.assertEqual(json.loads(fake.read('c1', self.OBSERVERS))['revision'], 'sha256:a')
        self.assertEqual(self.collector_journal().status, 'rolled-back')
        # A new inventory whose collection never succeeds is restored and verified on the old revision.
        fake.unhealthy.add(('c1', json.dumps({'revision': 'sha256:c'})))
        with self.assertRaisesRegex(RuntimeError, 'previous files restored'):
            self.install_observers(fake, 'sha256:c')
        self.assertEqual(json.loads(fake.read('c1', self.OBSERVERS))['revision'], 'sha256:a')
        self.assertEqual(self.collector_journal().status, 'rolled-back')

    def test_an_unverified_restoration_stays_open_and_reconcile_finishes_it(self):
        fake = self.collector_host()
        self.install_observers(fake, 'sha256:a')
        fake.unhealthy.update({('c1', json.dumps({'revision': r})) for r in ('sha256:a', 'sha256:b')})
        with self.assertRaisesRegex(RuntimeError, 'stays open for reconcile'):
            self.install_observers(fake, 'sha256:b')
        journal = self.collector_journal()
        self.assertEqual((journal.status, journal.hosts[0]['phase']), ('restoring', 'restore-failed'))
        lock = fake.hold_lock('c1', '/run/lock/wallet-pir-production.lock')
        with self.assertRaises(RuntimeError):
            monitoring_certs.reconcile(fake, lock, 'c1', state_dir=self.root / 'journal', directory='/etc/pir-apm',
                                       restore_ready=monitoring_certs.collector_restore_ready, ready_timeout=0,
                                       sleep=lambda _: None)
        fake.unhealthy.clear()
        lock = fake.hold_lock('c1', '/run/lock/wallet-pir-production.lock')
        self.assertEqual(monitoring_certs.reconcile(
            fake, lock, 'c1', state_dir=self.root / 'journal', directory='/etc/pir-apm',
            restore_ready=monitoring_certs.collector_restore_ready, ready_timeout=0, sleep=lambda _: None),
            journal.id)
        self.assertEqual(self.collector_journal().status, 'rolled-back')
        self.assertEqual(json.loads(fake.read('c1', self.OBSERVERS))['revision'], 'sha256:a')

    def test_a_failed_first_install_leaves_no_files_and_the_unit_stopped_as_before(self):
        fake = self.collector_host(active=False)
        fake.unhealthy.add(('c1', json.dumps({'revision': 'sha256:a'})))
        with self.assertRaisesRegex(RuntimeError, 'previous files restored'):
            self.install_observers(fake, 'sha256:a')
        self.assertIsNone(fake.read('c1', self.OBSERVERS))
        self.assertEqual(fake.host('c1').units[monitoring_certs.COLLECTOR_UNIT]['active'], 'inactive')
        self.assertEqual(self.collector_journal().status, 'rolled-back')
        # Then a working first install commits, and its rollback stops the unit again.
        fake.unhealthy.clear()
        self.assertEqual(self.install_observers(fake, 'sha256:a'), ['observers.json'])
        lock = fake.hold_lock('c1', '/run/lock/wallet-pir-production.lock')
        monitoring_certs.rollback(fake, lock, 'c1', state_dir=self.root / 'journal', directory='/etc/pir-apm',
                                  restore_ready=monitoring_certs.collector_restore_ready, ready_timeout=0,
                                  sleep=lambda _: None)
        self.assertIsNone(fake.read('c1', self.OBSERVERS))
        self.assertEqual(fake.host('c1').units[monitoring_certs.COLLECTOR_UNIT]['active'], 'inactive')

    def test_collector_readiness_needs_the_installed_revision_not_plain_ok(self):
        fake = FakeFleet()
        answers = {'body': 'ok', 'status': 200}
        fake.http_get = lambda host, url, timeout=5: (answers['status'], answers['body'])
        ready = monitoring_certs.collector_ready('sha256:abc')
        with self.assertRaises(ValueError):
            ready(fake, 'c')  # `/healthz`'s plain ok is not this contract
        answers['body'] = json.dumps({'ready': True, 'revision': 'sha256:other'})
        self.assertFalse(ready(fake, 'c'))

        valid = {'ready': True, 'revision': 'sha256:abc', 'expected_hosts': 2, 'valid_hosts': 2,
                 'evaluated_at': time.time()}
        answers['body'] = json.dumps(valid)
        self.assertTrue(ready(fake, 'c'))
        for changes in ({'valid_hosts': 0}, {'valid_hosts': 1}, {'expected_hosts': 0, 'valid_hosts': 0},
                        {'evaluated_at': time.time() - 46}, {'evaluated_at': time.time() + 300}):
            answers['body'] = json.dumps(dict(valid, **changes))
            self.assertFalse(ready(fake, 'c'), changes)
        answers['status'] = 503
        self.assertFalse(ready(fake, 'c'))

    def test_an_unchanged_inventory_still_requires_live_authenticated_collection(self):
        fake = self.collector_host()
        self.install_observers(fake, 'sha256:a')
        fake.unhealthy.add(('c1', json.dumps({'revision': 'sha256:a'})))
        with self.assertRaisesRegex(RuntimeError, 'unchanged files but is not ready'):
            self.install_observers(fake, 'sha256:a')


class VolumeBinding(unittest.TestCase):
    """A cache-volume activation binds its filesystem into the observer and coverage."""

    def test_uninstalled_monitoring_leaves_volume_pending_and_visible_to_coverage(self):
        import argparse
        import hashlib
        from wallet_pir_ops.deploy import cache_data
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / 'state'
            state.mkdir()
            worker = {'host_id': 'txid-01', 'address': '10.0.0.40'}
            document = inventory(COORDINATOR, worker)
            document.update(lock={'type': 'remote', 'host': 'coordinator'}, ssh={'mode': 'config'})
            document['services']['transparent-display'] = {'roles': {'worker': [{'host': 'txid-01'}]}}
            path = root / 'inventory.json'
            path.write_text(json.dumps(document))
            before = monitoring.enrollment_record(worker)['config_digest']
            request = {'host': 'txid-01', 'unit': 'transparent-txid-display-worker.service',
                       'configuration': '/etc/x.json', 'volume_id': 'v1', 'volume_name': 'txid-cache', 'size_gib': 50}
            (state / 'volume-1.json').write_text(json.dumps({'phase': 'attached', 'volume_id': 'v1', 'plan': {
                'request': {'host': 'txid-01', 'name': 'txid-cache', 'size_gib': 50}}}))
            raw = json.dumps(request).encode()
            (root / 'request.json').write_bytes(raw)
            fake = FakeFleet()
            fake.host('coordinator')
            summary = {'transaction': 'cache-data-t1', 'phase': 'serving-started', 'active_cache': '/srv/txid-cache',
                       'mount': '/mnt/txid-cache', 'destination': '/mnt/txid-cache/cache',
                       'device': {'uuid': 'abcd-0001'}}
            fake.run = lambda host, argv, timeout: (0, json.dumps(summary))
            args = argparse.Namespace(request=root / 'request.json', request_sha256=hashlib.sha256(raw).hexdigest(),
                                      inventory=str(path), state_dir=str(state), command='cache-data-activate',
                                      transaction='cache-data-t1')
            with self.assertRaisesRegex(cache_data.DeployError, 'monitoring adoption remains pending'):
                cache_data.dispatch(args, fake)
            receipt = json.loads((state / 'cache-data-t1.json').read_text())
            self.assertEqual(receipt['phase'], 'monitoring-pending')
            self.assertEqual(receipt['monitoring']['status'], 'pending')
            bound = json.loads(path.read_text())['services']['apm-sidecar']['roles']['observer'][1]['settings']
            self.assertEqual(bound['required_volumes'], [{'path': '/srv/txid-cache', 'uuid': 'abcd-0001'}])
            self.assertNotEqual(monitoring.enrollment_record(bound)['config_digest'], before)
            observers = monitoring.observers(json.loads(path.read_text()), mode='shadow')
            self.assertIn('volume:/srv/txid-cache', observers['hosts'][1]['checks'])
            expected = monitoring.activated_volumes(state)
            self.assertEqual(expected, {'txid-01': [{'path': '/srv/txid-cache', 'uuid': 'abcd-0001'}]})
            report = monitoring.coverage(json.loads(path.read_text()), expected_volumes=expected)
            self.assertEqual(report['missing_volumes'], {})
            # Without the binding, coverage names the activated volume.
            report = monitoring.coverage(document, expected_volumes=expected)
            self.assertEqual(report['missing_volumes'], {'txid-01': ['/srv/txid-cache']})
            # Reconciliation does not repeat storage activation while prerequisites are missing.
            args.command = 'cache-data-reconcile'
            fake.run = lambda *args, **kwargs: self.fail('monitoring reconciliation must not rerun storage')
            with self.assertRaisesRegex(cache_data.DeployError, 'monitoring adoption remains pending'):
                cache_data.dispatch(args, fake)
            with self.assertRaisesRegex(ValueError, 'another filesystem'):
                monitoring.bind_volume(path, 'txid-01', {'path': '/srv/txid-cache', 'uuid': 'ffff'})


BOOT = ROOT / 'tools/deploy/monitoring/pir-apm-sidecar-boot.sh'
CURL = """#!/bin/bash
# curl for the boot script: files from $FAKE_FILES, DigitalOcean metadata from fixed answers.
output=; url=
while [ $# -gt 0 ]; do
  case "$1" in -o) output="$2"; shift 2 ;; -*) shift ;; *) url="$1"; shift ;; esac
done
case "$url" in
  */hostname) printf transparent-pir-recent-09 ;;
  */interfaces/private/0/ipv4/address) printf 10.10.0.31 ;;
  *) cp "$FAKE_FILES/$(basename "$url")" "$output" ;;
esac
"""


class BootInstall(unittest.TestCase):
    """A new droplet's first-boot sidecar install, run for real against a scratch root."""

    def boot(self, binary, sha):
        directory = Path(self.dir.name)
        tools, files, root = directory / 'bin', directory / 'files', directory / 'root'
        for path in (tools, files, root):
            path.mkdir(exist_ok=True)
        (tools / 'curl').write_text(CURL)
        (tools / 'systemctl').write_text('#!/bin/bash\necho "$@" >>"$FAKE_FILES/systemctl.log"\n')
        for stub in ('curl', 'systemctl'):
            (tools / stub).chmod(0o755)
        (files / 'pir-apm-sidecar').write_bytes(binary)
        environment = {'PATH': '%s:%s' % (tools, os.environ['PATH']), 'FAKE_FILES': str(files),
                       'PIR_APM_SIDECAR_ROOT': str(root), 'PIR_APM_SIDECAR_SHA256': sha,
                       'PIR_APM_SIDECAR_URL': 'https://releases.example/pir-apm-sidecar',
                       'PIR_APM_SIDECAR_UNIT_TEMPLATE': base64.b64encode(monitoring.TEMPLATE.read_bytes()).decode()}
        result = subprocess.run(['bash', str(BOOT)], env=environment, capture_output=True, text=True, timeout=30)
        return result, root, files

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)

    def test_boot_installs_what_the_renderer_renders_and_enables_without_starting(self):
        binary = b'pinned sidecar'
        sha = hashlib.sha256(binary).hexdigest()
        result, root, files = self.boot(binary, sha)
        self.assertEqual(result.returncode, 0, result.stderr)
        release = '/opt/pir-apm-sidecar/releases/' + sha
        self.assertEqual((root / release.lstrip('/') / 'pir-apm-sidecar').read_bytes(), binary)
        settings = {'host_id': 'transparent-pir-recent-09', 'address': '10.10.0.31'}
        rendered = monitoring.sidecar(settings, release=release)
        self.assertEqual((root / monitoring.CONFIG.lstrip('/')).read_bytes(),
                         monitoring.config_bytes(monitoring.sidecar_document(settings)))
        self.assertEqual((root / 'etc/systemd/system/pir-apm-sidecar.service').read_text(), rendered.unit,
                         'the first deploy sees no unit drift')
        self.assertEqual((files / 'systemctl.log').read_text().split('\n')[:2],
                         ['daemon-reload', 'enable pir-apm-sidecar.service'])
        self.assertEqual(os.listdir(root / 'etc/pir-apm-sidecar/credentials'), [], 'no key material at boot')

    def test_a_binary_other_than_the_pinned_one_is_not_installed(self):
        result, root, files = self.boot(b'other bytes', hashlib.sha256(b'pinned').hexdigest())
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(list(root.glob('opt/pir-apm-sidecar/releases/*/pir-apm-sidecar')))
        self.assertFalse((files / 'systemctl.log').exists())

    def test_terraform_appends_this_script_and_template_to_every_managed_droplet(self):
        infra = ROOT / 'tools/infra/digitalocean'
        wiring = (infra / 'apm-sidecar/main.tf').read_text()
        self.assertIn('deploy/monitoring/pir-apm-sidecar-boot.sh', wiring)
        self.assertIn('deploy/monitoring/pir-apm-sidecar.service.in', wiring)
        for module in ('fleet/shared.tf', 'fleet/monitor.tf', 'fleet/receiver.tf', 'enhance/main.tf',
                       'status/main.tf', 'transparent/main.tf', 'transparent-tx-detail/main.tf',
                       'enhance/worker-pool/main.tf', 'transparent/worker-pool/main.tf'):
            text = (infra / module).read_text()
            with self.subTest(module=module):
                self.assertEqual(text.count('/apm-sidecar/user-data.tftpl"'),
                                 len(re.findall(r'(?m)^\s*user_data\s*=', text)),
                                 'every droplet user_data is wrapped once')


class HostTransport:
    """A controller's host access over one fake host: files by path, and a sidecar that starts."""

    def __init__(self, sidecar_digest):
        self.files, self.commands, self.digest = {}, [], sidecar_digest
        self.started = False

    def run(self, host, command, data=None, timeout=120):
        self.commands.append(command)
        if data is not None:
            self.files[shlex.split(command)[-1]] = data
        if 'systemctl restart' in command:
            self.started = True
        if command.startswith('curl'):
            config = json.loads(self.files[monitoring.CONFIG])
            return json.dumps({'ready': self.started, 'binary_sha256': self.digest,
                               'config_digest': monitoring.config_digest(config)})
        return ''

    def copy(self, host, sources, destination, **_):
        self.files[destination] = Path(sources[0]).read_bytes()


class ElasticEnrollment(unittest.TestCase):
    def test_credentials_are_private_during_the_write_and_after_rename(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / 'credentials'
            directory.mkdir(mode=0o700)
            target = directory / 'server.key'

            class LocalTransport:
                def run(inner, host, command, data=None, **_):
                    with subprocess.Popen(['/bin/sh', '-c', 'umask 022; ' + command],
                                          stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                          stderr=subprocess.PIPE) as process:
                        try:
                            process.stdin.write(data)
                            process.stdin.flush()
                            deadline = time.monotonic() + 3
                            while not list(directory.iterdir()) and time.monotonic() < deadline:
                                time.sleep(.01)
                            pending = list(directory.iterdir())
                            self.assertTrue(pending, 'inspect the file before the input stream closes')
                            self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
                            for path in pending:
                                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
                        finally:
                            process.stdin.close()
                            self.assertEqual(process.wait(timeout=3), 0)
                            process.stderr.close()

            monitoring_install._put(LocalTransport(), 'test', str(target), b'nonsecret fixture', 0o600)
            self.assertEqual(target.read_bytes(), b'nonsecret fixture')
            self.assertEqual(target.stat().st_mode & 0o777, 0o600)

    def test_actuator_does_not_accept_a_member_without_monitoring_configuration(self):
        import importlib.util
        from types import SimpleNamespace
        spec = importlib.util.spec_from_file_location('apm_test_actuator',
                                                     ROOT / 'tools/transparent/scripts/transparent-fleet-actuator.py')
        actuator = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(actuator)
        self.assertFalse(actuator.install_monitoring(SimpleNamespace(c={}), 'recent-09', '10.0.0.19'))

    def test_controller_install_issues_credentials_and_returns_the_enrollment(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            monitoring_certs.issue_ca(root / 'ca')
            environ = {monitoring_install.CA_PEM: (root / 'ca/ca.pem').read_text(),
                       monitoring_install.CA_KEY: (root / 'ca/ca.key').read_text()}
            credentials = monitoring_install.issue_from_environment('recent-09', environ)
            self.assertIn(b'BEGIN CERTIFICATE', credentials['server.pem'])
            with self.assertRaisesRegex(RuntimeError, 'runtime references'):
                monitoring_install.issue_from_environment('recent-09', {})
            binary = root / 'pir-apm-sidecar'
            binary.write_bytes(b'sidecar')
            import hashlib
            transport = HostTransport(hashlib.sha256(b'sidecar').hexdigest())
            settings = {'host_id': 'recent-09', 'address': '10.0.0.19'}
            record = monitoring_install.install_and_enroll(transport, '10.0.0.19', settings, binary=binary,
                                                           credentials=credentials, sleep=lambda _: None)
            self.assertEqual(record, monitoring.enrollment_record(settings))
            self.assertEqual(transport.files[monitoring_certs.SIDECAR_CREDENTIALS + '/server.key'],
                             credentials['server.key'])
            self.assertTrue(any('chmod 600' in c and 'server.key' in c for c in transport.commands))
            self.assertFalse(any(b'PRIVATE KEY' in c.encode() for c in transport.commands),
                             'key material travels on stdin, never in a command line')
            self.assertIn('--config /etc/pir-apm-sidecar/sidecar.json', transport.files[monitoring_install.UNIT_PATH].decode())
            transport.started = False
            transport.run = (lambda original: lambda host, command, data=None, timeout=120: (
                '{}' if command.startswith('curl') else original(host, command, data, timeout)))(transport.run)
            with self.assertRaisesRegex(RuntimeError, 'not ready'):
                monitoring_install.install_and_enroll(transport, '10.0.0.19', settings, binary=binary,
                                                      credentials=credentials, ready_timeout=0,
                                                      sleep=lambda _: None)

    def test_inventory_monitoring_view_lists_enrollments_and_retirements(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            'fleet_inventory', ROOT / 'tools/transparent/scripts/transparent-fleet-inventory.py')
        inventory = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(inventory)
        record = {'address': '10.0.0.19:9443', 'config_digest': 'sha256:' + '0' * 64, 'checks': ['memory']}
        view = inventory.monitoring_view({'members': [
            {'id': 'recent-09', 'intent': 'enrolled', 'monitoring': record},
            {'id': 'recent-10', 'intent': 'draining'},
            {'id': 'recent-03', 'intent': 'retired', 'monitoring': record}]})
        self.assertEqual(view, [{'id': 'recent-09', 'intent': 'enrolled', 'monitoring': record},
                                {'id': 'recent-10', 'intent': 'draining'},
                                {'id': 'recent-03', 'intent': 'retired'}])


def binaries():
    configured = os.environ.get('PIR_MONITORING_BIN')
    candidates = [Path(configured)] if configured else []
    target = os.environ.get('CARGO_TARGET_DIR')
    candidates += [Path(target) / 'release-fast'] if target else []
    candidates.append(ROOT / 'target/release-fast')
    for directory in candidates:
        if (directory / 'pir-apm-sidecar').is_file() and (directory / 'pir-apm').is_file():
            return directory
    return None


class LocalHTTP:
    """The executor's `http_get` for processes on this machine."""

    def http_get(self, host, url, timeout=5):
        import urllib.error
        import urllib.request
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                return response.status, response.read().decode()
        except urllib.error.HTTPError as error:
            return error.code, error.read().decode()


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class RenderedProcess:
    """Start a release binary as its rendered unit would, on this machine and without systemd.

    `Environment=` comes from the unit; path settings the unit fixes to host
    locations are remapped into the rehearsal root, and each `LoadCredential=`
    source is read from the files the credential installs place below that
    root, as systemd copies them into `$CREDENTIALS_DIRECTORY`.
    """

    def __init__(self, root, unit, remap):
        self.root, self.remap = Path(root), remap
        self.service = units.effective([unit])['Service']
        self.executable = units.split_exec(units.exec_start(units.effective([unit])))[1]

    def environment(self, name):
        credentials = self.root / ('credentials-' + name)
        credentials.mkdir()
        for entry in self.service.get('LoadCredential', []):
            credential, source = entry.split(':', 1)
            (credentials / credential).write_bytes((self.root / source.lstrip('/')).read_bytes())
        environment = dict(self.service.get('Environment', {}), **self.remap)
        return dict(environment, PATH=os.environ['PATH'], CREDENTIALS_DIRECTORY=str(credentials))

    def run(self, name, *arguments):
        return subprocess.run([self.executable, *arguments], env=self.environment(name), capture_output=True,
                              text=True, timeout=30)

    def start(self, name):
        return subprocess.Popen([self.executable], env=self.environment(name),
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def install_file(root, path, data):
    """Where `wallet-pir-monitoring.py install-credentials` or `install-observers` puts a file."""
    target = Path(root) / path.lstrip('/')
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)


def wait_for(condition, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = condition()
        if value:
            return value
        time.sleep(0.5)
    return condition()


@unittest.skipIf(binaries() is None, 'pir-apm and pir-apm-sidecar are not built')
class FreshRebuild(unittest.TestCase):
    """A complete monitoring rebuild from the repository alone, in a temporary root.

    Every process starts from its rendered role: the sidecar from its
    configuration, the central APM and the independent monitor from their
    rendered units, with credentials at the paths their installs use.
    Nothing reaches Slack: each rendered webhook is checked by a process that
    exits before any delivery.
    """

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.root = Path(self.dir.name)
        self.bin = binaries()
        self.processes = []
        self.addCleanup(self.stop)

    def stop(self):
        for process in self.processes:
            process.terminate()
            process.wait(timeout=10)

    def apm_unit(self, **settings):
        role = SERVICES['apm'].roles['central']
        rendered = monitoring_roles.apm(dict({'environment': {'PIR_APM_ALERT_MODE': 'shadow',
                                                              'PIR_APM_SCRAPE_URL': 'http://127.0.0.1:9'},
                                              'chain_cookie': None}, **settings),
                                        release=str(self.bin), template=role.template.read_text())
        return rendered.unit

    def test_wrong_collector_identity_never_passes_the_real_http_readiness_contract(self):
        monitoring_certs.issue_ca(self.root / 'ca')
        monitoring_certs.issue_leaf(self.root / 'ca', self.root / 'sidecar', 'rehearsal.pir-monitoring', 'server')
        monitoring_certs.issue_leaf(self.root / 'ca', self.root / 'wrong', 'another-host.pir-monitoring', 'client')
        settings = {'host_id': 'rehearsal', 'address': '127.0.0.1', 'port': free_port(), 'health_port': free_port()}
        config = self.root / 'sidecar.json'
        config.write_bytes(monitoring.config_bytes(monitoring.sidecar_document(settings)))
        self.processes.append(subprocess.Popen(
            [str(self.bin / 'pir-apm-sidecar'), '--config', str(config)],
            env={'PATH': os.environ['PATH'], 'CREDENTIALS_DIRECTORY': str(self.root / 'sidecar')},
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        observers = monitoring.observers(inventory(settings), mode='shadow')
        install_file(self.root, monitoring_roles.APM_OBSERVERS, json.dumps(observers).encode())
        for name in monitoring_certs.COLLECTOR_FILES:
            install_file(self.root, '%s/%s' % (monitoring_certs.COLLECTOR_CREDENTIALS, name),
                         (self.root / 'wrong' / name).read_bytes())
        port = free_port()
        remap = {'PIR_APM_STATE_PATH': str(self.root / 'incidents.sqlite'),
                 'PIR_APM_LISTEN': '127.0.0.1:%d' % port,
                 'PIR_APM_OBSERVERS': str(self.root / monitoring_roles.APM_OBSERVERS.lstrip('/'))}
        self.processes.append(RenderedProcess(self.root, self.apm_unit(slack=False), remap).start('wrong-apm'))
        url = 'http://127.0.0.1:%d/observers/ready' % port
        answer = {}

        def completed_round():
            nonlocal answer
            try:
                status, body = LocalHTTP().http_get('rehearsal', url)
                answer = json.loads(body)
                return status == 503 and answer.get('rounds', 0) > 0
            except OSError:
                return False
        self.assertTrue(wait_for(completed_round), 'the actual APM completes a failed authenticated poll')
        self.assertEqual((answer['ready'], answer['valid_hosts'], answer['expected_hosts']), (False, 0, 1))
        self.assertFalse(monitoring_certs.collector_ready(observers['revision'], url)(LocalHTTP(), 'rehearsal'))

    def test_rendered_sidecar_apm_and_monitor_converge_on_healthy_host_incidents(self):
        root = self.root
        monitoring_certs.issue_ca(root / 'ca')
        monitoring_certs.issue_leaf(root / 'ca', root / 'sidecar', 'rehearsal.pir-monitoring', 'server')
        monitoring_certs.issue_leaf(root / 'ca', root / 'collector', monitoring.COLLECTOR_IDENTITY, 'client')
        port, health = free_port(), free_port()
        settings = {'host_id': 'rehearsal', 'address': '127.0.0.1', 'port': port, 'health_port': health}
        rendered = monitoring.sidecar(settings, release=str(self.bin))
        config = root / 'sidecar.json'
        config.write_text(rendered.files[monitoring.CONFIG])
        check = json.loads(subprocess.run([str(self.bin / 'pir-apm-sidecar'), '--config', str(config), '--check'],
                                          capture_output=True, check=True, timeout=10).stdout)
        document = monitoring.sidecar_document(settings)
        self.assertEqual(check['config_digest'], monitoring.config_digest(document))
        self.assertEqual(check['checks'], monitoring.expected_checks(document))
        inventory_doc = {'hosts': {'rehearsal': {}}, 'services': {'apm-sidecar': {'roles': {'observer': [
            {'host': 'rehearsal', 'install': True, 'settings': settings}]}}}}
        observers = monitoring.observers(inventory_doc, mode='shadow')
        self.processes.append(subprocess.Popen(
            [str(self.bin / 'pir-apm-sidecar'), '--config', str(config)],
            env={'PATH': os.environ['PATH'], 'CREDENTIALS_DIRECTORY': str(root / 'sidecar')},
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))

        # The central APM host: observer inventory, collector identity and Slack webhook installed first.
        install_file(root, monitoring_roles.APM_OBSERVERS, json.dumps(observers).encode())
        for name in monitoring_certs.COLLECTOR_FILES:
            install_file(root, '%s/%s' % (monitoring_certs.COLLECTOR_CREDENTIALS, name), (root / 'collector' / name).read_bytes())
        install_file(root, '%s/%s' % (monitoring_roles.APM_ALERTING, monitoring_roles.SLACK),
                     b'https://hooks.slack.com/services/T0/B0/rehearsal\n')
        state, apm_port = root / 'incidents.sqlite', free_port()
        remap = {'PIR_APM_STATE_PATH': str(state), 'PIR_APM_LISTEN': '127.0.0.1:%d' % apm_port,
                 'PIR_APM_OBSERVERS': str(root / monitoring_roles.APM_OBSERVERS.lstrip('/'))}
        # The rendered webhook credential loads: queueing a test alert reads it and exits without sending.
        loaded = RenderedProcess(root, self.apm_unit(), dict(remap, PIR_APM_STATE_PATH=str(root / 'queue.sqlite')))
        result = loaded.run('queue', '--enqueue-test-alert')
        self.assertEqual(result.returncode, 0, result.stderr)
        install_file(root, '%s/%s' % (monitoring_roles.APM_ALERTING, monitoring_roles.SLACK), b'https://example.test/\n')
        refused = loaded.run('refused', '--enqueue-test-alert')
        self.assertNotEqual(refused.returncode, 0, 'a credential that is not a Slack webhook stops the APM')
        self.assertNotIn('example.test', refused.stdout + refused.stderr)
        # The serving APM runs without a webhook, so the rehearsal sends nothing.
        self.processes.append(RenderedProcess(root, self.apm_unit(slack=False), remap).start('apm'))
        keys = {}

        def converged():
            nonlocal keys
            if state.exists():
                with sqlite3.connect(state) as db:
                    keys = {k: json.loads(v) for k, v in db.execute(
                        "SELECT key, value FROM incidents WHERE key LIKE 'host:rehearsal:%'")}
            return keys.get('host:rehearsal:coverage', {}).get('condition', {}).get('firing') is False
        wait_for(converged)
        self.assertEqual(keys['host:rehearsal:coverage']['condition']['firing'], False)
        self.assertIn('host:rehearsal:check:certificate', keys)
        self.assertFalse(keys['host:rehearsal:check:certificate']['condition']['firing'])
        # This machine's own filesystems decide the rest; they need only be reported.
        self.assertIn('host:rehearsal:check:memory', keys)
        self.assertIn('host:rehearsal:check:filesystems', keys)
        # The deployment's readiness contracts against the real APM: the install's and the role's.
        url = 'http://127.0.0.1:%d/observers/ready' % apm_port
        self.assertTrue(monitoring_certs.collector_ready(observers['revision'], url)(LocalHTTP(), 'rehearsal'))
        self.assertFalse(monitoring_certs.collector_ready('sha256:other', url)(LocalHTTP(), 'rehearsal'))
        with self.assertRaises(ValueError):
            monitoring_certs.collector_ready(
                observers['revision'], 'http://127.0.0.1:%d/healthz' % apm_port)(LocalHTTP(), 'rehearsal')
        role = SERVICES['apm'].roles['central']
        for descriptor_url in (role.health, role.ready_url):
            status, _ = LocalHTTP().http_get('rehearsal', descriptor_url.replace('3002', str(apm_port)))
            self.assertEqual(status, 200, descriptor_url)

        # The independent monitor host, reading the rehearsal APM's status.
        if not (self.bin / 'pir-monitor').is_file():
            self.skipTest('pir-monitor is not built')
        oracle = json.dumps({'anchor_height': 1, 'anchor_hash': '00' * 32,
                             'records': [{'position': 0, 'record_hex': '00' * 653}]}).encode()
        install_file(root, '/etc/pir-monitor/oracle.json', oracle)
        role = SERVICES['monitor'].roles['checker']
        unit = monitoring_roles.monitor({'environment': {
            'PIR_MONITOR_ORIGIN': 'http://127.0.0.1:9',
            'PIR_MONITOR_APM_STATUS_URL': 'http://127.0.0.1:%d/monitor-status' % apm_port,
            'PIR_MONITOR_ORACLE': '/etc/pir-monitor/oracle.json',
            'PIR_MONITOR_ORACLE_SHA256': hashlib.sha256(oracle).hexdigest()}, 'slack': False},
            release=str(self.bin), template=role.template.read_text()).unit
        monitor_port = free_port()
        remap = {'PIR_MONITOR_STATE_PATH': str(root / 'monitor.sqlite'),
                 'PIR_MONITOR_LISTEN': '127.0.0.1:%d' % monitor_port,
                 'PIR_MONITOR_ORACLE': str(root / 'etc/pir-monitor/oracle.json')}
        self.processes.append(RenderedProcess(root, unit, remap).start('monitor'))
        health = role.health.replace('3003', str(monitor_port))

        def ready():
            try:
                status, body = LocalHTTP().http_get('monitor', health)
            except OSError:
                return False
            return status == 200 and bool(json.loads(body).get(role.ready['field']))
        self.assertTrue(wait_for(ready), 'the monitor meets its descriptor readiness contract')
        # Its webhook credential is read at start: a value that is not a Slack webhook stops it.
        install_file(root, '%s/%s' % (monitoring_roles.MONITOR_CREDENTIALS, monitoring_roles.SLACK), b'not a webhook')
        unit = monitoring_roles.monitor({'environment': {
            'PIR_MONITOR_ORIGIN': 'http://127.0.0.1:9', 'PIR_MONITOR_APM_STATUS_URL': 'http://127.0.0.1:9/x',
            'PIR_MONITOR_ORACLE': '/etc/pir-monitor/oracle.json',
            'PIR_MONITOR_ORACLE_SHA256': hashlib.sha256(oracle).hexdigest()}},
            release=str(self.bin), template=role.template.read_text()).unit
        refused = RenderedProcess(root, unit, dict(remap, PIR_MONITOR_LISTEN='127.0.0.1:%d' % free_port())).run(
            'monitor-refused')
        self.assertNotEqual(refused.returncode, 0)
        self.assertNotIn('not a webhook', refused.stdout + refused.stderr)


if __name__ == '__main__':
    unittest.main()

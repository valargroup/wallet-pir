"""Observer inventory, coverage, monitoring certificates and a fresh monitoring rebuild.

The rebuild rehearsal runs the real `pir-apm-sidecar` and `pir-apm` binaries
when `PIR_MONITORING_BIN` (or the release-fast target) holds them, with
ephemeral test certificates, rendered configuration and no SSH.
"""
import datetime
import json
import os
from pathlib import Path
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
from wallet_pir_ops import monitoring, monitoring_certs, monitoring_install  # noqa: E402
from wallet_pir_ops.deploy import remote, units  # noqa: E402

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
        fake.host('w1').units[monitoring_certs.SIDECAR_UNIT] = {}
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

    def test_collector_readiness_needs_the_installed_revision_not_plain_ok(self):
        fake = FakeFleet()
        answers = {'body': 'ok', 'status': 200}
        fake.http_get = lambda host, url, timeout=5: (answers['status'], answers['body'])
        ready = monitoring_certs.collector_ready('sha256:abc')
        with self.assertRaises(ValueError):
            ready(fake, 'c')  # `/healthz`'s plain ok is not this contract
        answers['body'] = json.dumps({'ready': True, 'revision': 'sha256:other'})
        self.assertFalse(ready(fake, 'c'))
        answers['body'] = json.dumps({'ready': True, 'revision': 'sha256:abc'})
        self.assertTrue(ready(fake, 'c'))
        answers['status'] = 503
        self.assertFalse(ready(fake, 'c'))


class VolumeBinding(unittest.TestCase):
    """A cache-volume activation binds its filesystem into the observer and coverage."""

    def test_activation_receipt_binds_the_volume_identity_and_changes_the_digest(self):
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
            self.assertEqual(cache_data.dispatch(args, fake)['monitoring'], 'bound')
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
            # Repeating is unchanged; another identity at the same path is refused.
            self.assertEqual(cache_data.dispatch(args, fake)['monitoring'], 'unchanged')
            with self.assertRaisesRegex(ValueError, 'another filesystem'):
                monitoring.bind_volume(path, 'txid-01', {'path': '/srv/txid-cache', 'uuid': 'ffff'})


class HostTransport:
    """A controller's host access over one fake host: files by path, and a sidecar that starts."""

    def __init__(self, sidecar_digest):
        self.files, self.commands, self.digest = {}, [], sidecar_digest
        self.started = False

    def run(self, host, command, data=None, timeout=120):
        self.commands.append(command)
        if data is not None:
            self.files[command.split('cat > ')[1].split('.next')[0]] = data
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


@unittest.skipIf(binaries() is None, 'pir-apm and pir-apm-sidecar are not built')
class FreshRebuild(unittest.TestCase):
    """A complete monitoring rebuild from the repository alone, in a temporary root."""

    def test_rendered_sidecar_and_apm_converge_on_healthy_host_incidents(self):
        bin_dir = binaries()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            monitoring_certs.issue_ca(root / 'ca')
            monitoring_certs.issue_leaf(root / 'ca', root / 'sidecar', 'rehearsal.pir-monitoring', 'server')
            monitoring_certs.issue_leaf(root / 'ca', root / 'collector', monitoring.COLLECTOR_IDENTITY, 'client')
            port, health = free_port(), free_port()
            settings = {'host_id': 'rehearsal', 'address': '127.0.0.1', 'port': port, 'health_port': health}
            rendered = monitoring.sidecar(settings, release=str(bin_dir))
            config = root / 'sidecar.json'
            config.write_text(rendered.files[monitoring.CONFIG])
            check = json.loads(subprocess.run([str(bin_dir / 'pir-apm-sidecar'), '--config', str(config), '--check'],
                                              capture_output=True, check=True, timeout=10).stdout)
            document = monitoring.sidecar_document(settings)
            self.assertEqual(check['config_digest'], monitoring.config_digest(document))
            self.assertEqual(check['checks'], monitoring.expected_checks(document))
            inventory_doc = {'hosts': {'rehearsal': {}}, 'services': {'apm-sidecar': {'roles': {'observer': [
                {'host': 'rehearsal', 'install': True, 'settings': settings}]}}}}
            (root / 'observers.json').write_text(json.dumps(monitoring.observers(inventory_doc, mode='shadow')))
            env = {'PATH': os.environ['PATH']}
            sidecar = subprocess.Popen([str(bin_dir / 'pir-apm-sidecar'), '--config', str(config)],
                                       env=dict(env, CREDENTIALS_DIRECTORY=str(root / 'sidecar')),
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            state = root / 'incidents.sqlite'
            apm_port = free_port()
            apm = subprocess.Popen([str(bin_dir / 'pir-apm')], env=dict(
                env, CREDENTIALS_DIRECTORY=str(root / 'collector'), PIR_APM_OBSERVERS=str(root / 'observers.json'),
                PIR_APM_STATE_PATH=str(state), PIR_APM_LISTEN='127.0.0.1:%d' % apm_port,
                PIR_APM_SCRAPE_URL='http://127.0.0.1:9'), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                deadline = time.monotonic() + 20
                keys = {}
                while time.monotonic() < deadline:
                    time.sleep(1)
                    if state.exists():
                        with sqlite3.connect(state) as db:
                            keys = {k: json.loads(v) for k, v in db.execute(
                                "SELECT key, value FROM incidents WHERE key LIKE 'host:rehearsal:%'")}
                    if keys.get('host:rehearsal:coverage', {}).get('condition', {}).get('firing') is False:
                        break
                self.assertEqual(keys['host:rehearsal:coverage']['condition']['firing'], False)
                self.assertIn('host:rehearsal:check:certificate', keys)
                self.assertFalse(keys['host:rehearsal:check:certificate']['condition']['firing'])
                # This machine's own filesystems decide the rest; they need only be reported.
                self.assertIn('host:rehearsal:check:memory', keys)
                self.assertIn('host:rehearsal:check:filesystems', keys)
                # The deployment's readiness contract against the real APM.
                revision = json.loads((root / 'observers.json').read_text())['revision']
                url = 'http://127.0.0.1:%d/observers/ready' % apm_port
                self.assertTrue(monitoring_certs.collector_ready(revision, url)(LocalHTTP(), 'rehearsal'))
                self.assertFalse(monitoring_certs.collector_ready('sha256:other', url)(LocalHTTP(), 'rehearsal'))
                with self.assertRaises(ValueError):
                    monitoring_certs.collector_ready(
                        revision, 'http://127.0.0.1:%d/healthz' % apm_port)(LocalHTTP(), 'rehearsal')
            finally:
                for process in (apm, sidecar):
                    process.terminate()
                    process.wait(timeout=10)


if __name__ == '__main__':
    unittest.main()

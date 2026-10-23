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
from wallet_pir_ops import monitoring, monitoring_certs  # noqa: E402
from wallet_pir_ops.deploy import units  # noqa: E402

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

    def test_rotation_installs_under_the_lock_and_restores_on_failed_start(self):
        fake = FakeFleet()
        fake.host('w1')
        fake.endpoints[('w1', monitoring_certs.SIDECAR_HEALTH)] = monitoring_certs.SIDECAR_UNIT
        fake.host('w1').units[monitoring_certs.SIDECAR_UNIT] = {}
        fake._reload = lambda host: None
        fake._start = lambda host, unit: fake.host(host).units[unit].update(active='active', exe='new')
        old = {name: b'old ' + name.encode() for name in monitoring_certs.SIDECAR_FILES}
        for name, data in old.items():
            fake.put('w1', '%s/%s' % (monitoring_certs.SIDECAR_CREDENTIALS, name), data)
        new = dict(old, **{'server.pem': b'new cert', 'server.key': b'new key'})
        lock = lambda: fake.hold_lock('w1', '/run/lock/wallet-pir-production.lock')
        self.assertEqual(monitoring_certs.install(fake, lock(), 'w1', new), ['server.key', 'server.pem'])
        self.assertEqual(fake.read('w1', monitoring_certs.SIDECAR_CREDENTIALS + '/server.key'), 'new key')
        self.assertEqual(monitoring_certs.install(fake, lock(), 'w1', new), [], 'idempotent')
        fake.unhealthy.add(('w1', 'new'))
        newer = dict(new, **{'server.key': b'newer key'})
        with self.assertRaisesRegex(RuntimeError, 'previous files restored'):
            monitoring_certs.install(fake, lock(), 'w1', newer, ready_timeout=0)
        self.assertEqual(fake.read('w1', monitoring_certs.SIDECAR_CREDENTIALS + '/server.key'), 'new key')
        with fake.hold_lock('w1', '/run/lock/wallet-pir-production.lock'):
            with self.assertRaises(Exception):
                monitoring_certs.install(fake, lock(), 'w1', newer)


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
            apm = subprocess.Popen([str(bin_dir / 'pir-apm')], env=dict(
                env, CREDENTIALS_DIRECTORY=str(root / 'collector'), PIR_APM_OBSERVERS=str(root / 'observers.json'),
                PIR_APM_STATE_PATH=str(state), PIR_APM_LISTEN='127.0.0.1:%d' % free_port(),
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
            finally:
                for process in (apm, sidecar):
                    process.terminate()
                    process.wait(timeout=10)


if __name__ == '__main__':
    unittest.main()

"""APM sidecars: one rendered observer per managed host, installed fresh, updated and rolled back.

Uses the repository's descriptor, renderer and template; only hosts are fake.
"""
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'tools/lib'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from fake_fleet import FakeFleet, sha256  # noqa: E402
from wallet_pir_ops import monitoring  # noqa: E402
from wallet_pir_ops.deploy import descriptors, units  # noqa: E402
from wallet_pir_ops.deploy.engine import Deployer, DeployError  # noqa: E402

SERVICES = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')
BINARY = b'pir-apm-sidecar'
SHA = sha256(BINARY)
UNIT = 'pir-apm-sidecar.service'
HEALTH = 'http://127.0.0.1:9444/healthz'
HOSTS = {
    'coordinator': {'host_id': 'coordinator', 'address': '10.0.0.2', 'units': ['pir-apm.service'],
                    'required_volumes': [{'path': p, 'uuid': '0000-000%d' % i}
                                         for i, p in enumerate(monitoring.COORDINATOR_VOLUMES)]},
    'recent-07': {'host_id': 'recent-07', 'address': '10.0.0.17'},
}


class SidecarDeploys(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        root = Path(self.dir.name)
        self.state = root / 'state'
        self.binary = root / 'pir-apm-sidecar'
        self.binary.write_bytes(BINARY)
        self.fake = FakeFleet()
        for host in HOSTS:
            self.fake.host(host)
            self.fake.endpoints[(host, HEALTH)] = UNIT
        self.fake.product_check = lambda host, argv: (
            (0, 'ok') if json.loads(self.fake.read(host, argv[argv.index('--config') + 1]))['host_id'] == host
            else (1, 'wrong host'))
        self.inventory = root / 'inventory.json'
        self.write_inventory(HOSTS)

    def tearDown(self):
        self.dir.cleanup()

    def write_inventory(self, hosts):
        self.inventory.write_text(json.dumps({
            'lock': {'type': 'remote', 'host': 'coordinator'}, 'ssh': {'mode': 'config'},
            'hosts': {name: {} for name in HOSTS},
            'services': {'apm-sidecar': {'roles': {'observer': [
                {'host': name, 'install': True, 'settings': settings} for name, settings in hosts.items()]}}},
        }))

    def deployer(self):
        now = [0.0]
        def sleep(seconds):
            now[0] += seconds
        return Deployer(SERVICES['apm-sidecar'], descriptors.load_inventory(self.inventory), self.fake, self.state,
                        lock=lambda: self.fake.hold_lock('coordinator', descriptors.LOCK_PATH),
                        sleep=sleep, clock=lambda: now[0], out=lambda _: None)

    def test_every_host_gets_the_same_rendered_observer_and_rollback_returns_to_absence(self):
        deployer = self.deployer()
        deployer.capture_baseline()
        journal = deployer.deploy(SHA, str(self.binary), skip_exact_check=True)
        self.assertEqual(journal.status, 'committed')
        for host, settings in HOSTS.items():
            state = self.fake.probe_unit(host, UNIT)
            self.assertEqual((state['active_state'], state['exe_sha256']), ('active', SHA))
            installed = self.fake.read(host, monitoring.CONFIG)
            document = monitoring.sidecar_document(settings)
            self.assertEqual(installed.encode(), monitoring.config_bytes(document),
                             'the APM digest is of exactly the installed bytes')
            unit = units.effective([self.fake.read(host, '/etc/systemd/system/' + UNIT)])['Service']
            self.assertNotIn('EnvironmentFile', unit, 'no deployment secret bundle')
            self.assertTrue(all('slack' not in v.lower() for v in unit.get('LoadCredential', [])))
        # Idempotent: nothing changes the second time.
        self.assertIsNone(self.deployer().deploy(SHA, str(self.binary), skip_exact_check=True))
        self.deployer().rollback(journal.id)
        for host in HOSTS:
            self.assertEqual(self.fake.probe_unit(host, UNIT)['load_state'], 'not-found')
            self.assertIsNone(self.fake.read(host, monitoring.CONFIG))

    def test_a_volume_added_after_activation_updates_the_configuration_and_its_digest(self):
        deployer = self.deployer()
        deployer.capture_baseline()
        deployer.deploy(SHA, str(self.binary), skip_exact_check=True)
        before = monitoring.config_digest(monitoring.sidecar_document(HOSTS['recent-07']))
        grown = dict(HOSTS, **{'recent-07': dict(HOSTS['recent-07'], required_volumes=[
            {'path': '/srv/query-cache', 'uuid': 'aaaa-bbbb'}])})
        self.write_inventory(grown)
        journal = self.deployer().deploy(SHA, str(self.binary), skip_exact_check=True)
        self.assertEqual(journal.status, 'committed')
        installed = json.loads(self.fake.read('recent-07', monitoring.CONFIG))
        self.assertEqual(installed['required_volumes'][0]['path'], '/srv/query-cache')
        self.assertNotEqual(before, monitoring.config_digest(installed))

    def test_a_sidecar_that_never_becomes_ready_is_rolled_back(self):
        self.fake.unhealthy.add(('recent-07', SHA))
        deployer = self.deployer()
        deployer.capture_baseline()
        with self.assertRaises(DeployError):
            deployer.deploy(SHA, str(self.binary), skip_exact_check=True)
        for host in HOSTS:
            self.assertNotEqual(self.fake.probe_unit(host, UNIT)['active_state'], 'active')
            self.assertIsNone(self.fake.read(host, monitoring.CONFIG))


if __name__ == '__main__':
    unittest.main()

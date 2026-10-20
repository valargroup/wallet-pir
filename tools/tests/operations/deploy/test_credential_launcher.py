"""Change the execed sidecar, preserving the pinned credential loader and state."""
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / 'tools/lib'))
from wallet_pir_ops.deploy import descriptors, units
from wallet_pir_ops.deploy.engine import Deployer, DeployError
from wallet_pir_ops.deploy.transaction import Journal
from fake_fleet import FakeFleet, sha256

LAUNCHER = ('/usr/bin/python3', '/opt/credential-exec.py')


class LaunchedFleet(FakeFleet):
    def _start(self, host, unit):
        super()._start(host, unit)
        state = self.host(host).units[unit]
        if state['active'] == 'active':
            _, executable, _ = units.split_exec(state['exec_start'], LAUNCHER)
            state['exe'] = self.sha256(host, executable)


class LauncherTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.binary = self.root / 'pir-apm'
        self.binary.write_bytes(b'new sidecar')
        self.ex = LaunchedFleet()
        self.ex.install('coordinator', 'pir-apm.service', '[Service]\nEnvironmentFile=/etc/apm.env\nExecStart=' +
                        ' '.join(LAUNCHER) + ' /opt/old/pir-apm --retain-state\n',
                        files={LAUNCHER[0]: b'python', LAUNCHER[1]: b'credential loader',
                               '/opt/old/pir-apm': b'old sidecar', '/etc/apm.env': b''})
        role = descriptors.Role('server', 'pir-apm.service', 'exec-drop-in', 'http://127.0.0.1:3002/healthz',
                                launcher=tuple((p, self.ex.sha256('coordinator', p)) for p in LAUNCHER))
        service = descriptors.Service('apm', '/opt/pir-apm', 'pir-apm', ('server',), {'server': role})
        inventory = descriptors.Inventory({'coordinator': {'address': '127.0.0.1'}}, {},
                                           {'type': 'remote', 'host': 'coordinator'},
                                           {'apm': {'roles': {'server': [{'host': 'coordinator'}]},
                                                    'exact_check': {'host': 'coordinator', 'argv': ['/probe']}}})
        self.ex.endpoints[('coordinator', role.health)] = role.unit
        self.deployer = Deployer(service, inventory, self.ex, self.root, out=lambda _: None)
        self.deployer.capture_baseline()

    def test_deploy_and_rollback_keep_loader_arguments_and_credentials(self):
        journal = self.deployer.deploy(sha256(self.binary.read_bytes()), self.binary)
        start = units.exec_start(units.effective(t for _, t in self.ex._disk('coordinator', 'pir-apm.service')))
        self.assertTrue(start.startswith(' '.join(LAUNCHER) + ' /opt/pir-apm/releases/'))
        self.assertTrue(start.endswith('--retain-state'))
        self.deployer.rollback()
        self.assertEqual(self.ex.probe_unit('coordinator', 'pir-apm.service')['exe_sha256'], sha256(b'old sidecar'))
        self.assertEqual(Journal.load(self.root, 'apm', journal.id).status, 'rolled-back')

    def test_changed_loader_is_refused_before_restart(self):
        self.ex.put('coordinator', LAUNCHER[1], b'changed loader')
        with self.assertRaisesRegex(DeployError, 'credential launcher changed'):
            self.deployer.deploy(sha256(self.binary.read_bytes()), self.binary)

    def test_loader_changed_during_staging_is_refused_before_restart(self):
        def change_loader(host, operation, detail):
            if operation == 'upload':
                self.ex.put(host, LAUNCHER[1], b'changed loader')
        self.ex.observer = change_loader
        with self.assertRaisesRegex(DeployError, 'credential launcher changed before restart'):
            self.deployer.deploy(sha256(self.binary.read_bytes()), self.binary)
        self.assertEqual(self.ex.probe_unit('coordinator', 'pir-apm.service')['exe_sha256'], sha256(b'old sidecar'))

    def test_changed_loader_blocks_rollback(self):
        self.deployer.deploy(sha256(self.binary.read_bytes()), self.binary)
        self.ex.put('coordinator', LAUNCHER[1], b'changed loader')
        with self.assertRaisesRegex(DeployError, 'credential launcher changed'):
            self.deployer.rollback()

    def test_other_python_commands_are_not_treated_as_this_launcher(self):
        with self.assertRaises(units.UnitError):
            units.split_exec('/usr/bin/python3 /opt/other.py /opt/old/pir-apm', LAUNCHER)


if __name__ == '__main__':
    unittest.main()

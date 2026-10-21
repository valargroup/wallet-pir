"""The independent watchdog stops only its captured forward process."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'tools/lib'))
from wallet_pir_ops.deploy import cutover_runner
from wallet_pir_ops.deploy.cutover_guard import Decision
from wallet_pir_ops.deploy.engine import DeployError


class Target:
    identifier = 'publisher-fixture'

    def __init__(self, root):
        self.root = root

    def latest(self):
        return {'id': self.identifier, 'request': {'fixture': True}, 'phase': 'installing'}

    def rollback(self):
        (self.root / 'restored').write_text('locked rollback invoked')


class WatchdogTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
        self.addCleanup(self.close_child)
        self.now = 1000
        self.runner = cutover_runner.Runner.__new__(cutover_runner.Runner)
        self.runner.root, self.runner.target = root, Target(root)
        self.runner.request = {'fixture': True}
        self.runner.name = 'wallet-pir-cutover-publisher-fixture'
        self.runner.verify_units = lambda: None
        self.runner.decision = Decision(root, 'publisher-fixture', clock=lambda: self.now)
        self.runner.decision.create('a' * 64)
        self.runner.decision.started('owned-invocation', self.child.pid)
        self.runner.decision.arm()
        self.now += 3600

    def close_child(self):
        if self.child.poll() is None:
            self.child.terminate()
        self.child.wait(timeout=5)

    def observed(self, _identifier):
        return {'MainPID': str(self.child.pid) if self.child.poll() is None else '0',
                'InvocationID': 'owned-invocation'}

    def test_deadline_stops_owned_process_and_executes_recovery(self):
        calls = []

        def systemctl(*arguments):
            calls.append(arguments)
            self.close_child()

        with patch.object(cutover_runner, 'observe', self.observed), patch.object(cutover_runner, 'systemctl', systemctl):
            result = self.runner.watchdog()
        self.assertEqual(calls, [('stop', self.runner.name + '.service')])
        self.assertEqual(result['phase'], 'restored')
        self.assertTrue((self.runner.root / 'restored').is_file())
        self.assertEqual(json.loads(self.runner.decision.path.read_text())['phase'], 'restored')

    def test_changed_invocation_is_never_terminated(self):
        def other(identifier):
            return {**self.observed(identifier), 'InvocationID': 'another-owner'}

        with patch.object(cutover_runner, 'observe', other), patch.object(cutover_runner, 'systemctl') as systemctl:
            with self.assertRaisesRegex(DeployError, 'another owner'):
                self.runner.watchdog()
        systemctl.assert_not_called()
        self.assertIsNone(self.child.poll())
        self.assertFalse((self.runner.root / 'restored').exists())

    def test_lost_forward_process_starts_recovery_without_waiting_an_hour(self):
        self.now = 1001
        self.close_child()
        with patch.object(cutover_runner, 'observe', self.observed), patch.object(cutover_runner, 'systemctl') as systemctl:
            self.assertEqual(self.runner.watchdog()['phase'], 'restored')
        systemctl.assert_not_called()
        self.assertTrue((self.runner.root / 'restored').is_file())


if __name__ == '__main__':
    unittest.main()

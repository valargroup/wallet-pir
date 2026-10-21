"""The independent watchdog stops only its captured forward process."""
import json
from pathlib import Path
import stat
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
from wallet_pir_ops import durable


class DurableInputTests(unittest.TestCase):
    def test_unit_replacement_preserves_text_and_explicit_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'owned.service'
            path.write_text('old')
            text = '[Service]\nExecStart="/path with spaces/run"\n'
            cutover_runner.write_text(path, text, 0o644)
            self.assertEqual(path.read_text(), text)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o644)
            self.assertEqual(list(Path(directory).iterdir()), [path])

    def test_json_encoding_stays_identical_and_nonfinite_values_refuse(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'request.json'
            durable.atomic_json(path, {'z': '\u00e9', 'a': [True, None]}, mode=0o600)
            expected = b'{\n  "a": [\n    true,\n    null\n  ],\n  "z": "\\u00e9"\n}\n'
            self.assertEqual(path.read_bytes(), expected)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            with self.assertRaises(ValueError):
                durable.atomic_json(path, {'invalid': float('nan')})
            self.assertEqual(path.read_bytes(), expected)

    def test_failed_sync_propagates_and_cleans_the_staging_file(self):
        for directory_failure in (False, True):
            with self.subTest(directory_failure=directory_failure), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'request.json'
                path.write_text('old')
                real_sync = durable.os.fsync
                calls = 0

                def sync(descriptor):
                    nonlocal calls
                    calls += 1
                    if calls == (2 if directory_failure else 1):
                        raise OSError('sync failed')
                    real_sync(descriptor)

                with patch.object(durable.os, 'fsync', side_effect=sync):
                    with self.assertRaisesRegex(OSError, 'sync failed'):
                        cutover_runner.write_text(path, 'new', 0o600)
                self.assertEqual(path.read_text(), 'new' if directory_failure else 'old')
                self.assertEqual(list(Path(directory).iterdir()), [path])


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

    def test_failed_service_before_runner_start_does_not_leave_a_staging_transaction(self):
        self.close_child()
        state = json.loads(self.runner.decision.path.read_text())
        state.update(phase='staging', runner_started=False, interrupted_unix=None, deadline_unix=None)
        self.runner.decision.path.write_text(json.dumps(state))
        with patch.object(cutover_runner, 'observe', return_value={'MainPID': '0', 'ActiveState': 'failed'}), \
                patch.object(cutover_runner, 'systemctl') as systemctl:
            self.assertEqual(self.runner.watchdog()['phase'], 'restored')
        systemctl.assert_not_called()
        self.assertIsNone(json.loads(self.runner.decision.path.read_text())['interrupted_unix'])


if __name__ == '__main__':
    unittest.main()

"""Operator jobs use launchd identity and the existing recovery arbitration."""
import json
import os
from pathlib import Path
import plistlib
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / 'tools/lib'))
from wallet_pir_ops.deploy import cutover_operator as operator
from wallet_pir_ops.deploy.cutover_guard import Decision
from wallet_pir_ops.deploy.engine import DeployError


class OperatorTests(unittest.TestCase):
    def test_launchd_identity_comes_from_the_running_job(self):
        text = 'job {\n pid = 123\n environment = {\n WALLET_PIR_CUTOVER_INVOCATION => owned\n }\n}'
        with patch.object(operator, 'launchctl', return_value=subprocess.CompletedProcess([], 0, text, '')):
            self.assertEqual(operator.job_state('owned'),
                             {'MainPID': '123', 'InvocationID': 'owned', 'ActiveState': 'active'})

    def test_changed_invocation_cannot_be_stopped(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = operator.OperatorRunner.__new__(operator.OperatorRunner)
            runner.root = Path(directory)
            runner.name = 'wallet-pir-cutover-publisher-test'
            runner.decision = Decision(runner.root, 'publisher-test')
            runner.decision.create('a' * 64)
            runner.decision.started('owned', 123)
            with patch.object(runner, 'observe_forward', return_value={
                    'MainPID': '123', 'InvocationID': 'foreign'}), patch.object(operator, 'launchctl') as ctl:
                with self.assertRaisesRegex(DeployError, 'another owner'):
                    runner.stop_forward()
                ctl.assert_not_called()

    def test_private_inputs_reject_symlinks_and_public_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'input'
            path.write_text('private')
            path.chmod(0o600)
            operator.private(path)
            link = path.with_name('link')
            link.symlink_to(path)
            with self.assertRaises(DeployError):
                operator.private(link)
            path.chmod(0o644)
            with self.assertRaises(DeployError):
                operator.private(path)

    @unittest.skipUnless(sys.platform == 'darwin', 'real launchd rehearsal requires macOS')
    def test_real_launchagent_reports_identity_and_stops_only_its_job(self):
        label = 'wallet-pir-cutover-publisher-rehearsal-' + uuid.uuid4().hex
        invocation = uuid.uuid4().hex
        path = Path.home() / 'Library/LaunchAgents' / (label + '.plist')
        path.parent.mkdir(parents=True, exist_ok=True)
        job = {'Label': label, 'ProgramArguments': [sys.executable, '-c', 'import time; time.sleep(20)'],
               'EnvironmentVariables': {'WALLET_PIR_CUTOVER_INVOCATION': invocation}, 'RunAtLoad': True}
        operator.write_text(path, plistlib.dumps(job).decode(), 0o600)
        loaded = False
        try:
            operator.launchctl('bootstrap', operator.domain(), str(path))
            loaded = True
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                state = operator.job_state(label)
                if state['MainPID'] != '0':
                    break
                time.sleep(.1)
            self.assertNotEqual(state['MainPID'], '0')
            self.assertEqual(state['InvocationID'], invocation)
            operator.launchctl('kill', 'SIGTERM', operator.domain() + '/' + label)
            deadline = time.monotonic() + 5
            while operator.job_state(label)['MainPID'] != '0' and time.monotonic() < deadline:
                time.sleep(.1)
            self.assertEqual(operator.job_state(label)['MainPID'], '0')
        finally:
            if loaded:
                operator.launchctl('bootout', operator.domain() + '/' + label, required=False)
            path.unlink()


if __name__ == '__main__':
    unittest.main()

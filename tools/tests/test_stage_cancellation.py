"""Cancellation releases real descendant listeners without retrying the check."""
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'shared/dev'))
from target_lease import inherited_fds


@unittest.skipUnless(os.name == 'posix', 'process-group cancellation is POSIX-specific')
class CancellationTests(unittest.TestCase):
    def test_fifty_cancellations_leave_no_descendant_listener(self):
        leaf = ("import os,signal,socket; s=socket.socket(); s.bind(('127.0.0.1',0)); s.listen(); "
                "print('READY:'+str(s.getsockname()[1])+':'+str(os.getpid()),flush=True); signal.pause()")
        nested = [sys.executable, str(ROOT / 'tools/ci/stage.py'), 'nested', '--', sys.executable, '-c', leaf]
        controller = f'import subprocess; subprocess.run({nested!r},check=True)'
        for trial in range(50):
            with self.subTest(trial=trial):
                command = [sys.executable, str(ROOT / 'tools/ci/stage.py'), 'cancellation', '--',
                           sys.executable, '-c', controller]
                env = dict(os.environ)
                env.pop('WALLET_PIR_COMMAND_GROUP', None)
                process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, **inherited_fds())
                child = None
                try:
                    for line in process.stdout:
                        if line.startswith('READY:'):
                            _, port, child = line.strip().split(':')
                            child = int(child)
                            break
                    else:
                        self.fail('descendant exited before listening: '+process.stderr.read())
                    process.send_signal(signal.SIGTERM)
                    _, errors = process.communicate(timeout=5)
                    self.assertEqual(process.returncode, 143, errors)
                    with socket.socket() as probe:
                        probe.bind(('127.0.0.1', int(port)))
                    child = None
                finally:
                    if child is not None:
                        try:
                            os.kill(child, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                    if process.poll() is None:
                        process.kill()
                    process.communicate(timeout=5)

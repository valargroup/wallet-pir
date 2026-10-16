"""The test runner owns and removes public fixture caches on every exit path."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class RunnerTests(unittest.TestCase):
    def test_success_failure_and_cancellation_remove_the_owned_fixture_directory(self):
        for mode in ['success', 'failure', 'cancel']:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                report = root / 'cache-path'
                cargo = root / 'cargo'
                cargo.write_text('#!'+sys.executable+'\n'+
                    'import os,pathlib,signal,sys\n'
                    'cache=pathlib.Path(os.environ["WALLET_PIR_TEST_RUNTIME_CACHE"])\n'
                    'pathlib.Path(os.environ["FIXTURE_REPORT"]).write_text(str(cache))\n'
                    '(cache/"fixture").write_text("public fixture")\n'
                    'if os.environ["FIXTURE_MODE"]=="cancel":\n'
                    ' print("CACHE_READY",flush=True)\n'
                    ' signal.pause()\n'
                    'sys.exit(7 if os.environ["FIXTURE_MODE"]=="failure" else 0)\n')
                cargo.chmod(0o755)
                env = {k: v for k, v in os.environ.items() if not k.startswith('WALLET_PIR_')}
                env.update(PATH=str(root)+os.pathsep+env['PATH'], FIXTURE_MODE=mode,
                           FIXTURE_REPORT=str(report), GITHUB_ACTIONS='false', CARGO_TARGET_DIR=str(root/'target'))
                process = subprocess.Popen(['bash', str(ROOT/'tools/ci/full-test.sh'), '--lib'], cwd=ROOT,
                                           env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                           text=True, start_new_session=True)
                try:
                    if mode == 'cancel':
                        for line in process.stdout:
                            if line.strip() == 'CACHE_READY':
                                break
                        else:
                            self.fail('fixture did not become ready: '+process.stderr.read())
                        os.killpg(process.pid, signal.SIGTERM)
                    _, errors = process.communicate(timeout=10)
                    self.assertEqual(process.returncode == 0, mode == 'success', errors)
                    self.assertTrue(report.is_file(), errors)
                    self.assertFalse(Path(report.read_text()).exists(), 'owned fixture directory leaked')
                finally:
                    if process.poll() is None:
                        os.killpg(process.pid, signal.SIGKILL)
                    process.communicate(timeout=5)


if __name__ == '__main__':
    unittest.main()

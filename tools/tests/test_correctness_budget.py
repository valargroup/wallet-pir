"""A slow or missing test cannot produce a successful timing receipt."""
import contextlib
import io
import json
import os
import subprocess
from pathlib import Path
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/ci'))
from test_budget import execute, case_seconds


class CaseBudgetTests(unittest.TestCase):
    def run_fixture(self, source, *, seconds=2, enforce=True):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            program = root / 'test-program'
            program.write_text('#!' + sys.executable + '\nimport sys,time\n' + source)
            program.chmod(0o755)
            report = root / 'cases.jsonl'
            with contextlib.redirect_stdout(io.StringIO()):
                count = execute([str(program)], cwd=root, env=os.environ.copy(),
                                seconds=seconds, report=report, enforce=enforce)
            return count, [json.loads(line) for line in report.read_text().splitlines()]

    def test_nonpositive_or_nonfinite_deadlines_are_refused(self):
        for value in ('0', '-1', 'nan', 'inf'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                case_seconds(value)

    def test_environment_deadline_stops_a_stalled_case(self):
        from unittest.mock import patch
        with patch.dict(os.environ, WALLET_PIR_CASE_SECONDS='.05'):
            with self.assertRaisesRegex(RuntimeError, 'timeout=True'):
                self.run_fixture(
                    'if "--list" in sys.argv: print("stalled: test")\n'
                    'else:\n print("test stalled ... ",end="",flush=True)\n time.sleep(10)\n',
                    seconds=None)

    def test_python_deadline_includes_setup_and_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            program = root / 'test_case.py'
            program.write_text(
                'import time,unittest\n'
                'from python_timing import install\ninstall()\n'
                'class Case(unittest.TestCase):\n'
                ' def setUp(self):\n  time.sleep(.03)\n  self.addCleanup(time.sleep,.03)\n'
                ' def test_answer(self): self.assertEqual(2+2,4)\n'
                'unittest.main()\n')
            report = root / 'cases.jsonl'
            env = dict(os.environ, PYTHONPATH=str(ROOT / 'tools/ci'),
                       WALLET_PIR_CASE_SECONDS='.05', WALLET_PIR_ENFORCE_BUDGET='1',
                       WALLET_PIR_CASE_LOG=str(report))
            result = subprocess.run([sys.executable, str(program)], env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('exceeded 0.05 seconds', result.stderr)
            self.assertGreater(json.loads(report.read_text())['seconds'], .05)

    def test_serial_cases_keep_one_process_and_account_for_panics_and_ignored_cases(self):
        count, cases = self.run_fixture(
            'if "--list" in sys.argv:\n'
            ' print("first: test\\npanic_case: test\\nignored_case: test")\n'
            'else:\n'
            ' print("test first ... ",end="",flush=True)\n'
            ' time.sleep(.02)\n print("ok",flush=True)\n'
            ' print("test panic_case - should panic ... ok",flush=True)\n'
            ' print("test ignored_case ... ignored, explicit hardware qualification",flush=True)\n')
        self.assertEqual(count, 3)
        self.assertGreater(cases[0]['seconds'], .01)
        self.assertEqual([case['status'] for case in cases], ['ok', 'ok', 'ignored'])

    def test_stalled_case_is_killed_before_its_final_newline(self):
        began = time.monotonic()
        with self.assertRaisesRegex(RuntimeError, 'timeout=True'):
            self.run_fixture(
                'if "--list" in sys.argv: print("stalled: test")\n'
                'else:\n print("test stalled ... ",end="",flush=True)\n time.sleep(10)\n',
                seconds=.15)
        # Include interpreter startup and discovery under concurrent compilation.
        # The ten-second body must still be interrupted well before completion.
        self.assertLess(time.monotonic() - began, 5)

    def test_a_successful_process_cannot_omit_a_discovered_case(self):
        with self.assertRaisesRegex(RuntimeError, 'missing=.*second'):
            self.run_fixture(
                'if "--list" in sys.argv: print("first: test\\nsecond: test")\n'
                'else: print("test first ... ok",flush=True)\n')

    def test_diagnostic_mode_records_an_overrun_without_skipping_the_case(self):
        count, cases = self.run_fixture(
            'if "--list" in sys.argv: print("slow: test")\n'
            'else:\n print("test slow ... ",end="",flush=True)\n time.sleep(.05)\n print("ok",flush=True)\n',
            seconds=.01, enforce=False)
        self.assertEqual(count, 1)
        self.assertGreater(cases[0]['seconds'], .01)


class CorrectnessSelectionTests(unittest.TestCase):
    def test_complete_native_selection_retains_reference_exporter_docs_and_lint(self):
        import correctness
        import fast_build
        from unittest.mock import patch
        metadata = dict(packages=[dict(id='one', name='one'), dict(id='two', name='two')])
        environment = dict(WALLET_PIR_CASE_LOG='/tmp/outer-cases.jsonl',
                           WALLET_PIR_ENFORCE_BUDGET='1')
        calls = []
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(correctness.subprocess, 'check_output',
                             side_effect=[json.dumps(metadata).encode(), json.dumps(metadata).encode()]), \
                patch.object(correctness, 'run', side_effect=lambda command, **kw: calls.append(command) or []), \
                patch.object(fast_build, 'execute') as native:
            correctness.rust_checks(environment, Path(directory), ['--offline'])
        native.assert_called_once_with(
            ['one', 'two'], offline=True, jobs=3,
            additional_features=correctness.FEATURES.split(','),
            additional_integrations={('status-query-server', 'status_wallet'),
                                     ('status-query-server', 'status_wallet_http')},
            environment=environment,
            case_log=environment['WALLET_PIR_CASE_LOG'], enforce_budget=True)
        self.assertFalse(any('--workspace' in command and '--no-run' in command for command in calls))
        self.assertTrue(any('-p' in command and 'enhance-publisher' in command and 'ci_smoke' in command
                            for command in calls))
        self.assertTrue(any('enhance-query-server' in command and 'routing_contract' in command
                            and 'v7_contract' in command for command in calls))
        # Native-gated wallet integrations run with the native build, not as
        # empty reference executables.
        self.assertIn('status-query-server/native-reinspiring', correctness.FEATURES.split(','))
        [status] = [command for command in calls if 'status-query-server' in command]
        self.assertIn('--lib', status)
        self.assertFalse({'--tests', '--test'} & set(status))
        self.assertTrue(any('--doc' in command and '--workspace' in command for command in calls))
        self.assertTrue(any('--manifest-path' in command and
                            'tools/txid/txid-sizing/export/Cargo.toml' in command for command in calls))
        self.assertTrue(any(command[:2] == ['cargo', 'clippy'] and '--all-targets' in command
                            and 'warnings' in command for command in calls))
        self.assertNotIn('qualification-tests', correctness.FEATURES)


if __name__ == '__main__':
    unittest.main()

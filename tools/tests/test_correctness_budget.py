"""A slow or missing test cannot produce a successful timing receipt."""
import contextlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/ci'))
from test_budget import execute


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
        self.assertLess(time.monotonic() - began, 2)

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


if __name__ == '__main__':
    unittest.main()

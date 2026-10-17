"""The parallel driver preserves discovery, fixtures and failing outcomes."""
from collections import Counter
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[2]
DRIVER = ROOT / 'tools/ci/python_tests.py'


class PythonTestDriverTests(unittest.TestCase):
    def run_driver(self, directory, *, jobs=2):
        env = os.environ.copy()
        env.pop('WALLET_PIR_FIXTURE_MODULE_OWNER', None)
        return subprocess.run([sys.executable, str(DRIVER), str(directory / 'suite'),
                               '--jobs', str(jobs)], cwd=directory, env=env,
                              capture_output=True, text=True, timeout=15)

    def test_modules_keep_their_fixtures_and_caller_directory_on_two_isolated_workers(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            suite = root / 'suite'
            suite.mkdir()
            events = root / 'events.jsonl'
            source = '''import json,os,time,unittest
from pathlib import Path
ROOT=Path(ROOT_VALUE)
EVENTS=Path(EVENTS_VALUE)
def record(phase):
 with EVENTS.open("a") as out: out.write(json.dumps(dict(module=__name__,pid=os.getpid(),phase=phase,time=time.monotonic()))+"\\n")
def setUpModule():
 assert "WALLET_PIR_FIXTURE_MODULE_OWNER" not in os.environ
 os.environ["WALLET_PIR_FIXTURE_MODULE_OWNER"]=__name__
 (ROOT/("ready-"+__name__)).touch()
 end=time.monotonic()+5
 while len(list(ROOT.glob("ready-*")))<2 and time.monotonic()<end:time.sleep(.01)
 assert len(list(ROOT.glob("ready-*")))>=2,"two module workers must overlap"
 record("start")
def tearDownModule():record("end")
class Cases(unittest.TestCase):
 def test_first(self):
  self.assertEqual(Path.cwd(),ROOT)
  self.assertEqual(os.environ["WALLET_PIR_FIXTURE_MODULE_OWNER"],__name__)
  time.sleep(.08)
 def test_second(self):time.sleep(.08)
'''.replace('ROOT_VALUE', repr(str(root))).replace('EVENTS_VALUE', repr(str(events)))
            for _ in range(3):
                (suite / ('test_' + uuid.uuid4().hex + '.py')).write_text(source)
            result = self.run_driver(root)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn('6 cases in 3 modules', result.stdout)
            rows = [json.loads(line) for line in events.read_text().splitlines()]
            self.assertEqual(Counter(row['phase'] for row in rows), {'start': 3, 'end': 3})
            self.assertEqual(len({row['pid'] for row in rows}), 3)
            active = peak = 0
            for row in sorted(rows, key=lambda row: row['time']):
                active += 1 if row['phase'] == 'start' else -1
                peak = max(peak, active)
            self.assertEqual((active, peak), (0, 2))

    def test_generated_duplicate_case_ids_are_not_deduplicated(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            suite = root / 'suite'
            suite.mkdir()
            name = 'test_' + uuid.uuid4().hex
            output = root / 'executed'
            (suite / (name + '.py')).write_text(
                'import unittest\nfrom pathlib import Path\n'
                'class Cases(unittest.TestCase):\n'
                f' def test_case(self):\n  with Path({str(output)!r}).open("a") as out:out.write("ran\\n")\n'
                'def load_tests(loader,tests,pattern):\n'
                ' tests.addTest(loader.loadTestsFromName(__name__+".Cases.test_case"))\n'
                ' return tests\n')
            result = self.run_driver(root, jobs=1)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(output.read_text().splitlines(), ['ran', 'ran'])
            self.assertIn('2 cases in 1 modules', result.stdout)

    def test_failure_and_discovery_errors_cannot_report_completion(self):
        for source in ['import unittest\nclass Cases(unittest.TestCase):\n def test_failure(self):self.fail("expected fixture failure")\n',
                       'this is not valid Python\n', '']:
            with self.subTest(source=source), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve()
                suite = root / 'suite'
                suite.mkdir()
                (suite / ('test_' + uuid.uuid4().hex + '.py')).write_text(source)
                result = self.run_driver(root)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn('Python tests complete:', result.stdout)


if __name__ == '__main__':
    unittest.main()

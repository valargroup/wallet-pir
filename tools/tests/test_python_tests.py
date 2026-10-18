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
sys.path.insert(0, str(ROOT / 'tools/ci'))
from stage import inherited_fds
from python_tests import discover, selected_modules


class PythonTestDriverTests(unittest.TestCase):
    def test_deployment_fast_cases_are_discovered_and_full_tier_is_larger(self):
        discovered = discover(ROOT / 'tools/tests/operations/deploy', ['test_ops_package'])
        selection = json.loads((ROOT / 'tools/ci/deploy-fast-tests.json').read_text())
        fast = selected_modules(discovered, selection)
        self.assertEqual({name for names in fast.values() for name in names}, set(selection))
        self.assertLess(sum(map(len, fast.values())), sum(map(len, discovered.values())))
        # Keep real install, recovery, tamper refusal, rollback and unit rollout
        # paths represented; the remaining matrices belong to full qualification.
        self.assertTrue({'test_ops_package.InstallTests', 'test_ops_package.RecoveryTests',
                         'test_ops_package.RetainedReleaseTests', 'test_wallet_pir_deploy'} <= set(fast))

    def run_driver(self, directory, *, jobs=2, split=(), selection=None, isolate=False, extra_env=None):
        env = os.environ.copy()
        env.pop('WALLET_PIR_FIXTURE_MODULE_OWNER', None)
        env.update(extra_env or {})
        return subprocess.run([sys.executable, str(DRIVER), str(directory / 'suite'),
                               '--jobs', str(jobs), *[arg for name in split for arg in ('--split-module', name)],
                               *(['--selection', str(selection)] if selection else []),
                               *(['--isolate-ci-output'] if isolate else [])], cwd=directory, env=env,
                              capture_output=True, text=True, timeout=15, **inherited_fds(env))

    def test_ci_tool_fixtures_cannot_write_into_the_enclosing_job(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            suite = root / 'suite'
            suite.mkdir()
            names = ['GITHUB_ENV', 'GITHUB_OUTPUT', 'GITHUB_STEP_SUMMARY',
                     'WALLET_PIR_STAGE_LOG', 'WALLET_PIR_ARTIFACT_LOG']
            env = {name: str(root / name) for name in names}
            (suite / ('test_' + uuid.uuid4().hex + '.py')).write_text(
                'import os,unittest\nfrom pathlib import Path\n'
                'class Cases(unittest.TestCase):\n'
                ' def test_fixture(self):\n'
                f'  for key in {names!r}:\n'
                '   if key in os.environ: Path(os.environ[key]).write_text("fixture output")\n')
            for isolated in (False, True):
                for path in env.values():
                    Path(path).write_text('outer job\n')
                result = self.run_driver(root, isolate=isolated, extra_env=env)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                for key, path in env.items():
                    if isolated:
                        self.assertEqual(Path(path).read_text(), 'outer job\n', key)
                    else:
                        self.assertTrue(Path(path).read_text().startswith('fixture output'), key)

    def test_ci_isolation_keeps_the_correctness_case_receipt(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            suite = root / 'suite'
            suite.mkdir()
            name = 'test_' + uuid.uuid4().hex
            (suite / (name + '.py')).write_text(
                'import unittest\nfrom python_timing import install\ninstall()\n'
                'class Cases(unittest.TestCase):\n def test_answer(self): self.assertEqual(2+2,4)\n')
            report = root / 'cases.jsonl'
            result = self.run_driver(root, isolate=True, extra_env={
                'PYTHONPATH': str(ROOT / 'tools/ci'), 'WALLET_PIR_CASE_LOG': str(report)})
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            [record] = [json.loads(line) for line in report.read_text().splitlines()]
            self.assertEqual(record['test'], name + '.Cases.test_answer')

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
            self.assertIn('6 cases in 3 groups', result.stdout)
            rows = [json.loads(line) for line in events.read_text().splitlines()]
            self.assertEqual(Counter(row['phase'] for row in rows), {'start': 3, 'end': 3})
            self.assertEqual(len({row['pid'] for row in rows}), 3)
            active = peak = 0
            for row in sorted(rows, key=lambda row: row['time']):
                active += 1 if row['phase'] == 'start' else -1
                peak = max(peak, active)
            self.assertEqual((active, peak), (0, 2))

    def test_class_splitting_preserves_class_fixtures_and_refuses_module_fixtures(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            suite = root / 'suite'
            suite.mkdir()
            name = 'test_' + uuid.uuid4().hex
            source = 'import os,unittest\nfrom pathlib import Path\n'
            for kind in ['First', 'Second']:
                source += (f'class {kind}(unittest.TestCase):\n'
                           ' @classmethod\n def setUpClass(cls):\n'
                           '  assert "OWNER" not in os.environ\n  os.environ["OWNER"]=cls.__name__\n'
                           f'  Path({str(root / kind)!r}).write_text(str(os.getpid()))\n'
                           ' def test_first(self):self.assertEqual(os.environ["OWNER"],type(self).__name__)\n'
                           ' def test_second(self):self.assertEqual(os.environ["OWNER"],type(self).__name__)\n')
            path = suite / (name + '.py')
            path.write_text(source)
            result = self.run_driver(root, split=[name])
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn('4 cases in 2 groups', result.stdout)
            self.assertNotEqual((root / 'First').read_text(), (root / 'Second').read_text())
            path.write_text(source + 'def setUpModule():pass\n')
            result = self.run_driver(root, split=[name])
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('module fixtures require one process', result.stderr)
            result = self.run_driver(root, split=['unknown'])
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('unknown modules', result.stderr)

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
            self.assertIn('2 cases in 1 groups', result.stdout)

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

    def test_fast_selection_preserves_fixtures_and_full_discovery_keeps_every_case(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            suite = root / 'suite'
            suite.mkdir()
            name = 'test_' + uuid.uuid4().hex
            (suite / (name + '.py')).write_text(
                'import unittest\nfrom pathlib import Path\n'
                'def setUpModule(): Path("ready").touch()\n'
                'class Cases(unittest.TestCase):\n'
                ' def test_fast(self): self.assertTrue(Path("ready").is_file())\n'
                ' def test_full(self): self.fail("full case still runs")\n')
            selection = root / 'fast.json'
            selection.write_text(json.dumps([name + '.Cases.test_fast']))
            result = self.run_driver(root, selection=selection)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn('1 cases in 1 groups', result.stdout)
            result = self.run_driver(root)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('full case still runs', result.stderr)
            for invalid in [None, [], ['renamed'], [name + '.Cases.test_fast'] * 2, {'cases': []}, [1]]:
                selection.write_text(json.dumps(invalid))
                result = self.run_driver(root, selection=selection)
                self.assertNotEqual(result.returncode, 0, invalid)
                self.assertNotIn('Python tests complete:', result.stdout)
                self.assertNotIn('full case still runs', result.stderr)


if __name__ == '__main__':
    unittest.main()

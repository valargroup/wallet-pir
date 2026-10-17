"""Batching must preserve target coverage, exclusions and Cargo's runtime contract."""
import copy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/ci'))
import fast_build
import integration_tests


class FastBuildTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.packages = []
        self.artifacts = []
        for name in ['a', 'b']:
            directory = root / name
            directory.mkdir()
            targets = [dict(name=name, kind=['lib']), dict(name='smoke', kind=['test']),
                       dict(name='campaign', kind=['test'])]
            if name == 'a':
                targets.append(dict(name='client', kind=['bin'], **{'required-features': ['cli']}))
            package = dict(id=name, name=name, version='1.0.0',
                           manifest_path=str(directory / 'Cargo.toml'), targets=targets)
            self.packages.append(package)
            for target in targets:
                if target['name'] != 'campaign':
                    self.artifacts.append(dict(package_id=name, target=target,
                                               executable=f'/fixture/{name}-{target["name"]}'))
        self.registry = {name: dict(fast=['smoke'], full=['campaign']) for name in ['a', 'b']}
        self.slow = {'a': {'slow': dict(features=['reqwest'], reason='Real compatibility oracle.')}}
        self.listings = {artifact['executable']: {'fast', 'slow'} for artifact in self.artifacts}
        self.ignored = {}
        self.calls, self.plans = [], []

    def execute(self, *, compile_only=False, jobs=2):
        original_read = Path.read_text

        def read(path, *args, **kwargs):
            if path == ROOT / 'tools/ci/slow-tests.json':
                return json.dumps(self.slow)
            return original_read(path, *args, **kwargs)

        def output(command, **kwargs):
            if command[:2] == ['cargo', 'metadata']:
                return json.dumps(dict(packages=self.packages)).encode()
            names = self.ignored.get(command[0], set()) if '--ignored' in command else self.listings[command[0]]
            return ''.join(f'{name}: test\n' for name in sorted(names))

        def run(command, **kwargs):
            self.calls.append((command, kwargs))
            if command[:2] == ['cargo', 'test']:
                return copy.deepcopy(self.artifacts)
            if command[:2] == ['python3', 'tools/ci/unit_tests.py']:
                self.plans.append(json.loads(Path(command[2]).read_text()))
                self.assertTrue(Path(kwargs['env']['WALLET_PIR_TEST_RUNTIME_CACHE']).is_dir())

        with patch.object(integration_tests, 'inventory', return_value=self.registry), \
                patch.object(Path, 'read_text', autospec=True, side_effect=read), \
                patch.object(fast_build.subprocess, 'check_output', side_effect=output), \
                patch.object(fast_build, 'run', side_effect=run), \
                patch.dict(os.environ, QUALIFY_PUBLICATIONS='100000'):
            fast_build.execute([p['name'] for p in self.packages], offline=True,
                               compile_only=compile_only, jobs=jobs)

    def test_one_build_preserves_units_binaries_fast_targets_and_package_specific_exclusions(self):
        self.execute()
        builds = [command for command, _ in self.calls if command[:2] == ['cargo', 'test']]
        self.assertEqual(len(builds), 1)
        build = builds[0]
        self.assertIn('--lib', build)
        self.assertIn('--bins', build)
        self.assertIn('--offline', build)
        self.assertEqual(build[build.index('--features') + 1], 'a/cli,a/reqwest')
        self.assertEqual([build[i + 1] for i, word in enumerate(build[:-1]) if word == '--test'], ['smoke'])
        self.assertNotIn('campaign', build)
        plan = self.plans[0]
        self.assertEqual(len(plan), 5)
        for program in plan:
            self.assertTrue(program['directory'].endswith('/' + program['package']))
            self.assertEqual(program['version'], '1.0.0')
            self.assertEqual(program['exclusive'], program['executable'].endswith('smoke'))
            expected = ['--skip', 'slow'] if program['package'] == 'a' and not program['executable'].endswith('smoke') else []
            self.assertEqual(program['args'], expected)
        runner, options = self.calls[-1]
        self.assertEqual(runner[-2:], ['--jobs', '2'])
        self.assertIn('--budget', runner)
        self.assertEqual(options['env']['WALLET_PIR_ENFORCE_BUDGET'], '1')
        self.assertNotIn('QUALIFY_PUBLICATIONS', options['env'])
        self.assertFalse(Path(options['env']['WALLET_PIR_TEST_RUNTIME_CACHE']).exists())

    def test_filter_library_runs_alone_without_changing_cases_or_arguments(self):
        self.execute()
        baseline = {p['executable']: p for p in self.plans[0]}
        self.calls.clear()
        self.plans.clear()
        self.packages[0]['name'] = 'transparent-filter-server'
        self.registry['transparent-filter-server'] = self.registry.pop('a')
        self.slow['transparent-filter-server'] = self.slow.pop('a')
        self.execute()
        plan = {p['executable']: p for p in self.plans[0]}
        self.assertEqual(set(plan), set(baseline))
        self.assertEqual({binary: p['exclusive'] for binary, p in plan.items()}, {
            '/fixture/a-a': True,
            '/fixture/a-client': False,
            '/fixture/a-smoke': True,
            '/fixture/b-b': False,
            '/fixture/b-smoke': True,
        })
        for binary, program in plan.items():
            for field in ['args', 'case_count', 'directory', 'version']:
                self.assertEqual(program[field], baseline[binary][field])
        runner, options = self.calls[-1]
        self.assertIn('--budget', runner)
        self.assertEqual(options['env']['WALLET_PIR_ENFORCE_BUDGET'], '1')

    def test_discovered_case_count_orders_programs_independently_of_cargo_output(self):
        self.listings['/fixture/b-b'] = {f'case_{i}' for i in range(7)}
        self.execute()
        self.assertEqual(self.plans[0][0]['executable'], '/fixture/b-b')
        self.assertEqual(self.plans[0][0]['case_count'], 7)
        self.assertEqual(len(self.plans[0]), len(self.artifacts))

    def test_priming_builds_the_same_units_without_running_any_program(self):
        self.execute()
        baseline = next(command for command, _ in self.calls if command[:2] == ['cargo', 'test'])
        self.calls.clear()
        self.execute(compile_only=True)
        self.assertEqual(self.calls[-1][0], baseline)
        self.assertEqual(len(self.calls), 2)

    def test_missing_library_cli_or_integration_artifacts_fail_before_execution(self):
        original = self.artifacts
        for omitted in original:
            with self.subTest(omitted=omitted['executable']):
                self.artifacts = [artifact for artifact in original if artifact != omitted]
                with self.assertRaisesRegex(ValueError, 'executables mismatch'):
                    self.execute()
                self.assertEqual(self.plans, [])

    def test_a_campaign_artifact_cannot_become_a_fast_program(self):
        self.artifacts.append(dict(package_id='a', target=dict(name='campaign', kind=['test']),
                                   executable='/fixture/a-campaign'))
        self.listings['/fixture/a-campaign'] = {'campaign'}
        with self.assertRaisesRegex(ValueError, 'extra_integrations'):
            self.execute()
        self.assertEqual(self.plans, [])

    def test_disabled_and_ignored_only_fast_targets_cannot_pass(self):
        for names in [set(), {'ignored'}]:
            with self.subTest(names=names):
                self.listings['/fixture/a-smoke'] = names
                self.ignored['/fixture/a-smoke'] = names
                with self.assertRaisesRegex(ValueError, 'no enabled, unignored'):
                    self.execute()
                self.assertEqual(self.plans, [])

    def test_feature_gated_exclusions_are_rejected_after_a_test_rename(self):
        self.listings['/fixture/a-a'] = {'renamed'}
        self.listings['/fixture/a-client'] = set()
        with self.assertRaisesRegex(ValueError, 'stale slow-test classification'):
            self.execute()
        self.assertEqual(self.plans, [])

    def test_a_fast_name_cannot_silently_select_another_packages_campaign(self):
        self.registry['b'] = dict(fast=[], full=['smoke', 'campaign'])
        with self.assertRaisesRegex(ValueError, 'also select campaign'):
            self.execute()
        self.assertEqual(self.calls, [])

    def test_hardware_features_cannot_silently_replace_reference_arithmetic(self):
        self.packages[0]['targets'][1]['required-features'] = ['native-reinspiring']
        with self.assertRaisesRegex(ValueError, 'reference arithmetic separate'):
            self.execute()
        self.assertEqual(self.calls, [])


if __name__ == '__main__':
    unittest.main()

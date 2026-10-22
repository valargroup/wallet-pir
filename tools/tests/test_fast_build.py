"""Batching must preserve target coverage, exclusions and Cargo's runtime contract."""
import copy
import json
import io
from contextlib import redirect_stdout
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

    def execute(self, *, compile_only=False, jobs=2, cpus=4, selected=None, **options):
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

        with patch.object(fast_build, 'available_cpus', return_value=cpus), \
                patch.object(integration_tests, 'inventory', return_value=self.registry), \
                patch.object(Path, 'read_text', autospec=True, side_effect=read), \
                patch.object(fast_build.subprocess, 'check_output', side_effect=output), \
                patch.object(fast_build, 'run', side_effect=run), \
                patch.dict(os.environ, QUALIFY_PUBLICATIONS='100000'), redirect_stdout(io.StringIO()):
            fast_build.execute(selected if selected is not None else [p['name'] for p in self.packages],
                               offline=True, compile_only=compile_only, jobs=jobs, **options)

    def test_large_unit_partitions_cover_each_discovered_case_exactly_once(self):
        names = {f'module::case_{index:03}' for index in range(137)}
        program = dict(integration=False, exclusive=False, args=[], case_count=len(names))
        shards = fast_build.bounded_programs(program, names)
        self.assertEqual(len(shards), 3)
        actual = [case for shard in shards for case in shard['args'][1:]]
        self.assertEqual(set(actual), names)
        self.assertEqual(len(actual), len(names))
        self.assertTrue(all(shard['args'][0] == '--exact' for shard in shards))
        self.assertLessEqual(max(shard['case_count'] for shard in shards), 46)
        for key in ('integration', 'exclusive'):
            guarded = dict(program, **{key: True})
            self.assertEqual(fast_build.bounded_programs(guarded, names), [guarded])

    def test_publisher_distribution_consumers_share_one_producer_process(self):
        distribution = {f'history::controller::distribution_tests::manager_tests::case_{index}'
                        for index in range(40)}
        names = distribution | {f'other::case_{index}' for index in range(102)}
        program = dict(package='transparent-publisher', integration=False,
                       exclusive=False, args=[], case_count=len(names))
        shards = fast_build.bounded_programs(program, names)
        self.assertEqual(len(shards), 3)
        self.assertEqual(set(shards[0]['args'][1:]), distribution)
        actual = [case for shard in shards for case in shard['args'][1:]]
        self.assertCountEqual(actual, names)
        self.assertTrue(all(shard['args'][0] == '--exact' for shard in shards))
        only_distribution = {f'history::controller::distribution_tests::case_{index}'
                             for index in range(60)}
        [shard] = fast_build.bounded_programs(program, only_distribution)
        self.assertCountEqual(shard['args'][1:], only_distribution)

    def test_status_preparation_splits_once_and_other_suites_keep_larger_groups(self):
        names = {f'status::sync::tests::case_{index:02d}' for index in range(37)}
        status = dict(package='pir-query-server', integration=False, exclusive=False,
                      args=[], case_count=len(names))
        shards = fast_build.bounded_programs(status, names)
        self.assertEqual(len(shards), 2)
        actual = [case for shard in shards for case in shard['args'][1:]]
        self.assertCountEqual(actual, names)
        other = dict(status, package='receiver-indexer')
        self.assertEqual(fast_build.bounded_programs(other, names), [other])

    def test_bins_without_unit_tests_are_omitted_from_the_fast_compile(self):
        root = Path(self.directory.name) / 'sources'
        root.mkdir()
        empty = root / 'empty.rs'
        tested = root / 'client.rs'
        empty.write_text('fn main() {}\n')
        tested.write_text('fn main() {}\n#[test]\nfn keeps_the_binary() {}\n')
        for package in self.packages:
            for target in package['targets']:
                if target['kind'] == ['bin']:
                    target['src_path'] = str(tested)
        self.packages[0]['targets'].append(
            dict(name='empty', kind=['bin'], src_path=str(empty)))
        self.execute()
        build = next(command for command, _ in self.calls if command[:2] == ['cargo', 'test'])
        self.assertNotIn('--bins', build)
        names = [build[i + 1] for i, word in enumerate(build[:-1]) if word == '--bin']
        self.assertEqual(names, ['client'])
        self.assertNotIn('empty', build)

    def test_target_declares_tests_follows_modules_and_fails_open(self):
        root = Path(self.directory.name) / 'scan'
        root.mkdir()
        (root / 'empty.rs').write_text('fn main() {}\n')
        (root / 'direct.rs').write_text('#[test]\nfn keeps() {}\n')
        (root / 'async_bin.rs').write_text('#[tokio::test]\nasync fn keeps() {}\n')
        (root / 'parent.rs').write_text('mod child;\nfn main() {}\n')
        (root / 'child.rs').write_text('#[test]\nfn nested() {}\n')
        self.assertFalse(fast_build.target_declares_tests(dict(src_path=str(root / 'empty.rs'))))
        self.assertTrue(fast_build.target_declares_tests(dict(src_path=str(root / 'direct.rs'))))
        self.assertTrue(fast_build.target_declares_tests(dict(src_path=str(root / 'async_bin.rs'))))
        self.assertTrue(fast_build.target_declares_tests(dict(src_path=str(root / 'parent.rs'))))
        self.assertTrue(fast_build.target_declares_tests(dict()))
        self.assertTrue(fast_build.target_declares_tests(dict(src_path=str(root / 'missing.rs'))))

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
            self.assertFalse(program['exclusive'])
            self.assertEqual(program['integration'], program['executable'].endswith('smoke'))
            expected = ['--skip', 'slow'] if program['package'] == 'a' and not program['executable'].endswith('smoke') else []
            self.assertEqual(program['args'], expected)
        runner, options = self.calls[-1]
        self.assertEqual(runner[-2:], ['--jobs', '2'])
        self.assertIn('--budget', runner)
        self.assertEqual(options['env']['WALLET_PIR_ENFORCE_BUDGET'], '1')
        self.assertNotIn('QUALIFY_PUBLICATIONS', options['env'])
        self.assertFalse(Path(options['env']['WALLET_PIR_TEST_RUNTIME_CACHE']).exists())
        self.assertEqual(Path(options['env']['WALLET_PIR_CASE_LOG']).name, 'cases.jsonl')


    def test_complete_correctness_keeps_native_features_and_the_outer_case_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / 'cases.jsonl'
            self.execute(additional_features=['a/native-reinspiring'],
                         environment=dict(os.environ, CORRECTNESS_OWNER='outer'),
                         case_log=receipt, enforce_budget=False)
        build = next(command for command, _ in self.calls if command[:2] == ['cargo', 'test'])
        self.assertIn('a/native-reinspiring', build[build.index('--features') + 1].split(','))
        environment = self.calls[-1][1]['env']
        self.assertEqual(environment['CORRECTNESS_OWNER'], 'outer')
        self.assertEqual(environment['WALLET_PIR_CASE_LOG'], str(receipt))
        self.assertEqual(environment['WALLET_PIR_ENFORCE_BUDGET'], '0')
        self.assertTrue(all('campaign' not in command for command, _ in self.calls))

    def test_full_campaign_features_cannot_expand_ordinary_correctness(self):
        with self.assertRaisesRegex(ValueError, 'cannot enable qualification'):
            self.execute(additional_features=['a/qualification-tests'])
        self.assertEqual(self.calls, [])
        self.artifacts[0]['features'] = ['qualification-tests']
        with self.assertRaisesRegex(ValueError, 'leaked into ordinary'):
            self.execute()
        self.assertEqual(self.plans, [])


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

    def test_affected_subset_compiles_feature_roots_without_running_them(self):
        root = Path(self.directory.name) / 'anchor'
        root.mkdir()
        source = root / 'lib.rs'
        source.write_text('#[test]\nfn anchor_case() {}\n')
        package = dict(id='transparent-shared', name='transparent-shared', version='1.0.0',
                       manifest_path=str(root / 'Cargo.toml'),
                       targets=[dict(name='transparent_shared', kind=['lib'], src_path=str(source))])
        self.packages.append(package)
        self.artifacts.append(dict(package_id='transparent-shared',
                                   target=package['targets'][0],
                                   executable='/fixture/transparent-shared'))
        self.listings['/fixture/transparent-shared'] = {'anchor_case'}
        self.execute(selected=['a', 'b'])
        build = next(command for command, _ in self.calls if command[:2] == ['cargo', 'test'])
        self.assertIn('-p', build)
        self.assertIn('transparent-shared', build)
        planned = {program['package'] for program in self.plans[0]}
        self.assertIn('a', planned)
        self.assertNotIn('transparent-shared', planned)

    def _cargo_tests(self, selected, **options):
        recorded = []

        def run(command, **kwargs):
            recorded.append(command)
            return []

        with patch.object(fast_build, 'run', side_effect=run):
            fast_build.execute(selected, compile_only=True, **options)
        return [command for command in recorded if command[:2] == ['cargo', 'test']]

    def _command_parts(self, build):
        def values(flag):
            return [build[index + 1] for index, word in enumerate(build[:-1]) if word == flag]
        features = build[build.index('--features') + 1].split(',') if '--features' in build else []
        return values('-p'), values('--bin'), values('--test'), features

    def test_enhance_subset_compiles_the_warm_dev_dependency_closure(self):
        builds = self._cargo_tests(['enhance-query-server'])
        self.assertEqual(len(builds), 2)
        stable, hint = builds
        stable_packages, bins, stable_tests, stable_features = self._command_parts(stable)
        hint_packages, hint_bins, hint_tests, hint_features = self._command_parts(hint)
        # Hosts stay named on the stable command. Taking them off for an Enhance
        # diff and putting them back afterwards rebuilds the closure.
        self.assertEqual(stable_packages, sorted(fast_build.FAST_FEATURE_ROOTS))
        self.assertEqual(hint_packages, ['pir-e2e-tests', 'pir-query-server'])
        self.assertIn('--lib', stable)
        self.assertIn('--lib', hint)
        self.assertNotIn('--bins', stable)
        self.assertEqual(bins, [
            'enhance-pir-load-test',
            'hierarchical-filter-eval', 'journal-convert-v1', 'pir-monitor', 'rate-query',
            'txid-quality-canary', 'txid-rate'])
        self.assertEqual(hint_bins, [])
        # Shared crates keep a feature set that does not name a product host.
        self.assertFalse(set(stable_features) & set(fast_build.PRODUCT_HOST_FEATURES.values()))
        self.assertEqual(hint_features, ['pir-e2e-tests/enhance-fast'])
        # status-publisher stays in the stable closure, so its fast object-access
        # HTTP target is compiled for every subset. Host tests stay there too.
        self.assertEqual(stable_tests, [
            'ci_smoke', 'commands', 'immutable_objects', 'lifecycle', 'object_access_http', 'operator'])
        self.assertEqual(hint_tests, ['ci_hint', 'operator'])
        transparent = self._cargo_tests(['transparent-publisher'])
        self.assertEqual(len(transparent), 1)
        self.assertEqual(transparent[0], stable)

    def test_each_product_compiles_only_its_own_host_features(self):
        # History and detail reach the query host through the publisher
        # library's dev-dependency. Enabling them on pir-publisher links that
        # library again. The fixture feature would compile it a third time.
        builds = self._cargo_tests(['transparent-publisher'])
        self.assertEqual(len(builds), 1)
        packages, _bins, tests, features = self._command_parts(builds[0])
        self.assertIn('pir-publisher', packages)
        self.assertIn('pir-query-server', packages)
        self.assertEqual(features, [])
        self.assertNotIn('pir-e2e-tests/transparent-tests', features)
        self.assertNotIn('pir-publisher/transparent-history', features)
        self.assertIn('commands', tests)
        self.assertIn('operator', tests)
        status_builds = self._cargo_tests(['status-publisher'])
        self.assertEqual(len(status_builds), 1)
        _packages, _bins, tests, features = self._command_parts(status_builds[0])
        self.assertNotIn('pir-publisher/status-tests', features)
        self.assertNotIn('ci_hint', tests)
        builds = self._cargo_tests(['enhance-query-server', 'transparent-publisher'])
        self.assertEqual(self._command_parts(builds[0])[3], [])
        _packages, _bins, tests, features = self._command_parts(builds[1])
        self.assertEqual(tests, ['ci_hint', 'operator'])
        self.assertEqual(features, ['pir-e2e-tests/enhance-fast'])
        self.assertIn('--lib', builds[1])

    def test_a_shared_diff_leaves_host_product_features_off(self):
        # Dependents of a shared crate are selected, but the diff itself is not a product.
        builds = self._cargo_tests(
            ['enhance-query-server', 'pir-publisher', 'pir-e2e-tests', 'transparent-publisher'],
            products=frozenset())
        features, tests = [], []
        for build in builds:
            _packages, _bins, build_tests, build_features = self._command_parts(build)
            features.extend(build_features)
            tests.extend(build_tests)
        self.assertFalse(set(features) & set(fast_build.PRODUCT_HOST_FEATURES.values()))
        self.assertNotIn('ci_hint', tests)

    def test_hardware_features_cannot_silently_replace_reference_arithmetic(self):
        self.packages[0]['targets'][1]['required-features'] = ['native-reinspiring']
        with self.assertRaisesRegex(ValueError, 'reference arithmetic separate'):
            self.execute()
        self.assertEqual(self.calls, [])


class DiffProductTests(unittest.TestCase):
    def test_a_diff_enables_only_the_products_it_touches(self):
        from fast import diff_products, workspace_packages
        packages = workspace_packages()
        self.assertEqual(diff_products(packages, ['src/enhance/enhance-publisher/src/lib.rs']),
                         frozenset({'enhance'}))
        self.assertEqual(diff_products(packages, ['src/status/status-publisher/src/lib.rs']),
                         frozenset({'status'}))
        self.assertEqual(diff_products(packages, ['src/transparent/transparent-publisher/src/lib.rs']),
                         frozenset({'transparent'}))
        self.assertEqual(diff_products(packages, [
            'src/enhance/enhance-publisher/src/lib.rs',
            'src/transparent/transparent-publisher/src/lib.rs']),
            frozenset({'enhance', 'transparent'}))
        self.assertEqual(diff_products(packages, ['src/shared/pir-pipeline/src/lib.rs']), frozenset())
        self.assertEqual(diff_products(packages, ['src/pir-publisher/src/lib.rs']), frozenset())
        self.assertEqual(diff_products(packages, ['src/pir-publisher/tests/lifecycle.rs']), frozenset())
        self.assertEqual(diff_products(packages, ['src/pir-query-server/src/lib.rs']), frozenset())
        self.assertEqual(diff_products(packages, ['src/pir-query-server/Cargo.toml']), frozenset())
        self.assertEqual(diff_products(packages, ['src/transparent/transparent-publisher/Cargo.toml']),
                         frozenset({'transparent'}))
        self.assertEqual(diff_products(packages, ['crates/deleted/Cargo.toml']),
                         frozenset({'enhance', 'status', 'transparent'}))
        self.assertEqual(diff_products(packages, ['tools/pir-e2e-tests/tests/ci_hint.rs']),
                         frozenset({'enhance'}))
        self.assertEqual(diff_products(packages, ['tools/pir-e2e-tests/tests/shard_filters.rs']),
                         frozenset({'transparent'}))
        self.assertEqual(diff_products(packages, ['tools/pir-e2e-tests/tests/support/mod.rs']),
                         frozenset({'enhance', 'transparent'}))
        self.assertEqual(diff_products(packages, ['tools/ci/fast_build.py']), frozenset())
        self.assertEqual(diff_products(packages, ['Cargo.toml']),
                         frozenset({'enhance', 'status', 'transparent'}))
        self.assertEqual(diff_products(packages, ['src/receiver/receiver-store/src/lib.rs']), frozenset())


if __name__ == '__main__':
    unittest.main()


class PublisherCampaignTierTests(unittest.TestCase):
    def test_repeated_fault_campaigns_keep_full_coverage_and_fast_representatives(self):
        import json
        from pathlib import Path
        root = Path(__file__).resolve().parents[2]
        # Former fault campaigns remain ordinary executable cases; their input
        # size changes under the explicit full-qualification feature instead.
        self.assertEqual(json.loads((root / 'tools/ci/slow-tests.json').read_text()), {})
        import sys
        sys.path.insert(0, str(root / 'tools/ci'))
        from full_packages import commands, inventory
        full = commands('transparent', inventory())
        self.assertFalse(any('--skip' in command for command in full))
        self.assertIn('transparent-publisher', full[-1])
        features = full[-1][full[-1].index('--features') + 1].split(',')
        self.assertIn('transparent-publisher/qualification-tests', features)
        self.assertIn('transparent-shared/qualification-tests', features)

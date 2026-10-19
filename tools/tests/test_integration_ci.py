"""Fast selection must not silently omit a new integration executable."""
import json
import re
from pathlib import Path
import sys
import tempfile
import tomllib
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/ci'))
import integration_tests as tests


class IntegrationTests(unittest.TestCase):
    def test_repository_registry_preserves_fast_checks_and_full_encrypted_lifecycle(self):
        registry = tests.inventory()
        for package in ['receiver-pir-server', 'transparent-query-server']:
            self.assertIn('ci_smoke', registry[package]['fast'])
        host = registry['pir-publisher']
        self.assertIn('ci_hint', host['fast'])
        self.assertIn('encrypted_lifecycle', host['full'])

    def test_publisher_smoke_feedback_runs_its_encrypted_smoke_only(self):
        registry = {'enhance-publisher': {'fast': ['ci_smoke', 'layout'], 'full': []}}
        with patch.object(tests, 'inventory', return_value=registry), \
                patch.object(tests, 'run') as run, \
                patch.object(tests, 'target_features', return_value=[]), \
                patch.object(tests.subprocess, 'check_output', side_effect=['case: test\n', '']):
            tests.execute(['enhance-publisher'], 'fast', smoke_only=True)
        commands = [call.args[0] for call in run.call_args_list]
        self.assertEqual(commands[0][commands[0].index('-p') + 1], 'enhance-publisher')
        self.assertIn('ci_smoke', commands[0])
        self.assertNotIn('layout', str(commands))

    def test_workspace_rust_literal_module_and_fixture_paths_exist(self):
        checked = set()
        for package in tests.workspace_packages():
            directory = Path(package['manifest_path']).parent
            for source in directory.rglob('*.rs'):
                if source in checked:
                    continue
                checked.add(source)
                text = source.read_text()
                paths = re.findall(r'#\[path\s*=\s*"([^"]+)"\]', text)
                paths += re.findall(r'include_(?:bytes|str)!\(\s*"([^"]+)"', text)
                for relative in paths:
                    self.assertTrue((source.parent / relative).is_file(),
                                    f'{source.relative_to(ROOT)}: missing {relative}')

    def test_operational_filter_and_txid_canary_targets_are_service_owned(self):
        directory = ROOT / 'tools/transparent'
        manifest = tomllib.loads((directory / 'Cargo.toml').read_text())
        self.assertEqual(manifest['package']['name'], 'transparent-tools')
        for binary in ['transparent-filter-cli', 'txid-quality-canary']:
            self.assertTrue((directory / 'src/bin' / (binary + '.rs')).is_file(), binary)
        self.assertIn('transparent', manifest['dependencies']['zakura-pir-client']['features'])
        self.assertNotIn('wallet', manifest['dependencies']['zakura-pir-client']['features'])
        registry = json.loads((ROOT / 'tools/ci/integration-tests.json').read_text())
        self.assertIn('service_tools', registry['transparent-tools']['full'])

    def test_missing_stale_duplicate_and_malformed_assignments_fail(self):
        for registry in [{}, {'p': {'fast': [], 'full': []}},
                         {'p': {'fast': ['smoke'], 'full': ['smoke']}},
                         {'p': {'fast': ['smoke'], 'full': ['old']}},
                         {'p': {'fast': 'smoke', 'full': []}},
                         {'p': {'fast': ['smoke'], 'manual': []}}]:
            with self.subTest(registry=registry), self.assertRaises(ValueError):
                tests.validate({'p': {'smoke'}}, registry)

    def test_selected_targets_enable_only_their_declared_features(self):
        # The receiver oracle now belongs to wallet-libraries. Keep the driver
        # regression independent of any particular service package's features.
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / 'Cargo.toml'
            manifest.write_text('[package]\nname = "fixture"\nversion = "0.0.0"\n'
                                '[[test]]\nname = "golden"\nrequired-features = ["server"]\n'
                                '[[test]]\nname = "plain"\n')
            with patch.object(tests, 'workspace_packages', return_value=[
                    {'name': 'fixture', 'manifest_path': str(manifest)}]):
                self.assertEqual(tests.target_features('fixture', ['golden']), ['--features', 'server'])
                self.assertEqual(tests.target_features('fixture', ['plain']), [])
        self.assertEqual(tests.target_features('enhance-publisher', ['ci_smoke']), [])

    def test_each_integration_command_selects_a_service_workspace_package_and_targets(self):
        packages = {package['name'] for package in tests.workspace_packages()}
        discovered = tests.targets()
        for package, tiers in tests.inventory().items():
            self.assertIn(package, packages)
            for tier in ['fast', 'full']:
                command = tests.cargo_args(package, tiers, tier) + tests.target_features(
                    package, tiers['fast'] if tier == 'fast' else tiers['fast'] + tiers['full'])
                self.assertEqual(command[command.index('-p') + 1], package)
                selected = {command[index + 1] for index, word in enumerate(command[:-1])
                            if word == '--test'}
                self.assertFalse(selected - discovered[package])

    def test_fast_never_compiles_the_campaign_target(self):
        tiers = {'fast': ['smoke'], 'full': ['campaign']}
        self.assertEqual(tests.cargo_args('p', tiers, 'fast')[-2:], ['--test', 'smoke'])
        self.assertNotIn('campaign', tests.cargo_args('p', tiers, 'fast'))
        self.assertIn('campaign', tests.cargo_args('p', tiers, 'full'))

    def test_disabled_or_ignored_only_fast_targets_cannot_pass(self):
        registry = {'p': {'fast': ['smoke'], 'full': []}}
        for listing in [('', ''), ('case: test\n', 'case: test\n')]:
            with self.subTest(listing=listing), patch.object(tests, 'inventory', return_value=registry), \
                    patch.object(tests, 'run') as run, \
                    patch.object(tests, 'target_features', return_value=[]), \
                    patch.object(tests.subprocess, 'check_output', side_effect=listing):
                with self.assertRaisesRegex(ValueError, 'no enabled, unignored'):
                    tests.execute(['p'], 'fast')
                self.assertEqual(run.call_count, 1)
                self.assertIn('--no-run', run.call_args.args[0])

    def test_fast_compiles_every_program_before_one_reclaim_and_preserves_each_run(self):
        registry = {p: {'fast': ['smoke'], 'full': []} for p in ['a', 'b']}
        with patch.object(tests, 'inventory', return_value=registry), \
                patch.object(tests, 'run') as run, \
                patch.object(tests, 'target_features', return_value=[]), \
                patch.object(tests.subprocess, 'check_output', side_effect=['case: test\n', ''] * 2):
            tests.execute(['a', 'b'], 'fast')
        commands = [call.args[0] for call in run.call_args_list]
        self.assertEqual([command[:3] for command in commands[:2]], [['cargo', 'test', '--no-run']] * 2)
        self.assertEqual(commands[2], ['python3', 'tools/ci/reclaim-build-cache.py'])
        self.assertEqual([command[command.index('-p') + 1] for command in commands[3:]], ['a', 'b'])
        self.assertTrue(all('--no-run' not in command for command in commands[3:]))
        cache = run.call_args_list[3].kwargs['env']['WALLET_PIR_TEST_RUNTIME_CACHE']
        self.assertFalse(Path(cache).exists())

    def test_discovery_includes_explicit_and_directory_targets_and_respects_autotests(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'tests/nested').mkdir(parents=True)
            (root / 'tests/automatic.rs').write_text('')
            (root / 'tests/nested/main.rs').write_text('')
            manifest = root / 'Cargo.toml'
            for enabled, expected in [(True, {'manual', 'automatic', 'nested'}), (False, {'manual'})]:
                manifest.write_text('[package]\nname="p"\nautotests='+str(enabled).lower()+'\n[[test]]\nname="manual"\npath="elsewhere.rs"\n')
                self.assertEqual(tests.targets([{'name': 'p', 'manifest_path': str(manifest)}]), {'p': expected})


if __name__ == '__main__':
    unittest.main()

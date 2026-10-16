"""Fast selection must not silently omit a new integration executable."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/ci'))
import integration_tests as tests


class IntegrationTests(unittest.TestCase):
    def test_repository_registry_is_complete_and_each_service_has_a_smoke(self):
        registry = tests.inventory()
        for package in ['receiver-pir-server', 'transparent-shard-server', 'enhance-pir-server']:
            self.assertIn('ci_smoke', registry[package]['fast'])

    def test_missing_stale_duplicate_and_malformed_assignments_fail(self):
        for registry in [{}, {'p': {'fast': [], 'full': []}},
                         {'p': {'fast': ['smoke'], 'full': ['smoke']}},
                         {'p': {'fast': ['smoke'], 'full': ['old']}},
                         {'p': {'fast': 'smoke', 'full': []}},
                         {'p': {'fast': ['smoke'], 'manual': []}}]:
            with self.subTest(registry=registry), self.assertRaises(ValueError):
                tests.validate({'p': {'smoke'}}, registry)

    def test_the_receiver_oracle_enables_its_declared_server_feature(self):
        self.assertEqual(tests.target_features('receiver-pir', ['golden']), ['--features', 'server'])
        self.assertEqual(tests.target_features('enhance-pir-server', ['ci_smoke']), [])

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

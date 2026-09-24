"""A new integration target must acquire an executable CI classification."""
import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('enhance_tests', ROOT / 'tools/ci/enhance_tests.py')
enhance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(enhance)


def metadata(names):
    return {'packages': [{'name': enhance.PACKAGE, 'targets': [
        {'name': enhance.PACKAGE, 'kind': ['lib']},
        *({'name': name, 'kind': ['test']} for name in names)]}]}


class ClassificationTests(unittest.TestCase):
    def test_complete_classification(self):
        enhance.validate(metadata(['small', 'crypto']), {'fast': ['small'], 'full': ['crypto']})

    def test_missing_stale_duplicate_and_unknown_tier_are_rejected(self):
        for registry in [
            {'fast': [], 'full': []},
            {'fast': ['new'], 'full': ['stale']},
            {'fast': ['new'], 'full': ['new']},
            {'fast': ['new'], 'full': [], 'manual': []},
            {'fast': 'new', 'full': []},
        ]:
            with self.subTest(registry=registry), self.assertRaises(ValueError):
                enhance.validate(metadata(['new']), registry)

    def test_full_runs_both_tiers_and_fast_only_runs_fast(self):
        registry = {'fast': ['zebra', 'alpha'], 'full': ['crypto']}
        prefix = ['--locked', '--profile', 'release-fast', '-p', enhance.PACKAGE]
        self.assertEqual(enhance.cargo_args(registry, 'fast'),
                         prefix + ['--test', 'alpha', '--test', 'zebra'])
        self.assertEqual(enhance.cargo_args(registry, 'full'),
                         prefix + ['--lib', '--bins', '--test', 'alpha', '--test', 'crypto', '--test', 'zebra'])

    def test_repository_registry_matches_source_targets(self):
        registry = json.loads((ROOT / 'tools/ci/enhance-tests.json').read_text())
        targets = ROOT / 'enhance/services/enhance-pir-server/tests'
        enhance.validate(metadata([p.stem for p in targets.glob('*.rs')]), registry)


if __name__ == '__main__':
    unittest.main()

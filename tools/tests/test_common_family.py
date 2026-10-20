"""Keep the service dependency graph on one Common source revision."""
import importlib.util
from pathlib import Path
import unittest


spec = importlib.util.spec_from_file_location(
    'boundary', Path(__file__).resolve().parents[1] / 'ci/boundary.py')
boundary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(boundary)


class CommonFamilyTests(unittest.TestCase):
    def setUp(self):
        self.revision = 'a' * 40
        self.patch = {'git': 'https://github.com/zakura-core/common.git',
                      'rev': self.revision}
        self.patches = {'zakura-protocol': dict(self.patch),
                        'zakura-reddsa': dict(self.patch)}
        self.package = {'name': 'zakura-reddsa', 'version': '4.0.0-rc0',
                        'source': f"git+{self.patch['git']}?rev={self.revision}#{self.revision}"}

    def test_current_family_passes(self):
        self.assertEqual(boundary.common_family_violations([self.package], self.patches), [])

    def test_old_transitive_copy_is_rejected_alongside_current_copy(self):
        old = {**self.package, 'version': '3.0.0',
               'source': 'registry+https://github.com/rust-lang/crates.io-index'}
        problems = boundary.common_family_violations([self.package, old], self.patches)
        self.assertEqual(len(problems), 1)
        self.assertIn('zakura-reddsa 3.0.0', problems[0])

    def test_current_version_from_another_source_is_rejected(self):
        other = {**self.package, 'source': 'registry+https://github.com/rust-lang/crates.io-index'}
        self.assertTrue(boundary.common_family_violations([other], self.patches))

    def test_mixed_patch_revisions_are_rejected(self):
        self.patches['zakura-reddsa']['rev'] = 'b' * 40
        self.assertTrue(boundary.common_family_violations([self.package], self.patches))

    def test_unrelated_version_three_package_is_allowed(self):
        other = {'name': 'tempfile', 'version': '3.0.0', 'source': 'registry'}
        self.assertEqual(boundary.common_family_violations([other], self.patches), [])

    def test_published_rc3_without_overrides(self):
        """Published Common is accepted while stale and mixed-source copies are refused."""
        published = {**self.package, 'version': '4.0.0-rc3',
                     'source': 'registry+https://github.com/rust-lang/crates.io-index'}
        self.assertEqual(boundary.common_family_violations([published], {}), [])
        for bad in [{**published, 'version': '4.0.0-rc2'}, self.package]:
            self.assertTrue(boundary.common_family_violations([published, bad], {}))
        self.assertTrue(boundary.common_family_violations([published],
                        {'zakura-reddsa': self.patch}))


if __name__ == '__main__':
    unittest.main()

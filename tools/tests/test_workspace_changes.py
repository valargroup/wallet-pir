"""Workspace refactors keep unrelated services out of the fast build."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/ci'))
import workspace_changes


class WorkspaceChangesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.git('init', '-q')
        self.write('Cargo.toml', '[workspace]\nmembers = ["core", "other"]\nresolver = "2"\n')
        self.lock = ('version = 4\n[[package]]\nname = "core"\nversion = "1.0.0"\n'
                     '[[package]]\nname = "other"\nversion = "1.0.0"\n'
                     '[[package]]\nname = "external"\nversion = "1.0.0"\nsource = "registry+example"\n')
        self.write('Cargo.lock', self.lock)
        self.git('add', 'Cargo.toml', 'Cargo.lock')
        self.git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.test',
                 '-c', 'commit.gpgsign=false', 'commit', '-qm', 'Original workspace')
        self.base = self.git('rev-parse', 'HEAD').strip()
        self.packages = [{'name': name, 'manifest_path': str(self.root / name / 'Cargo.toml')}
                         for name in ('core', 'other', 'serving')]

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.root, text=True,
                                       stderr=subprocess.DEVNULL)

    def write(self, path, text):
        (self.root / path).write_text(text)

    def selected(self, paths, *, local=True, head='HEAD'):
        return workspace_changes.package_paths(paths, self.packages, base=self.base,
                                              head=head, local=local, root=self.root)

    def test_library_split_selects_local_lock_entries_and_keeps_source_changes(self):
        self.write('Cargo.toml', '[workspace]\nmembers = ["core", "other", "serving"]\nresolver = "2"\n')
        self.write('Cargo.lock', self.lock.replace('name = "core"\nversion = "1.0.0"',
                   'name = "core"\nversion = "1.0.0"\ndependencies = ["serving"]') +
                   '[[package]]\nname = "serving"\nversion = "1.0.0"\n')
        paths = ['Cargo.toml', 'Cargo.lock', 'core/src/lib.rs']
        self.assertEqual(self.selected(paths), ['core/Cargo.toml', 'core/src/lib.rs', 'serving/Cargo.toml'])

    def test_committed_selection_does_not_read_later_working_edits(self):
        self.write('Cargo.lock', self.lock.replace('name = "core"\nversion = "1.0.0"',
                                                 'name = "core"\nversion = "1.1.0"'))
        self.git('add', 'Cargo.lock')
        self.git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.test',
                 '-c', 'commit.gpgsign=false', 'commit', '-qm', 'Change one local package')
        self.write('Cargo.lock', 'invalid TOML')
        self.assertEqual(self.selected(['Cargo.lock'], local=False), ['core/Cargo.toml'])
        self.assertEqual(self.selected(['Cargo.lock']), ['Cargo.lock'])

    def test_external_dependency_edits_keep_complete_coverage(self):
        for lock in [self.lock.replace('source = "registry+example"', 'source = "registry+other"'),
                     self.lock.replace('name = "external"\nversion = "1.0.0"',
                                       'name = "external"\nversion = "2.0.0"'),
                     self.lock + '[[package]]\nname = "outside"\nversion = "1"\n']:
            with self.subTest(lock=lock):
                self.write('Cargo.lock', lock)
                self.assertEqual(self.selected(['Cargo.lock']), ['Cargo.lock'])

    def test_global_build_settings_missing_history_and_unknown_members_stay_broad(self):
        original = (self.root / 'Cargo.toml').read_text()
        for manifest in [original.replace('resolver = "2"', 'resolver = "3"'),
                         original + '[profile.release-fast]\nlto = true\n',
                         original.replace('"core", "other"', '"core"'),
                         original.replace('"core", "other"', '"core", "other", "unknown/*"')]:
            with self.subTest(manifest=manifest):
                self.write('Cargo.toml', manifest)
                self.assertEqual(self.selected(['Cargo.toml']), ['Cargo.toml'])
        self.base = '0' * 40
        self.assertEqual(self.selected(['Cargo.lock']), ['Cargo.lock'])


if __name__ == '__main__':
    unittest.main()

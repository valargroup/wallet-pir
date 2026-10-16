"""Exercise artifact rules and exact Git snapshots without invoking Cargo or GitHub."""
from contextlib import redirect_stdout
import importlib.util
import io
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/ci'))
import hygiene


class HygieneTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.git('init', '-q')
        self.git('config', 'user.name', 'Fixture')
        self.git('config', 'user.email', 'fixture@example.invalid')
        self.write('tools/ci/hygiene.toml', '''[warnings]
added_lines = 1500
files = 30
large_bytes = 1048576
[[allow_data]]
pattern = "product/tests/fixtures/*.json"
reason = "Consumed by the regression tests."
[[allow_data]]
pattern = "transparent/evidence/correctness/*.json"
reason = "Essential correctness certificate."
''')
        self.write('README.md', '# Fixture\n')
        self.commit()
        self.base = self.git('rev-parse', 'HEAD').strip()

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.root, text=True, stderr=subprocess.PIPE)

    def write(self, path, content):
        file = self.root / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(content)
        return file

    def commit(self):
        paths = self.git('ls-files', '--cached', '--others', '--exclude-standard', '-z').split('\0')
        self.git('add', '--', *[p for p in paths if p])
        self.git('commit', '-qm', 'Fixture snapshot')

    def inspect(self, **options):
        return hygiene.inspect(self.root, base=self.base, **options)

    def test_configuration_and_consumed_fixtures_pass(self):
        self.write('product/tests/fixtures/answer.json', '{"answer": 42}\n')
        self.write('product/deploy/settings.toml', 'enabled = true\n')
        self.assertEqual(self.inspect()[0:2], ([], []))

    def test_dump_in_an_unrelated_directory_is_rejected(self):
        self.write('ops/accidental.json', '{}\n')
        self.write('product/run.log', 'captured output\n')
        self.write('self-review.md', '# Approved\n')
        errors, _, _ = self.inspect()
        self.assertEqual(len(errors), 3)
        self.assertTrue(any('ops/accidental.json: run data' in e for e in errors))

    def test_evidence_cannot_be_used_as_a_script_directory(self):
        self.write('transparent/evidence/run/summarize.py', 'print(1)\n')
        self.assertIn('experiment script', self.inspect()[0][0])

    def test_local_check_sees_staged_unstaged_and_untracked_changes(self):
        self.write('staged.json', '{}')
        self.git('add', '--', 'staged.json')
        self.write('README.md', 'Edited\n')
        self.write('untracked.log', 'output')
        errors, _, areas = self.inspect()
        self.assertEqual(len(errors), 2)
        self.assertIn('README.md', areas)

    def test_committed_check_uses_committed_bytes_despite_local_edits(self):
        self.write('transparent/evidence/correctness/proof.json', '{"original": true}\n')
        self.commit()
        self.base = self.git('rev-parse', 'HEAD').strip()
        self.write('README.md', 'Next revision\n')
        self.commit()
        self.write('transparent/evidence/correctness/proof.json', '{"rewritten": true}\n')
        self.assertEqual(self.inspect(head='HEAD', local=False)[0], [])
        self.assertIn('immutable', self.inspect()[0][0])

    def test_renames_and_deletions_do_not_inflate_size_warnings(self):
        self.write('long.md', 'line\n' * 1700)
        self.write('old.log', 'historical output')
        self.commit()
        self.base = self.git('rev-parse', 'HEAD').strip()
        self.git('mv', 'long.md', 'renamed.md')
        self.git('rm', 'old.log')
        self.assertEqual(self.inspect()[0:2], ([], []))

    def test_large_source_change_warns_without_failing(self):
        self.write('feature.rs', 'fn example() {}\n' * 1600)
        errors, warnings, _ = self.inspect()
        self.assertEqual(errors, [])
        self.assertIn('1,600 added text lines', warnings[0])
        self.assertTrue(hygiene.size_warnings(12573, 100, [], hygiene.read_policy(self.root)['warnings']))

    def test_large_allowed_data_warns_without_failing(self):
        self.write('product/tests/fixtures/large.json', ' ' * 1048577)
        self.assertEqual(self.inspect()[0], [])
        self.assertTrue(self.inspect()[1])

    def test_unusual_names_are_not_shell_commands(self):
        self.write('product/tests/fixtures/space\n$(false).json', '{}')
        self.assertEqual(self.inspect()[0], [])

    def test_full_audit_detects_existing_artifacts(self):
        self.write('historic.log', 'old')
        self.commit()
        self.base = self.git('rev-parse', 'HEAD').strip()
        self.assertEqual(self.inspect()[0], [])
        self.assertTrue(hygiene.inspect(self.root, all_files=True)[0])

    def test_malformed_policy_and_broad_allowances_are_rejected(self):
        path = self.root / 'tools/ci/hygiene.toml'
        original = path.read_text()
        for malformed in [original.replace('1500', '0'), original.replace('product/tests/fixtures/*.json', '*.json'),
                          original.replace('reason = "Consumed by the regression tests."', 'reason = ""')]:
            path.write_text(malformed)
            with self.assertRaises(ValueError):
                hygiene.read_policy(self.root)

    def test_pre_push_checks_feature_branches_and_skips_tags_and_deletions(self):
        self.write('dump.json', '{}')
        self.commit()
        head = self.git('rev-parse', 'HEAD').strip()
        with redirect_stdout(io.StringIO()):
            self.assertEqual(hygiene.pre_push(self.root, f'refs/heads/x {head} refs/heads/x {self.base}\n'), 1)
            self.assertEqual(hygiene.pre_push(self.root, f'refs/tags/x {head} refs/tags/x {self.base}\n'), 0)
            self.assertEqual(hygiene.pre_push(self.root, f'(delete) {"0" * 40} refs/heads/x {head}\n'), 0)

    def test_new_branch_uses_its_main_ancestor(self):
        self.git('update-ref', 'refs/remotes/origin/main', self.base)
        self.write('dump.json', '{}')
        self.commit()
        head = self.git('rev-parse', 'HEAD').strip()
        with redirect_stdout(io.StringIO()):
            self.assertEqual(hygiene.pre_push(self.root, f'refs/heads/x {head} refs/heads/x {"0" * 40}\n'), 1)

    def test_symlink_outside_checkout_is_rejected(self):
        (self.root / 'escape.json').symlink_to('/etc/hosts')
        self.assertIn('symlink leaves', self.inspect()[0][0])


if __name__ == '__main__':
    unittest.main()

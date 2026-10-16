"""Qualification must remain cumulative and independent for each service."""
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/ci'))
sys.path.insert(0, str(ROOT / 'ops/lib'))
import qualification as q
import runner_storage as storage
import launch
from wallet_pir_ops import release

SHA, BASE = 'a' * 40, 'b' * 40
REPO = 'valargroup/wallet-pir'


def candidate(**changes):
    return dict(id=1, head_sha=SHA, head_branch='main', event='push', status='in_progress',
                conclusion=None, path='.github/workflows/ci-qualify.yml', run_attempt=2,
                head_repository={'full_name': REPO}, **changes)


def job(family, **changes):
    value = dict(name=f'{q.FAMILIES[family]} / Prepare qualified release artifacts',
                 status='completed', conclusion='success')
    return {**value, **changes}


def complete_jobs(family):
    prefix = q.FAMILIES[family]
    names = ['Select full checks', 'Validate selected helper checks', 'Full checks complete',
             'Build candidate release artifacts', 'Prepare qualified release artifacts']
    if family != 'ops':
        names.append('Shared lint and tests')
    names += {'receiver': ['Receiver lint and tests', 'Validate Enhance infrastructure'],
              'ops': ['Validate Enhance infrastructure', 'Validate transparent elastic infrastructure'],
              'enhance': ['Enhance lint', 'Enhance tests', 'Validate Enhance infrastructure'],
              'transparent': ['Transparent lint', 'Transparent tests', 'Validate transparent elastic infrastructure']}[family]
    return [{'name': f'{prefix} / {name}', 'status': 'completed', 'conclusion': 'success'} for name in names]


class QualificationTests(unittest.TestCase):
    def test_expensive_checks_can_be_cancelled_when_a_new_commit_arrives(self):
        workflow = (ROOT / '.github/workflows/ci-full.yml').read_text()
        for name in ['transparent-lint', 'transparent-test', 'enhance-lint',
                     'enhance-test', 'shared-test', 'receiver-test']:
            job = workflow.split(f'\n  {name}:\n', 1)[1]
            expression = re.search(r'^    if: (.+)$', job, re.M).group(1)
            self.assertIn('!cancelled()', expression, name)
            self.assertNotIn('always()', expression, name)

    def test_publication_survives_skipped_other_families_but_requires_its_gates(self):
        workflow = (ROOT / '.github/workflows/ci-full.yml').read_text()
        for name, gates in [('release', ['complete', 'build']), ('release-cuda', ['complete'])]:
            job = workflow.split(f'\n  {name}:\n', 1)[1]
            expression = re.search(r'^    if: (.+)$', job, re.M).group(1)

            def permitted(results, cancelled=False):
                # GitHub implicitly adds success() without a status function;
                # an unrelated skipped ancestor then skips this job as well.
                if not re.search(r'\b(always|cancelled|success|failure)\(', expression):
                    return False
                condition = expression.removeprefix('${{').removesuffix('}}').strip()
                condition = condition.replace('!cancelled()', repr(not cancelled)).replace('always()', 'True')
                condition = condition.replace('&&', ' and ').replace('||', ' or ')
                values = {'inputs.family': 'enhance' if name == 'release-cuda' else 'receiver',
                          'github.event_name': 'push', 'github.ref': 'refs/heads/main'}
                values.update({f'needs.{gate}.result': result for gate, result in results.items()})
                for key, value in values.items():
                    condition = condition.replace(key, repr(value))
                return eval(condition, {'__builtins__': {}}, {})

            passed = dict.fromkeys(gates, 'success')
            self.assertTrue(permitted(passed), name)
            self.assertFalse(permitted(passed, cancelled=True), name)
            for gate in gates:
                for result in ['failure', 'skipped', 'cancelled', 'queued']:
                    self.assertFalse(permitted({**passed, gate: result}), (name, gate, result))

    def test_only_obsolete_automatic_main_regressions_are_cancelled(self):
        runs = [{'id': i, 'event': event, 'head_branch': branch, 'head_sha': sha, 'status': status}
                for i, (event, branch, sha, status) in enumerate([
                    ('push', 'main', BASE, 'queued'), ('workflow_run', 'main', BASE, 'in_progress'),
                    ('workflow_dispatch', 'main', BASE, 'in_progress'),
                    ('pull_request', 'feature', BASE, 'in_progress'), ('push', 'main', SHA, 'in_progress'),
                    ('push', 'main', BASE, 'completed')])]
        with patch.object(release, 'api', return_value={'workflow_runs': runs}), \
                patch.dict(os.environ, GH_TOKEN='fixture-not-a-credential'), \
                patch.object(q.urllib.request, 'urlopen') as request:
            q.cancel_regressions(REPO, SHA)
        self.assertEqual([call.args[0].full_url.rsplit('/', 2)[-2] for call in request.call_args_list], ['0', '1'])

    def test_ci_only_feedback_does_not_compile_unchanged_rust_but_full_coverage_remains(self):
        packages = q.workspace_packages()
        for paths in [['tools/ci/fast.py', 'ops/lib/wallet_pir_ops/release.py'],
                      ['.github/workflows/ci-qualify.yml']]:
            result = q.plan(packages, paths)
            self.assertEqual(result['packages'], [])
            self.assertEqual(set(result['groups']), q.FULL_GROUPS)
            self.assertIn('check-tools', result['helpers'])
        for path in ['Cargo.toml', 'Cargo.lock', '.cargo/config.toml', 'unknown.file']:
            self.assertTrue(q.plan(packages, ['tools/ci/fast.py', path])['packages'])
        self.assertIn('receiver-indexer', q.plan(packages, ['tools/ci/fast.py',
                      'receiver/services/receiver-indexer/src/main.rs'])['packages'])

    def test_only_main_service_runs_from_the_repository_are_trusted(self):
        self.assertTrue(release.service_run(candidate(), SHA, REPO))
        for key, value in [('head_sha', BASE), ('head_branch', 'feature'), ('event', 'pull_request'),
                           ('path', '.github/workflows/ci.yml'), ('run_attempt', None),
                           ('head_repository', {'full_name': 'outsider/fork'})]:
            with self.subTest(key=key):
                self.assertFalse(release.service_run({**candidate(), key: value}, SHA, REPO))

    def test_failed_unrelated_job_does_not_invalidate_completed_service(self):
        jobs = [*complete_jobs('receiver'), job('enhance', conclusion='failure')]
        self.assertTrue(release.service_jobs(jobs, 'receiver'))
        self.assertFalse(release.service_jobs(jobs, 'enhance'))
        for jobs in [[], [job('receiver', status='in_progress')],
                     [job('receiver', conclusion='skipped')], [job('receiver'), job('receiver')]]:
            self.assertFalse(release.service_jobs(jobs, 'receiver'))
        for missing in complete_jobs('receiver'):
            with self.subTest(missing=missing['name']):
                jobs = [j for j in complete_jobs('receiver') if j != missing]
                self.assertFalse(release.service_jobs(jobs, 'receiver'))

    def test_resolution_accepts_only_completed_service_and_digest(self):
        for digest in ['sha256:' + 'c' * 64, '', 'invalid']:
            with self.subTest(digest=digest), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / 'output'
                artifact = {'name': f'receiver-pir-{SHA}', 'expired': False, 'digest': digest}
                replies = [{'workflow_runs': [candidate()]}, {'jobs': complete_jobs('receiver')},
                           {'artifacts': [artifact]}, {'workflow_runs': []}]
                with patch.dict(os.environ, GITHUB_REPOSITORY=REPO, GITHUB_OUTPUT=str(output)), \
                        patch.object(release, 'api', side_effect=replies):
                    if digest.startswith('sha256:'):
                        release.resolve(SHA, 'receiver-pir')
                        self.assertIn('run-id=1', output.read_text())
                        self.assertIn(digest, output.read_text())
                    else:
                        with self.assertRaises(ValueError):
                            release.resolve(SHA, 'receiver-pir')

    def test_baseline_ignores_nonancestors_and_uses_current_attempt_jobs(self):
        previous = {**candidate(), 'head_sha': BASE}
        for status in ['ahead', 'diverged', 'behind']:
            with patch.object(release, 'api', side_effect=[{'jobs': complete_jobs('receiver')}, {'status': status}]) as api:
                self.assertEqual(q.baseline(REPO, SHA, 'receiver', [previous]), BASE if status == 'ahead' else None)
                self.assertIn('/attempts/2/jobs?', api.call_args_list[0].args[0])

    def test_selection_uses_each_service_baseline_and_broadens_truncated_diffs(self):
        for files in [[{'filename': 'receiver/services/receiver-indexer/src/main.rs'}],
                      [{'filename': 'Cargo.lock'}], [{'filename': 'unknown.file'}],
                      [{'filename': 'unknown.file'}] * 300]:
            with patch.object(q, 'history', return_value=[]), patch.object(q, 'baseline', return_value=BASE) as base, \
                    patch.object(release, 'api', return_value={'files': files}):
                selected = q.selection(REPO, SHA)
                self.assertEqual(base.call_count, 4)
                self.assertTrue(selected['receiver']['selected'])
                self.assertEqual(selected['enhance']['selected'], len(files) == 300 or files[0]['filename'] != 'receiver/services/receiver-indexer/src/main.rs')

    def test_operations_only_changes_do_not_select_native_suites(self):
        with patch.object(q, 'history', return_value=[]), patch.object(q, 'baseline', return_value=BASE), \
                patch.object(release, 'api', return_value={'files': [{'filename': 'ops/lib/wallet_pir_ops/bundle.py'}]}):
            result = q.selection(REPO, SHA)
        self.assertTrue(result['ops']['selected'])
        self.assertFalse(any(result[name]['selected'] for name in ['enhance', 'receiver', 'transparent']))

    def test_missing_baseline_qualifies_every_family(self):
        with patch.object(q, 'history', return_value=[]):
            self.assertTrue(all(v['selected'] for v in q.selection(REPO, SHA).values()))

    def test_operations_build_runs_no_cargo(self):
        with patch.object(q, 'run') as run:
            q.build('ops')
            run.assert_not_called()


class StorageTests(unittest.TestCase):
    def test_cleanup_retains_recent_open_and_newest_generations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            generations = []
            for i in range(5):
                path = root / 'full-test' / f'{i:016x}'
                path.mkdir(parents=True)
                os.utime(path, (i, i))
                generations.append(path)
            expected = storage.candidates(root, now=200000, references=[str(generations[1] / 'release-fast/deps/x')])
            self.assertEqual(expected, [generations[2], generations[0]])
            self.assertEqual(storage.candidates(root, now=10), [])
            link = root / 'fast'
            link.symlink_to(root / 'full-test', target_is_directory=True)
            self.assertNotIn(link, storage.candidates(root, now=200000))

    def test_last_used_marker_preserves_a_recently_reused_old_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for i in range(4):
                path = root / 'fast' / f'{i:016x}'
                path.mkdir(parents=True)
                os.utime(path, (i, i))
            (root / 'fast' / f'{0:016x}' / '.last-used').touch()
            selected = storage.candidates(root, now=time.time(), keep=1)
            self.assertNotIn(root / 'fast' / f'{0:016x}', selected)


class LauncherTests(unittest.TestCase):
    def test_explicit_permissions_receipt_and_changed_source_refusal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'repo'
            root.mkdir()
            for args in [('init', '-q'), ('config', 'user.email', 'fixture@example.invalid'), ('config', 'user.name', 'Fixture')]:
                subprocess.run(['git', *args], cwd=root, check=True)
            (root / 'rust-toolchain.toml').write_text('[toolchain]\nchannel="1.97.1"\n')
            subprocess.run(['git', 'add', 'rust-toolchain.toml'], cwd=root, check=True)
            subprocess.run(['git', 'commit', '-qm', 'Fixture'], cwd=root, check=True)
            previous = os.umask(0o002)
            try:
                receipt = Path(directory) / 'receipt.json'
                command = [sys.executable, '-c', 'import os; assert os.umask(0) == 0o022']
                self.assertEqual(launch.execute(command, receipt, cwd=root, rust=False), 0)
                self.assertTrue(json.loads(receipt.read_text())['passed'])
                with self.assertRaisesRegex(ValueError, 'already exists'):
                    launch.execute(command, receipt, cwd=root, rust=False)
                command = [sys.executable, '-c', 'open("rust-toolchain.toml","a").write("# changed")']
                changed = Path(directory) / 'changed.json'
                self.assertNotEqual(launch.execute(command, changed, cwd=root, rust=False), 0)
                self.assertFalse(json.loads(changed.read_text())['passed'])
            finally:
                os.umask(previous)


if __name__ == '__main__':
    unittest.main()

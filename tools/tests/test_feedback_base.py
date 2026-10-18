"""A newer docs push must cover changes from a canceled source push."""
from pathlib import Path
from contextlib import redirect_stdout
import io
import os
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'tools/ci'))
import feedback_base


class ComparisonTests(unittest.TestCase):
    def test_main_uses_successful_push_instead_of_event_before(self):
        run = dict(head_sha='a'*40, conclusion='success', event='push', head_branch='main')
        with patch.object(feedback_base, 'api', return_value={'workflow_runs': [run]}) as api:
            self.assertEqual(feedback_base.comparison('push', 'b'*40, 'v/p'), 'a'*40)
            self.assertIn('status=success', api.call_args.args[0])

    def test_absent_history_broadens_and_pr_keeps_its_base(self):
        with patch.object(feedback_base, 'api', return_value={'workflow_runs': []}) as api:
            self.assertEqual(feedback_base.comparison('push', 'b'*40, 'v/p'), '')
            self.assertEqual(feedback_base.comparison('pull_request', 'b'*40, 'v/p'), 'b'*40)
            self.assertEqual(api.call_count, 1)

    def test_bad_provenance_and_transport_failure_cannot_pass(self):
        with patch.object(feedback_base, 'api', side_effect=TimeoutError):
            with self.assertRaises(TimeoutError):
                feedback_base.comparison('push', 'b'*40, 'v/p')

    def test_preparation_uses_main_history_unless_given_a_full_commit(self):
        with patch.object(feedback_base, 'comparison', return_value='a'*40) as compare:
            self.assertEqual(feedback_base.preparation_comparison('', 'v/p', 'c'*40), 'a'*40)
            compare.assert_called_once_with('push', '', 'v/p', 'c'*40)
            compare.reset_mock()
            self.assertEqual(feedback_base.preparation_comparison('b'*40, 'v/p', 'c'*40), 'b'*40)
            compare.assert_not_called()
            for invalid in ('main', '--all', 'b'*39, 'b'*40 + '\n'):
                with self.assertRaisesRegex(ValueError, 'full commit SHA'):
                    feedback_base.preparation_comparison(invalid, 'v/p', 'c'*40)

    def test_preparation_cli_exports_the_same_comparison_it_prints(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / 'env'
            with patch.dict(os.environ, {'GITHUB_EVENT_NAME': 'workflow_dispatch',
                                         'GITHUB_REPOSITORY': 'v/p', 'GITHUB_SHA': 'c'*40,
                                         'GITHUB_ENV': str(destination), 'BASE': ''}), \
                    patch.object(feedback_base, 'comparison', return_value='a'*40) as compare, \
                    redirect_stdout(io.StringIO()) as output:
                feedback_base.main(['--prepare'])
                compare.assert_called_once_with('push', '', 'v/p', 'c'*40)
            self.assertEqual(output.getvalue(), 'a'*40 + '\n')
            self.assertEqual(destination.read_text(), 'FAST_BASE=' + 'a'*40 + '\n')


class PreparedComparisonTests(unittest.TestCase):
    def setUp(self):
        self.previous, self.prepared, self.head = 'a'*40, 'b'*40, 'c'*40
        self.run = dict(id=12, head_sha=self.prepared, status='completed', conclusion='success',
                        event='workflow_dispatch', head_branch='main', run_attempt=2,
                        path='.github/workflows/ci-prepare.yml', head_repository={'full_name': 'v/p'})
        self.job = dict(status='completed', conclusion='success', steps=[
            dict(name=name, status='completed', conclusion='success')
            for name in ('Compile fast units', 'Validate prepared units', 'Validate affected fast units')])
        self.ancestry = ['ahead', 'ahead']

    def api(self, path, **kwargs):
        if '/runs?' in path:
            return {'workflow_runs': [self.run]}
        if '/compare/' in path:
            return {'status': self.ancestry.pop(0)}
        self.assertIn('/attempts/2/jobs?', path)
        return {'jobs': [self.job]}

    def selected(self):
        with patch.object(feedback_base, 'api', side_effect=self.api):
            return feedback_base.prepared_base('v/p', self.head, self.previous)

    def test_completed_full_preparation_replaces_an_older_push_baseline(self):
        self.assertEqual(self.selected(), self.prepared)

    def test_preparation_can_establish_the_first_verified_baseline(self):
        self.previous = ''
        self.assertEqual(self.selected(), self.prepared)

    def test_preparation_without_affected_validation_cannot_establish_a_baseline(self):
        self.job['steps'].pop()
        self.assertEqual(self.selected(), self.previous)

    def test_wrong_repository_branch_workflow_and_unfinished_results_are_refused(self):
        for changes in [dict(head_repository={'full_name': 'fork/p'}), dict(head_branch='feature'),
                        dict(path='.github/workflows/ci.yml'), dict(event='push'),
                        dict(status='in_progress'), dict(conclusion='cancelled'), dict(run_attempt=0)]:
            with self.subTest(changes=changes):
                original = self.run
                self.run = dict(original, **changes)
                self.ancestry = ['ahead', 'ahead']
                self.assertEqual(self.selected(), self.previous)
                self.run = original

    def test_skipped_or_failed_steps_and_jobs_cannot_establish_a_baseline(self):
        for status in ['skipped', 'failure', 'cancelled', 'queued']:
            for kind in ['job', 'step']:
                with self.subTest(status=status, kind=kind):
                    job = self.job
                    self.job = dict(job, conclusion=status) if kind == 'job' else dict(job, steps=[
                        dict(step, conclusion=status) for step in job['steps']])
                    self.ancestry = ['ahead', 'ahead']
                    self.assertEqual(self.selected(), self.previous)
                    self.job = job

    def test_older_and_unrelated_preparations_never_skip_unchecked_commits(self):
        for statuses in [['behind'], ['diverged'], ['ahead', 'diverged'], ['ahead', 'behind']]:
            self.ancestry = statuses
            self.assertEqual(self.selected(), self.previous)
        with patch.object(feedback_base, 'api', return_value={'workflow_runs': [dict(
                head_sha='a'*40, conclusion='success', event='pull_request', head_branch='main')]}):
            with self.assertRaises(ValueError):
                feedback_base.comparison('push', 'b'*40, 'v/p')

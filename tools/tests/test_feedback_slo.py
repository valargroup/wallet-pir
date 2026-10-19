"""Fast feedback cannot be qualified by docs-only runs, skipped tests, or cold builds."""
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import contextlib
import io
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'tools/ci'))
import timings


def row(number, category='shared', seconds=120, **changes):
    families = [] if category in ('docs', 'ops', 'ci') else (
        ['transparent', 'enhance', 'receiver'] if category == 'shared' else [category])
    return dict(run=number, sha='a'*40, event='push', branch='main', complete=True,
                conclusion='success', dispatch_seconds=seconds,
                jobs=[{'name': 'Fast checks', 'cache_records': [
                    {'type': 'CI_SELECTION', 'category': category, 'families': families},
                    {'type': 'CI_CARGO_ARTIFACTS', 'third_party': {'compiled': 0}}]}], **changes)


class FeedbackTests(unittest.TestCase):
    def test_final_timing_check_reports_before_failing_at_the_limit(self):
        for seconds, fails in [(240, False), (299, False), (300, True), (360, True)]:
            result = {'jobs': [{'name': 'Fast checks', 'job_seconds': seconds,
                                'reused_from_previous_attempt': False}]}
            with patch.object(sys, 'argv', ['timings.py', '--run', '1', '--check-fast-seconds', '300']), patch.object(timings, 'report', return_value=result), patch.dict(timings.os.environ, {'GITHUB_STEP_SUMMARY': ''}), contextlib.redirect_stdout(io.StringIO()) as output:
                if fails:
                    with self.assertRaises(SystemExit):
                        timings.main()
                else:
                    timings.main()
                self.assertIn('job_seconds', output.getvalue())

    def samples(self):
        return [row(i, ['shared', 'docs', 'ops'][i % 3]) for i in range(20)]

    def test_timeout_cancellation_and_rerun_do_not_erase_failed_validation(self):
        for changes in [dict(conclusion='cancelled', dispatch_seconds=320), dict(attempt=2)]:
            rows = self.samples()
            rows[0].update(changes)
            result = timings.feedback_slo(rows)
            self.assertFalse(result['accepted'])
            self.assertIn(rows[0]['run'], result['failed_or_incomplete_runs'])

    def test_twenty_representative_warm_runs_are_needed(self):
        self.assertFalse(timings.feedback_slo(self.samples()[:19])['accepted'])
        self.assertTrue(timings.feedback_slo(self.samples())['accepted'])
        self.assertFalse(timings.feedback_slo([row(i, 'docs') for i in range(20)])['accepted'])

    def test_cold_missing_or_workflow_only_evidence_cannot_satisfy_the_target(self):
        for category in ['ci', 'shared']:
            rows = [row(i, category) for i in range(20)]
            for r in rows:
                r['jobs'][0]['cache_records'][-1]['third_party']['compiled'] = 1
            self.assertEqual(timings.feedback_slo(rows)['sample_count'], 0)
        rows = self.samples()
        rows[0]['jobs'][0]['cache_records'] = []
        self.assertFalse(timings.feedback_slo(rows)['accepted'])

    def test_a_shared_library_without_service_consumers_still_needs_warm_evidence(self):
        rows = self.samples()
        selection = rows[0]['jobs'][0]['cache_records'][0]
        selection.update(families=[], package_count=1)
        rows[0]['jobs'][0]['cache_records'].pop()
        self.assertFalse(timings.feedback_slo(rows)['accepted'])

    def test_failures_remain_visible_and_cancelled_runs_do_not_count(self):
        rows = self.samples()
        rows[0]['conclusion'] = 'failure'
        result = timings.feedback_slo(rows)
        self.assertFalse(result['accepted'])
        self.assertEqual(result['failed_or_incomplete_runs'], [0])
        rows[0]['conclusion'] = 'cancelled'
        self.assertEqual(timings.feedback_slo(rows)['sample_count'], 19)

    def test_slow_tail_and_non_main_runs_are_not_hidden(self):
        rows = self.samples()
        rows[0]['dispatch_seconds'] = rows[1]['dispatch_seconds'] = 301
        self.assertFalse(timings.feedback_slo(rows)['accepted'])
        rows = self.samples()
        rows[0]['branch'] = 'feature'
        self.assertEqual(timings.feedback_slo(rows)['sample_count'], 19)

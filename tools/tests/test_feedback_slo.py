"""Fast feedback cannot be qualified by docs-only runs, skipped tests, or cold builds."""
from pathlib import Path
import sys
import unittest
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
    def samples(self):
        return [row(i, ['shared', 'docs', 'ops'][i % 3]) for i in range(20)]

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
        rows[0]['dispatch_seconds'] = rows[1]['dispatch_seconds'] = 181
        self.assertFalse(timings.feedback_slo(rows)['accepted'])
        rows = self.samples()
        rows[0]['branch'] = 'feature'
        self.assertEqual(timings.feedback_slo(rows)['sample_count'], 19)

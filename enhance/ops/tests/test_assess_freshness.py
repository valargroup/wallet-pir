"""Freshness bounds distinguish a proven pass, definite failure, and uncertainty."""
import importlib.util
from pathlib import Path
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/assess-freshness.py'
spec = importlib.util.spec_from_file_location('assess_freshness', SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
SECOND = 10**9


def trace(published_at, *, never=False):
    rows = []
    for second in range(0, 341, 10):
        tip = 100 if second == 0 else 101
        anchor = 100 if never or second < published_at else 101
        rows.append({'wall_time_ns': second * SECOND, 'node_tip': tip,
                     'published_anchor': anchor, 'generation': 1,
                     'ingestion_failed': False, 'publication_blocked': False, 'error': None})
    return rows


class FreshnessAssessmentTests(unittest.TestCase):
    def assess(self, rows):
        return module.assess(rows, 10 * SECOND, 30 * SECOND)

    def test_timely_coverage_has_conservative_bound(self):
        result = self.assess(trace(20))
        self.assertEqual(result['status'], 'freshness_observation_passed')
        self.assertEqual(result['sampled_tip_advances'], 1)
        self.assertEqual(result['max_conservative_lag_seconds'], 20)

    def test_tip_still_uncovered_after_deadline_is_definite_failure(self):
        result = self.assess(trace(330))
        self.assertEqual(result['status'], 'freshness_failed')
        self.assertIn('definite_freshness_failure', result['findings'])

    def test_boundary_uncertainty_cannot_be_called_a_pass(self):
        result = self.assess(trace(310))
        self.assertEqual(result['status'], 'evidence_incomplete')
        self.assertIn('lag_bound_inconclusive', result['findings'])

    def test_missing_samples_and_initial_backlog_are_exposed(self):
        rows = [row for row in trace(20) if row['wall_time_ns'] not in (40 * SECOND, 50 * SECOND)]
        rows[0]['node_tip'] = 101
        result = self.assess(rows)
        self.assertIn('sampling_gap', result['findings'])
        self.assertIn('initial_uncovered_tip', result['findings'])

    def test_missing_failure_flags_cannot_look_healthy(self):
        rows = trace(20)
        del rows[1]['ingestion_failed']
        with self.assertRaisesRegex(ValueError, 'invalid publication state'):
            self.assess(rows)


if __name__ == '__main__':
    unittest.main()

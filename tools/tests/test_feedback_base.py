"""A newer docs push must cover changes from a canceled source push."""
from pathlib import Path
import sys
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
        with patch.object(feedback_base, 'api', return_value={'workflow_runs': [dict(
                head_sha='a'*40, conclusion='success', event='pull_request', head_branch='main')]}):
            with self.assertRaises(ValueError):
                feedback_base.comparison('push', 'b'*40, 'v/p')

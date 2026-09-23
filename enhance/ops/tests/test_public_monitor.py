import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('monitor_public', Path(__file__).parents[1] / 'scripts/monitor-public.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class PublicMonitorTests(unittest.TestCase):
    def report(self):
        return dict(protocol='ironwood-enhance-pir-v6', exact_answer_oracle=True,
                    seconds=300.1, offered_qps=0.2, completed=60, succeeded=60,
                    incorrect_answers=0, unstarted_arrivals=0, errors={},
                    warmup_errors={}, warmup_incorrect_answers=0,
                    successful_p99_ms=250, successful_scheduled_p99_ms=275,
                    scheduled_p99_ms=275)

    def test_healthy_window(self):
        self.assertEqual(module.assess(self.report(), 0.2, 300), [])

    def test_wrong_slow_and_lost_queries_fail(self):
        cases = [dict(incorrect_answers=1, succeeded=59),
                 dict(successful_scheduled_p99_ms=1001),
                 dict(completed=59, succeeded=59, unstarted_arrivals=1),
                 dict(succeeded=59, errors={'http_503': 1}),
                 dict(warmup_errors={'timeout': 1})]
        for changes in cases:
            with self.subTest(changes=changes):
                self.assertTrue(module.assess(self.report() | changes, 0.2, 300))

    def test_missing_or_inconsistent_evidence_fails(self):
        cases = [dict(seconds=299.9), dict(exact_answer_oracle=False),
                 dict(completed=59), dict(successful_p99_ms=None),
                 dict(scheduled_p99_ms=float('nan'))]
        for changes in cases:
            with self.subTest(changes=changes):
                self.assertTrue(module.assess(self.report() | changes, 0.2, 300))
        with self.assertRaises(ValueError):
            module.assess(self.report() | dict(errors={'http_503': True}), 0.2, 300)

    def test_wall_clock_and_window_gap_are_checked(self):
        previous = dict(started_ns=0, finished_ns=300_000_000_000)
        good = dict(started_ns=301_000_000_000, finished_ns=601_000_000_000)
        self.assertEqual(module.assess_timing(good, 300, previous), [])
        self.assertIn('short_wall_window', module.assess_timing(
            good | dict(finished_ns=600_000_000_000), 300, previous))
        self.assertIn('observation_gap_invalid_or_over_30s', module.assess_timing(
            good | dict(started_ns=331_000_000_000, finished_ns=631_000_000_000), 300, previous))


if __name__ == '__main__':
    unittest.main()

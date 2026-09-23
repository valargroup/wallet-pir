import importlib.util
from pathlib import Path
import unittest
import tempfile

spec = importlib.util.spec_from_file_location('qualify_public', Path(__file__).parents[1] / 'scripts/qualify-public.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class PublicQualificationTests(unittest.TestCase):
    def report(self):
        return dict(protocol=module.PROTOCOL, exact_answer_oracle=True, seconds=1800.1,
                    offered_qps=4, completed=7200, succeeded=7200, incorrect_answers=0,
                    unstarted_arrivals=0, errors={}, warmup_errors={}, warmup_correct_answers=100,
                    warmup_incorrect_answers=0, successful_p99_ms=200,
                    successful_scheduled_p99_ms=220, scheduled_p99_ms=220)

    def test_clean_report(self):
        self.assertEqual(module.assess(self.report(), 4, 1800), [])

    def test_rejections_missing_arrivals_and_warmup_count(self):
        for changes in [dict(succeeded=7180, errors={'http_429': 20}),
                        dict(completed=7180, succeeded=7180, unstarted_arrivals=20),
                        dict(warmup_errors={'http_502': 20})]:
            with self.subTest(changes=changes):
                report = self.report() | changes
                self.assertIn('availability_below_99_9_percent', module.assess(report, 4, 1800))

    def test_incomplete_and_slow_reports_cannot_pass(self):
        for changes in [dict(successful_p99_ms=None), dict(successful_p99_ms=1200),
                        dict(successful_scheduled_p99_ms=1200), dict(scheduled_p99_ms=float('nan')),
                        dict(seconds=10), dict(completed=7199), dict(protocol='v5'),
                        dict(exact_answer_oracle=False), dict(warmup_incorrect_answers=1)]:
            with self.subTest(changes=changes):
                self.assertTrue(module.assess(self.report() | changes, 4, 1800))

    def test_bursts_allow_rejections_but_never_wrong_answers(self):
        report = self.report() | dict(offered_qps=None, succeeded=7000, errors={'http_429': 200})
        self.assertEqual(module.assess(report, None, 1800, burst=True), [])
        report.update(succeeded=6999, incorrect_answers=1)
        self.assertIn('incorrect_answers', module.assess(report, None, 1800, burst=True))

    def test_changed_binary_or_oracle_cannot_mix_campaign_results(self):
        with tempfile.TemporaryDirectory() as directory:
            binary, oracle = Path(directory) / 'binary', Path(directory) / 'oracle'
            binary.write_bytes(b'candidate')
            oracle.write_bytes(b'records')
            manifest = dict(binary_sha256=module.digest(binary), oracle_sha256=module.digest(oracle))
            module.verify_inputs(binary, oracle, manifest)
            for path in (binary, oracle):
                before = path.read_bytes()
                path.write_bytes(b'changed')
                with self.assertRaises(ValueError):
                    module.verify_inputs(binary, oracle, manifest)
                path.write_bytes(before)

    def test_invalid_counters_are_rejected(self):
        for changes in [dict(completed=-1), dict(errors={'http_502': True})]:
            with self.assertRaises(ValueError):
                module.assess(self.report() | changes, 4, 1800)


if __name__ == '__main__':
    unittest.main()

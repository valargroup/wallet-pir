import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('cuda_smoke', Path(__file__).resolve().parents[1] / 'scripts/cuda-smoke.py')
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


class SmokeAcceptance(unittest.TestCase):
    def report(self):
        return {'protocol': smoke.PROTOCOL, 'exact_answer_oracle': True,
                'completed': 5, 'succeeded': 5, 'incorrect_answers': 0,
                'warmup_incorrect_answers': 0, 'unstarted_arrivals': 0,
                'errors': {}, 'warmup_errors': {}}

    def test_fixture_has_two_distinct_replicas_and_coordinator_port(self):
        with patch.object(smoke, 'port', side_effect=[8101, 8101, 8102, 8103]):
            workers, origin, inventory = smoke.fixture_endpoints()
        self.assertEqual(workers, ['http://127.0.0.1:8101', 'http://127.0.0.1:8102'])
        self.assertEqual(origin, 'http://127.0.0.1:8103')
        replicas = inventory['groups'][0]['replicas']
        self.assertEqual(len(replicas), 2)
        self.assertEqual([entry['url'] for entry in replicas], workers)
        self.assertEqual(len({entry['name'] for entry in replicas}), 2)

    def test_each_worker_requires_published_native_cuda_health(self):
        good = {'protocol': smoke.PROTOCOL, 'matvec': {'matvec_backend': 'cuda', 'cuda_device': 0},
                'published': [1]}
        smoke.validate_worker_health(good, 0)
        for bad in [{**good, 'published': []}, {**good, 'protocol': 'wrong'},
                    {**good, 'matvec': {'matvec_backend': 'cpu', 'cuda_device': None}}]:
            with self.assertRaises(ValueError):
                smoke.validate_worker_health(bad, 0)

    def test_exact_report_passes(self):
        smoke.validate_report(self.report())

    def test_fail_closed_report(self):
        for key, value in [('protocol', 'cpu-protocol'), ('completed', 0),
                           ('succeeded', 4), ('incorrect_answers', 1),
                           ('warmup_incorrect_answers', 1), ('unstarted_arrivals', 1),
                           ('errors', {'transport': 1}), ('warmup_errors', {'http_503': 1}),
                           ('exact_answer_oracle', False)]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                report = self.report()
                report[key] = value
                smoke.validate_report(report)

    def test_counter_requires_one_finite_value(self):
        self.assertEqual(smoke.counter(smoke.METRIC + ' 7\n'), 7)
        for value in ['', smoke.METRIC + ' NaN', smoke.METRIC + ' -1',
                      smoke.METRIC + ' 1\n' + smoke.METRIC + ' 2']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                smoke.counter(value)


if __name__ == '__main__':
    unittest.main()

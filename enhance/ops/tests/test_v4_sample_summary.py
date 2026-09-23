"""Trace summaries preserve gaps and failures instead of certifying hardware."""
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('sample_summary', Path(__file__).resolve().parents[1] / 'scripts/summarize-v4-samples.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def sample(second):
    return {'sample': dict(kind='enhance-v4-hardware-sample', version=2, error=None,
        wall_time_ns=second * 10**9, host_id_sha256='a' * 64, boot_id='boot',
        main_pid=1, main_start_ticks=2, source_revision='b' * 40, binary_sha256='c' * 64,
        process_memory=[{'pid': 1, 'bytes': {'Rss': 100}}], disk_free_bytes=1000,
        runtime_metrics={'values': {'memory_sample_available': 1}, 'state_consistent': True},
        **{'memory.stat': {'kernel': 20}, 'cgroup_bytes': {'memory.current': 150,
           'memory.peak': 200, 'memory.max': 1000, 'memory.swap.current': 0},
           'memory.events': {'high': 1, 'max': 0, 'oom': 0, 'oom_kill': 0},
           'host_swap_pages': {'pswpin': 10, 'pswpout': 20}})}


class SummaryTests(unittest.TestCase):
    def summarize(self, records, **kwargs):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'samples.jsonl'
            raw = ''.join(json.dumps(record) + '\n' for record in records).encode()
            path.write_bytes(raw)
            result = module.summarize(path, **kwargs)
            self.assertEqual(result['trace_sha256'], hashlib.sha256(raw).hexdigest())
            self.assertEqual(result['qualification'], 'unqualified')
            return result

    def test_healthy_trace_with_window(self):
        result = self.summarize([sample(1), sample(2), sample(3)], window=[10**9, 3 * 10**9])
        self.assertEqual(result['findings'], [])
        self.assertEqual(result['max_sampled_rss_plus_kernel_bytes'], 120)
        self.assertEqual(result['counter_deltas']['memory.events']['high'], 0)

    def test_gap_and_error_are_preserved(self):
        error = {'sample': {'error': 'collection_failed', 'wall_time_ns': 2 * 10**9}}
        result = self.summarize([sample(1), error, sample(100)])
        self.assertEqual(result['error_samples'], 1)
        self.assertEqual(result['max_gap_seconds'], 98)
        self.assertIn('sampling_gap', result['findings'])
        self.assertIn('sample_errors', result['findings'])

    def test_restart_counter_reset_and_guard_violation(self):
        second = sample(2)
        second['sample']['main_pid'] = 3
        second['sample']['process_memory'] = [{'pid': 3, 'bytes': {'Rss': module.GUARD}}]
        second['sample']['memory.events']['high'] = 0
        result = self.summarize([sample(1), second])
        self.assertIn('host_process_or_binary_changed', result['findings'])
        self.assertIn('counters_reset_or_changed', result['findings'])
        self.assertIn('observed_resident_guard_violation', result['findings'])
        self.assertEqual(result['sampled_guard_headroom_bytes'], -20)

    def test_swap_oom_and_missing_window(self):
        second = sample(2)
        second['sample']['memory.events']['oom_kill'] = 1
        second['sample']['cgroup_bytes']['memory.swap.current'] = 200
        result = self.summarize([sample(1), second], window=[0, 3 * 10**9])
        for finding in ('worker_swap_observed', 'worker_oom_event', 'measurement_window_not_covered'):
            self.assertIn(finding, result['findings'])

    def test_empty_capture_cannot_supply_memory_headroom(self):
        result = self.summarize([])
        self.assertIn('no_valid_samples', result['findings'])
        self.assertIsNone(result['sampled_guard_headroom_bytes'])

    def test_malformed_and_partial_evidence_rejected(self):
        for raw in (b'{"sample":{},"sample":{}}\n', b'{"x":NaN}\n', b'{}', b'x' * (1024 * 1024 + 1)):
            with self.subTest(raw=raw[:40]), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'samples.jsonl'
                path.write_bytes(raw)
                with self.assertRaises(ValueError):
                    module.summarize(path)

    def test_nonmonotonic_clock(self):
        result = self.summarize([sample(2), sample(1)])
        self.assertIn('nonmonotonic_wall_clock', result['findings'])

    def test_pressure_uses_delta_and_preserves_peak(self):
        first, second = sample(1), sample(2)
        first['sample']['memory.pressure'] = {'full': {'total': 1000, 'avg10': 1.5}}
        second['sample']['memory.pressure'] = {'full': {'total': 1250, 'avg10': 0.1}}
        result = self.summarize([first, second])
        self.assertEqual(result['pressure_total_delta_us'], {'full': 250})
        self.assertEqual(result['max_pressure_avg10'], {'full': 1.5})


if __name__ == '__main__':
    unittest.main()

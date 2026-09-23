"""Synthetic evidence exercises audit gates; it never qualifies real hardware."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

OPS = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('v4_assessment', OPS / 'scripts/assess-v4-campaign.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def write(path, value):
    path.write_text(json.dumps(value))


def fixture(root):
    workload = root / 'workload'
    workload.mkdir()
    stages = ['loan_growth', 'return', 'owned_8k', 'owned_16k', 'owned_near_32k', 'rewind_return']
    publications = [{'manifest': {'generation': i + 2}, 'elapsed_seconds': 72 * (i + 1), 'publication_ms': 1,
        'stage': stages[i % 6], 'placement': {'groups': [{'role': 'ACTIVE', 'shards': 5}],
        'published_replica_counts': {'g0': 2}, 'retained_generations': list(range(i + 2, max(0, i - 3), -1))}}
        for i in range(300)]
    path = workload / 'publications.jsonl'
    path.write_text(''.join(json.dumps(v) + '\n' for v in publications))
    report = {'status': 'recorded', 'profile': 'active', 'measurement_seconds': 21600, 'publications': 300,
        'background_correct_answers': 1000, 'background_errors': 0, 'expired_refreshes': 296,
        'exact_boundary_and_retained_probes': 300, 'background_p99_ms': 100, 'started_wall_ns': 10**9,
        'finished_wall_ns': 21601 * 10**9, 'publications_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
        'workload_host_id_sha256': '3' * 64, 'max_fixture_publication_ms': 1, 'binary_sha256': 'a' * 64}
    write(workload / 'exercise.json', report)
    limits = {'memory_high_bytes': 7 * 1024**3, 'memory_max_bytes': 15 * 1024**3 // 2, 'memory_swap_max_bytes': 2 * 1024**3}
    expected = {'binary_sha256': 'a' * 64, 'source_revision': 'b' * 40, 'bootstrap_manifest_sha256': 'c' * 64,
        'boot_id': 'fixture-boot', 'memory_total_bytes': 8 * 1024**3, 'limits': limits}
    targets = [{'resource_id': str(i), 'name': f'r{i}', 'private_ipv4': f'10.0.0.{i}', 'expected': copy.deepcopy(expected)} for i in (1, 2)]
    hardware = root / 'hardware.jsonl'
    with hardware.open('w') as handle:
        for second in range(0, 21621, 10):
            for target in targets:
                sample = {k: expected[k] for k in ('binary_sha256', 'source_revision', 'bootstrap_manifest_sha256', 'boot_id')}
                sample.update(error=None, version=2, hostname=target['name'], worker_private_ipv4=target['private_ipv4'],
                    host_memory_bytes={'MemTotal': expected['memory_total_bytes']},
                    cgroup_bytes={'memory.high': limits['memory_high_bytes'], 'memory.max': limits['memory_max_bytes'],
                                  'memory.swap.max': limits['memory_swap_max_bytes'], 'memory.peak': 10000},
                    runtime_metrics={'values': {'memory_sample_available': 1}, 'state_consistent': True},
                    host_id_sha256=target['resource_id'] * 64, main_pid=123, main_start_ticks=100,
                    wall_time_ns=second * 10**9, process_memory=[{'pid': 123, 'bytes': {'Rss': 1024}}])
                sample['memory.events'] = {'high': 0, 'max': 0, 'oom': 0, 'oom_kill': 0}
                sample['memory.stat'] = {'kernel': 1024}
                handle.write(json.dumps({'resource_id': target['resource_id'], 'worker': target['name'],
                    'scheduled_offset_ns': second * 10**9, 'sample': sample}) + '\n')
    config = {'revision': 'b' * 40}
    manifest = {'status': 'recorded', 'sample_version': 2, 'targets': targets, 'seconds_requested': 21620,
        'elapsed_ns': 21620 * 10**9, 'interval_seconds': 10, 'controller_host_id_sha256': '3' * 64,
        'trace_sha256': hashlib.sha256(hardware.read_bytes()).hexdigest(),
        'bootstrap_config_digest': module.observer.pair_module.journal_module.digest(config), 'records': 4326,
        'error_samples': 0, 'unavailable_memory_samples': 0, 'inconsistent_runtime_samples': 0}
    return workload, (manifest, hardware, targets, config)


class AssessmentTests(unittest.TestCase):
    def test_complete_synthetic_metadata_is_never_a_qualification_receipt(self):
        with tempfile.TemporaryDirectory() as temp:
            workload, observation = fixture(Path(temp))
            result = module.assess(workload, [observation])
            self.assertEqual(result['failures'], [])
            self.assertEqual(result['status'], 'evidence_checks_passed')
            self.assertEqual(result['qualification'], 'unqualified')
            self.assertTrue(result['unproven_gates'])
            self.assertEqual(len(result['worker_statistics']), 2)

    def test_workload_must_identify_a_non_worker_host(self):
        with tempfile.TemporaryDirectory() as temp:
            workload, observation = fixture(Path(temp))
            report = module.read(workload / 'exercise.json')
            for identity, failure in [(None, 'missing_workload_host_identity'),
                                      ('1' * 64, 'workload_runs_on_worker_host')]:
                report['workload_host_id_sha256'] = identity
                write(workload / 'exercise.json', report)
                self.assertIn(failure, module.assess(workload, [observation])['failures'])

    def test_short_smoke_and_incomplete_publications_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            workload, observation = fixture(Path(temp))
            report = module.read(workload / 'exercise.json')
            report.update(profile='smoke', measurement_seconds=10, publications=6, background_errors=1)
            write(workload / 'exercise.json', report)
            result = module.assess(workload, [observation])
            self.assertTrue({'requires_full_size_profile', 'workload_shorter_than_six_hours',
                'fewer_than_300_publications', 'query_correctness_or_errors', 'publication_count_differs'} <= set(result['failures']))

    def test_mutated_trace_and_duplicate_json_keys_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            workload, observation = fixture(Path(temp))
            with observation[1].open('a') as handle:
                handle.write('{}\n')
            with self.assertRaises((ValueError, KeyError)):
                module.assess(workload, [observation])
            with self.assertRaises(ValueError):
                module.decode('{"status":1,"status":2}')

    def test_missing_slot_artifact_drift_oom_and_guard_violation_are_detected(self):
        with tempfile.TemporaryDirectory() as temp:
            workload, observation = fixture(Path(temp))
            manifest, path, targets, config = observation
            lines = path.read_text().splitlines()
            changed = json.loads(lines[100])
            changed['sample']['binary_sha256'] = 'f' * 64
            changed['sample']['process_memory'][0]['bytes']['Rss'] = 7 * 1024**3
            changed['sample']['memory.events']['oom'] = 1
            lines[100] = json.dumps(changed)
            del lines[102]
            path.write_text('\n'.join(lines) + '\n')
            manifest['trace_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
            result = module.assess(workload, [(manifest, path, targets, config)])
            self.assertTrue({'trace_bootstrap_binding_differs', 'observed_resident_guard_violation',
                'worker_oom_event', 'memory_counters_reset', 'observation_slots_missing_or_extra'} <= set(result['failures']))

    def test_same_host_and_missing_off_host_identity_fail(self):
        with tempfile.TemporaryDirectory() as temp:
            workload, observation = fixture(Path(temp))
            observation[0]['controller_host_id_sha256'] = '1' * 64
            result = module.assess(workload, [observation])
            self.assertIn('workers_or_observer_share_host', result['failures'])
            observation[0]['controller_host_id_sha256'] = None
            result = module.assess(workload, [observation])
            self.assertIn('missing_off_host_observer_identity', result['failures'])

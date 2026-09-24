"""A direct worker trace must bind the real six-hour exercise and both hosts."""
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


OPS = Path(__file__).resolve().parents[1]


def load(path):
    spec = importlib.util.spec_from_file_location(path.stem.replace('-', '_'), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


direct = load(OPS / 'scripts/assess-direct-campaign.py')
legacy = load(Path(__file__).with_name('test_campaign_assessment.py'))


def fixture(root):
    workload, _ = legacy.fixture(root)
    workers = []
    for number in (1, 2):
        name = f'enhance-pir-worker-0{number}'
        directory = root / f'observation-{number}'
        directory.mkdir()
        policy_path = root / f'policy-{number}.json'
        policy = {'kind': 'enhance-direct-worker-sampling-v1', 'worker_name': name,
                  'revision': 'b' * 40, 'binary_sha256': 'a' * 64,
                  'manifest_sha256': 'c' * 64, 'private_ipv4': f'10.0.0.{number}',
                  'private_port': 8091, 'sealed_shards': 6, 'campaign_profile': 'active',
                  'data_dir': f'/srv/enhance-pir-v6/qualification/active-worker-{number}',
                  'limits': {'memory_high_bytes': 7 * 1024**3,
                             'memory_max_bytes': 15 * 1024**3 // 2,
                             'memory_swap_max_bytes': 2 * 1024**3}}
        policy_path.write_text(json.dumps(policy))
        digest = hashlib.sha256(policy_path.read_bytes()).hexdigest()
        trace = directory / 'samples.jsonl'
        with trace.open('w') as handle:
            for second in range(21602):
                sample = {'kind': 'enhance-hardware-sample', 'version': 2, 'error': None,
                          'wall_time_ns': second * 10**9, 'host_id_sha256': str(number) * 64,
                          'boot_id': 'fixture-boot', 'main_pid': 123, 'main_start_ticks': 100,
                          'source_revision': policy['revision'], 'binary_sha256': policy['binary_sha256'],
                          'identity_source': 'direct-release-policy', 'identity_policy_sha256': digest,
                          'worker_data_dir': policy['data_dir'], 'hostname': name,
                          'bootstrap_manifest_sha256': policy['manifest_sha256'],
                          'worker_private_ipv4': policy['private_ipv4'], 'worker_private_port': 8091,
                          'service': {'ActiveState': 'active', 'NRestarts': '0', 'MainPID': '123'},
                          'cgroup_bytes': {'memory.current': 4096, 'memory.peak': 8192,
                                           'memory.high': policy['limits']['memory_high_bytes'],
                                           'memory.max': policy['limits']['memory_max_bytes'],
                                           'memory.swap.current': 0,
                                           'memory.swap.max': policy['limits']['memory_swap_max_bytes']},
                          'process_memory': [{'pid': 123, 'bytes': {'Rss': 1024}}],
                          'memory.stat': {'kernel': 1024},
                          'memory.events': {'high': 0, 'max': 0, 'oom': 0, 'oom_kill': 0},
                          'host_swap_pages': {'pswpin': 0, 'pswpout': 0},
                          'disk_free_bytes': 32 * 1024**3,
                          'runtime_metrics': {'values': {'memory_sample_available': 1},
                                              'state_consistent': True},
                          'worker': {'protocol': 'ironwood-enhance-pir-v6'}}
                handle.write(json.dumps({'elapsed_seconds': second, 'sample': sample}) + '\n')
        manifest = {'status': 'recorded', 'direct_policy_sha256': digest,
                    'interval_seconds': 1, 'seconds_requested': 21601,
                    'started_wall_ns': 0, 'finished_wall_ns': 21601 * 10**9,
                    'errors': 0, 'samples': 21602,
                    'samples_sha256': hashlib.sha256(trace.read_bytes()).hexdigest()}
        (directory / 'manifest.json').write_text(json.dumps(manifest))
        workers.append((directory, policy_path))
    return workload, workers


class DirectCampaignTests(unittest.TestCase):
    def test_complete_direct_traces_are_still_unqualified(self):
        with tempfile.TemporaryDirectory() as temp:
            workload, workers = fixture(Path(temp))
            result = direct.assess(workload, workers)
            self.assertEqual(result['status'], 'evidence_checks_passed')
            self.assertEqual(result['qualification'], 'unqualified')
            self.assertTrue(result['unproven_gates'])

    def test_changed_trace_or_isolation_policy_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            workload, workers = fixture(Path(temp))
            directory, policy = workers[0]
            manifest_path = directory / 'manifest.json'
            manifest = json.loads(manifest_path.read_text())
            manifest['samples_sha256'] = '0' * 64
            manifest_path.write_text(json.dumps(manifest))
            self.assertIn('worker_trace_digest_or_count_differs', direct.assess(workload, workers)['failures'])
            manifest['samples_sha256'] = hashlib.sha256((directory / 'samples.jsonl').read_bytes()).hexdigest()
            manifest_path.write_text(json.dumps(manifest))
            altered = json.loads(policy.read_text())
            altered['data_dir'] = '/srv/enhance-pir-v6/worker'
            policy.write_text(json.dumps(altered))
            failures = direct.assess(workload, workers)['failures']
            self.assertIn('isolated_direct_policy_missing', failures)
            self.assertIn('worker_observation_not_recorded_or_bound', failures)
            altered['data_dir'] = '/srv/enhance-pir-v6/qualification/active-worker-1'
            policy.write_text(json.dumps(altered))
            manifest['direct_policy_sha256'] = hashlib.sha256(policy.read_bytes()).hexdigest()
            trace = directory / 'samples.jsonl'
            lines = trace.read_text().splitlines()
            first = json.loads(lines[0])
            first['sample']['worker_data_dir'] = '/srv/enhance-pir-v6/worker'
            lines[0] = json.dumps(first)
            trace.write_text('\n'.join(lines) + '\n')
            manifest['samples_sha256'] = hashlib.sha256(trace.read_bytes()).hexdigest()
            manifest_path.write_text(json.dumps(manifest))
            self.assertIn('worker_sample_policy_binding_differs', direct.assess(workload, workers)['failures'])


if __name__ == '__main__':
    unittest.main()

"""Off-host collection retains transport failures and missed deadlines explicitly."""
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/observe-pair.py'
spec = importlib.util.spec_from_file_location('observation', SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
EXPECTED = {'binary_sha256': 'b' * 64, 'source_revision': 'c' * 40, 'bootstrap_manifest_sha256': 'a' * 64,
            'boot_id': 'fixture-boot', 'memory_total_bytes': 8 * 1024**3,
            'limits': {'memory_high_bytes': 7 * 1024**3, 'memory_max_bytes': 15 * 1024**3 // 2, 'memory_swap_max_bytes': 2 * 1024**3}}
TARGETS = [{'name': 'r1', 'resource_id': '101', 'private_ipv4': '10.0.0.1', 'expected': EXPECTED},
           {'name': 'r2', 'resource_id': '102', 'private_ipv4': '10.0.0.2', 'expected': EXPECTED}]
CONFIG = {'manifest_sha256': 'a' * 64}


class Clock:
    def __init__(self):
        self.now = 0

    def read(self):
        return self.now

    def sleep(self, seconds):
        self.now += round(seconds * 1e9)


class Remote:
    def __init__(self, *, fail=False, clock=None, memory_available=1, consistent=True, target=TARGETS[0]):
        self.target = target
        self.memory_available = memory_available
        self.consistent = consistent
        self.fail = fail
        self.clock = clock
        self.calls = 0

    def command(self, args, timeout):
        self.calls += 1
        if self.clock and self.calls == 1:
            self.clock.sleep(6)
        if self.fail:
            raise OSError('private connection detail')
        value = {'kind': 'enhance-hardware-sample', 'version': 2, 'error': None,
                 'boot_id': 'fixture-boot', 'host_id_sha256': 'fixture-host', 'binary_sha256': 'b' * 64,
                 'main_pid': 123, 'main_start_ticks': 100, 'process_memory': [], 'cgroup_bytes': {'memory.high': EXPECTED['limits']['memory_high_bytes'], 'memory.max': EXPECTED['limits']['memory_max_bytes'], 'memory.swap.max': EXPECTED['limits']['memory_swap_max_bytes']},
                 'memory.events': {}, 'service': {}, 'worker': {}, 'worker_after_metrics': {},
                 'hostname': self.target['name'], 'worker_private_ipv4': self.target['private_ipv4'],
                 'source_revision': EXPECTED['source_revision'], 'bootstrap_manifest_sha256': EXPECTED['bootstrap_manifest_sha256'],
                 'host_memory_bytes': {'MemTotal': EXPECTED['memory_total_bytes']},
                 'runtime_metrics': {'state_consistent': self.consistent, 'values': {'memory_sample_available': self.memory_available}}}
        return json.dumps(value).encode()


class ObservationTests(unittest.TestCase):
    def test_transport_errors_are_retained_and_trace_is_bound(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as temp, patch.object(module.time, 'monotonic_ns', side_effect=clock.read), \
                patch.object(module.time, 'sleep', side_effect=clock.sleep):
            out = Path(temp) / 'run'
            result = module.observe(TARGETS, CONFIG, [Remote(), Remote(fail=True)], out, 2, 1)
            records = [json.loads(line) for line in (out / 'hardware.jsonl').read_text().splitlines()]
            self.assertEqual(len(records), 6)
            self.assertEqual(result['records'], 6)
            self.assertEqual(result['error_samples'], 3)
            self.assertEqual(result['qualification'], 'unqualified')
            self.assertNotIn('passed', result)
            self.assertEqual(result['trace_sha256'], hashlib.sha256((out / 'hardware.jsonl').read_bytes()).hexdigest())
            self.assertNotIn('private connection detail', (out / 'hardware.jsonl').read_text())
            self.assertEqual((out / 'hardware.jsonl').stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                module.observe(TARGETS, CONFIG, [Remote(), Remote()], out, 2, 1)

    def test_slow_collection_does_not_silently_drop_scheduled_slots(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as temp, patch.object(module.time, 'monotonic_ns', side_effect=clock.read), \
                patch.object(module.time, 'sleep', side_effect=clock.sleep):
            out = Path(temp) / 'run'
            result = module.observe(TARGETS, CONFIG, [Remote(clock=clock), Remote(target=TARGETS[1])], out, 8, 1)
            records = [json.loads(line) for line in (out / 'hardware.jsonl').read_text().splitlines()]
            self.assertEqual(result['records'], 18)
            self.assertGreater(result['error_samples'], 0)
            self.assertTrue(any(r['sample']['error'] == 'missed_sampling_deadline' for r in records))
            self.assertEqual({r['scheduled_offset_ns'] for r in records}, {i * 1_000_000_000 for i in range(9)})

    def test_incomplete_or_nonfinite_json_becomes_error_sample(self):
        remote = Remote()
        for data in [b'{"kind":"enhance-hardware-sample","version":1,"error":null}', b'{"value":NaN}', b'not-json']:
            with self.subTest(data=data), patch.object(remote, 'command', return_value=data):
                result = module.sample(remote, '/fixture', module.time.monotonic_ns(), 0)
                self.assertEqual(result['sample']['error'], 'transport_or_decode_failed')

    def test_unavailable_and_inconsistent_runtime_samples_are_counted_separately(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as temp, patch.object(module.time, 'monotonic_ns', side_effect=clock.read), \
                patch.object(module.time, 'sleep', side_effect=clock.sleep):
            out = Path(temp) / 'run'
            result = module.observe(TARGETS, CONFIG, [Remote(memory_available=0), Remote(consistent=False, target=TARGETS[1])], out, 2, 1)
            self.assertEqual(result['sample_version'], 2)
            self.assertEqual(result['error_samples'], 0)
            self.assertEqual(result['unavailable_memory_samples'], 3)
            self.assertEqual(result['inconsistent_runtime_samples'], 3)
            self.assertEqual(result['qualification'], 'unqualified')

    def test_old_schema_and_missing_runtime_state_are_not_success(self):
        remote = Remote()
        original = json.loads(remote.command([], 1))
        for mutation in ('old', 'missing', 'bad-flag'):
            sample = json.loads(json.dumps(original))
            if mutation == 'old':
                sample['version'] = 1
            elif mutation == 'missing':
                del sample['runtime_metrics']
            else:
                sample['runtime_metrics']['state_consistent'] = 'true'
            with self.subTest(mutation=mutation), patch.object(remote, 'command', return_value=json.dumps(sample).encode()):
                result = module.sample(remote, '/fixture', module.time.monotonic_ns(), 0)
                self.assertEqual(result['sample']['error'], 'transport_or_decode_failed')

    def test_changed_artifact_host_or_limits_preserve_a_failed_record(self):
        original = json.loads(Remote().command([], 1))
        for key in ('binary_sha256', 'source_revision', 'bootstrap_manifest_sha256', 'boot_id', 'hostname', 'worker_private_ipv4', 'limit', 'memory'):
            value = json.loads(json.dumps(original))
            if key == 'limit':
                value['cgroup_bytes']['memory.max'] -= 1
            elif key == 'memory':
                value['host_memory_bytes']['MemTotal'] -= 1
            else:
                value[key] = 'different'
            module.bind_sample(value, TARGETS[0])
            self.assertEqual(value['error'], 'sample_differs_from_bootstrap_binding')
            self.assertIn('runtime_metrics', value)
        module.bind_sample(original, TARGETS[0])
        self.assertIsNone(original['error'])

    def test_target_expectations_come_from_verified_bundle_and_bootstrap(self):
        fixture_spec = importlib.util.spec_from_file_location('pair_fixtures', SCRIPT.parents[1] / 'tests/test_bootstrap_pair.py')
        fixtures = importlib.util.module_from_spec(fixture_spec)
        fixture_spec.loader.exec_module(fixtures)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with fixtures.module.journal_module.Journal(root / 'journal') as journal:
                bundle, config = fixtures.prepare(journal, root)
                remote = fixtures.FakeRemote()
                fixtures.module.bootstrap_pair(journal, fixtures.PAIR, config, bundle, lambda _: remote)
                targets = module.observed_targets(journal.state['operation'], config, bundle)
                self.assertEqual(len(targets), 2)
                for target in targets:
                    expected = target['expected']
                    receipt = journal.state['operation']['bootstrap']['receipts'][target['address']]
                    self.assertEqual(expected['binary_sha256'], receipt['binary_sha256'])
                    self.assertEqual(expected['limits'], config['limits'][target['name']])
                    self.assertEqual(expected['boot_id'], receipt['host']['boot_id'])
                changed = dict(config, revision='f' * 40)
                with self.assertRaises(ValueError):
                    module.observed_targets(journal.state['operation'], changed, bundle)

    def test_invalid_targets_or_intervals_do_not_create_evidence(self):
        with tempfile.TemporaryDirectory() as temp:
            out = Path(temp) / 'run'
            for targets, remotes, seconds, interval in [(TARGETS, [Remote()], 2, 1), (TARGETS, [Remote(), Remote()], 0, 1),
                                                       (TARGETS, [Remote(), Remote()], 2, float('nan'))]:
                with self.assertRaises(ValueError):
                    module.observe(targets, CONFIG, remotes, out, seconds, interval)
                self.assertFalse(out.exists())

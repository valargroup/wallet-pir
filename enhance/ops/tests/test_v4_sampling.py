"""Hardware evidence must preserve failures, units, counters and process identity."""
import hashlib
import importlib.util
import json
import platform
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/sample-v4-worker.py'
spec = importlib.util.spec_from_file_location('v4_sampling', SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def write(root, name, data):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data)


def fixture(root):
    write(root, 'etc/machine-id', 'test-machine\n')
    write(root, 'proc/sys/kernel/random/boot_id', 'test-boot\n')
    write(root, 'proc/meminfo', 'MemTotal: 8388608 kB\nMemAvailable: 7000000 kB\nSwapTotal: 2097152 kB\nSwapFree: 2000000 kB\n')
    write(root, 'proc/vmstat', 'pswpin 5\npswpout 8\n')
    write(root, 'proc/123/stat', '123 (worker (name)) S ' + '0 ' * 18 + '100 ' + '0 ' * 10)
    write(root, 'proc/123/smaps_rollup', '000-fff ---p 000000 00:00 0 [rollup]\nRss: 400 kB\nPss: 300 kB\nPss_Anon: 200 kB\nPss_File: 90 kB\nPss_Shmem: 10 kB\nSwap: 0 kB\n')
    write(root, 'proc/123/status', 'Name: worker\nVmHWM: 500 kB\nVmRSS: 400 kB\nVmSwap: 0 kB\n')
    binary = b'fixture-binary'
    checksum = hashlib.sha256(binary).hexdigest()
    path = root / 'opt/enhance-pir-v4/releases' / checksum / 'enhance-pir-v4'
    path.parent.mkdir(parents=True)
    path.write_bytes(binary)
    (root / 'proc/123/exe').symlink_to(path)
    group = root / 'sys/fs/cgroup/system.slice' / module.SERVICE
    group.mkdir(parents=True)
    values = {'memory.current': 600000, 'memory.peak': 700000, 'memory.high': 7 * 1024**3,
              'memory.max': 15 * 1024**3 // 2, 'memory.swap.current': 0, 'memory.swap.max': 2 * 1024**3,
              'cgroup.procs': 123}
    for name, value in values.items():
        (group / name).write_text(str(value) + '\n')
    for name in ('memory.events', 'memory.events.local'):
        (group / name).write_text('low 0\nhigh 7\nmax 0\noom 0\noom_kill 0\n')
    (group / 'memory.stat').write_text('anon 204800\nfile 200000\nshmem 10240\nkernel 100000\n')
    (group / 'memory.swap.events').write_text('high 0\nmax 0\nfail 0\n')
    (group / 'cpu.stat').write_text('usage_usec 10000\nuser_usec 8000\nsystem_usec 2000\n')
    (group / 'memory.pressure').write_text('some avg10=1.2 avg60=0.5 avg300=0.1 total=123\nfull avg10=0.0 avg60=0.0 avg300=0.0 total=12\n')
    limits = {'memory_high_bytes': values['memory.high'], 'memory_max_bytes': values['memory.max'],
              'memory_swap_max_bytes': values['memory.swap.max']}
    write(root, 'srv/enhance-pir-v4/bootstrap.json', json.dumps({'binary_sha256': checksum, 'revision': 'a' * 40,
        'manifest_sha256': 'b' * 64, 'limits': limits, 'private_ipv4': '10.0.0.3', 'worker_name': 'fixture-host', 'phase': 'bootstrapped'}))
    (root / 'srv/enhance-pir-v4/worker').mkdir()
    return {'MainPID': '123', 'ControlGroup': '/system.slice/' + module.SERVICE, 'ActiveState': 'active',
            'SubState': 'running', 'NRestarts': '0', 'MemoryHigh': str(values['memory.high']),
            'MemoryMax': str(values['memory.max']), 'MemorySwapMax': str(values['memory.swap.max'])}


METRICS = (SCRIPT.parents[1] / 'fixtures/v4-worker-metrics.prom').read_bytes()
VALUES = module.parse_runtime_metrics(METRICS)
HEALTH = {'protocol': module.PROTOCOL, 'incarnation': 'fixture-incarnation', 'epoch': VALUES['epoch'],
          'revision': VALUES['revision'], 'published': list(range(VALUES['retained_generations'])),
          'resident_database_bytes': VALUES['live_database_bytes'], 'candidate_present': bool(VALUES['candidate_present']),
          'candidate_sha256': None}


class SamplingTests(unittest.TestCase):
    def sample(self, root, properties):
        with patch.object(module, 'health', return_value=HEALTH), \
                patch.object(module, 'runtime_metrics', return_value={'values': VALUES.copy(), 'exposition_sha256': hashlib.sha256(METRICS).hexdigest()}), \
                patch.object(module.socket, 'gethostname', return_value='fixture-host'):
            return module.collect(root, properties)

    def test_counters_are_not_reset_and_memory_units_are_bytes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            properties = fixture(root)
            before = {str(p.relative_to(root)): p.read_bytes() for p in root.rglob('*') if p.is_file()}
            result = self.sample(root, properties)
            self.assertIsNone(result['error'])
            self.assertEqual(result['version'], 2)
            self.assertEqual(result['worker_private_ipv4'], '10.0.0.3')
            self.assertTrue(result['runtime_metrics']['state_consistent'])
            self.assertEqual(result['runtime_metrics']['values'], VALUES)
            self.assertEqual(result['memory.events']['high'], 7)
            self.assertEqual(result['process_memory'][0]['bytes']['Rss'], 400 * 1024)
            self.assertEqual(result['main_start_ticks'], 100)
            self.assertEqual(result['process_memory'][0]['status_bytes']['VmHWM'], 500 * 1024)
            self.assertEqual(result['host_swap_pages'], {'pswpin': 5, 'pswpout': 8})
            self.assertIsNone(result['cgroup_bytes']['memory.swap.peak'])
            self.assertEqual(result['memory.pressure']['some']['total'], 123)
            after = {str(p.relative_to(root)): p.read_bytes() for p in root.rglob('*') if p.is_file()}
            self.assertEqual(before, after)

    def test_inactive_service_is_an_error_sample_not_zero_usage(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            properties = fixture(root)
            properties.update(ActiveState='inactive', MainPID='0')
            result = self.sample(root, properties)
            self.assertEqual(result['error'], 'service_not_active')
            self.assertNotIn('cgroup_bytes', result)
            self.assertIn('boot_id', result)

    def test_limits_binary_and_cgroup_identity_must_match(self):
        for mutation in ('limit', 'binary', 'group', 'membership', 'missing-rss', 'missing-events'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                properties = fixture(root)
                group = root / 'sys/fs/cgroup/system.slice' / module.SERVICE
                if mutation == 'limit':
                    (group / 'memory.max').write_text('999')
                elif mutation == 'binary':
                    (root / 'proc/123/exe').resolve().write_bytes(b'changed')
                elif mutation == 'group':
                    properties['ControlGroup'] = '/system.slice/legacy.service'
                elif mutation == 'membership':
                    (group / 'cgroup.procs').write_text('456')
                elif mutation == 'missing-rss':
                    (root / 'proc/123/smaps_rollup').write_text('Rss: 1 kB\n')
                else:
                    (group / 'memory.events').write_text('')
                with self.assertRaises(ValueError):
                    self.sample(root, properties)

    def test_process_reuse_during_read_fails_sample(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            properties = fixture(root)
            with patch.object(module, 'start_ticks', side_effect=[100, 100, 101]):
                with self.assertRaisesRegex(ValueError, 'identity changed'):
                    self.sample(root, properties)

    def test_collection_failure_emits_explicit_record(self):
        with patch.object(module, 'collect', side_effect=OSError('private host details')), patch('builtins.print') as output:
            module.main()
            record = json.loads(output.call_args.args[0])
            self.assertEqual(record['error'], 'collection_failed')
            self.assertNotIn('private host details', output.call_args.args[0])
            self.assertNotIn('passed', record)

    def test_metric_contract_rejects_invalid_or_incomplete_samples(self):
        text = METRICS.decode()
        self.assertEqual(module.parse_runtime_metrics(METRICS), VALUES)
        scientific = text.replace('enhance_v4_worker_live_database_bytes 251658240',
                                  'enhance_v4_worker_live_database_bytes 2.5165824e8')
        self.assertEqual(module.parse_runtime_metrics(scientific.encode()), VALUES)
        for bad in [text + 'enhance_v4_worker_up 1\n',
                    text.replace('enhance_v4_worker_up 1', 'enhance_v4_worker_up NaN'),
                    text.replace('enhance_v4_worker_up 1', 'enhance_v4_worker_up -1'),
                    text.replace('enhance_v4_worker_up 1', 'enhance_v4_worker_up 0.5'),
                    text.replace('enhance_v4_worker_up 1', 'enhance_v4_worker_up 9007199254740993'),
                    text.replace('enhance_v4_worker_up 1', 'enhance_v4_worker_up 1e999999999999999999999'),
                    text.replace('enhance_v4_worker_up 1', 'enhance_v4_worker_up{query="0"} 1'),
                    text.replace('enhance_v4_worker_up 1', 'enhance_v4_worker_unexpected 1'),
                    text.replace('enhance_v4_worker_up 1\n', ''),
                    text.replace('enhance_v4_worker_memory_sample_available 1', 'enhance_v4_worker_memory_sample_available 0'),
                    text + ' ' * 65537]:
            with self.subTest(bad=bad[-100:]), self.assertRaises(ValueError):
                module.parse_runtime_metrics(bad.encode())

    def test_busy_engine_is_missing_memory_not_zero_bytes(self):
        values = {key: value for key, value in VALUES.items() if key in module.CORE_METRICS}
        values.update(memory_sample_available=0, memory_model_available=0, preparation_busy=1)
        data = ''.join(f'enhance_v4_worker_{key} {value}\n' for key, value in values.items()).encode()
        parsed = module.parse_runtime_metrics(data)
        self.assertEqual(parsed['memory_sample_available'], 0)
        self.assertNotIn('live_database_bytes', parsed)
        self.assertNotIn('model_total_bytes', parsed)

    def test_runtime_scrape_failure_preserves_kernel_evidence(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            properties = fixture(root)
            with patch.object(module, 'health', return_value=HEALTH), \
                    patch.object(module, 'runtime_metrics', side_effect=OSError('private connection detail')), \
                    patch.object(module.socket, 'gethostname', return_value='fixture-host'):
                sample = module.collect(root, properties)
            self.assertEqual(sample['error'], 'runtime_metrics_failed')
            self.assertEqual(sample['memory.events']['high'], 7)
            self.assertNotIn('private connection detail', json.dumps(sample))
            self.assertNotIn('values', sample['runtime_metrics'])

    def test_publication_between_scrapes_is_explicitly_inconsistent(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            properties = fixture(root)
            with patch.object(module, 'health', side_effect=[HEALTH, HEALTH | {'revision': HEALTH['revision'] + 1}]), \
                    patch.object(module, 'runtime_metrics', return_value={'values': VALUES.copy()}), \
                    patch.object(module.socket, 'gethostname', return_value='fixture-host'):
                sample = module.collect(root, properties)
            self.assertIsNone(sample['error'])
            self.assertFalse(sample['runtime_metrics']['state_consistent'])
            self.assertEqual(sample['runtime_metrics']['values'], VALUES)

    def test_replaced_candidate_is_not_a_consistent_runtime_snapshot(self):
        before = HEALTH | {'candidate_present': True, 'candidate_sha256': 'a' * 64}
        after = before | {'candidate_sha256': 'b' * 64}
        values = VALUES | {'candidate_present': 1}
        self.assertFalse(module.runtime_consistent(before, values, after))
        self.assertTrue(module.runtime_consistent(before, values, before))

    @unittest.skipUnless(platform.system() == 'Linux', 'live procfs parser check requires Linux')
    def test_live_linux_procfs_and_cgroup_formats(self):
        rollup = module.kilobytes(Path('/proc/self/smaps_rollup').read_text())
        self.assertTrue({'Rss', 'Pss', 'Pss_Anon', 'Pss_File', 'Pss_Shmem', 'Swap'} <= rollup.keys())
        self.assertGreater(rollup['Rss'], 0)
        self.assertGreater(module.start_ticks(Path('/proc/self/stat')), 0)
        group = Path('/sys/fs/cgroup')
        if (group / 'memory.stat').exists():
            memory = module.counters((group / 'memory.stat').read_text())
            self.assertTrue({'anon', 'file', 'shmem', 'kernel'} <= memory.keys())
            events = module.counters((group / 'memory.events').read_text())
            self.assertTrue({'high', 'max', 'oom', 'oom_kill'} <= events.keys())

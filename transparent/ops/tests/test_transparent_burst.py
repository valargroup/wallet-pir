#!/usr/bin/env python3
"""The burst runner must preserve failures and finish both configurations."""
import json
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

RUNNER = Path(__file__).resolve().parents[1] / 'scripts/run-transparent-burst.py'


class BurstRunnerTests(unittest.TestCase):
    def test_external_success_without_drain_proof_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / 'probe'
            binary.write_text('''#!/usr/bin/env python3
import json, os, sys
if 'burst::external_query_clients' in sys.argv: sys.exit(0)
json.dump({'completed':True,'cold_persistence_complete':True,'persistence_complete':True,'exact_queries':True,'external_clients':True,
'worker_30s_budget_passed':True,'activations':[{'worker_visibility_seconds':1}],
'memory_samples':[]},open(os.environ['TRANSPARENT_BURST_REPORT'],'w'))
''')
            binary.chmod(0o755)
            out = root / 'out'
            result = subprocess.run([sys.executable, str(RUNNER), '--test-binary', str(binary),
                                     '--source-sha', 'fixture', '--external-clients', '--out', str(out),
                                     '--repetitions', '1'], capture_output=True)
            self.assertEqual(result.returncode, 1)
            runs = json.loads((out / 'summary.json').read_text())['runs']
            self.assertTrue(all(r['exit_code'] == 0 for r in runs))
            self.assertTrue(all(not r['client_shutdown_complete'] for r in runs))

    def test_persistence_must_drain_before_and_after_measured_load(self):
        for missing in [None, 'cold_persistence_complete', 'persistence_complete']:
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                record = dict(completed=True, exact_queries=True, worker_30s_budget_passed=True,
                              cold_persistence_complete=True, persistence_complete=True,
                              activations=[dict(worker_visibility_seconds=1)], memory_samples=[])
                if missing:
                    del record[missing]
                binary = root / 'probe'
                binary.write_text('#!/usr/bin/env python3\nimport json,os\njson.dump(' + repr(record) +
                                  ',open(os.environ["TRANSPARENT_BURST_REPORT"],"w"))\n')
                binary.chmod(0o755)
                result = subprocess.run([sys.executable, str(RUNNER), '--test-binary', str(binary),
                                         '--source-sha', 'fixture', '--out', str(root/'out'),
                                         '--build-slots', '2', '--repetitions', '1'], capture_output=True)
                self.assertEqual(result.returncode, 1 if missing else 0)

    def test_repeated_build_configuration_is_rejected(self):
        result = subprocess.run([sys.executable, str(RUNNER), '--build-slots', '1', '1'], capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'--build-slots', result.stderr)

    def test_headroom_requires_kernel_peak_and_verified_limits(self):
        spec = importlib.util.spec_from_file_location('burst_runner', RUNNER)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        kernel = {'memory.peak': str(5 << 30), 'memory.current': '1',
                  'memory.high': '5905580032', 'memory.max': '7516192768',
                  'memory.events': 'oom 0\noom_kill 0\nhigh 12\n',
                  'memory.swap.max': '0', 'cpuset.cpus.effective': '0-3'}
        def passed(value, isolated=True):
            return module.memory_qualification(value, isolated, 512 << 20)['memory_qualification_passed']
        self.assertTrue(passed(kernel))
        self.assertFalse(passed(kernel, False))
        self.assertFalse(passed({**kernel, 'memory.peak': str(7 << 30)}))
        self.assertFalse(passed({**kernel, 'memory.events': 'oom 1\noom_kill 0\n'}))
        for key in ['memory.peak', 'memory.high', 'memory.max', 'memory.events', 'memory.swap.max', 'cpuset.cpus.effective']:
            self.assertFalse(passed({k: v for k, v in kernel.items() if k != key}), key)

    def test_failed_budget_is_retained_and_does_not_skip_other_configuration(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / 'probe'
            binary.write_text('''#!/usr/bin/env python3
import json,os
slots=int(os.environ['TRANSPARENT_BURST_BUILD_SLOTS'])
json.dump({'completed':True,'cold_persistence_complete':True,'persistence_complete':True,'exact_queries':True,'worker_30s_budget_passed':slots==2,
'activations':[{'worker_visibility_seconds':31 if slots==1 else 15}],
'memory_samples':[{'process_rss_bytes':100}]},open(os.environ['TRANSPARENT_BURST_REPORT'],'w'))
''')
            binary.chmod(0o755)
            out = root / 'out'
            command = [sys.executable, str(RUNNER), '--test-binary', str(binary),
                       '--source-sha', 'fixture', '--out', str(out)]
            result = subprocess.run(command, capture_output=True)
            self.assertEqual(result.returncode, 1)
            runs = json.loads((out / 'summary.json').read_text())['runs']
            self.assertEqual([r['build_slots'] for r in runs], [1, 2, 2, 1])
            self.assertEqual(len(list(out.glob('repeat-*.json'))), 4)
            original = (out / 'summary.json').read_bytes()
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
            self.assertEqual((out / 'summary.json').read_bytes(), original)

    def test_worker_budget_cannot_weaken_public_ceiling(self):
        for budget in ['0', '31', 'nan']:
            result = subprocess.run([sys.executable, str(RUNNER), '--worker-budget-seconds', budget], capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b'--worker-budget-seconds', result.stderr)

    def test_external_client_failure_fails_otherwise_successful_worker(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / 'probe'
            binary.write_text('''#!/usr/bin/env python3
import json, os, sys, time
from pathlib import Path
if 'burst::external_query_clients' in sys.argv:
    stop = Path(os.environ['TRANSPARENT_BURST_CLIENT_DIR']) / 'stop'
    while not stop.exists(): time.sleep(.01)
    sys.exit(23)
json.dump({'completed':True,'cold_persistence_complete':True,'persistence_complete':True,'exact_queries':True,'worker_30s_budget_passed':True,
'activations':[{'worker_visibility_seconds':1}], 'memory_samples':[]},
open(os.environ['TRANSPARENT_BURST_REPORT'],'w'))
''')
            binary.chmod(0o755)
            out = root / 'out'
            result = subprocess.run([sys.executable, str(RUNNER), '--test-binary', str(binary),
                                     '--source-sha', 'fixture', '--external-clients', '--out', str(out),
                                     '--repetitions', '1'], capture_output=True)
            self.assertEqual(result.returncode, 1)
            runs = json.loads((out / 'summary.json').read_text())['runs']
            self.assertEqual([r['client_exit_code'] for r in runs], [23, 23])
            self.assertTrue(all(r['completed'] for r in runs))

    def test_crashed_probe_still_produces_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / 'probe'
            binary.write_text('#!/bin/sh\nexit 17\n')
            binary.chmod(0o755)
            out = root / 'out'
            result = subprocess.run([sys.executable, str(RUNNER), '--test-binary', str(binary),
                                     '--source-sha', 'fixture', '--out', str(out), '--repetitions', '1'], capture_output=True)
            self.assertEqual(result.returncode, 1)
            runs = json.loads((out / 'summary.json').read_text())['runs']
            self.assertEqual([r['exit_code'] for r in runs], [17, 17])


if __name__ == '__main__':
    unittest.main()

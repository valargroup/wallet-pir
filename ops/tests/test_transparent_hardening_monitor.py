import importlib.util
from pathlib import Path
import unittest
SPEC=importlib.util.spec_from_file_location('monitor', Path(__file__).resolve().parents[1]/'scripts/observe-transparent-hardening.py')
M=importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(M)
class MonitorTests(unittest.TestCase):
    def test_restart_cannot_hide_behind_reset_cgroup_counters(self):
        baseline=dict(NRestarts=2, ExecMainStartTimestampMonotonic=10, oom=0, oom_kill=0, MemoryCurrent=500, MemTotal=1)
        self.assertIsNone(M.worker_failure(baseline, baseline))
        restarted={**baseline,'ExecMainStartTimestampMonotonic':20}
        self.assertIsNotNone(M.worker_failure(baseline,restarted))
        self.assertIsNotNone(M.worker_failure(baseline,{**baseline,'oom_kill':1}))
        self.assertIsNotNone(M.worker_failure(baseline,{**baseline,'MemoryCurrent':900}))
    def test_facts_preserve_process_identity_and_kernel_oom_count(self):
        self.assertEqual(M.facts('NRestarts=2\nExecMainStartTimestampMonotonic=42\noom_kill 1\nMemTotal: 8192 kB\n'),dict(NRestarts=2, ExecMainStartTimestampMonotonic=42, oom_kill=1, MemTotal=8192))

    def test_late_arrival_cannot_escape_the_uncovered_timeout(self):
        with self.assertRaisesRegex(RuntimeError, 'arrived late'):
            M.completed_blocks({10: 0}, {}, 10, 31, 30)
        self.assertEqual(M.completed_blocks({10: 0, 11: 5}, {}, 11, 30, 30), {10: 30, 11: 25})
        self.assertEqual(M.completed_blocks({10: 0}, {10: 10}, 10, 90, 30), {})

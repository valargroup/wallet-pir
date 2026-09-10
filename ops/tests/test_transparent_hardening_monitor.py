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

    def test_public_and_replica_budgets_are_independent(self):
        seen = {38: 0}
        with self.assertRaisesRegex(RuntimeError, 'public arrived late'):
            M.completed_blocks(seen, {}, 38, 40, 30, 'public')
        self.assertEqual(M.completed_blocks(seen, {}, 38, 40, 60, 'replica recent'), {38: 40})
        with self.assertRaisesRegex(RuntimeError, 'replica recent arrived late'):
            M.completed_blocks(seen, {}, 38, 61, 60, 'replica recent')

    def test_host_pressure_and_memory_peak_cannot_hide_in_current_charge(self):
        sample=dict(NRestarts=0,ExecMainStartTimestampMonotonic=1,oom=0,oom_kill=0,MemoryCurrent=500,MemoryPeak=600,MemTotal=1,MemAvailable=1)
        self.assertIsNone(M.worker_failure(sample,sample))
        self.assertIsNotNone(M.worker_failure(sample,{**sample,'MemoryPeak':900}))
        self.assertIsNotNone(M.worker_failure(sample,{**sample,'MemAvailable':0}))
        parsed=M.facts('MemAvailable: 2000 kB\nanon 100\nfile 200\nsome avg10=1.23 avg60=2.0 total=30\n')
        self.assertEqual(parsed['MemAvailable'],2000)
        self.assertEqual(parsed['anon'],100)
        self.assertEqual(parsed['pressure_some_avg10'],1.23)

    def test_recovered_outage_and_replaced_evidence_cannot_pass(self):
        baseline = dict(epoch='a'*32, unavailable_events=4, available=True)
        self.assertIsNone(M.routing_failure(None, baseline))
        self.assertIsNone(M.routing_failure(baseline, baseline))
        self.assertIsNotNone(M.routing_failure(baseline, {**baseline, 'unavailable_events':5}))
        self.assertIsNotNone(M.routing_failure(baseline, {**baseline, 'unavailable_events':0}))
        self.assertIsNotNone(M.routing_failure(baseline, {**baseline, 'epoch':'b'*32}))
        self.assertIsNotNone(M.routing_failure(None, {**baseline, 'available':False}))

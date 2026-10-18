import json
from pathlib import Path
import subprocess
import tempfile
import unittest


class FocusedAssessment(unittest.TestCase):
    def assess(self, mutation=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'exercise.json').write_text(json.dumps({
                'measurement_started_wall_ns': 0, 'measurement_seconds': 1800,
                'status': 'recorded', 'publications': 30, 'background_correct_answers': 100,
                'exact_boundary_and_retained_probes': 10, 'background_errors': 0,
                'binary_sha256': 'a' * 64}))
            samples = [{
                'wall_ns': second * 10**9, 'error': None,
                'process': {'VmRSS': 1024, 'VmSwap': 0},
                'cgroup': {'memory.current': '2048', 'memory.swap.current': '0'},
                'events': {'high': 0, 'max': 0, 'oom': 0, 'oom_kill': 0},
                'host_swap': {'pswpin': 0, 'pswpout': 0},
                'identity': ['1', '2', '/binary'], 'service': {'NRestarts': '0'},
            } for second in range(0, 1801, 2)]
            if mutation:
                mutation(samples)
            for name in ['worker', 'coordinator']:
                (root / name).write_text('\n'.join(json.dumps(s) for s in samples))
            result = subprocess.run(['python3', str(Path(__file__).with_name('assess.py')),
                                     str(root / 'exercise.json'), '--worker', str(root / 'worker'),
                                     '--coordinator', str(root / 'coordinator'), '--out', str(root / 'result')],
                                    capture_output=True, text=True)
            self.assertIn(result.returncode, [0, 1], result.stderr)
            return json.loads((root / 'result').read_text())

    def test_complete_trace_is_focused_evidence_only(self):
        result = self.assess()
        self.assertTrue(result['passed'])
        self.assertFalse(result['six_hour_hardware_qualification'])

    def test_gap_is_rejected(self):
        self.assertFalse(self.assess(lambda s: s.__delitem__(slice(100, 110)))['passed'])

    def test_swap_activity_is_rejected(self):
        self.assertFalse(self.assess(lambda s: s[-1]['host_swap'].update(pswpout=1))['passed'])

    def test_restart_is_rejected(self):
        self.assertFalse(self.assess(lambda s: s[-1]['identity'].__setitem__(0, '3'))['passed'])


if __name__ == '__main__':
    unittest.main()

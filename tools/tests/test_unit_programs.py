"""Cargo selects executables; bounded workers preserve Cargo's runtime contract."""
import io
import json
import subprocess
from pathlib import Path
import sys
import threading
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/ci'))
import stage
import unit_tests


class ProgramTests(unittest.TestCase):
    def test_collection_uses_cargo_test_artifacts_only(self):
        records = [dict(reason='compiler-artifact', package_id='path+file:///p#p@1.0.0',
                        target={'name': 'p', 'kind': ['lib']}, profile={'test': test},
                        executable=binary, fresh=True) for test, binary in
                   [(False, '/normal'), (True, '/test'), (True, None)]]
        process = type('Process', (), {'stdout': io.StringIO(''.join(json.dumps(r)+'\n' for r in records))})()
        found = []
        stage.relay(process, None, 'compile', found)
        self.assertEqual(found, ['/test'])

    def test_two_programs_overlap_with_one_thread_per_program_and_package_cwd(self):
        barrier = threading.Barrier(2, timeout=5)
        lock = threading.Lock()
        active, peak = 0, 0
        seen = []

        def run(command, **kwargs):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            barrier.wait()
            self.assertEqual(kwargs['env']['RUST_TEST_THREADS'], '1')
            self.assertEqual(kwargs['env']['CARGO_MANIFEST_DIR'], kwargs['cwd'])
            self.assertEqual(command[1:], ['--skip', 'campaign'])
            self.assertIn('/deps', kwargs['env'].get('LD_LIBRARY_PATH', '') + kwargs['env'].get('DYLD_FALLBACK_LIBRARY_PATH', ''))
            with lock:
                seen.append(command[0])
                active -= 1

        programs = [dict(executable='/deps/p'+str(i), directory='/package'+str(i),
                         package='p', version='1.0.0', args=['--skip', 'campaign']) for i in range(4)]
        with patch.object(unit_tests.subprocess, 'check_output', return_value='/rust/lib\n'), \
                patch.object(unit_tests, 'run', side_effect=run):
            unit_tests.execute(programs, 2)
        self.assertEqual(peak, 2)
        self.assertCountEqual(seen, [p['executable'] for p in programs])

    def test_failed_program_is_not_retried(self):
        program = dict(executable='/deps/p', directory='/p', package='p', version='1', args=[])
        with patch.object(unit_tests.subprocess, 'check_output', return_value='/rust/lib\n'), \
                patch.object(unit_tests, 'run', side_effect=subprocess.CalledProcessError(1, 'p')) as run:
            with self.assertRaises(subprocess.CalledProcessError):
                unit_tests.execute([program], 2)
            self.assertEqual(run.call_count, 1)

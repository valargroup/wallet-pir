"""Cargo selects executables; bounded workers preserve Cargo's runtime contract."""
import io
import json
import os
import subprocess
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/ci'))
import stage
import unit_tests
import test_budget


class ProgramTests(unittest.TestCase):
    def setUp(self):
        # Fixture program timings must not look like real CI execution.
        output = patch("sys.stdout", new=io.StringIO())
        output.start()
        self.addCleanup(output.stop)


    def test_budget_is_enforced_unless_diagnostic_mode_is_explicit(self):
        program = dict(executable='/deps/p', directory='/p', package='p', version='1', args=[])
        for value, enforced in [(None, True), ('1', True), ('0', False)]:
            with self.subTest(value=value), patch.dict(os.environ), \
                    patch.object(unit_tests.subprocess, 'check_output', return_value='/rust/lib\n'), \
                    patch.object(test_budget, 'execute', return_value=1) as execute:
                os.environ.pop('WALLET_PIR_ENFORCE_BUDGET', None)
                if value is not None:
                    os.environ['WALLET_PIR_ENFORCE_BUDGET'] = value
                unit_tests.execute([program], 1, budget=True)
                self.assertEqual(execute.call_args.kwargs['enforce'], enforced)

    def test_diagnostics_collect_all_program_failures_without_cancelling_queued_work(self):
        programs = [dict(executable=f'/deps/{index}', directory='/p', package='p',
                         version='1', args=[]) for index in range(3)]
        futures = [MagicMock() for _ in programs]
        futures[0].result.side_effect = RuntimeError('first program failed')
        pool = MagicMock()
        pool.__enter__.return_value = pool
        pool.submit.side_effect = futures
        with patch.dict(os.environ, WALLET_PIR_ENFORCE_BUDGET='0'), \
                patch.object(unit_tests.subprocess, 'check_output', return_value='/rust/lib\n'), \
                patch.object(unit_tests, 'ThreadPoolExecutor', return_value=pool), \
                patch.object(unit_tests, 'as_completed', return_value=iter(futures)), \
                patch.object(test_budget, 'stop') as stop:
            with self.assertRaisesRegex(RuntimeError, '1 diagnostic test programs failed'):
                unit_tests.execute(programs, 1, budget=True)
        stop.assert_not_called()
        for future in futures:
            future.result.assert_called_once_with()
            future.cancel.assert_not_called()

    def test_publisher_unit_build_pool_is_scoped_bounded_and_respects_explicit_settings(self):
        programs = [dict(executable='/deps/' + name, directory='/p', package=package,
                         version='1', args=[], integration=integration)
                    for name, package, integration in [
                        ('publisher', 'transparent-publisher', False),
                        ('http', 'transparent-publisher', True),
                        ('other', 'other', False)]]
        for cpus, configured, expected in [(2, None, '2'), (4, None, '4'),
                                            (16, None, '4'), (4, '1', '1')]:
            with self.subTest(cpus=cpus, configured=configured), patch.dict(os.environ), \
                    patch.object(unit_tests.os, 'sched_getaffinity', return_value=set(range(cpus)), create=True), \
                    patch.object(unit_tests.subprocess, 'check_output', return_value='/rust/lib\n'), \
                    patch.object(unit_tests, 'run') as run:
                os.environ.pop('TRANSPARENT_BUILD_THREADS', None)
                if configured is not None:
                    os.environ['TRANSPARENT_BUILD_THREADS'] = configured
                unit_tests.execute(programs, 1)
                environments = {call.args[0][0]: call.kwargs['env'] for call in run.call_args_list}
                self.assertEqual(environments['/deps/publisher']['TRANSPARENT_BUILD_THREADS'], expected)
                for name in ('http', 'other'):
                    self.assertEqual(environments['/deps/' + name].get('TRANSPARENT_BUILD_THREADS'), configured)
                self.assertEqual(os.environ.get('TRANSPARENT_BUILD_THREADS'), configured)

    def test_timed_program_scratch_is_removed_after_success_and_failure(self):
        for fails in (False, True):
            scratch = []
            def execute(command, **kwargs):
                directory = Path(kwargs['env']['TMPDIR'])
                self.assertLess(len(str(directory)), 40)
                self.assertEqual(directory.parent, Path('/tmp'))
                (directory / 'unfinished-upload').write_bytes(b'partial body')
                scratch.append(directory)
                if fails:
                    raise RuntimeError('fixture failed')
                return 1
            program = dict(executable='/deps/p', directory='/p', package='p', version='1', args=[])
            with patch.dict(os.environ, WALLET_PIR_ENFORCE_BUDGET='1'), \
                    patch.object(unit_tests.subprocess, 'check_output', return_value='/rust/lib\n'), \
                    patch.object(test_budget, 'execute', side_effect=execute), \
                    patch.object(test_budget, 'stop'):
                if fails:
                    with self.assertRaisesRegex(RuntimeError, 'fixture failed'):
                        unit_tests.execute([program], 1, budget=True)
                else:
                    unit_tests.execute([program], 1, budget=True)
            self.assertEqual(len(scratch), 1)
            self.assertFalse(scratch[0].exists())

    def test_explicit_command_environment_records_artifacts_and_target_ownership(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cargo = root / 'cargo'
            artifact = dict(reason='compiler-artifact', package_id='path+file:///p#p@1.0.0',
                            target={'name': 'p', 'kind': ['lib']}, profile={'test': True},
                            executable='/test/p', fresh=False)
            cargo.write_text('#!' + sys.executable + '\nprint(' + repr(json.dumps(artifact)) + ')\n')
            cargo.chmod(0o755)
            env = dict(os.environ, WALLET_PIR_ARTIFACT_LOG=str(root / 'artifacts'),
                       WALLET_PIR_STAGE_LOG=str(root / 'stages'), WALLET_PIR_STAGE_PARENT='owner')
            with patch('sys.stdout', new=io.StringIO()):
                found = stage.run([str(cargo), 'test', '--no-run'], env=env,
                                  collect_tests=True, test_details=True, stage='fixture')
            self.assertEqual(found[0]['package_id'], artifact['package_id'])
            self.assertEqual(json.loads((root / 'artifacts').read_text())['fresh'], False)
            self.assertEqual(json.loads((root / 'stages').read_text())['parent'], 'owner')


    def test_bounded_programs_overlap_with_one_thread_per_program_and_package_cwd(self):
        for jobs in (2, 3):
            barrier = threading.Barrier(jobs, timeout=5)
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
                             package='p', version='1.0.0', args=['--skip', 'campaign']) for i in range(6)]
            with patch.object(unit_tests.subprocess, 'check_output', return_value='/rust/lib\n'), \
                    patch.object(unit_tests, 'run', side_effect=run):
                unit_tests.execute(programs, jobs)
            self.assertEqual(peak, jobs)
            self.assertCountEqual(seen, [p['executable'] for p in programs])

    def test_exclusive_programs_finish_before_ordinary_programs_overlap(self):
        barrier = threading.Barrier(2, timeout=5)
        finished = []
        def run(command, **kwargs):
            if command[0] == '/deps/http':
                self.assertEqual(finished, [])
                finished.append('http')
            else:
                self.assertEqual(finished, ['http'])
                barrier.wait()
        programs = [dict(executable='/deps/' + name, directory='/p', package='p',
                         version='1', args=[], exclusive=name == 'http')
                    for name in ['a', 'http', 'b']]
        with patch.object(unit_tests.subprocess, 'check_output', return_value='/rust/lib\n'), \
                patch.object(unit_tests, 'run', side_effect=run) as execute:
            unit_tests.execute(programs, 2)
        self.assertCountEqual([call.args[0][0] for call in execute.call_args_list],
                              [program['executable'] for program in programs])

    def test_status_groups_overlap_each_other_and_finish_before_the_pool(self):
        order = []
        lock = threading.Lock()
        both = threading.Barrier(3, timeout=5)

        def run(command, **kwargs):
            name = command[0].rsplit('/', 1)[-1]
            if name.startswith('status') or name == 'publisher':
                with lock:
                    self.assertNotIn('ordinary', order)
                both.wait()
            elif name == 'ordinary':
                with lock:
                    self.assertCountEqual(order, ['hint', 'publisher', 'status-a', 'status-b'])
            with lock:
                order.append(name)

        programs = [
            dict(executable='/deps/hint', directory='/p', package='p', version='1', args=[],
                 exclusive=True),
            dict(executable='/deps/publisher', directory='/p', package='enhance-publisher',
                 version='1', args=[], exclusive=True),
            dict(executable='/deps/status-a', directory='/p', package='pir-query-server',
                 version='1', args=[], exclusive=True, exclusive_group='status'),
            dict(executable='/deps/status-b', directory='/p', package='pir-query-server',
                 version='1', args=[], exclusive=True, exclusive_group='status'),
            dict(executable='/deps/ordinary', directory='/p', package='p', version='1', args=[]),
        ]
        with patch.object(unit_tests, 'available_cpus', return_value=8), \
                patch.object(unit_tests.subprocess, 'check_output', return_value='/rust/lib\n'), \
                patch.object(unit_tests, 'run', side_effect=run):
            unit_tests.execute(programs, 3)
        self.assertEqual(order[0], 'hint')
        self.assertEqual(order[-1], 'ordinary')

    def test_status_groups_stay_serial_below_eight_cores(self):
        seen = []

        def run(command, **kwargs):
            self.assertNotIn('/deps/status-b', seen)
            seen.append(command[0])

        programs = [
            dict(executable='/deps/status-a', directory='/p', package='pir-query-server',
                 version='1', args=[], exclusive=True, exclusive_group='status'),
            dict(executable='/deps/status-b', directory='/p', package='pir-query-server',
                 version='1', args=[], exclusive=True, exclusive_group='status'),
        ]
        with patch.object(unit_tests, 'available_cpus', return_value=4), \
                patch.object(unit_tests.subprocess, 'check_output', return_value='/rust/lib\n'), \
                patch.object(unit_tests, 'run', side_effect=run):
            unit_tests.execute(programs, 3)
        self.assertEqual(seen, ['/deps/status-a', '/deps/status-b'])

    def test_failed_program_is_not_retried(self):
        program = dict(executable='/deps/p', directory='/p', package='p', version='1', args=[])
        with patch.object(unit_tests.subprocess, 'check_output', return_value='/rust/lib\n'), \
                patch.object(unit_tests, 'run', side_effect=subprocess.CalledProcessError(1, 'p')) as run:
            with self.assertRaises(subprocess.CalledProcessError):
                unit_tests.execute([program], 2)
            self.assertEqual(run.call_count, 1)

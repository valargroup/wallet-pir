import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'tools' / 'ci' / f'{name}.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


workflow_paths = load('workflow_paths')
main_health = load('main_health')
ZERO = '0' * 40


class WorkflowPathsTest(unittest.TestCase):
    def repo(self, files):
        root = Path(tempfile.mkdtemp())
        self.addCleanup(subprocess.run, ['rm', '-rf', str(root)])
        subprocess.run(['git', 'init', '-q'], cwd=root, check=True)
        for path, text in files.items():
            (root / path).parent.mkdir(parents=True, exist_ok=True)
            (root / path).write_text(text)
        return root

    def test_a_removed_script_a_workflow_runs_is_reported(self):
        # The 2026-10-14 break: the deploy script was deleted while ci-full.yml still ran it.
        root = self.repo({
            '.github/workflows/ci-full.yml': 'jobs:\n  x:\n    steps:\n'
                                             '      - run: tools/transparent/scripts/deploy-filter.sh validate\n'
                                             '      - run: python3 -B tools/transparent/tests/test_kept.py\n',
            'tools/transparent/tests/test_kept.py': '',
        })
        self.assertEqual(workflow_paths.missing(root), [
            '.github/workflows/ci-full.yml:4: runs tools/transparent/scripts/deploy-filter.sh, which does not exist'])

    def test_outputs_comments_and_runner_paths_are_not_scripts(self):
        root = self.repo({
            '.github/workflows/deploy.yml': 'jobs:\n  x:\n    steps:\n'
                                            '      # the retired tools/transparent/scripts/old.sh wrapper is gone\n'
                                            '      - run: jq . artifact/roster.json > transparent/out.json\n'
                                            '      - run: bash "$RUNNER_TEMP/tools/setup.sh" && ./tools/run.sh\n'
                                            '      - run: python3 build.py\n'
                                            '        working-directory: tools/sub\n',
            'tools/run.sh': '', 'tools/sub/build.py': '', 'transparent/keep.py': '',
        })
        self.assertEqual(workflow_paths.missing(root), [])

    def test_the_repository_workflows_name_only_existing_scripts(self):
        self.assertEqual(workflow_paths.missing(), [])


class MainHealthTest(unittest.TestCase):
    RED = {'databaseId': 7, 'status': 'completed', 'conclusion': 'failure', 'headSha': 'c29fb883' + 'a' * 32, 'url': 'u'}
    GREEN = {'databaseId': 8, 'status': 'completed', 'conclusion': 'success', 'headSha': 'b' * 40, 'url': 'v'}
    JOBS = [{'name': 'Validate selected helper checks', 'conclusion': 'failure',
             'steps': [{'name': 'Validate transparent filter inputs', 'conclusion': 'failure'}]},
            {'name': 'Shared tests', 'conclusion': 'success', 'steps': []}]

    def test_development_pushes_follow_fast_ci_not_release_qualification(self):
        self.assertEqual(main_health.WORKFLOW, 'CI')

    def test_only_pushes_that_move_main_are_checked(self):
        self.assertTrue(main_health.pushes_main([f'refs/heads/main {"c" * 40} refs/heads/main {"d" * 40}']))
        self.assertFalse(main_health.pushes_main([f'refs/heads/x {"c" * 40} refs/heads/x {ZERO}']))
        self.assertFalse(main_health.pushes_main([f'(delete) {ZERO} refs/heads/main {"d" * 40}']))

    def test_a_red_main_refuses_with_the_failing_step_and_the_override_allows(self):
        allowed, message = main_health.verdict([self.RED, self.GREEN], lambda _: self.JOBS)
        self.assertFalse(allowed)
        self.assertIn('c29fb883', message)
        self.assertIn('Validate selected helper checks: Validate transparent filter inputs', message)
        self.assertIn('WALLET_PIR_PUSH_ON_RED=1', message)
        allowed, _ = main_health.verdict([self.RED], lambda _: self.JOBS, override=True)
        self.assertTrue(allowed)

    def test_deadline_cancellation_is_red_but_superseded_work_is_not(self):
        cancelled = dict(self.RED, conclusion='cancelled')
        job = dict(name='Fast checks', conclusion='cancelled', steps=[],
                   startedAt='2026-10-16T03:21:36Z', completedAt='2026-10-16T03:26:57Z')
        allowed, message = main_health.verdict([cancelled, self.GREEN], lambda _: [job])
        self.assertFalse(allowed)
        self.assertIn('Fast checks', message)
        self.assertTrue(main_health.verdict([cancelled, self.GREEN], lambda _: [job], override=True)[0])
        job['completedAt'] = '2026-10-16T03:22:00Z'
        self.assertEqual(main_health.verdict([cancelled, self.GREEN], lambda _: [job]), (True, ''))

    def test_running_and_cancelled_runs_defer_to_the_last_finished_one(self):
        running = {'databaseId': 9, 'status': 'in_progress', 'conclusion': '', 'headSha': 'e' * 40}
        cancelled = {'databaseId': 10, 'status': 'completed', 'conclusion': 'cancelled', 'headSha': 'f' * 40}
        self.assertEqual(main_health.verdict([running, cancelled, self.GREEN, self.RED], lambda _: self.JOBS), (True, ''))
        self.assertFalse(main_health.verdict([running, cancelled, self.RED], lambda _: self.JOBS)[0])
        self.assertEqual(main_health.verdict([], lambda _: self.JOBS), (True, ''))


if __name__ == '__main__':
    unittest.main()

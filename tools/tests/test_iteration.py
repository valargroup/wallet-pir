"""Selection, coverage, isolation and stale-result regressions for the shared gate."""
import importlib.util
from contextlib import nullcontext, redirect_stdout
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'tools/ci' / f'{name}.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fast, timings, snapshot = load('fast'), load('timings'), load('snapshot_check')
import sys
sys.path.insert(0, str(ROOT / 'tools/ci'))
full = load('full_packages')
scaler = load('scaler_tests')
helpers = load('helpers')


class PlannerTests(unittest.TestCase):
    def setUp(self):
        self.packages = fast.workspace_packages()


    def test_docs_do_not_need_rust_or_other_helpers(self):
        result = fast.plan(self.packages, ['README.md', 'docs/ci.md', 'evidence/receiver/run.json'])
        self.assertEqual(result['packages'], [])
        self.assertEqual(result['helpers'], ['check-docs'])
        with patch.object(fast.subprocess, 'check_output', side_effect=AssertionError('no Cargo in manifest selection')):
            self.assertEqual(fast.workspace_packages(), self.packages)

    def test_ops_and_deploy_workflow_do_not_select_workspace_rust(self):
        for path in ['tools/tests/operations/control_sessions/fake_ssh.py', '.github/workflows/deploy-enhance-cuda.yml',
                     '.github/workflows/publish-pir-service-discovery.yml',
                     '.github/workflows/configure-pir-apm-slack.yml',
                     'tools/scripts/example.py', 'tools/receiver/digitalocean/Caddyfile']:
            result = fast.plan(self.packages, [path])
            self.assertEqual(result['packages'], [])
            self.assertEqual(result['groups'], ['ops'])
        self.assertEqual(fast.plan(self.packages, ['tools/tests/operations/control_sessions/fake_ssh.py'])['helpers'], ['check-ops-control-sessions'])

    def test_shared_ops_helpers_select_all_consumers(self):
        result = fast.plan(self.packages, ['tools/lib/wallet_pir_ops/control_sessions.py'])
        self.assertTrue(fast.OPS <= set(result['helpers']))
        self.assertNotIn('check-ops-deploy', result['helpers'])

    def test_transparent_schema_helpers_are_not_misclassified_as_parent_filters(self):
        for path in ['tools/transparent/lib/activity_source_stage.py',
                     'tools/transparent/tests/test_activity_schema_operation.py',
                     'tools/transparent/scripts/transparent-activity-routing.py']:
            result = fast.plan(self.packages, [path])
            self.assertNotIn('check-ops-deploy', result['helpers'], path)
            self.assertIn('check-ops-publication', result['helpers'], path)
            self.assertEqual(result['packages'], [])
        result = fast.plan(self.packages, ['tools/transparent/scripts/stage-transparent-parents.py'])
        self.assertEqual(set(result['helpers']), {'check-ops-contracts', 'check-ops-parents'})

    def test_txid_display_ops_route_to_their_own_check(self):
        for path in ['tools/txid/lib/txid_display_poc.py', 'tools/txid/scripts/txid-display-fleet.py',
                     'tools/txid/deploy/txid-display-routes.caddy.in',
                     'tools/txid/tests/test_txid_display_router.py']:
            result = fast.plan(self.packages, [path])
            self.assertEqual(set(result['helpers']), {'check-ops-txid-display', 'check-ops-contracts'}, path)
            self.assertEqual(result['packages'], [])
        for path in ['tools/txid/lib/roles.py', 'tools/txid/deploy/services.toml']:
            self.assertEqual(set(fast.plan(self.packages, [path])['helpers']),
                             {'check-ops-txid-display', 'check-ops-shared', 'check-ops-contracts'}, path)
        # The history adapter's hook and the pinned firewall sources reach it too.
        for path in ['tools/transparent/scripts/transparent-live-fleet.py',
                     'tools/infra/digitalocean/transparent/main.tf']:
            self.assertIn('check-ops-txid-display', fast.plan(self.packages, [path])['helpers'], path)

    def test_status_ops_route_to_deploy_roles_and_control_sessions(self):
        for path in ['tools/status/lib/roles.py', 'tools/status/deploy/services.toml',
                     'tools/status/deploy/control-sessions.example.json']:
            result = fast.plan(self.packages, [path])
            self.assertEqual(set(result['helpers']), {'check-ops-shared', 'check-ops-control-sessions',
                                                      'check-ops-contracts'}, path)
            self.assertEqual(result['packages'], [])

    def test_leaf_reverse_dependencies_and_embedded_fixture(self):
        result = fast.plan(self.packages, ['src/shared/pir-apm/src/dashboard.rs'])
        self.assertEqual(set(result['packages']), {'pir-apm', 'pir-monitor'})
        self.assertEqual(result['groups'], ['enhance'])
        self.assertEqual(result['helpers'], [])
        result = fast.plan(self.packages, ['src/enhance/enhance-publisher/tests/fixtures/README.md'])
        self.assertIn('enhance-publisher', result['packages'])
        self.assertIn('check-docs', result['helpers'])

    def test_receiver_packages_also_qualify_the_composed_publisher(self):
        result = fast.plan(self.packages, ['src/receiver/receiver-store/src/lib.rs'])
        self.assertEqual(result['groups'], ['enhance', 'receiver'])
        self.assertIn('receiver-store', full.commands('receiver', full.inventory())[0])

    def test_shared_publisher_engine_selects_all_three_product_adapters(self):
        result = fast.plan(self.packages, ['src/shared/pir-pipeline/src/engine.rs'])
        self.assertTrue({'pir-publisher', 'enhance-publisher', 'status-publisher',
                         'transparent-publisher'} <= set(result['packages']))
        self.assertTrue({'enhance', 'transparent'} <= set(result['groups']))


    def test_shared_rust_and_unknown_changes_broaden_full_coverage(self):
        result = fast.plan(self.packages, ['src/shared/pir-control/src/lib.rs'])
        self.assertTrue({'shared', 'enhance', 'transparent', 'receiver'} <= set(result['groups']))
        for path in ['unknown.file', 'Cargo.lock', '.cargo/config.toml', 'tools/ci/fast.py', '.github/workflows/ci-full.yml']:
            result = fast.plan(self.packages, [path])
            self.assertEqual(set(result['groups']), fast.FULL_GROUPS)
            self.assertEqual(len(result['packages']), 0 if path.startswith(('tools/ci/', '.github/workflows/ci')) else len(self.packages))

    def test_infra_maps_to_its_own_validation(self):
        result = fast.plan(self.packages, ['tools/infra/digitalocean/transparent/worker-pool/main.tf'])
        self.assertIn('transparent_infra', result['groups'])
        self.assertNotIn('enhance_infra', result['groups'])

    def test_deleted_staged_unstaged_untracked_and_committed_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            def git(*args):
                return subprocess.check_output(['git', *args], cwd=root, stderr=subprocess.DEVNULL).decode().strip()
            git('init'); git('config', 'user.name', 'fixture'); git('config', 'user.email', 'fixture@example.invalid')
            for name in ['deleted', 'staged', 'unstaged']:
                (root / name).write_text('old')
            git('add', '.'); git('commit', '-m', 'base'); base = git('rev-parse', 'HEAD')
            (root / 'committed').write_text('new'); git('add', '.'); git('commit', '-m', 'next')
            (root / 'deleted').unlink(); (root / 'staged').write_text('new'); git('add', 'staged')
            (root / 'unstaged').write_text('new'); (root / 'untracked').write_text('new')
            self.assertEqual(set(fast.changed_paths(base, root=root)), {'committed', 'deleted', 'staged', 'unstaged', 'untracked'})
            self.assertEqual(fast.changed_paths(base, local=False, root=root), ['committed'])

    @unittest.skipUnless(os.environ.get("WALLET_PIR_CARGO_METADATA_TEST") == "1", "opt-in Rust dependency-graph comparison")
    def test_manifest_selection_matches_cargo_dependency_graph(self):
        # Offline metadata is cheap; it does not fetch or compile dependencies.
        metadata = json.loads(subprocess.check_output(['cargo', 'metadata', '--locked', '--offline', '--no-deps', '--format-version', '1'], cwd=ROOT))
        for member in self.packages:
            path = str(Path(member['manifest_path']).parent.relative_to(ROOT) / 'src/changed.rs')
            self.assertEqual(fast.select(self.packages, [path]), fast.select(metadata['packages'], [path]))

    def test_main_selects_every_family_and_explicit_shared_coverage(self):
        self.assertEqual(set(fast.plan(self.packages, [], all_checks=True)['groups']), fast.FULL_GROUPS)
        workflow = (ROOT / '.github/workflows/ci-full.yml').read_text()
        groups = full.inventory()
        for group in groups:
            self.assertIn('--group ' + group, workflow)
        for name in ['pir-control', 'pir-observability', 'pir-monitor', 'transparent-shared']:
            command = next(command for group in groups for command in full.commands(group, groups) if '-p' in command and name in command)
            self.assertIn('tools/ci/full-test.sh', command)
        self.assertIn('needs: [complete, build-cuda]', workflow)
        self.assertIn('validate-transparent-elastic-infra', workflow)

    def test_full_coverage_rejects_unclassified_or_duplicate_packages(self):
        groups = full.inventory()
        with self.assertRaisesRegex(ValueError, 'unclassified'):
            full.inventory([*self.packages, {'name': 'new-package'}], groups)
        groups['shared'].append('pir-apm')
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            full.inventory(self.packages, groups)

    def test_explicit_slow_unit_filter_runs_without_skipping(self):
        package = 'transparent-publisher'
        selected = 'publication::tests::block_by_block_publication_equals_a_fresh_build'
        listing = selected + ': test\n'
        metadata = {'packages': [{'name': package, 'targets': [{'kind': ['lib']}]}]}
        calls = []
        with patch.object(fast.subprocess, 'check_output', side_effect=[json.dumps(metadata).encode(), listing]), \
             patch.object(fast, 'run', side_effect=lambda command, **kw: calls.append(command)):
            fast.rust_checks([package], test=selected)
        self.assertIn(selected, calls[-1])
        self.assertNotIn('--skip', calls[-1])

    def test_explicit_integration_target_preserves_features_and_filter(self):
        metadata = {'packages': [{'name': 'example', 'targets': [{'name': 'http', 'kind': ['test']}]}]}
        calls = []
        with patch.object(fast.subprocess, 'check_output', side_effect=[json.dumps(metadata).encode(), 'answers: test\n']), \
             patch.object(fast, 'run', side_effect=lambda command, **kw: calls.append(command)):
            fast.rust_checks(['example'], features='native', test_target='http', test='answers')
        self.assertIn('native', calls[-1])
        self.assertEqual(calls[-1][-2:], ['--', 'answers'])
        self.assertIn('--test', calls[-1])
        self.assertNotIn('--lib', calls[-1])
        self.assertNotIn('--skip', calls[-1])


    def test_scaler_fast_excludes_exhaustive_models_and_registry_rejects_renames(self):
        result = fast.plan(self.packages, ['tools/transparent/scaler/decide.py'])
        self.assertIn('check-ops-scaler-fast', result['helpers'])
        self.assertNotIn('check-ops-scaler', result['helpers'])
        registry = json.loads((ROOT/'tools/ci/scaler-tests.json').read_text())
        scaler.validate(registry)
        self.assertIn('test_membership_model', registry['full'])
        registry['fast'][0] = 'renamed'
        with self.assertRaisesRegex(ValueError, 'classification mismatch'):
            scaler.validate(registry)


    def test_full_helpers_restore_full_fixtures_even_with_a_fast_parent(self):
        for full in (False, True):
            arguments = ['helpers.py', '--targets', '["check-tools"]']
            if full:
                arguments.append('--full')
            with patch.dict(os.environ, {'WALLET_PIR_FAST_TESTS': '1'}), \
                 patch.object(sys, 'argv', arguments), patch.object(helpers, 'run') as run:
                helpers.main()
                environment = run.call_args.kwargs['env']
                if full:
                    self.assertNotIn('WALLET_PIR_FAST_TESTS', environment)
                else:
                    self.assertEqual(environment['WALLET_PIR_FAST_TESTS'], '1')
                self.assertEqual(os.environ['WALLET_PIR_FAST_TESTS'], '1')

    def test_rust_execution_can_start_while_independent_helpers_are_running(self):
        started = threading.Event()
        helper_fails = False

        def run(command, **kwargs):
            if kwargs.get('stage') == 'helpers':
                self.assertTrue(started.wait(1), 'Rust execution waited for the helper suite')
                if helper_fails:
                    raise RuntimeError('helper check failed')

        def rust(selected, **kwargs):
            # Reproduce an orchestration barrier if one is supplied. Both
            # suites must still finish successfully before the gate can pass.
            if kwargs.get('before_tests'):
                kwargs['before_tests']()
            started.set()

        paths = ['tools/scripts/wallet-pir-deploy.py',
                 'src/transparent/transparent-publisher/src/lib.rs']
        # Eight or more CPUs overlap the suites; smaller hosts are covered by
        # test_rust_tests_wait_for_helpers_only_on_small_hosts.
        with patch.object(sys, 'argv', ['fast.py', '--base', 'HEAD', '--committed-only']), \
                patch.object(fast, 'changed_paths', return_value=paths), \
                patch('hygiene.check', return_value=0), \
                patch.object(fast, 'local_target', return_value=nullcontext()), \
                patch('fast_build.available_cpus', return_value=8), \
                patch.object(fast, 'run', side_effect=run), \
                patch.object(fast, 'rust_checks', side_effect=rust), redirect_stdout(io.StringIO()):
            fast.main()
            started.clear()
            helper_fails = True
            with self.assertRaisesRegex(RuntimeError, 'helper check failed'):
                fast.main()


    def test_full_commands_only_select_workspace_packages_and_feature_owners(self):
        workspace = {package['name'] for package in self.packages}
        groups = full.inventory()
        for group in groups:
            for lint, compile_only in [(False, False), (False, True), (True, False)]:
                for command in full.commands(group, groups, lint=lint, compile_only=compile_only):
                    selected = {command[index + 1] for index, word in enumerate(command[:-1]) if word == '-p'}
                    self.assertFalse(selected - workspace, (group, command))
                    for index, word in enumerate(command[:-1]):
                        if word == '--features':
                            for feature in command[index + 1].split(','):
                                if '/' in feature:
                                    owner, _ = feature.split('/', 1)
                                    self.assertIn(owner, selected, (group, command))

    def test_required_cli_binary_tests_are_enabled_for_a_focused_filter(self):
        metadata = {'packages': [{'name': 'example', 'targets': [
            {'name': 'client', 'kind': ['bin'], 'required-features': ['cli']}]}]}
        calls = []
        with patch.object(fast.subprocess, 'check_output', side_effect=[json.dumps(metadata).encode(), 'args: test\n']), \
             patch.object(fast, 'run', side_effect=lambda command, **kw: calls.append(command)):
            fast.rust_checks(['example'], test='args')
        self.assertIn('cli', calls[-1])
        self.assertNotIn('native-reinspiring', calls[-1])
        groups = full.inventory()
        self.assertIn('enhance-pir-client/native-reinspiring', full.commands('enhance', groups)[0][-1].split(','))
        self.assertEqual(len(full.commands('transparent', groups)), 1)


    def test_explicit_disabled_integration_target_cannot_pass_with_zero_tests(self):
        metadata = {'packages': [{'name': 'example', 'targets': [{'name': 'native', 'kind': ['test']}]}]}
        with patch.object(fast.subprocess, 'check_output', side_effect=[json.dumps(metadata).encode(), '0 tests\n']), \
             patch.object(fast, 'run'):
            with self.assertRaisesRegex(ValueError, 'no enabled tests'):
                fast.rust_checks(['example'], test_target='native')


class IntegrityTests(unittest.TestCase):
    def test_partial_rerun_labels_reused_jobs_without_negative_queue_time(self):
        run = dict(head_sha='a'*40, status='completed', run_attempt=2,
                   created_at='2026-09-14T00:00:00Z', run_started_at='2026-09-14T00:10:00Z')
        jobs = {'jobs': [dict(name='old', started_at='2026-09-14T00:00:05Z', completed_at='2026-09-14T00:01:00Z', steps=[]),
                         dict(name='new', started_at='2026-09-14T00:10:03Z', completed_at='2026-09-14T00:10:30Z', steps=[])]}
        with patch.object(timings, 'api', side_effect=[run, jobs]) as api:
            result = timings.report('org/repo', 1)
        # Jobs come from the current attempt, timed from its start, not the original creation.
        api.assert_called_with('repos/org/repo/actions/runs/1/attempts/2/jobs?per_page=100')
        self.assertEqual(result['attempt'], 2)
        self.assertTrue(result['jobs'][0]['reused_from_previous_attempt'])
        self.assertIsNone(result['jobs'][0]['dispatch_to_job_start_seconds'])
        self.assertEqual(result['jobs'][1]['dispatch_to_job_start_seconds'], 3)
        self.assertEqual(result['dispatch_seconds'], 30)
        self.assertEqual(result['total_dispatch_seconds'], 630)

    def test_dirty_result_is_rejected_even_with_unchanged_head(self):
        with patch.object(snapshot, 'snapshot', side_effect=[(b'sha', b''), (b'sha', b' M changed')]), \
             patch.object(snapshot.subprocess, 'run') as run, patch.object(snapshot.sys, 'argv', ['check', '--', 'true']):
            run.return_value.returncode = 0
            with self.assertRaisesRegex(SystemExit, 'stale'):
                snapshot.main()


if __name__ == '__main__':
    unittest.main()

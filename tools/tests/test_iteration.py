"""Selection, coverage, isolation and stale-result regressions for the shared gate."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
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
        result = fast.plan(self.packages, ['README.md'])
        self.assertEqual(result['packages'], [])
        self.assertEqual(result['helpers'], ['check-docs'])
        with patch.object(fast.subprocess, 'check_output', side_effect=AssertionError('no Cargo in manifest selection')):
            self.assertEqual(fast.workspace_packages(), self.packages)

    def test_ops_and_deploy_workflow_do_not_select_workspace_rust(self):
        for path in ['ops/tests/control_sessions/fake_ssh.py', '.github/workflows/deploy-enhance-cuda.yml']:
            result = fast.plan(self.packages, [path])
            self.assertEqual(result['packages'], [])
            self.assertEqual(result['groups'], ['ops'])
        self.assertEqual(fast.plan(self.packages, ['ops/tests/control_sessions/fake_ssh.py'])['helpers'], ['check-ops-control-sessions'])

    def test_shared_ops_helpers_select_all_consumers(self):
        result = fast.plan(self.packages, ['ops/lib/wallet_pir_ops/control_sessions.py'])
        self.assertTrue(fast.OPS <= set(result['helpers']))

    def test_transparent_schema_helpers_are_not_misclassified_as_parent_filters(self):
        for path in ['transparent/ops/lib/activity_source_stage.py',
                     'transparent/ops/tests/test_activity_schema_operation.py',
                     'transparent/ops/scripts/deploy-transparent-publisher.py']:
            result = fast.plan(self.packages, [path])
            self.assertIn('check-ops-deploy', result['helpers'], path)
            self.assertIn('check-ops-publication', result['helpers'], path)
            self.assertEqual(result['packages'], [])
        result = fast.plan(self.packages, ['transparent/ops/scripts/stage-transparent-parents.py'])
        self.assertEqual(set(result['helpers']), {'check-ops-contracts', 'check-ops-parents'})

    def test_leaf_reverse_dependencies_and_embedded_fixture(self):
        result = fast.plan(self.packages, ['enhance/services/pir-apm/src/dashboard.rs'])
        self.assertEqual(set(result['packages']), {'pir-apm', 'pir-monitor'})
        self.assertEqual(result['groups'], ['enhance'])
        self.assertEqual(result['helpers'], [])
        result = fast.plan(self.packages, ['enhance/services/enhance-pir-server/tests/fixtures/README.md'])
        self.assertIn('enhance-pir-server', result['packages'])
        self.assertIn('check-docs', result['helpers'])

    def test_shared_rust_and_unknown_changes_broaden_full_coverage(self):
        result = fast.plan(self.packages, ['shared/pir-control/src/lib.rs'])
        self.assertTrue({'shared', 'enhance', 'transparent'} <= set(result['groups']))
        for path in ['unknown.file', 'Cargo.lock', '.cargo/config.toml', 'tools/ci/fast.py', '.github/workflows/ci-full.yml']:
            result = fast.plan(self.packages, [path])
            self.assertEqual(set(result['groups']), fast.FULL_GROUPS)
            self.assertEqual(len(result['packages']), len(self.packages))

    def test_infra_maps_to_its_own_validation(self):
        result = fast.plan(self.packages, ['ops/infra/digitalocean/transparent-elastic/main.tf'])
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
        for name in ['pir-control', 'pir-observability', 'pir-monitor', 'transparent-native']:
            command = next(command for group in groups for command in full.commands(group, groups) if '-p' in command and name in command)
            self.assertIn('tools/ci/full-test.sh', command)
        self.assertIn('needs: [complete]', workflow)
        self.assertIn('validate-transparent-elastic-infra', workflow)

    def test_full_coverage_rejects_unclassified_or_duplicate_packages(self):
        groups = full.inventory()
        with self.assertRaisesRegex(ValueError, 'unclassified'):
            full.inventory([*self.packages, {'name': 'new-package'}], groups)
        groups['shared'].append('pir-apm')
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            full.inventory(self.packages, groups)

    def test_explicit_slow_unit_filter_runs_without_skipping(self):
        metadata = {'packages': [{'name': 'pir-apm', 'targets': [{'kind': ['bin']}]}]}
        calls = []
        with patch.object(fast.subprocess, 'check_output', side_effect=[json.dumps(metadata).encode(), 'fleet::integration_tests::failed_and_slow_workers_do_not_block_pages_or_good_samples: test\npacking_fleet::tests::discovery_pages_and_failed_scrapes_preserve_last_valid_sample: test\n']), \
             patch.object(fast, 'run', side_effect=lambda command, **kw: calls.append(command)):
            fast.rust_checks(['pir-apm'], test='failed_and_slow')
        self.assertIn('failed_and_slow', calls[-1])
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
        result = fast.plan(self.packages, ['transparent/ops/scaler/decide.py'])
        self.assertIn('check-ops-scaler-fast', result['helpers'])
        self.assertNotIn('check-ops-scaler', result['helpers'])
        registry = json.loads((ROOT/'tools/ci/scaler-tests.json').read_text())
        scaler.validate(registry)
        self.assertIn('test_membership_model', registry['full'])
        registry['fast'][0] = 'renamed'
        with self.assertRaisesRegex(ValueError, 'classification mismatch'):
            scaler.validate(registry)


    def test_full_helper_dispatch_keeps_docs_precise_and_restores_models(self):
        self.assertEqual(helpers.targets(['check-docs'], full=True), ['check-docs'])
        self.assertEqual(helpers.targets(['check-ops-scaler-fast'], full=True), ['check-ops-scaler'])
        self.assertEqual(set(helpers.targets(list(fast.HELPERS), full=True)),
                         (fast.HELPERS - {'check-ops-scaler-fast'}) | {'check-ops-scaler'})
        with self.assertRaisesRegex(ValueError, 'unknown helper'):
            helpers.targets(['check-docs; unsafe'])


    def test_required_cli_binary_tests_are_enabled_without_changing_protocol(self):
        metadata = {'packages': [{'name': 'example', 'targets': [
            {'name': 'client', 'kind': ['bin'], 'required-features': ['cli']}]}]}
        calls = []
        with patch.object(fast.subprocess, 'check_output', side_effect=[json.dumps(metadata).encode(), 'args: test\n']), \
             patch.object(fast, 'run', side_effect=lambda command, **kw: calls.append(command)):
            fast.rust_checks(['example'])
        self.assertIn('cli', calls[-1])
        self.assertNotIn('native-reinspiring', calls[-1])
        groups = full.inventory()
        self.assertIn('enhance-pir/cli', full.commands('enhance', groups)[0])
        self.assertIn('transparent-filter/cli', full.commands('transparent', groups)[0])


    def test_explicit_disabled_integration_target_cannot_pass_with_zero_tests(self):
        metadata = {'packages': [{'name': 'example', 'targets': [{'name': 'native', 'kind': ['test']}]}]}
        with patch.object(fast.subprocess, 'check_output', side_effect=[json.dumps(metadata).encode(), '0 tests\n']), \
             patch.object(fast, 'run'):
            with self.assertRaisesRegex(ValueError, 'no enabled tests'):
                fast.rust_checks(['example'], test_target='native')


    def test_feature_gated_classifications_enable_discovery_and_reject_renames(self):
        metadata = {'packages': [{'name': 'transparent-wallet', 'targets': [{'kind': ['lib']}]}]}
        registry = json.loads((ROOT/'tools/ci/slow-tests.json').read_text())['transparent-wallet']
        listing = ''.join(name + ': test\n' for name in registry)
        calls = []
        with patch.object(fast.subprocess, 'check_output', side_effect=[json.dumps(metadata).encode(), listing]), \
             patch.object(fast, 'run', side_effect=lambda command, **kw: calls.append(command)):
            fast.rust_checks(['transparent-wallet'])
        self.assertIn('reqwest', calls[-1])
        self.assertIn('--skip', calls[-1])
        with patch.object(fast.subprocess, 'check_output', side_effect=[json.dumps(metadata).encode(), 'renamed: test\n']), \
             patch.object(fast, 'run'):
            with self.assertRaisesRegex(ValueError, 'stale'):
                fast.rust_checks(['transparent-wallet'])



class IntegrityTests(unittest.TestCase):
    def test_partial_rerun_labels_reused_jobs_without_negative_queue_time(self):
        run = dict(head_sha='a'*40, status='completed', run_attempt=2,
                   created_at='2026-09-14T00:00:00Z', run_started_at='2026-09-14T00:10:00Z')
        jobs = {'jobs': [dict(name='old', started_at='2026-09-14T00:00:05Z', completed_at='2026-09-14T00:01:00Z', steps=[]),
                         dict(name='new', started_at='2026-09-14T00:10:03Z', completed_at='2026-09-14T00:10:30Z', steps=[])]}
        with patch.object(timings, 'api', side_effect=[run, jobs]):
            result = timings.report('org/repo', 1)
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

    def test_release_native_does_not_use_checkout_local_target(self):
        workflow = (ROOT / '.github/workflows/ci-full.yml').read_text()
        self.assertIn('lane: release-native', workflow)
        self.assertNotIn('CARGO_TARGET_DIR: target-native', workflow)
        self.assertIn('--lane native-cuda --scope cuda', workflow)

    def test_cuda_toolchain_overrides_the_checkout_toolchain(self):
        # Installing 1.91 alone does not select it: rust-toolchain.toml wins
        # over rustup's default. Every command and the cache must share the
        # compiler recorded in the CUDA ABI manifest.
        workflow = (ROOT / '.github/workflows/ci-full.yml').read_text()
        cuda_job = workflow.split('\n  release-cuda:\n', 1)[1]
        metadata = load('release').CUDA_BUILD
        self.assertIn('    env:\n      RUSTUP_TOOLCHAIN: ' + metadata['rust'], cuda_job)


if __name__ == '__main__':
    unittest.main()

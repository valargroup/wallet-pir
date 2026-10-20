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

    def test_operations_routes_survive_the_directory_move(self):
        paths = ['lib/wallet_pir_ops/durable.py', 'scripts/wallet-pir-deploy.py',
                 'infra/digitalocean/transparent/main.tf', 'enhance/lib/roles.py',
                 'transparent/lib/roles.py', 'receiver/lib/roles.py',
                 'tests/deploy/test_ops_package.py', 'tests/test_operations_bundle.py']
        for suffix in paths:
            old = 'ops/' + suffix
            new = ('tools/tests/operations/' + suffix[len('tests/'):]
                   if suffix.startswith('tests/') else 'tools/' + suffix)
            with self.subTest(path=new):
                before = fast.plan(self.packages, [old])
                after = fast.plan(self.packages, [new])
                self.assertEqual(before['helpers'], after['helpers'])
                self.assertEqual(before['groups'], after['groups'])
                self.assertEqual(after['packages'], [])
                self.assertNotIn('check-ops-deploy', after['helpers'])

    def test_product_analysis_keeps_tooling_checks(self):
        for path in ('tools/transparent/filters/transparent_filter_build.py',
                     'tools/transparent/parent-filters/transparent_parent_evaluate.py',
                     'tools/enhance/schema11-interop/run.py',
                     'tools/status/status_qualification.py',
                     'tools/txid/txid-sizing/survey.py', 'tools/txid/txid-display/demo.py'):
            self.assertEqual(fast.route(path), ({'check-tools'}, {'ops'}), path)

    def test_docs_do_not_need_rust_or_other_helpers(self):
        result = fast.plan(self.packages, ['README.md', 'docs/ci.md', 'evidence/receiver/run.json'])
        self.assertEqual(result['packages'], [])
        self.assertEqual(result['helpers'], ['check-docs'])
        with patch.object(fast.subprocess, 'check_output', side_effect=AssertionError('no Cargo in manifest selection')):
            self.assertEqual(fast.workspace_packages(), self.packages)

    def test_ops_and_deploy_workflow_do_not_select_workspace_rust(self):
        for path in ['tools/tests/operations/control_sessions/fake_ssh.py', '.github/workflows/deploy-enhance-cuda.yml',
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
                     'tools/transparent/scripts/deploy-transparent-publisher.py']:
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

    def test_receiver_ops_config_changes_run_their_contract_suite(self):
        makefile = (ROOT / 'Makefile').read_text()
        self.assertIn("check-ops-shared:\n\tpython3 -m unittest discover -s tools/tests/operations -p 'test_*.py'", makefile)
        self.assertTrue((ROOT / 'tools/tests/operations/test_receiver_ops_config.py').is_file())
        self.assertTrue((ROOT / 'tools/tests/operations/test_receiver_caddy.py').is_file())
        for path in ['tools/receiver/digitalocean/Caddyfile', 'tools/receiver/digitalocean/cloud-init.yaml',
                     'tools/receiver/digitalocean/receiver-pir.service.in', 'tools/receiver/digitalocean/caddy.py',
                     'tools/receiver/digitalocean/README.md']:
            result = fast.plan(self.packages, [path])
            self.assertIn('check-ops-shared', result['helpers'], path)
            self.assertNotIn('check-ops-deploy', result['helpers'], path)
            self.assertEqual(result['packages'], [], path)

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


    def test_full_helper_dispatch_keeps_docs_precise_and_restores_models(self):
        self.assertEqual(helpers.targets(['check-docs'], full=True), ['check-docs'])
        self.assertEqual(helpers.targets(['check-ops-scaler-fast']), ['check-ops-scaler-fast'])
        self.assertEqual(helpers.targets(['check-ops-scaler-fast'], full=True), ['check-ops-scaler'])
        self.assertEqual(set(helpers.targets(list(fast.HELPERS), full=True)),
                         (fast.HELPERS - {'check-ops-scaler-fast'}) | {'check-ops-scaler'})
        with self.assertRaisesRegex(ValueError, 'unknown helper'):
            helpers.targets(['check-docs; unsafe'])

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
        with patch.object(sys, 'argv', ['fast.py', '--base', 'HEAD', '--committed-only']), \
                patch.object(fast, 'changed_paths', return_value=paths), \
                patch('hygiene.check', return_value=0), \
                patch.object(fast, 'local_target', return_value=nullcontext()), \
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
        self.assertNotIn('--features', full.commands('transparent', groups)[0])


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

    def test_release_build_produces_every_default_bundle_binary(self):
        # Each CPU candidate must contain every binary its family publishes.
        workflow = (ROOT / '.github/workflows/ci-full.yml').read_text()
        job = (ROOT / '.github/actions/qualify-artifacts/action.yml').read_text()
        qualification = load('qualification')
        release = load('release')
        for family in ['receiver', 'transparent', 'enhance']:
            with patch.object(qualification, 'run') as run:
                qualification.build(family)
            build = run.call_args.args[0]
            built = {build[i + 1] for i, word in enumerate(build) if word == '--bin'}
            for kind in qualification.KINDS[family]:
                if kind in release.ON_REQUEST:
                    continue
                self.assertLessEqual(set(release.BINARIES[kind]), built, kind)
                self.assertIn('name: %s-${{ github.sha }}' % kind, job)
                self.assertIn('path: release-bundles/%s/%s.tar.gz' % (kind, kind), job)
            self.assertEqual(build[:4], ['cargo', 'build', '--locked', '--release'])
            if family == 'enhance':
                self.assertEqual(build[build.index('--features') + 1], 'pir-query-server/enhance')

    def test_workflows_invoke_only_scripts_that_exist(self):
        import re
        for workflow in sorted((ROOT / '.github/workflows').glob('*.yml')):
            for path in re.findall(r'(?<![\w/$.-])((?:ops|tools|enhance|transparent|receiver)/[\w./-]+\.(?:sh|py))',
                                   workflow.read_text()):
                self.assertTrue((ROOT / path).is_file(), '%s invokes missing %s' % (workflow.name, path))

    def test_operations_package_is_published_without_rust(self):
        workflow = (ROOT / '.github/workflows/ci-full.yml').read_text()
        build = workflow.split('\n  build:\n', 1)[1].split('\n  release:\n', 1)[0]
        release = (ROOT / '.github/actions/qualify-artifacts/action.yml').read_text()
        self.assertIn("if: inputs.family == 'ops'", build)
        self.assertIn('qualification.py assemble --family ops', build)
        self.assertIn("if: inputs.family != 'ops'", build)
        self.assertIn('qualification.py verify --family', release)
        self.assertIn('name: wallet-pir-ops-${{ github.sha }}', release)
        self.assertIn('wallet-pir-ops', load('release').ON_REQUEST)

    def test_release_native_does_not_use_checkout_local_target(self):
        workflow = (ROOT / '.github/workflows/ci-full.yml').read_text()
        self.assertIn('lane: release-native', workflow)
        self.assertNotIn('CARGO_TARGET_DIR: target-native', workflow)
        self.assertIn('--lane native-cuda --scope cuda', workflow)

    def test_cuda_toolchain_overrides_the_checkout_toolchain(self):
        # Installing the pinned compiler alone does not select it: rust-toolchain.toml wins
        # over rustup's default. Every command and the cache must share the
        # compiler recorded in the CUDA ABI manifest.
        workflow = (ROOT / '.github/workflows/ci-full.yml').read_text()
        cuda_job = workflow.split('\n  build-cuda:\n', 1)[1].split('\n  release-cuda:\n', 1)[0]
        metadata = load('release').CUDA_BUILD
        self.assertIn('    env:\n      RUSTUP_TOOLCHAIN: ' + metadata['rust'], cuda_job)
        script = (ROOT / 'tools/ci/build-cuda.sh').read_text()
        clients, query_server = script.split('compile:cuda --', 1)
        self.assertNotIn('-p enhance-publisher', script)
        self.assertNotIn('-p pir-query-server', clients)
        self.assertIn('-p pir-query-server --bin pir-query-server', query_server)
        self.assertIn('--features enhance,native-reinspiring,cuda', query_server)
        native = workflow.split('compile:native --', 1)[1].split('assemble --family enhance --native', 1)[0]
        publisher, query = native.split('compile:native-query-server --', 1)
        self.assertNotIn('pir-query-server', publisher)
        self.assertIn('-p pir-query-server --bin pir-query-server', query)
        self.assertIn('--features enhance,status,native-reinspiring\n', query)


if __name__ == '__main__':
    unittest.main()

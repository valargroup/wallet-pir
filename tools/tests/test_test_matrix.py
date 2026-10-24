"""Qualification splitting preserves coverage and rejects incomplete evidence."""
import copy
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import subprocess
import textwrap
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/ci'))
import test_matrix as matrix
import cargo_cache
from stage import inherited_fds


class MatrixTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.plan = matrix.load_plan()

    def test_all_enabled_targets_are_assigned_once(self):
        registry, _, targets, _, _ = self.plan
        self.assertEqual(len(registry), 17)
        self.assertEqual({family: sum(s['family'] == family for s in registry.values())
                          for family in ('enhance', 'transparent', 'receiver')},
                         {'enhance': 8, 'transparent': 7, 'receiver': 2})
        assigned = [key for shard in registry.values() for key in shard['targets']]
        self.assertEqual(set(assigned), set(targets))
        self.assertEqual(len(assigned), len(set(assigned)))
        for shard in registry:
            self.assertIn(('full-test', shard), cargo_cache.SCOPES)
        for kind in ('bin', 'doc', 'example', 'check'):
            self.assertTrue(any(t['kind'] == kind for t in targets.values()), kind)

    def test_missing_duplicate_stale_and_new_targets_fail_closed(self):
        registry, _, _, packages, _ = self.plan
        for change in ('missing', 'duplicate', 'stale', 'new'):
            selected, discovered = copy.deepcopy(registry), copy.deepcopy(packages)
            shard = selected['transparent-wallet-sync']['targets']
            if change == 'missing': shard.pop()
            elif change == 'duplicate': shard.append(shard[0])
            elif change == 'stale': shard[0] += '-removed'
            else:
                target = copy.deepcopy(discovered['pir-query-server']['targets'][-1])
                target['name'] = 'new_wallet_boundary'
                discovered['pir-query-server']['targets'].append(target)
            with self.subTest(change=change), self.assertRaises(ValueError):
                matrix.load_plan(selected, discovered)

    def test_reference_filters_and_cuda_modes_do_not_expand_or_disappear(self):
        registry, contexts, targets, packages, _ = self.plan
        reference = registry['enhance-reference']['targets']
        self.assertIn('reference:enhance-shared:lib:enhance_shared', reference)
        self.assertIn('reference:pir-query-server:lib:pir_query_server', reference)
        self.assertFalse(any(key.startswith('reference:pir-query-server:')
                             and key != 'reference:pir-query-server:lib:pir_query_server'
                             for key in reference))
        host_features = matrix.package_features('pir-query-server', contexts['reference'], packages)
        self.assertIn('pir-query-server/status', host_features)
        self.assertNotIn('pir-query-server/native-reinspiring', host_features)
        self.assertNotIn('reference:enhance-publisher:test:http', reference)
        native = matrix.package_features('enhance-shared', contexts['native'], packages)
        cuda = matrix.package_features('enhance-shared', contexts['cuda'], packages)
        self.assertIn('enhance-shared/native-reinspiring', native)
        self.assertNotIn('enhance-shared/cuda', native)
        self.assertIn('enhance-shared/native-reinspiring', cuda)
        self.assertIn('enhance-shared/cuda', cuda)
        reference_features = matrix.package_features('enhance-shared', contexts['reference'], packages)
        self.assertIn('enhance-shared/client', reference_features)
        self.assertIn('enhance-shared/qualification-tests', reference_features)
        self.assertNotIn('enhance-shared/native-reinspiring', reference_features)
        self.assertIn('pir-e2e-tests/qualification-tests', contexts['reference']['features'])
        self.assertIn('status-shared/qualification-tests', contexts['reference']['features'])
        self.assertIn('status-shared/qualification-tests', contexts['native']['features'])
        self.assertNotIn('enhance-publisher/native-reinspiring', native)
        commands = matrix.cargo_commands(registry['enhance-cuda-check']['targets'], contexts, targets, packages)
        self.assertEqual(len(commands), 1)
        command = commands[0][3]
        self.assertEqual({command[i + 1] for i, arg in enumerate(command) if arg == '-p'},
                         {'enhance-shared',
                          'enhance-pir-load-test', 'pir-query-server', 'pir-cuda'})
        self.assertEqual(command[command.index('--features') + 1], ','.join(matrix.CUDA))
        self.assertTrue(all('--all-targets' in cmd[3] for cmd in commands))

    def test_reference_host_lists_and_runs_the_moved_status_cases(self):
        package = self.plan[3]['pir-query-server']
        artifact = dict(package_id=package['id'], target=dict(name='pir_query_server', kind=['lib']),
                        executable='/not-executed', features=['status', 'qualification-tests'])

        def listing(command, **_):
            self.assertEqual(command[1], 'status::')
            return '' if '--ignored' in command else 'status::controller::authority: test\n'

        with patch.object(matrix.subprocess, 'check_output', side_effect=listing):
            _, programs, cases = matrix.test_programs([artifact], 'reference',
                                                      {package['name']: package}, {})
        self.assertEqual(programs[0]['args'], ['status::'])
        self.assertEqual(cases['reference:pir-query-server:pir_query_server']['tests'],
                         ['status::controller::authority'])

    def test_reference_host_baseline_uses_the_same_host_features(self):
        _, contexts, _, packages, _ = self.plan
        commands = [command for variant, command in matrix.baseline_commands('enhance')
                    if variant == 'reference' and command[command.index('-p') + 1] == 'pir-query-server']
        self.assertEqual(len(commands), 1)
        features = commands[0][commands[0].index('--features') + 1].split(',')
        baseline = {'features': [feature if '/' in feature else 'pir-query-server/' + feature
                                 for feature in features]}
        host_features = lambda context: matrix.enabled_features(
            packages['pir-query-server'], matrix.package_features('pir-query-server', context, packages))
        self.assertEqual(host_features(baseline), host_features(contexts['reference']))

    def test_artifact_withdrawal_cases_keep_the_shared_validator_idle(self):
        package = self.plan[3]['pir-query-server']
        for target in ('txid_sharded', 'revisions_and_cache'):
            artifact = dict(package_id=package['id'], target=dict(name=target, kind=['test']),
                            executable='/not-executed', features=['qualification-tests'])
            def listing(command, **_):
                self.assertIn('--test-threads=1', command)
                return '' if '--ignored' in command else 'withdrawal: test\n'
            with patch.object(matrix.subprocess, 'check_output', side_effect=listing):
                _, programs, cases = matrix.test_programs([artifact], 'transparent',
                                                         {package['name']: package}, {})
            self.assertEqual(programs[0]['args'], ['--test-threads=1'])
            self.assertEqual(cases[f'transparent:pir-query-server:{target}']['tests'], ['withdrawal'])

    def test_database_reader_binaries_remain_in_full_qualification(self):
        registry, contexts, targets, packages, _ = self.plan
        keys = registry['transparent-publisher']['targets']
        commands = matrix.cargo_commands(keys, contexts, targets, packages)
        for package, binary, feature in (
                ('pir-publisher', 'pir-publisher', 'pir-publisher/rocksdb-reader'),
                ('transparent-tools', 'shard-cutoff', 'transparent-tools/rocksdb-reader'),
                ('transparent-tools', 'journal-inventory', 'transparent-tools/rocksdb-reader'),
                ('transparent-tools', 'compact-export', 'transparent-tools/compact-export')):
            key = f'transparent:{package}:bin:{binary}'
            self.assertIn(key, keys)
            selected = [command for _, _, covered, command in commands
                        if any(target['target'] == binary for target in covered)]
            self.assertTrue(selected, binary)
            for command in selected:
                features = command[command.index('--features') + 1].split(',')
                self.assertIn(feature, features)

    def receipts(self, family):
        registry, _, _, _, digest = self.plan
        return [dict(shard=name, family=family, sha='a' * 40, inventory=digest,
                     workflow={'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '2'},
                     passed=True, compile_only=False, completed=sorted(shard['targets']), cases={})
                for name, shard in registry.items() if shard['family'] == family]

    def test_cpu_and_cuda_receipts_are_required_independently(self):
        rows = self.receipts('enhance')
        cpu = [row for row in rows if not row['shard'].startswith('enhance-cuda-')]
        cuda = [row for row in rows if row['shard'].startswith('enhance-cuda-')]
        self.assertEqual(len(cpu), 6)
        self.assertEqual(len(cuda), 2)
        for selected, mode in ((cpu, {}), (cuda, {'cuda_only': True}),
                               (rows, {'include_cuda': True})):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for index, row in enumerate(selected):
                    (root / f'{index}.json').write_text(json.dumps(row))
                matrix.verify_receipts('enhance', root, self.plan, 'a' * 40,
                                       rows[0]['workflow'], **mode)
                (root / '0.json').unlink()
                with self.assertRaisesRegex(ValueError, 'missing qualification shards'):
                    matrix.verify_receipts('enhance', root, self.plan, 'a' * 40,
                                           rows[0]['workflow'], **mode)

    def test_cpu_baseline_does_not_enable_cuda_without_opt_in(self):
        self.assertNotIn('cuda', {variant for variant, _ in matrix.baseline_commands('enhance')})
        self.assertIn('cuda', {variant for variant, _ in matrix.baseline_commands('enhance', True)})

    def test_mixed_cpu_and_cuda_shards_are_rejected_before_setup(self):
        registry = copy.deepcopy(self.plan[0])
        registry['enhance-supporting']['targets'].append('cuda:pir-cuda:check:pir_cuda')
        with self.assertRaisesRegex(ValueError, 'separate nonempty shards'):
            matrix.selected_shards(registry)

    def test_reference_qualification_keeps_oracles_and_native_keeps_http_lifecycles(self):
        package = self.plan[3]['pir-e2e-tests']
        lifecycle = {'encrypted_publication_advances_with_a_pending_commit_and_survives_restart'}
        oracles = {'single_unit_hint_matches_independent_generic_67',
                   'single_unit_hint_matches_independent_generic_68',
                   'single_unit_hint_rejects_damage_and_pins_retired_file'}
        reference = self.plan[0]['enhance-reference']['targets']
        from integration_tests import inventory
        self.assertNotIn('encrypted_lifecycle', inventory()['pir-e2e-tests']['fast'])
        self.assertIn('encrypted_lifecycle', inventory()['pir-e2e-tests']['full'])
        self.assertIn('reference:pir-e2e-tests:test:ci_hint', reference)
        self.assertIn('reference:pir-e2e-tests:test:encrypted_lifecycle', reference)
        for case in ('unequal_units_match_monolithic_preprocessing_and_evaluation',
                     'tail_domain_reuses_b_units_and_decodes_b_at_unchanged_coordinates'):
            self.assertIn('runtime::tests::' + case, matrix.REFERENCE_LIB_FILTERS)
        for variant, target, expected in [('reference', 'ci_hint', oracles),
                                          ('native', 'ci_hint', oracles),
                                          ('native', 'encrypted_lifecycle', lifecycle),
                                          ('reference', 'encrypted_lifecycle', lifecycle)]:
            artifact = dict(package_id=package['id'], target=dict(name=target, kind=['test']),
                            executable='/not-executed', features=['qualification-tests'])

            def listing(command, **_):
                names = set() if '--ignored' in command else expected
                return ''.join(f'{name}: test\n' for name in sorted(names))

            with self.subTest(variant=variant, target=target), \
                    patch.object(matrix.subprocess, 'check_output', side_effect=listing):
                _, programs, cases = matrix.test_programs([artifact], variant, {package['name']: package}, {})
            self.assertEqual(set(cases[f'{variant}:pir-e2e-tests:{target}']['tests']), expected)
            self.assertEqual(programs[0]['args'], [])

    def verify(self, receipts):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index, receipt in enumerate(receipts):
                (root / f'{index}.json').write_text(json.dumps(receipt))
            matrix.verify_receipts('receiver', root, self.plan, 'a' * 40,
                                   {'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '2'})

    def test_qualification_rejects_a_bounded_executable_in_place_of_full_fixtures(self):
        package = copy.deepcopy(self.plan[3]['transparent-shared'])
        artifact = dict(package_id=package['id'],
                        target=dict(name='transparent_server_backend', kind=['lib']),
                        executable='/not-executed', features=[])
        with self.assertRaisesRegex(ValueError, 'qualification fixture feature missing'):
            matrix.test_programs([artifact], 'transparent', {package['name']: package}, {})

    def test_receipts_require_every_shard_and_the_exact_source_and_attempt(self):
        receipts = self.receipts('receiver')
        self.verify(receipts)
        changes = {'sha': 'b' * 40, 'inventory': 'other', 'passed': False,
                   'compile_only': True, 'family': 'enhance', 'completed': [],
                   'workflow': {'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '1'}}
        for key, value in changes.items():
            broken = copy.deepcopy(receipts)
            broken[0][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError): self.verify(broken)
        for broken in ([], receipts[:-1], receipts + [receipts[0]]):
            with self.assertRaises(ValueError): self.verify(broken)

    def test_baseline_comparison_rejects_changed_cases_ignored_state_and_features(self):
        for changed in (None, 'tests', 'ignored', 'features'):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                receipts = root / 'receipts'
                receipts.mkdir()
                rows = self.receipts('receiver')
                identity = 'receiver:receiver-pir-server:http'
                cases = {identity: dict(tests=['encrypted_round_trip'], ignored=[], features=['default'])}
                rows[0]['cases'] = copy.deepcopy(cases)
                if changed:
                    rows[0]['cases'][identity][changed] = ['unexpected']
                for index, row in enumerate(rows):
                    (receipts / f'{index}.json').write_text(json.dumps(row))
                baseline = root / 'baseline.json'
                baseline.write_text(json.dumps(dict(sha='a' * 40, family='receiver', passed=True, cases=cases)))
                with patch.object(matrix, 'source_identity', return_value='a' * 40), \
                        patch.object(matrix, 'workflow_identity', return_value=rows[0]['workflow']):
                    if changed:
                        with self.assertRaisesRegex(ValueError, 'inventory differs'):
                            matrix.compare_baseline('receiver', receipts, baseline, self.plan)
                    else:
                        matrix.compare_baseline('receiver', receipts, baseline, self.plan)

    def test_failed_or_cancelled_commands_never_write_a_success_receipt(self):
        import subprocess
        for failure in (subprocess.CalledProcessError(1, 'cargo'), KeyboardInterrupt()):
            with tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / 'result.json'
                with patch.object(matrix, 'source_identity', return_value='a' * 40), \
                        patch.object(matrix, 'run', side_effect=failure):
                    with self.assertRaises(type(failure)):
                        matrix.run_shard('receiver-server', self.plan, output)
                self.assertFalse(output.exists())

    def test_compile_only_does_not_run_or_list_test_programs_and_cannot_qualify(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'result.json'
            with patch.object(matrix, 'source_identity', return_value='a' * 40), \
                    patch.object(matrix, 'run', return_value=[]) as run:
                matrix.run_shard('receiver-server', self.plan, output, compile_only=True)
            result = json.loads(output.read_text())
            self.assertFalse(result['passed'])
            self.assertTrue(result['compile_only'])
            self.assertTrue(all(call.args[0][0] == 'cargo' for call in run.call_args_list))
            self.assertFalse(result['cases'])

    def test_real_program_environment_and_fixture_cleanup_on_success_and_failure(self):
        import subprocess
        registry, _, targets, packages, _ = self.plan
        for failed in (False, True):
            for shard in ('receiver-server', 'transparent-wallet-sync'):
                with self.subTest(failed=failed, shard=shard), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    observed = root / 'observed.jsonl'
                    script = root / 'program'
                    script.write_text('#!' + sys.executable + '\n' +
                        'import json,os,pathlib,sys\n'
                        'if "--list" in sys.argv:\n'
                        ' if "--ignored" not in sys.argv: print("boundary: test")\n'
                        'else:\n'
                        ' with open(os.environ["MATRIX_FIXTURE_LOG"],"a") as out: out.write(json.dumps(dict(cache=os.environ["WALLET_PIR_TEST_RUNTIME_CACHE"],threads=os.environ["RUST_TEST_THREADS"],package=os.environ["CARGO_PKG_NAME"]))+"\\n")\n'
                        ' sys.exit(int(os.environ["MATRIX_FIXTURE_FAILURE"]))\n')
                    script.chmod(0o700)
                    def command(command, **options):
                        if command[:3] == ['cargo', 'test', '--no-run']:
                            package = command[command.index('-p') + 1]
                            selected = (command[command.index('--features') + 1].split(',')
                                        if '--features' in command else [])
                            features = sorted(feature.split('/', 1)[1] for feature in selected
                                              if feature.startswith(package + '/'))
                            return [dict(executable=str(script), package_id=packages[package]['id'],
                                         target=dict(name=t['target'], kind=[t['kind']]), features=features)
                                    for key in registry[shard]['targets']
                                    if (t := targets[key])['package'] == package and t['kind'] in ('lib', 'bin', 'test')]
                        if 'tools/ci/unit_tests.py' in command:
                            subprocess.run(command, cwd=ROOT, env=options['env'], check=True,
                                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                           **inherited_fds(options['env']))
                    with patch.dict(os.environ, MATRIX_FIXTURE_LOG=str(observed), MATRIX_FIXTURE_FAILURE='1' if failed else '0'), \
                            patch.object(matrix, 'source_identity', return_value='a' * 40), \
                            patch.object(matrix, 'run', side_effect=command):
                        output = root / 'receipt.json'
                        if failed:
                            with self.assertRaises(subprocess.CalledProcessError):
                                matrix.run_shard(shard, self.plan, output)
                            self.assertFalse(output.exists())
                        else:
                            matrix.run_shard(shard, self.plan, output)
                            self.assertTrue(json.loads(output.read_text())['passed'])
                    rows = [json.loads(line) for line in observed.read_text().splitlines()]
                    self.assertTrue(rows)
                    self.assertEqual({r['threads'] for r in rows}, {'2' if shard.startswith('transparent') else '1'})
                    self.assertTrue(all(not Path(r['cache']).exists() for r in rows))


class WorkflowTests(unittest.TestCase):
    def job(self, text, name):
        return re.split(r'\n  [a-z][\w-]*:\n', text.split(f'\n  {name}:\n', 1)[1])[0]


    def test_hosted_checks_overlap_helpers_but_failed_helpers_block_completion(self):
        text = (ROOT / '.github/workflows/ci-full.yml').read_text()
        for name in ('test-shards', 'transparent-lint', 'enhance-lint',
                     'shared-test', 'receiver-lint'):
            job = self.job(text, name)
            self.assertIn('    needs: select\n', job, name)
            self.assertNotIn('needs.validate-ops', job, name)
            self.assertIn('!cancelled()', job, name)

        complete = self.job(text, 'complete')
        needs = next(line for line in complete.splitlines() if line.strip().startswith('needs:'))
        self.assertIn('validate-ops', needs)
        code = textwrap.dedent(complete.split("python3 - <<'PYCHECK'\n", 1)[1].split('          PYCHECK', 1)[0])
        outputs = dict.fromkeys(('transparent', 'enhance', 'receiver', 'shared',
                                 'ops', 'enhance_infra', 'transparent_infra'), 'true')
        checks = ('transparent-lint', 'transparent-test', 'enhance-lint', 'enhance-test',
                  'shared-test', 'receiver-test', 'validate-ops', 'validate-enhance-infra',
                  'validate-transparent-elastic-infra')
        results = {name: {'result': 'success'} for name in checks}
        results['cuda'] = {'result': 'skipped'}
        results['select'] = {'result': 'success', 'outputs': outputs}
        for outcome in ('success', 'failure', 'skipped', 'cancelled'):
            results['validate-ops']['result'] = outcome
            result = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True,
                                    env={**os.environ, 'RESULTS': json.dumps(results), 'INCLUDE_CUDA': 'false'})
            self.assertEqual(result.returncode == 0, outcome == 'success', result.stderr)
            if outcome != 'success':
                self.assertIn('validate-ops', result.stderr)


if __name__ == '__main__':
    unittest.main()

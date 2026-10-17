"""Qualification splitting preserves coverage and rejects incomplete evidence."""
import copy
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/ci'))
import test_matrix as matrix
import cargo_cache


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
        self.assertTrue(any(t['variant'] == 'export' for t in targets.values()))

    def test_missing_duplicate_stale_and_new_targets_fail_closed(self):
        registry, _, _, packages, _ = self.plan
        for change in ('missing', 'duplicate', 'stale', 'new'):
            selected, discovered = copy.deepcopy(registry), copy.deepcopy(packages)
            shard = selected['transparent-wallet-sync']['targets']
            if change == 'missing': shard.pop()
            elif change == 'duplicate': shard.append(shard[0])
            elif change == 'stale': shard[0] += '-removed'
            else:
                target = copy.deepcopy(discovered['transparent-shard-server']['targets'][-1])
                target['name'] = 'new_wallet_boundary'
                discovered['transparent-shard-server']['targets'].append(target)
            with self.subTest(change=change), self.assertRaises(ValueError):
                matrix.load_plan(selected, discovered)

    def test_reference_filters_and_cuda_modes_do_not_expand_or_disappear(self):
        registry, contexts, targets, packages, _ = self.plan
        reference = registry['enhance-reference']['targets']
        self.assertIn('reference:enhance-serving:lib:enhance_serving', reference)
        self.assertNotIn('reference:enhance-serving:test:http', reference)
        native = matrix.package_features('enhance-serving', contexts['native'], packages)
        cuda = matrix.package_features('enhance-serving', contexts['cuda'], packages)
        self.assertIn('enhance-serving/native-reinspiring', native)
        self.assertNotIn('enhance-serving/cuda', native)
        self.assertIn('enhance-serving/native-reinspiring', cuda)
        self.assertIn('enhance-serving/cuda', cuda)
        self.assertFalse(matrix.package_features('enhance-serving', contexts['reference'], packages))
        self.assertNotIn('enhance-pir-server/native-reinspiring', native)
        commands = matrix.cargo_commands(registry['enhance-cuda-check']['targets'], contexts, targets, packages)
        self.assertEqual({cmd[3][cmd[3].index('-p') + 1] for cmd in commands},
                         {'enhance-serving', 'status-serving', 'enhance-pir-server', 'enhance-pir-load-test'})
        self.assertTrue(all('--all-targets' in cmd[3] for cmd in commands))

    def receipts(self, family):
        registry, _, _, _, digest = self.plan
        return [dict(shard=name, family=family, sha='a' * 40, inventory=digest,
                     workflow={'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '2'},
                     passed=True, compile_only=False, completed=sorted(shard['targets']), cases={})
                for name, shard in registry.items() if shard['family'] == family]

    def verify(self, receipts):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index, receipt in enumerate(receipts):
                (root / f'{index}.json').write_text(json.dumps(receipt))
            matrix.verify_receipts('receiver', root, self.plan, 'a' * 40,
                                   {'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '2'})

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
                            return [dict(executable=str(script), package_id=packages[package]['id'],
                                         target=dict(name=t['target'], kind=[t['kind']]), features=[])
                                    for key in registry[shard]['targets']
                                    if (t := targets[key])['package'] == package and t['kind'] in ('lib', 'bin', 'test')]
                        if 'tools/ci/unit_tests.py' in command:
                            subprocess.run(command, cwd=ROOT, env=options['env'], check=True,
                                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
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

    def test_independent_work_starts_without_waiting_for_common_or_shared(self):
        text = (ROOT / '.github/workflows/ci-qualify.yml').read_text()
        for family in ('receiver', 'transparent', 'enhance', 'ops'):
            job = self.job(text, family)
            self.assertIn('    needs: select\n', job)
            self.assertIn('defer-release: true', job)
            publish = self.job(text, 'publish-' + family)
            for gate in ('common', family, *(['shared'] if family != 'ops' else [])):
                self.assertIn(f"needs.{gate}.result == 'success'", publish)
            self.assertIn('!cancelled()', publish)
            self.assertIn('qualify-artifacts', publish)

    def test_aggregate_checks_receipts_and_matrix_failures_cannot_hide(self):
        text = (ROOT / '.github/workflows/ci-full.yml').read_text()
        shards = self.job(text, 'test-shards')
        self.assertIn('fail-fast: false', shards)
        self.assertIn('runs-on: ubuntu-24.04', shards)
        self.assertNotIn('continue-on-error', shards.split('Report Cargo cache reuse')[0])
        for family in ('enhance', 'transparent', 'receiver'):
            aggregate = self.job(text, family + '-test')
            self.assertIn("needs.test-shards.result == 'success'", aggregate)
            self.assertIn('test_matrix.py verify --family ' + family, aggregate)
        for job in ('release', 'release-cuda'):
            self.assertIn('!inputs.defer-release', self.job(text, job))

    def test_cache_variants_remain_isolated_and_qualification_uses_source_snapshots(self):
        registry = json.loads(matrix.REGISTRY.read_text())
        scopes = [cargo_cache.SCOPES['full-test', name] for name in registry]
        self.assertEqual(len(scopes), len(set(scopes)))
        self.assertTrue(cargo_cache.workspace_lane('full-test'))
        setup = (ROOT / '.github/actions/rust-setup/action.yml').read_text()
        self.assertIn("inputs.lane == 'fast' || inputs.lane == 'full-test'", setup)
        self.assertIn('target/${{ inputs.lane }}', setup)
        self.assertIn('actions/cache@v4', setup)


if __name__ == '__main__':
    unittest.main()

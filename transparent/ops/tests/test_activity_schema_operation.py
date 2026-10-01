"""Failure/interruption coverage for the wrapper's schema transaction boundary."""
import copy
import importlib.util
import json
import os
import subprocess
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT/'ops/lib'))
from wallet_pir_ops.deploy import cli  # noqa: E402
from wallet_pir_ops.deploy.remote import LockHeld  # noqa: E402
SPEC = importlib.util.spec_from_file_location('schema', ROOT/'transparent/ops/lib/activity_schema_operation.py')
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


class Lock:
    def __init__(self):
        self.held = False
        self.lost = False
    def __enter__(self):
        self.held = True
        return self
    def __exit__(self, *args):
        self.held = False
    def verify(self):
        if not self.held or self.lost:
            raise LockHeld('lost lock')
    def descriptors(self):
        self.verify()
        return (123,)


class SchemaTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.script = self.root/'phase.py'
        self.script.write_text('public fixture program')
        self.input = {'path': str(self.script), 'sha256': module.file_hash(self.script)}
        def command(name, read_only=False):
            return {'name': name, 'argv': ['/usr/bin/python3', str(self.script), name, '{transaction}', '{journal}'],
                    'timeout': 10, 'read_only': read_only}
        self.recipe = {'version': 1, 'source_sha': 'a'*40, 'publication_sha256': 'b'*64,
                       'inputs': [self.input], 'rollback_inputs': [self.input],
                       'preflight': [command('verify-inputs', True)],
                       'steps': [command(n, n.startswith('verify-')) for n in module.FORWARD],
                       'rollback': [command(n, n.startswith('verify-')) for n in module.ROLLBACK]}
        self.lock = Lock()
        self.calls = []
        self.fail = None
        self.interrupt = None
        self.lose = None
        self.service = 'v10'
        def run(command, log, fds):
            name = command['name']
            if name != 'verify-inputs':
                self.assertTrue(self.lock.held)
                self.assertEqual(fds, (123,))
                record = self.runner.load()
                self.assertEqual(record['events'][-1]['status'], 'running')
                self.assertEqual(record['events'][-1]['name'], name)
                self.assertNotIn('{transaction}', command['argv'])
                self.assertNotIn('{journal}', command['argv'])
            self.calls.append(name)
            if name == self.interrupt:
                raise KeyboardInterrupt()
            if name == self.lose:
                self.lock.lost = True
            if name == self.fail:
                return 1
            if name == 'activate-prewarm':
                self.service = 'v11'
            if name == 'restore-v10':
                self.service = 'v10'
            return 0
        self.runner = module.Runner(SimpleNamespace(lock={'type': 'pinned_host', 'machine_id': 'c'*32}),
                                    self.root/'state', run, lambda: self.lock, out=lambda x: None)
        self.runner.coordinator = lambda: None

    def deploy(self):
        return self.runner.deploy(self.recipe, module.digest(self.recipe))

    def test_complete_recipe_commits_with_durable_intent_and_lock(self):
        record = self.deploy()
        self.assertEqual(record['status'], 'committed')
        self.assertEqual(self.service, 'v11')
        self.assertEqual(self.calls, ['verify-inputs']+list(module.FORWARD))
        self.assertTrue(all(e['status'] == 'passed' for e in record['events']))
        self.assertFalse(self.lock.held)

    def test_failure_restores_coherent_predecessor_before_unlock(self):
        self.fail = 'verify-canonical'
        with self.assertRaises(module.OperationError):
            self.deploy()
        self.assertEqual(self.runner.load()['status'], 'rolled-back')
        self.assertEqual(self.service, 'v10')
        self.assertNotIn('resume-load', self.calls)
        self.assertEqual(self.calls[-len(module.ROLLBACK):], list(module.ROLLBACK))

    def test_rollback_failure_blocks_reopening_and_new_deploy(self):
        self.fail = 'verify-canonical'
        original = self.runner.run
        def run(command, log, fds):
            if command['name'] == 'verify-rollback':
                return 1
            return original(command, log, fds)
        self.runner.run = run
        with self.assertRaises(module.OperationError):
            self.deploy()
        self.assertEqual(self.runner.load()['status'], 'rollback-failed')
        self.assertNotIn('reopen-v10', self.calls)
        with self.assertRaisesRegex(module.OperationError, 'unfinished'):
            self.deploy()

    def test_interruption_requires_explicit_recovery(self):
        self.interrupt = 'verify-canonical'
        with self.assertRaises(KeyboardInterrupt):
            self.deploy()
        self.assertEqual(self.runner.load()['status'], 'interrupted')
        self.assertNotIn('restore-v10', self.calls)
        with self.assertRaisesRegex(module.OperationError, 'unfinished'):
            self.deploy()
        self.interrupt = None
        self.runner.rollback()
        self.assertEqual(self.runner.load()['status'], 'rolled-back')
        self.assertEqual(self.service, 'v10')
        before = list(self.calls)
        self.runner.rollback()
        self.assertEqual(before, self.calls)

    def test_timeout_requires_explicit_recovery_without_racing_descendants(self):
        original = self.runner.run
        def run(command, log, fds):
            if command['name'] == 'verify-canonical':
                raise subprocess.TimeoutExpired(command['argv'], command['timeout'])
            return original(command, log, fds)
        self.runner.run = run
        with self.assertRaisesRegex(module.OperationError, 'timed out'):
            self.deploy()
        record = self.runner.load()
        self.assertEqual(record['status'], 'interrupted')
        self.assertEqual(record['events'][-1]['error_type'], 'TimeoutExpired')
        self.assertNotIn('restore-v10', self.calls)
        self.runner.run = original
        self.runner.rollback()
        self.assertEqual(self.service, 'v10')

    def test_real_subprocess_inherits_lock_and_failure_restores_fixture(self):
        machine = self.root/'machine-id'
        machine.write_text('c'*32)
        lock_path = self.root/'production.lock'
        class FixtureLock(module.ProductionLock):
            MACHINE_ID = machine
            ROOT_UID = os.getuid()
            PATH = lock_path
        marker = self.root/'fixture-service'
        marker.write_text('v10')
        self.script.write_text("""
import fcntl,os,sys
from pathlib import Path
phase=sys.argv[1]
if phase!='verify-inputs':
    fds=[int(x) for x in os.environ['WALLET_PIR_PRODUCTION_LOCK_FDS'].split(',')]
    expected=Path(sys.argv[2]).stat()
    assert any((os.fstat(fd).st_dev,os.fstat(fd).st_ino)==(expected.st_dev,expected.st_ino) for fd in fds)
marker=Path(sys.argv[3])
if phase=='activate-prewarm':marker.write_text('v11')
if phase=='restore-v10':marker.write_text('v10')
if phase=='verify-canonical':sys.exit(1)
""")
        self.input['sha256'] = module.file_hash(self.script)
        for group in ('preflight', 'steps', 'rollback'):
            for command in self.recipe[group]:
                command['argv'] = ['/usr/bin/python3', str(self.script), command['name'], str(lock_path), str(marker)]
        self.runner.run = module.run_command
        self.runner.lock_factory = lambda: FixtureLock(self.runner.inventory.lock)
        with self.assertRaises(module.OperationError):
            self.deploy()
        self.assertEqual(marker.read_text(), 'v10')
        self.assertEqual(self.runner.load()['status'], 'rolled-back')
        with FixtureLock(self.runner.inventory.lock) as held:
            held.verify()

    def test_lost_lock_prevents_following_mutations_and_rollback(self):
        self.lose = 'activate-prewarm'
        with self.assertRaises(LockHeld):
            self.deploy()
        self.assertNotIn('align-origins', self.calls)
        self.assertNotIn('restore-v10', self.calls)
        self.assertEqual(self.runner.load()['status'], 'applying')

    def test_changed_plan_digest_rejects_before_lock(self):
        with self.assertRaisesRegex(module.OperationError, 'reviewed plan'):
            self.runner.deploy(self.recipe, '0'*64)
        self.assertEqual(self.calls, [])
        self.assertFalse((self.root/'state').exists())

    def test_changed_forward_input_fails_before_side_effects(self):
        self.script.write_text('changed')
        with self.assertRaisesRegex(module.OperationError, 'checksum changed'):
            self.deploy()
        self.assertEqual(self.calls, [])
        self.assertFalse((self.root/'state').exists())

    def test_preflight_failure_never_starts_transaction(self):
        self.fail = 'verify-inputs'
        with self.assertRaisesRegex(module.OperationError, 'preflight failed'):
            self.deploy()
        self.assertEqual(self.calls, ['verify-inputs'])
        self.assertFalse((self.root/'state').exists())

    def test_changed_forward_input_does_not_block_independent_rollback(self):
        other = self.root/'rollback.py'
        other.write_text('retained predecessor restore')
        self.recipe['rollback_inputs'] = [{'path': str(other), 'sha256': module.file_hash(other)}]
        for c in self.recipe['rollback']:
            c['argv'][1] = str(other)
        record = self.deploy()
        self.script.unlink()
        self.runner.rollback(record['id'])
        self.assertEqual(self.service, 'v10')

    def test_input_is_rechecked_between_forward_phases(self):
        original = self.runner.run
        other = self.root/'rollback.py'
        other.write_text('retained restore')
        self.recipe['rollback_inputs'] = [{'path': str(other), 'sha256': module.file_hash(other)}]
        for c in self.recipe['rollback']:
            c['argv'][1] = str(other)
        def run(command, log, fds):
            code = original(command, log, fds)
            if command['name'] == 'maintenance':
                self.script.write_text('changed after preflight')
            return code
        self.runner.run = run
        with self.assertRaisesRegex(module.OperationError, 'checksum changed'):
            self.deploy()
        self.assertNotIn('stage-v11', self.calls)
        self.assertEqual(self.runner.load()['status'], 'rolled-back')

    def test_inline_interpreter_recipe_rejected(self):
        recipe = copy.deepcopy(self.recipe)
        recipe['steps'][0]['argv'] = ['/usr/bin/python3', '-c', 'dangerous inline program']
        with self.assertRaisesRegex(module.OperationError, 'checksum-bound script'):
            module.validate(recipe)

    def test_missing_phase_and_excessive_recovery_deadline_rejected(self):
        recipe = copy.deepcopy(self.recipe)
        recipe['steps'].pop()
        with self.assertRaisesRegex(module.OperationError, 'every forward phase'):
            module.validate(recipe)
        recipe = copy.deepcopy(self.recipe)
        for c in recipe['rollback']:
            c['timeout'] = 200
        with self.assertRaisesRegex(module.OperationError, '15 minutes'):
            module.validate(recipe)

    def test_record_tampering_and_path_traversal_rejected(self):
        record = self.deploy()
        path = self.root/'state'/(record['id']+'.json')
        record['recipe']['source_sha'] = 'd'*40
        path.write_text(json.dumps(record))
        with self.assertRaisesRegex(module.OperationError, 'recipe changed'):
            self.runner.load()
        with self.assertRaisesRegex(module.OperationError, 'identifier'):
            self.runner.load('../../credentials')

    def test_duplicate_json_and_oversized_recipe_rejected(self):
        path = self.root/'bad-recipe.json'
        path.write_text('{"version":1,"version":2}')
        with self.assertRaisesRegex(module.OperationError, 'duplicate JSON'):
            module.load_recipe(path)
        path.write_bytes(b' '*(module.MAX_RECIPE+1))
        with self.assertRaisesRegex(module.OperationError, 'size bound'):
            module.load_recipe(path)

    def test_read_only_schema_plan_uses_wrapper_cli(self):
        recipe_path = self.root/'recipe.json'
        recipe_path.write_text(json.dumps(self.recipe))
        inventory = self.root/'inventory.json'
        inventory.write_text(json.dumps({'hosts': {'fixture': {}}, 'ssh': {'mode': 'config'},
                                        'lock': {'type': 'pinned_host', 'machine_id': 'c'*32}, 'services': {}}))
        output = []
        code = cli.main(['--inventory', str(inventory), 'schema-plan', '--recipe', str(recipe_path)], out=output.append)
        self.assertEqual(code, 0)
        self.assertIn('recipe '+module.digest(self.recipe), output)
        self.assertFalse((self.root/'state').exists())


if __name__ == '__main__':
    unittest.main()

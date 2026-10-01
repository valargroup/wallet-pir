"""Failure/interruption coverage for the wrapper's schema transaction boundary."""
import copy
import importlib.util
import io
import json
import os
import subprocess
from pathlib import Path
import sys
import tempfile
import tarfile
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


HOST_SPEC = importlib.util.spec_from_file_location('source_host', ROOT/'transparent/ops/lib/activity_source_stage_host.py')
source_host = importlib.util.module_from_spec(HOST_SPEC)
HOST_SPEC.loader.exec_module(source_host)
STAGE_SPEC = importlib.util.spec_from_file_location('source_stage', ROOT/'transparent/ops/lib/activity_source_stage.py')
source_stage = importlib.util.module_from_spec(STAGE_SPEC)
STAGE_SPEC.loader.exec_module(source_stage)


class SourceStageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.meminfo = self.root/'meminfo'
        self.meminfo.write_text('MemTotal: 1000 kB\nMemAvailable: 800 kB\n')
        from unittest.mock import patch
        memory = patch.object(source_host, 'MEMINFO', self.meminfo)
        memory.start()
        self.addCleanup(memory.stop)
        self.lock = Lock()
        self.request = {'mode': 'stage', 'source_sha': 'a'*40, 'sha256': '', 'machine_id': 'b'*32}
        self.files = {'ops/scripts/wallet-pir-deploy.py': b'wrapper',
                      'transparent/ops/lib/activity_schema_operation.py': b'recipe runner'}

    def archive(self, extra=(), commit=None):
        data = io.BytesIO()
        with tarfile.open(fileobj=data, mode='w:gz', format=tarfile.PAX_FORMAT,
                          pax_headers={'comment': commit or self.request['source_sha']}) as tar:
            for name, contents in self.files.items():
                entry = tarfile.TarInfo(name)
                entry.size = len(contents)
                tar.addfile(entry, io.BytesIO(contents))
            for entry in extra:
                tar.addfile(entry)
        raw = data.getvalue()
        self.request['sha256'] = module.hashlib.sha256(raw).hexdigest()
        return raw

    def stage(self, data):
        with self.lock:
            return source_host.stage(self.request, self.lock, io.BytesIO(data), self.root/'ops')

    def test_bounded_archive_is_staged_idempotently_with_verified_files(self):
        data = self.archive()
        result = self.stage(data)
        self.assertEqual(result['status'], 'staged')
        target = Path(result['path'])
        for name, contents in self.files.items():
            self.assertEqual((target/name).read_bytes(), contents)
        # A retry verifies the retained set and does not consume replacement bytes.
        self.assertEqual(self.stage(b'not a second upload'), result)
        receipt = json.loads((self.root/'ops/staging'/('a'*40+'.json')).read_text())
        self.assertEqual(receipt['pid'], os.getpid())
        self.assertEqual(receipt['compressed_bytes'], len(data))
        self.assertEqual(set(receipt['files']), set(self.files))
        self.assertEqual((self.root/'ops/staging'/('a'*40+'.json')).stat().st_mode & 0o777, 0o600)

    def test_preflight_and_status_do_not_create_files(self):
        self.archive()
        for mode, expected in [('preflight', 'preflight-passed'), ('status', 'absent')]:
            self.request['mode'] = mode
            result = source_host.stage(self.request, source_stage.hostlock.PinnedHostLock(None), io.BytesIO(), self.root/'ops')
            self.assertEqual(result['status'], expected)
            self.assertFalse((self.root/'ops').exists())

    def test_transferred_checksum_failure_keeps_failed_receipt(self):
        data = self.archive()
        self.request['sha256'] = 'f'*64
        with self.assertRaisesRegex(ValueError, 'checksum differs'):
            self.stage(data)
        receipt = json.loads((self.root/'ops/staging'/('a'*40+'.json')).read_text())
        self.assertEqual(receipt['status'], 'failed')
        self.assertFalse((self.root/'ops/sources'/('a'*40)).exists())
        with self.assertRaisesRegex(ValueError, 'incomplete retained'):
            self.stage(data)

    def test_commit_marker_mismatch_rejected(self):
        with self.assertRaisesRegex(ValueError, 'requested git commit'):
            self.stage(self.archive(commit='c'*40))

    def test_traversal_links_special_files_and_duplicates_rejected(self):
        cases = [('unsafe', '../escape', tarfile.REGTYPE),
                 ('unsafe', '/absolute', tarfile.REGTYPE),
                 ('links', 'link', tarfile.SYMTYPE),
                 ('links', 'hard', tarfile.LNKTYPE),
                 ('special', 'fifo', tarfile.FIFOTYPE),
                 ('duplicate', 'ops/scripts/wallet-pir-deploy.py', tarfile.REGTYPE)]
        for expected, name, kind in cases:
            with self.subTest(name=name):
                entry = tarfile.TarInfo(name)
                entry.type, entry.linkname = kind, '../escape'
                target = self.root/('case-'+str(cases.index((expected, name, kind))))
                raw = self.archive([entry])
                with self.lock, self.assertRaises(ValueError):
                    source_host.stage(self.request, self.lock, io.BytesIO(raw), target)
                self.assertFalse((self.root/'escape').exists())

    def test_compressed_and_expanded_bounds_reject_without_install(self):
        from unittest.mock import patch
        data = self.archive()
        for field, limit in [('MAX_COMPRESSED', len(data)-1), ('MAX_EXPANDED', 1), ('MAX_ENTRIES', 1)]:
            with self.subTest(field=field), self.lock, patch.object(source_host, field, limit):
                with self.assertRaises(ValueError):
                    source_host.stage(self.request, self.lock, io.BytesIO(data), self.root/field)

    def test_existing_file_tamper_or_addition_or_symlink_is_rejected(self):
        data = self.archive()
        target = Path(self.stage(data)['path'])
        path = target/'ops/scripts/wallet-pir-deploy.py'
        path.write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError, 'checksum changed'):
            self.stage(data)
        path.write_bytes(self.files['ops/scripts/wallet-pir-deploy.py'])
        (target/'unreviewed.py').write_bytes(b'extra')
        with self.assertRaisesRegex(ValueError, 'file set changed'):
            self.stage(data)
        (target/'unreviewed.py').unlink()
        (target/'outside').symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'symlink'):
            self.stage(data)

    def test_status_verifies_files_without_requiring_free_disk(self):
        from unittest.mock import patch
        data = self.archive()
        self.stage(data)
        self.request['mode'] = 'status'
        with patch.object(source_host.os, 'statvfs', side_effect=AssertionError('status must remain readable')):
            result = self.stage(b'')
        self.assertEqual(result['verified_files'], 2)
        self.assertNotIn('files', result)

    def test_disk_floor_stops_before_receipt_or_upload(self):
        from unittest.mock import patch
        data = self.archive()
        disk = SimpleNamespace(f_bavail=1, f_frsize=4096, f_blocks=100)
        with patch.object(source_host.os, 'statvfs', return_value=disk):
            with self.assertRaisesRegex(ValueError, 'disk reserve'):
                self.stage(data)
        self.assertFalse((self.root/'ops').exists())

    def test_memory_floor_stops_before_receipt_or_upload(self):
        data = self.archive()
        self.meminfo.write_text('MemTotal: 1000 kB\nMemAvailable: 199 kB\n')
        with self.assertRaisesRegex(ValueError, 'memory below 20 percent'):
            self.stage(data)
        self.assertFalse((self.root/'ops').exists())
        self.meminfo.write_text('MemTotal: 1000 kB\nMemAvailable: 200 kB\n')
        self.assertEqual(self.stage(data)['status'], 'staged')

    def test_lost_lock_stops_before_mutation(self):
        data = self.archive()
        self.lock.lost = True
        with self.assertRaises(LockHeld):
            self.stage(data)
        self.assertFalse((self.root/'ops').exists())

    def test_wrapper_source_plan_has_no_remote_effects_and_checks_hash(self):
        archive = self.root/'source.tar.gz'
        archive.write_bytes(self.archive())
        inventory = self.root/'inventory.json'
        inventory.write_text(json.dumps({'hosts': {'coordinator': {'machine_id': 'b'*32}},
            'ssh': {'mode': 'config'}, 'lock': {'type': 'remote', 'host': 'coordinator'}, 'services': {}}))
        argv = ['--inventory', str(inventory), 'schema-source-plan', '--archive', str(archive),
                '--source-sha', self.request['source_sha'], '--sha256', self.request['sha256']]
        self.assertEqual(cli.main(argv, out=lambda x: None), 0)
        archive.write_bytes(b'changed')
        self.assertEqual(cli.main(argv, out=lambda x: None), 1)

    def test_source_plan_rejects_oversize_before_hash_or_remote_access(self):
        from unittest.mock import patch
        self.assertEqual(source_stage.MAX_COMPRESSED, source_host.MAX_COMPRESSED)
        archive = self.root/'oversized.tar.gz'
        with archive.open('wb') as stream:
            stream.truncate(source_stage.MAX_COMPRESSED+1)
        inventory = SimpleNamespace(hosts={'coordinator': {'machine_id': 'b'*32}},
            ssh={'mode': 'config'}, lock={'type': 'remote', 'host': 'coordinator'})
        client = source_stage.SourceStage(inventory, out=lambda x: None)
        with patch.object(source_stage.hashlib, 'file_digest', side_effect=AssertionError('no hash of oversized archive')):
            with self.assertRaisesRegex(ValueError, 'received-archive bound'):
                client.run('plan', 'a'*40, 'c'*64, archive)

    def test_real_helper_owns_lock_during_receipt_and_extraction(self):
        data = self.archive()
        machine = self.root/'machine-id'
        machine.write_text(self.request['machine_id'])
        lock_path = self.root/'production.lock'
        # Only fixture paths/UID differ from the exact transmitted helper.
        prefix = Path(source_stage.hostlock.__file__).read_text()
        prefix += '\nPinnedHostLock.MACHINE_ID = Path('+repr(str(machine))+')\n'
        prefix += 'PinnedHostLock.ROOT_UID = os.geteuid()\n'
        helper = source_stage.HELPER_PATH.read_text().replace(
            "STAGE_ROOT = Path('/srv/transparent-activity/ops')", 'STAGE_ROOT = Path('+repr(str(self.root/'ops'))+')').replace(
            "LOCK_PATH = Path('/run/lock/wallet-pir-production.lock')", 'LOCK_PATH = Path('+repr(str(lock_path))+')')
        helper = helper.replace("MEMINFO = Path('/proc/meminfo')", 'MEMINFO = Path('+repr(str(self.meminfo))+')')
        assertion = '''
_save = atomic_json
def atomic_json(path, value):
    competing = os.open(LOCK_PATH, os.O_RDWR)
    try:
        try:
            fcntl.flock(competing, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            pass
        else:
            raise AssertionError('receipt mutation ran without production lock')
    finally:
        os.close(competing)
    _save(path, value)
'''
        helper = helper.replace("if __name__ == '__main__':", assertion+"\nif __name__ == '__main__':")
        result = subprocess.run([sys.executable, '-c', prefix+'\n'+helper, json.dumps(self.request)],
                                input=data, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        reply = json.loads(result.stdout)
        self.assertTrue(reply['ok'], reply)
        from unittest.mock import patch
        with patch.object(source_stage.hostlock.PinnedHostLock, 'MACHINE_ID', machine), \
             patch.object(source_stage.hostlock.PinnedHostLock, 'ROOT_UID', os.geteuid()), \
             source_stage.hostlock.PinnedHostLock({'type': 'pinned_host', 'machine_id': self.request['machine_id']}, path=lock_path) as lock:
            lock.verify()


def load_tests(loader, tests, _pattern):
    # The wrapper's fixed preparation job shares this existing CI entry point;
    # registering it here avoids central Makefile churn selecting every Rust package.
    tests.addTests(loader.discover(str(Path(__file__).parent), pattern='test_activity_publication_job.py'))
    return tests


if __name__ == '__main__':
    unittest.main()

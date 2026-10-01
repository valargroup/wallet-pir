"""Real file/descriptor recovery checks for schema phase dependencies."""
import asyncio
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT/'ops/lib'))
from wallet_pir_ops import inherited_lock  # noqa: E402


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


B = load('baseline', ROOT/'transparent/ops/lib/activity_schema_baseline.py')
L = load('live', ROOT/'transparent/ops/scripts/transparent-live-fleet.py')
D = load('publisher', ROOT/'transparent/ops/scripts/deploy-transparent-publisher.py')


class BaselineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.live = self.root/'live'
        self.live.mkdir()
        self.file = self.live/'controller.json'
        self.file.write_text('v10')
        self.state = self.live/'state'
        self.state.mkdir()
        (self.state/'active.json').write_text('v10 active')
        (self.state/'request.json').write_text('v10 request')
        (self.state/'routing.lock').touch()
        self.retained = self.root/'v10-publications'
        self.retained.mkdir()
        (self.retained/'shards.json').write_text('v10 map')
        self.backup = self.root/'private-backup'
        self.missing = self.live/'optional-dropins'
        self.plan = {'version': 1, 'files': [
            {'path': str(self.file), 'required': True},
            {'path': str(self.state), 'required': True},
            {'path': str(self.missing), 'required': False}],
            'retained': [{'path': str(self.retained), 'sentinel': str(self.retained/'shards.json'),
                          'sha256': B.checksum(self.retained/'shards.json')}]}

    def capture(self):
        return B.capture(self.backup, self.plan)

    def test_complete_snapshot_is_private_independent_and_idempotent(self):
        record = self.capture()
        self.assertEqual(B.verify(self.backup), record)
        self.assertEqual(self.capture(), record)
        self.assertEqual(self.backup.stat().st_mode & 0o777, 0o700)
        self.assertNotEqual(self.file.stat().st_ino, (self.backup/'files/0').stat().st_ino)
        self.assertFalse((self.backup/'files/1/routing.lock').exists())
        self.file.write_text('v11')
        self.assertEqual((self.backup/'files/0').read_text(), 'v10')

    def test_restore_pairs_configs_active_records_and_absent_dropins(self):
        self.capture()
        self.file.write_text('v11')
        (self.state/'active.json').write_text('v11 active')
        (self.state/'new.json').write_text('candidate intent')
        self.missing.mkdir()
        (self.missing/'override.conf').write_text('candidate flags')
        B.restore(self.backup)
        self.assertEqual(self.file.read_text(), 'v10')
        self.assertEqual((self.state/'active.json').read_text(), 'v10 active')
        self.assertFalse((self.state/'new.json').exists())
        self.assertFalse(self.missing.exists())
        B.restore(self.backup)
        self.assertTrue(list(self.live.glob('state.schema-displaced-*')))

    def test_corrupt_snapshot_rejects_before_any_restore(self):
        self.capture()
        self.file.write_text('v11')
        (self.backup/'files/1/request.json').write_text('corrupt')
        with self.assertRaisesRegex(ValueError, 'rollback bytes'):
            B.restore(self.backup)
        self.assertEqual(self.file.read_text(), 'v11')

    def test_unknown_payload_rejects(self):
        self.capture()
        (self.backup/'unreviewed').write_text('payload')
        with self.assertRaisesRegex(ValueError, 'unexpected'):
            B.verify(self.backup)

    def test_partial_capture_cannot_be_reused(self):
        self.backup.mkdir()
        with self.assertRaises(FileNotFoundError):
            self.capture()
        self.assertFalse((self.backup/'complete.json').exists())

    def test_retained_data_identity_and_digest_are_fenced(self):
        self.capture()
        self.retained.rename(self.root/'original-v10')
        self.retained.mkdir()
        (self.retained/'shards.json').write_text('v10 map')
        with self.assertRaisesRegex(ValueError, 'namespace changed'):
            B.restore(self.backup)

    def test_retained_missing_or_changed_sentinel_rejects(self):
        self.capture()
        (self.retained/'shards.json').write_text('v11 map')
        with self.assertRaisesRegex(ValueError, 'sentinel changed'):
            B.restore(self.backup)

    def test_byte_bound_and_required_missing_abort_without_receipt(self):
        with patch.object(B, 'MAX_BYTES', 1):
            with self.assertRaisesRegex(ValueError, 'bound'):
                self.capture()
        self.assertFalse((self.backup/'complete.json').exists())

    def test_live_mutation_during_capture_aborts(self):
        copy = B.shutil.copyfile
        def mutate(src, dst, **kwargs):
            result = copy(src, dst, **kwargs)
            if Path(src) == self.file:
                self.file.write_text('concurrent update')
            return result
        with patch.object(B.shutil, 'copyfile', mutate):
            with self.assertRaisesRegex(ValueError, 'changed during'):
                self.capture()
        self.assertFalse((self.backup/'complete.json').exists())

    def test_routing_restore_can_be_deferred(self):
        self.capture()
        self.file.write_text('guarded')
        (self.state/'active.json').write_text('v11')
        B.restore(self.backup, include=[str(self.state)])
        self.assertEqual(self.file.read_text(), 'guarded')
        self.assertEqual((self.state/'active.json').read_text(), 'v10 active')
        with self.assertRaisesRegex(ValueError, 'not captured'):
            B.restore(self.backup, include=['/uncaptured'])

    def test_symlinks_are_preserved_without_copying_their_targets(self):
        (self.state/'release').symlink_to(self.retained)
        self.capture()
        self.assertTrue((self.backup/'files/1/release').is_symlink())
        self.assertEqual(os.readlink(self.backup/'files/1/release'), str(self.retained))

    def test_overlap_is_rejected_before_creating_backup(self):
        self.plan['files'].append({'path': str(self.state/'active.json'), 'required': True})
        with self.assertRaisesRegex(ValueError, 'overlapping'):
            self.capture()
        self.assertFalse(self.backup.exists())

    def test_capture_and_restore_preserve_directory_modes(self):
        os.chmod(self.state, 0o750)
        self.capture()
        self.assertEqual((self.backup/'files/1').stat().st_mode & 0o777, 0o750)
        os.chmod(self.state, 0o700)
        B.restore(self.backup)
        self.assertEqual(self.state.stat().st_mode & 0o777, 0o750)

    def test_required_missing_aborts(self):
        self.file.unlink()
        with self.assertRaisesRegex(ValueError, 'required'):
            self.capture()
        self.assertFalse((self.backup/'complete.json').exists())


class InheritanceTests(unittest.TestCase):
    def test_absence_is_optional_but_required_phase_refuses(self):
        with patch.dict(os.environ, {inherited_lock.VARIABLE: ''}):
            self.assertEqual(inherited_lock.options(), {})
            with self.assertRaisesRegex(ValueError, 'requires'):
                inherited_lock.descriptors(required=True)

    def test_malformed_or_closed_descriptor_refuses_before_execution(self):
        for value in ('garbage', '1', '3,3', '3,', '999999'):
            with self.subTest(value=value), patch.dict(os.environ, {inherited_lock.VARIABLE: value}):
                with self.assertRaises((ValueError, OSError)):
                    inherited_lock.options()

    def test_inherited_descriptor_must_name_expected_lock_path(self):
        with tempfile.TemporaryFile() as lock, tempfile.TemporaryFile() as other:
            with patch.dict(os.environ, {inherited_lock.VARIABLE: str(lock.fileno())}):
                with self.assertRaises(FileNotFoundError):
                    inherited_lock.descriptors(path='/nonexistent-fixture-lock')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'lock'
            path.write_text('')
            os.chmod(path, 0o600)
            with path.open() as lock, patch.dict(os.environ, {inherited_lock.VARIABLE: str(lock.fileno())}):
                self.assertEqual(inherited_lock.descriptors(path=path), (lock.fileno(),))
                other = Path(directory)/'other'
                other.write_text('')
                with self.assertRaisesRegex(ValueError, 'does not name'):
                    inherited_lock.descriptors(path=other)

    def test_real_sync_and_async_descendants_retain_fd_and_bytecode_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'lock'
            fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
            self.addCleanup(os.close, fd)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            program = Path(directory)/'child.py'
            program.write_text('import os; os.fstat(int(os.environ["WALLET_PIR_PRODUCTION_LOCK_FDS"])); '
                               'assert os.environ["PYTHONDONTWRITEBYTECODE"] == "1"; print("inherited")')
            with patch.dict(os.environ, {inherited_lock.VARIABLE: str(fd)}):
                D.execute([sys.executable, program], stdout=subprocess.DEVNULL)
                self.assertEqual(asyncio.run(L.run([sys.executable, program])), b'inherited\n')

    def test_surviving_grandchild_keeps_lock_after_parent_and_owner_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root/'lock'
            fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            grandchild = root/'grandchild.py'
            grandchild.write_text('import os,sys,time\nfrom pathlib import Path\n'
                                  'os.fstat(int(os.environ["WALLET_PIR_PRODUCTION_LOCK_FDS"]))\n'
                                  'Path(sys.argv[1]).write_text(str(os.getpid()))\n'
                                  'deadline=time.monotonic()+10\n'
                                  'while not Path(sys.argv[2]).exists() and time.monotonic()<deadline: time.sleep(.01)\n')
            program = root/'parent.py'
            program.write_text('import sys,subprocess\nsys.path.insert(0, sys.argv[1])\n'
                               'from wallet_pir_ops import inherited_lock\n'
                               'subprocess.Popen([sys.executable,*sys.argv[2:]], stdout=subprocess.DEVNULL, '
                               'stderr=subprocess.DEVNULL, **inherited_lock.options())\n')
            contender = os.open(path, os.O_RDWR)
            try:
                with patch.dict(os.environ, {inherited_lock.VARIABLE: str(fd)}):
                    asyncio.run(L.run([sys.executable, program, ROOT/'ops/lib', grandchild, root/'pid', root/'release']))
                os.close(fd)
                fd = None
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                (root/'release').touch()
                if fd is not None:
                    os.close(fd)
                os.close(contender)


if __name__ == '__main__':
    unittest.main()

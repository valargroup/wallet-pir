"""Retiring a cache must preserve serving state, links, and the reviewed plan."""
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'tools/lib'))
from wallet_pir_ops import durable
from wallet_pir_ops.deploy import cache_storage_host as host


class CacheStorageTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        self.active = self.root / 'https-active'
        self.old = self.root / 'https-old'
        self.active.mkdir()
        self.old.mkdir()
        (self.old / 'object').write_bytes(b'immutable bytes')
        self.proc = self.root / 'proc'
        (self.proc / 'self').mkdir(parents=True)
        (self.proc / 'self/mountinfo').write_text('')
        self.configuration = self.root / 'etc'
        self.configuration.mkdir()

    def unused(self):
        host.require_unused(self.old, self.active, self.proc, self.configuration)

    def test_active_outside_and_linked_directories_cannot_be_deleted(self):
        self.assertEqual(host.checked_cache(self.old, self.active), self.old)
        for root in (self.active, self.root, self.root / '..'):
            with self.assertRaises(ValueError):
                host.checked_cache(root, self.active)
        link = self.root / 'https-link'
        link.symlink_to(self.old, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'symbolic'):
            host.checked_cache(link, self.active)

    def test_plan_detects_replaced_files_and_excludes_shared_storage_from_estimate(self):
        before = host.tree_identity(self.old)
        self.assertEqual(host.tree_identity(self.old), before)
        os.link(self.old / 'object', self.active / 'shared')
        self.assertEqual(host.tree_identity(self.old)['reclaimable_bytes'], 0)
        (self.old / 'object').unlink()
        (self.old / 'object').write_bytes(b'replaced content')
        self.assertNotEqual(host.tree_identity(self.old)['tree_sha256'], before['tree_sha256'])

    def test_configuration_and_active_links_prevent_cleanup(self):
        self.unused()
        config = self.configuration / 'retained-worker.json'
        config.write_text(json.dumps({'state_dir': str(self.old)}))
        with self.assertRaisesRegex(ValueError, 'configuration'):
            self.unused()
        config.unlink()
        (self.active / 'reference').symlink_to(self.old / 'object')
        with self.assertRaisesRegex(ValueError, 'active cache links'):
            self.unused()

    def test_open_mapped_and_command_references_prevent_cleanup(self):
        process = self.proc / '123'
        (process / 'fd').mkdir(parents=True)
        (process / 'cmdline').write_bytes(b'worker\0')
        (process / 'maps').write_text('')
        (process / 'cwd').symlink_to(self.active)
        handle = process / 'fd/8'
        handle.symlink_to(self.old / 'object')
        with self.assertRaisesRegex(ValueError, 'open process handle'):
            self.unused()
        handle.unlink()
        (process / 'maps').write_text(str(self.old / 'object'))
        with self.assertRaisesRegex(ValueError, 'mapped'):
            self.unused()
        (process / 'maps').write_text('')
        (process / 'cmdline').write_bytes(str(self.old).encode())
        with self.assertRaisesRegex(ValueError, 'running process'):
            self.unused()

    def test_mounts_and_special_files_prevent_cleanup(self):
        (self.proc / 'self/mountinfo').write_text('1 0 0:1 / ' + str(self.old / 'nested') + ' rw - ext4 /dev/vda rw\n')
        with self.assertRaisesRegex(ValueError, 'mount point'):
            self.unused()
        os.mkfifo(self.old / 'pipe')
        with self.assertRaisesRegex(ValueError, 'special file'):
            host.tree_identity(self.old)

    def test_stale_plan_refuses_and_success_preserves_shared_active_file(self):
        reviewed = {'request': {'path': str(self.old)}, 'cache': host.tree_identity(self.old)}
        request = {'path': str(self.old)}
        receipts = self.root / 'receipts'
        with mock.patch.object(host, 'plan', return_value=reviewed):
            with self.assertRaisesRegex(ValueError, 'stale'):
                host.prune(request, '0' * 64, 'test', receipts)
            self.assertTrue(self.old.exists())
            self.assertFalse(receipts.exists())
            os.link(self.old / 'object', self.active / 'shared')
            result = host.prune(request, durable.digest(reviewed), 'test', receipts)
        self.assertFalse(self.old.exists())
        self.assertEqual((self.active / 'shared').read_bytes(), b'immutable bytes')
        self.assertEqual(result['phase'], 'complete')
        self.assertEqual(json.loads((receipts / 'test/state.json').read_text())['phase'], 'complete')


if __name__ == '__main__':
    unittest.main()

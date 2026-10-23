"""Storage cutover retains the original cache and refuses ambiguous mount state."""
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'tools/lib'))
from wallet_pir_ops.deploy import cache_data_host as host


class CacheDataTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()

    def test_original_directory_is_retained_and_repeated_move_is_safe(self):
        active, backup = self.root / 'active', self.root / 'backup'
        active.mkdir()
        (active / 'floor.json').write_text('durable authority')
        identity = [active.stat().st_dev, active.stat().st_ino]
        host.move_original(active, backup, identity)
        self.assertEqual((backup / 'floor.json').read_text(), 'durable authority')
        self.assertEqual(list(active.iterdir()), [])
        host.move_original(active, backup, identity)
        (active / 'unrelated').touch()
        with self.assertRaisesRegex(ValueError, 'not empty'):
            host.move_original(active, backup, identity)

    def test_replaced_source_refuses_before_renaming(self):
        active, backup = self.root / 'active', self.root / 'backup'
        active.mkdir()
        with self.assertRaisesRegex(ValueError, 'directory changed'):
            host.move_original(active, backup, [0, 0])
        self.assertTrue(active.exists())
        self.assertFalse(backup.exists())

    def test_boot_mounts_preserve_existing_entries_and_require_the_volume(self):
        before = 'UUID=root / ext4 defaults 0 1\n'
        planned = {'mount': '/mnt/data', 'active_cache': '/srv/cache',
                   'destination': '/mnt/data/cache', 'device': {'uuid': 'data'}}
        after = host.fstab_after(before, planned)
        self.assertTrue(after.startswith(before))
        self.assertIn('UUID=data /mnt/data ext4', after)
        self.assertIn('/mnt/data/cache /srv/cache none bind,x-systemd.requires-mounts-for=/mnt/data', after)
        with self.assertRaisesRegex(ValueError, 'already contains'):
            host.fstab_after(after, planned)

    def test_wrong_device_mount_prevents_copy_or_activation(self):
        state = {'plan': {'request': {'volume_name': 'data', 'size_gib': 500},
                          'device': {'resolved': '/dev/approved'}, 'mount': '/mnt/data',
                          'destination': '/mnt/data/cache'}}
        with mock.patch.object(host, 'device_info', return_value={'resolved': '/dev/other'}):
            with self.assertRaisesRegex(ValueError, 'identity changed'):
                host.require_volume(state)
        with mock.patch.object(host, 'device_info', return_value={'resolved': '/dev/approved'}), \
                mock.patch.object(host, 'mount_source', return_value='/dev/other'):
            with self.assertRaisesRegex(ValueError, 'not mounted'):
                host.require_volume(state)

    def test_same_filesystem_copy_is_refused_before_rsync(self):
        source, destination = self.root / 'source', self.root / 'destination'
        source.mkdir()
        with mock.patch.object(host.subprocess, 'run') as run:
            with self.assertRaisesRegex(ValueError, 'cross to the new volume'):
                host.copy_cache(source, destination, self.root / 'copy.log', False)
        run.assert_not_called()

    def test_failed_effect_keeps_intent_and_completed_effect_is_not_repeated(self):
        state = {'transaction': 'test', 'completed': [], 'intent': None}
        saved = []
        def save(value):
            saved.append(dict(value))
        with mock.patch.object(host, 'save', side_effect=save):
            with self.assertRaisesRegex(RuntimeError, 'lost reply'):
                host.effect(state, 'mount volume', mock.Mock(side_effect=RuntimeError('lost reply')))
            self.assertEqual(state['intent'], 'mount volume')
            self.assertNotIn('mount volume', state['completed'])
            operation = mock.Mock()
            host.effect(state, 'mount volume', operation)
            host.effect(state, 'mount volume', operation)
            operation.assert_called_once()
            self.assertIsNone(state['intent'])


if __name__ == '__main__':
    unittest.main()

"""A publisher's journal mode must agree with the rendered systemd sandbox."""
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'ops/lib'))
from wallet_pir_ops.deploy import units  # noqa: E402
from ops.transparent.lib.roles import publisher  # noqa: E402


class PublisherRoleTests(unittest.TestCase):
    def config(self, follow=False):
        return dict(data_dir='/srv/history/journal', publication_root='/srv/publications',
                    initial_publication='/srv/publications/initial', follow_journal=follow,
                    fleet_command='/opt/operations/ops/transparent/scripts/transparent-live-fleet.py',
                    fleet_config='/etc/history/fleet.json', listen='127.0.0.1:8096')

    def role(self, config, **kwargs):
        return publisher(config, config_path='/etc/history/controller.json',
                         state_dir='/srv/history-state', **kwargs)

    def test_writer_switch_removes_the_old_read_only_journal(self):
        follower = self.role(self.config(True))
        writer = self.role(self.config(False), template=follower.unit)
        effective = units.effective([writer.unit])['Service']
        self.assertIn('/srv/history/journal', [p.strip('"') for p in effective['ReadWritePaths']])
        self.assertNotIn('/srv/history/journal', [p.strip('"') for p in effective['ReadOnlyPaths']])
        self.assertFalse(json.loads(writer.files['/etc/history/controller.json'])['follow_journal'])
        self.assertEqual(effective['ProtectSystem'], 'strict')
        self.assertIn('/opt/transparent-publisher/credentials', [p.strip('"') for p in effective['ReadOnlyPaths']])

    def test_follower_switch_does_not_grant_journal_writes(self):
        writer = self.role(self.config(False))
        follower = self.role(self.config(True), template=writer.unit)
        effective = units.effective([follower.unit])['Service']
        self.assertIn('/srv/history/journal', [p.strip('"') for p in effective['ReadOnlyPaths']])
        self.assertNotIn('/srv/history/journal', [p.strip('"') for p in effective['ReadWritePaths']])
        self.assertTrue(json.loads(follower.files['/etc/history/controller.json'])['follow_journal'])

    def test_paths_and_mode_refuse_before_rendering(self):
        for bad in ('relative', '/', '/srv/../journal', '/srv/%n', '/srv/$STATE', '/srv/a\nExecStart=/bin/false'):
            config = self.config()
            config['data_dir'] = bad
            with self.subTest(path=bad), self.assertRaises(ValueError):
                self.role(config)
        for follow in ('false', 0, None):
            config = self.config()
            config['follow_journal'] = follow
            with self.subTest(mode=follow), self.assertRaises(ValueError):
                self.role(config)

    def test_follower_cannot_write_the_journal_through_an_ancestor(self):
        config = self.config(True)
        config['publication_root'] = '/srv'
        with self.assertRaisesRegex(ValueError, 'separate'):
            self.role(config)
        config = self.config()
        with self.assertRaisesRegex(ValueError, 'configuration'):
            publisher(config, config_path='/srv/history-state/controller.json', state_dir='/srv/history-state')

    def test_other_service_settings_and_configuration_fields_survive(self):
        template = '[Unit]\nAfter=node.service\n[Service]\nExecStart=/old --old\nEnvironmentFile=/etc/pir/runtime.env\nMemoryMax=7G\nProtectSystem=strict\n'
        config = self.config()
        config['operator_setting'] = {'unchanged': True}
        role = self.role(config, template=template)
        effective = units.effective([role.unit])['Service']
        self.assertEqual(effective['MemoryMax'], '7G')
        self.assertEqual(effective['EnvironmentFile'], ['/etc/pir/runtime.env'])
        self.assertEqual(json.loads(role.files['/etc/history/controller.json']), config)

    def test_continued_command_and_quoted_paths_render_without_leftover_arguments(self):
        template = '[Service]\nExecStart=/old \\\n --old\nReadWritePaths=/old \\\n "/old path"\nMemoryMax=7G\n'
        config = self.config()
        config['initial_publication'] = '/srv/publications/initial set'
        role = self.role(config, template=template)
        effective = units.effective([role.unit])['Service']
        self.assertIn('/srv/publications/initial set', effective['ReadWritePaths'])
        self.assertNotIn('/old path', effective['ReadWritePaths'])
        self.assertEqual(effective['MemoryMax'], '7G')


if __name__ == '__main__':
    unittest.main()

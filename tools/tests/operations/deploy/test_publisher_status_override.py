"""An ordinary publisher update owns only the exact Status HTTPS ExecStart override."""
import unittest
from publisher_fixture import PublisherFixture
from fake_fleet import sha256
from wallet_pir_ops.deploy import publisher
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.remote import RemoteError

STATUS_BINARY = b'status https publisher'
STATUS_RELEASE = publisher.ROOT_DIR + '/releases/' + sha256(STATUS_BINARY) + '/pir-publisher'
OVERRIDE = '[Service]\nExecStart=\nExecStart=' + STATUS_RELEASE + ' --config ' + publisher.CONFIG + '\n'


class StatusOverrideTests(PublisherFixture, unittest.TestCase):
    def install_override(self, text=OVERRIDE, path=publisher.STATUS_HTTPS_DROP):
        """Leave the publisher as the completed Status HTTPS cutover did."""
        self.ex.put('coordinator', STATUS_RELEASE, STATUS_BINARY)
        self.ex.edit('coordinator', path, text)
        self.ex.systemctl('coordinator', 'restart', publisher.UNIT)
        self.ex.effects.clear()
        self.ex.log.clear()

    def running(self):
        return self.ex.probe_unit('coordinator', publisher.UNIT)

    def test_override_path_is_the_status_renderer_path(self):
        # The retired renderer's DROP for the publisher unit (source 6b6b923b).
        self.assertEqual(publisher.STATUS_HTTPS_DROP, '/etc/systemd/system/pir-publisher.service.d/'
                         'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-status-https.conf')

    def test_update_retargets_the_override_and_rollback_restores_its_bytes(self):
        self.install_override()
        fragment = self.ex.read('coordinator', publisher.UNIT_PATH)
        plan = self.target.plan()['plan']
        self.assertEqual(plan['before'][publisher.STATUS_HTTPS_DROP]['text'], OVERRIDE)
        self.assertEqual(plan['executables'][STATUS_RELEASE], sha256(STATUS_BINARY))
        self.assertEqual(self.deploy()['phase'], 'committed')
        state = self.running()
        self.assertEqual(state['exe_sha256'], sha256(b'unified publisher executable'))
        self.assertEqual([d['path'] for d in state['drop_ins']], [publisher.STATUS_HTTPS_DROP])
        self.assertEqual(state['drop_ins'][0]['text'],
                         '[Service]\nExecStart=\nExecStart=' + plan['binary'] + ' --config ' + publisher.CONFIG + '\n')
        for setting in ('EnvironmentFile=/etc/pir/credentials.env', 'MemoryMax=40G', 'MemorySwapMax=0'):
            self.assertIn(setting, state['fragment_text'])
        completed = self.target.latest()['completed']
        self.assertLess(completed.index('stop ' + publisher.UNIT), completed.index('install ' + publisher.STATUS_HTTPS_DROP))
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertEqual(self.ex.read('coordinator', publisher.STATUS_HTTPS_DROP), OVERRIDE)
        self.assertEqual(self.ex.read('coordinator', publisher.UNIT_PATH), fragment)
        self.assertEqual(self.running()['exe_sha256'], sha256(STATUS_BINARY))

    def test_interrupted_update_recovers_the_status_release(self):
        self.install_override()
        self.ex.crash_on = ('stop', publisher.UNIT)
        with self.assertRaises(RemoteError):
            self.deploy()
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertEqual(self.ex.read('coordinator', publisher.STATUS_HTTPS_DROP), OVERRIDE)
        self.assertEqual(self.running()['exe_sha256'], sha256(STATUS_BINARY))

    def test_failed_probe_after_installing_the_override_rolls_back(self):
        self.install_override()
        self.ex.fail_probe = True
        with self.assertRaisesRegex(DeployError, 'encrypted-answer probe failed'):
            self.deploy()
        self.assertNotEqual(self.ex.read('coordinator', publisher.STATUS_HTTPS_DROP), OVERRIDE)
        self.ex.fail_probe = False
        self.target.rollback()
        self.assertEqual(self.ex.read('coordinator', publisher.STATUS_HTTPS_DROP), OVERRIDE)
        self.assertEqual(self.running()['exe_sha256'], sha256(STATUS_BINARY))

    def test_foreign_or_changed_overrides_refuse_without_effects(self):
        changed = {
            'foreign': (OVERRIDE, publisher.UNIT_PATH + '.d/override.conf'),
            'extra setting': (OVERRIDE + 'MemoryMax=80G\n', publisher.STATUS_HTTPS_DROP),
            'changed arguments': (OVERRIDE[:-1] + ' --verbose\n', publisher.STATUS_HTTPS_DROP),
            'unbound release': (OVERRIDE.replace(sha256(STATUS_BINARY), 'f' * 64), publisher.STATUS_HTTPS_DROP),
        }
        for name, (text, path) in changed.items():
            with self.subTest(name):
                self.setUp()
                if name == 'unbound release':
                    self.ex.put('coordinator', STATUS_RELEASE.replace(sha256(STATUS_BINARY), 'f' * 64), STATUS_BINARY)
                self.install_override(text, path)
                with self.assertRaisesRegex(DeployError, 'drop-ins|override'):
                    self.target.plan()
                self.assertEqual(self.ex.mutations(), [])

    def test_override_drift_after_review_refuses(self):
        self.install_override()
        plan = self.target.plan()
        self.ex.edit('coordinator', publisher.STATUS_HTTPS_DROP, OVERRIDE + 'Environment=X=1\n', reload=False)
        with self.assertRaisesRegex(DeployError, 'reload|override'):
            self.target.deploy(self.binary, plan['plan_sha256'], self.serving_binary)
        self.assertFalse(any(a[0] == 'stop' for a in self.ex.effects))

    def test_override_drift_after_update_blocks_rollback(self):
        self.install_override()
        self.deploy()
        self.ex.put('coordinator', publisher.STATUS_HTTPS_DROP, OVERRIDE + 'MemoryMax=80G\n')
        self.ex.effects.clear()
        with self.assertRaisesRegex(DeployError, 'outside transaction'):
            self.target.rollback()
        self.assertEqual(self.ex.effects, [])

    def test_other_schemas_still_refuse_the_override(self):
        self.install_override()
        self.target.request = dict(self.target.request, schema=publisher.HTTPS_SCHEMA, https={'publisher_budget_bytes': 0})
        with self.assertRaisesRegex(DeployError, 'drop-ins'):
            self.target.capture()


if __name__ == '__main__':
    unittest.main()

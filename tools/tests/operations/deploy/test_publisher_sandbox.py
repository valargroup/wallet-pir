"""Prepared sibling roots remain writable through publisher cutover and recovery."""
import copy
from types import SimpleNamespace
import unittest

from publisher_fixture import PublisherFixture
from wallet_pir_ops import durable
from wallet_pir_ops.deploy import publisher, publisher_products, publisher_sandbox, units
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.remote import RemoteError


class SandboxTests(PublisherFixture, unittest.TestCase):
    def broken_candidate(self):
        self.ex.systemctl('coordinator', 'stop', publisher.LOAD)
        self.request['resume_load'] = False
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        self.ex.fail_probe = True
        with self.assertRaises(DeployError):
            self.deploy()
        self.ex.fail_probe = False
        journal = self.target.latest()
        unit = journal['plan']['desired'][publisher.UNIT_PATH]
        # Reproduce a candidate produced by the predecessor renderer: the
        # configured root is writable only through its old sibling's mount.
        root = self.request['configuration']['transaction_details']['settings']['root']
        unit = unit.replace(root, root + '.old')
        journal['plan']['desired'][publisher.UNIT_PATH] = unit
        self.ex.put('coordinator', publisher.UNIT_PATH, unit)
        self.ex.systemctl('coordinator', 'daemon-reload')
        durable.atomic_json(self.root / (journal['id'] + '.json'), journal)
        self.ex.put('coordinator', publisher.ROOT_DIR + '/transactions/' + journal['id'] + '/journal.json',
                    durable.canonical(journal))
        self.ex.effects.clear()
        return journal

    def args(self, digest=None):
        return SimpleNamespace(command='publisher-cutover-repair-sandbox' + ('-plan' if digest is None else ''),
                               expect_plan_sha256=digest)

    def test_only_exact_missing_root_is_added_and_rendering_is_idempotent(self):
        config = self.request['configuration']
        history = config['transparent_history']['settings']['publication_root']
        details = config['transaction_details']['settings']['root']
        text = '[Service]\nProtectSystem=strict\nEnvironmentFile=/etc/private\nReadWritePaths=' + history + ' ' + details + '.old\n'
        after = publisher_products.storage_sandbox(text, config)
        self.assertTrue(after.startswith(text))
        self.assertEqual(units.effective([after])['Service']['ReadWritePaths'], [history, details + '.old', details])
        self.assertEqual(publisher_products.storage_sandbox(after, config), after)
        with self.assertRaisesRegex(DeployError, 'drop-in resets'):
            publisher_products.storage_sandbox(text, config, ['[Service]\nReadWritePaths=\n'])

    def test_repair_commits_same_binary_without_changing_config_or_starting_checker(self):
        journal = self.broken_candidate()
        config = self.ex.read('coordinator', publisher.CONFIG)
        baseline = copy.deepcopy(journal['plan']['before'])
        result = publisher_sandbox.dispatch(self.target, self.args())
        self.assertEqual(self.ex.effects, [])
        repaired = publisher_sandbox.dispatch(self.target, self.args(result['plan_sha256']))
        self.assertEqual(repaired['phase'], 'committed')
        self.assertEqual(repaired['plan']['before'], baseline)
        self.assertEqual(self.ex.read('coordinator', publisher.CONFIG), config)
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.request['release']['binary_sha256'])
        self.assertNotIn(('start', publisher.LOAD), self.ex.effects)
        self.assertNotIn(('restart', publisher.LOAD), self.ex.effects)
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')

    def test_repair_resumes_after_lost_start_reply(self):
        self.broken_candidate()
        digest = publisher_sandbox.dispatch(self.target, self.args())['plan_sha256']
        self.ex.crash_on = ('restart', publisher.UNIT)
        with self.assertRaises(RemoteError):
            publisher_sandbox.dispatch(self.target, self.args(digest))
        self.assertEqual(publisher_sandbox.dispatch(self.target, self.args(digest))['phase'], 'committed')

    def test_changed_managed_file_and_stale_plan_refuse_before_effects(self):
        self.broken_candidate()
        with self.assertRaisesRegex(DeployError, 'stale'):
            publisher_sandbox.dispatch(self.target, self.args('0' * 64))
        self.assertEqual(self.ex.effects, [])
        self.ex.put('coordinator', publisher.CONFIG, 'foreign change')
        with self.assertRaisesRegex(DeployError, 'managed file changed'):
            publisher_sandbox.dispatch(self.target, self.args())
        self.assertEqual(self.ex.effects, [])

"""Forward release repair retains the reviewed data and original rollback state."""
import copy
from pathlib import Path
import unittest

from publisher_fixture import PublisherFixture
from wallet_pir_ops import durable
from wallet_pir_ops.deploy import publisher, publisher_release_repair as repair
from wallet_pir_ops.deploy.engine import DeployError, file_sha256
from wallet_pir_ops.deploy.remote import production_lock, RemoteError


class ReleaseRepairTests(PublisherFixture, unittest.TestCase):
    def interrupted(self):
        self.ex.systemctl('coordinator', 'stop', publisher.LOAD)
        self.request['resume_load'] = False
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        self.ex.fail_probe = True
        with self.assertRaises(DeployError):
            self.deploy()
        self.ex.fail_probe = False
        previous = self.target.latest()
        self.fixed = self.root / 'fixed-publisher'
        self.fixed.write_bytes(b'publisher with the established authority field')
        self.request['release']['binary_sha256'] = file_sha256(self.fixed)
        self.request['release']['binary_bytes'] = self.fixed.stat().st_size
        self.ex.new_publisher = self.request['release']['binary_sha256']
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        self.ex.effects.clear()
        return previous

    def apply(self):
        with production_lock(self.target.inventory, self.ex) as self.target.lock:
            journal = self.target.latest()
            planned = repair.plan(self.target, journal)
            repair.apply(self.target, journal, planned, self.fixed)

    def test_release_repair_keeps_configuration_serving_and_original_rollback(self):
        previous = self.interrupted()
        before = copy.deepcopy(previous['plan']['before'])
        config = self.ex.read('coordinator', publisher.CONFIG)
        self.apply()
        result = self.target.verify()
        self.assertEqual(result['phase'], 'committed')
        self.assertEqual(result['plan']['before'], before)
        self.assertEqual(self.ex.read('coordinator', publisher.CONFIG), config)
        self.assertEqual(result['request']['serving'], previous['request']['serving'])
        self.assertNotIn(('start', publisher.LOAD), self.ex.effects)
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.old_publisher)

    def test_release_repair_resumes_after_losing_the_stop_reply(self):
        self.interrupted()
        self.ex.crash_on = ('stop', publisher.UNIT)
        with self.assertRaises(RemoteError):
            self.apply()
        self.apply()
        self.assertEqual(self.target.verify()['phase'], 'committed')

    def test_failed_acceptance_keeps_the_fixed_candidate_running(self):
        self.interrupted()
        self.apply()
        self.ex.fail_probe = True
        with self.assertRaises(DeployError):
            self.target.verify()
        self.assertEqual(self.target.latest()['phase'], 'verifying')
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'],
                         self.request['release']['binary_sha256'])
        self.assertNotIn(('start', publisher.LOAD), self.ex.effects)

    def test_configuration_change_refuses_before_any_effect(self):
        self.interrupted()
        self.request['configuration']['transaction_details']['settings']['root'] += '.other'
        self.target = self.make_target()
        with production_lock(self.target.inventory, self.ex) as self.target.lock:
            with self.assertRaisesRegex(DeployError, 'only the publisher executable'):
                repair.plan(self.target, self.target.latest())
        self.assertEqual(self.ex.effects, [])

    def test_changed_managed_file_refuses_before_any_effect(self):
        self.interrupted()
        self.ex.put('coordinator', publisher.CONFIG, 'foreign config')
        with production_lock(self.target.inventory, self.ex) as self.target.lock:
            with self.assertRaisesRegex(DeployError, 'managed file changed'):
                repair.plan(self.target, self.target.latest())
        self.assertEqual(self.ex.effects, [])

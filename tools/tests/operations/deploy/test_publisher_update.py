"""Publisher updates preserve product settings and recover after interrupted effects."""
import copy
import unittest
from publisher_fixture import PublisherFixture
from wallet_pir_ops.deploy import publisher
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.remote import RemoteError


class PublisherTests(PublisherFixture, unittest.TestCase):
    def test_plan_and_preflight_do_not_mutate(self):
        self.target.plan()
        self.assertEqual(self.ex.mutations(), [])

    def test_stale_input_refuses_before_staging(self):
        plan = self.target.plan()
        self.ex.put('coordinator', '/etc/enhance/workers.json', '{"changed":true}')
        with self.assertRaisesRegex(DeployError, 'stale'):
            self.target.deploy(self.binary, plan['plan_sha256'], self.serving_binary)
        self.assertEqual(self.ex.effects, [])

    def test_configuration_check_failure_never_stops_the_fleet(self):
        self.ex.refuse_configuration = True
        with self.assertRaisesRegex(DeployError, 'refuses its configuration'):
            self.deploy()
        self.assertFalse(any(a[0] == 'stop' for a in self.ex.effects))
        self.target.rollback()
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.old_publisher)

    def test_lost_stop_reply_restores_the_unified_owner(self):
        self.ex.crash_on = ('stop', publisher.UNIT)
        with self.assertRaises(RemoteError):
            self.deploy()
        with self.assertRaisesRegex(DeployError, 'unfinished'):
            self.target.plan()
        self.target.rollback()
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.old_publisher)

    def test_failed_exact_probe_never_commits(self):
        self.ex.fail_probe = True
        with self.assertRaisesRegex(DeployError, 'encrypted-answer probe failed'):
            self.deploy()
        self.assertEqual(self.target.latest()['phase'], 'verifying')
        self.ex.fail_probe = False
        self.target.rollback()

    def test_foreign_managed_file_blocks_restore_before_stopping(self):
        self.deploy()
        self.ex.put('coordinator', publisher.CONFIG, 'foreign configuration')
        self.ex.effects.clear()
        with self.assertRaisesRegex(DeployError, 'outside transaction'):
            self.target.rollback()
        self.assertEqual(self.ex.effects, [])

    def test_restart_preserves_the_rollback_baseline_and_credentials(self):
        self.deploy()
        self.assertIn('EnvironmentFile=/etc/pir/credentials.env', self.ex.read('coordinator', publisher.UNIT_PATH))
        self.target.restart()
        self.target.rollback()
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.old_publisher)
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')

    def test_low_resources_or_a_held_writer_lock_prevent_activation(self):
        self.ex.memory = 1 << 30
        with self.assertRaisesRegex(DeployError, '20 percent'):
            self.target.plan()
        self.ex.memory = 64 << 30
        self.ex.lock_held = True
        with self.assertRaisesRegex(DeployError, 'retains its lock'):
            self.deploy()
        self.assertNotIn(('start', publisher.UNIT), self.ex.effects)

    def test_each_product_check_and_fleet_distribution_are_required(self):
        original = copy.deepcopy(self.request)
        self.request['checks'].pop('status')
        with self.assertRaisesRegex(DeployError, 'independent exact check'):
            self.make_target()
        self.request = original
        self.request['configuration']['transparent_history']['settings']['http_only'] = True
        with self.assertRaisesRegex(DeployError, 'HTTP publishing is out of scope'):
            self.make_target()

    def test_old_ownership_requests_are_recovery_only(self):
        self.request['schema'] = publisher.EXPANDED_SCHEMA
        self.request.pop('serving')
        with self.assertRaisesRegex(DeployError, 'recovery-only'):
            self.make_target()
        recovered = self.make_target(recovery=True)
        with self.assertRaisesRegex(DeployError, 'recovery-only'):
            recovered.plan()

"""Publisher updates preserve product settings and recover after interrupted effects."""
import copy
import unittest
from publisher_fixture import PublisherFixture
from wallet_pir_ops.deploy import publisher
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.remote import RemoteError


class PublisherTests(PublisherFixture, unittest.TestCase):
    def test_rollback_terminates_only_the_stalled_candidate_invocation(self):
        self.deploy()
        original = self.ex.systemctl
        def systemctl(host, *args):
            if args == ('stop', publisher.UNIT, '--no-block'):
                self.ex.effects.append(args)
                return  # Native preparation ignores SIGTERM until its current work finishes.
            if args == ('kill', publisher.UNIT, '--signal=SIGKILL'):
                self.ex.effects.append(args)
                return original(host, 'stop', publisher.UNIT)
            return original(host, *args)
        self.ex.systemctl = systemctl
        journal = self.target.rollback()
        self.assertEqual(journal['phase'], 'rolled-back')
        self.assertIn('terminate stalled candidate publisher', journal['completed'])
        self.assertIn(('kill', publisher.UNIT, '--signal=SIGKILL'), self.ex.effects)
        self.assertGreaterEqual(self.ex.clock(), 60)

    def test_recovery_checks_teardown_after_an_auxiliary_signal_error(self):
        self.deploy()
        original = self.ex.systemctl
        def systemctl(host, *args):
            if args == ('stop', publisher.UNIT, '--no-block'):
                return
            if args == ('kill', publisher.UNIT, '--signal=SIGKILL'):
                original(host, 'stop', publisher.UNIT)
                raise RemoteError('Failed to send signal SIGKILL to auxiliary processes: Invalid argument')
            return original(host, *args)
        self.ex.systemctl = systemctl
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')

    def test_recovery_refuses_to_terminate_a_replaced_invocation(self):
        self.deploy()
        original = self.ex.systemctl
        def systemctl(host, *args):
            if args == ('stop', publisher.UNIT, '--no-block'):
                self.ex.host(host).units[publisher.UNIT]['pid'] += 1
                return
            return original(host, *args)
        self.ex.systemctl = systemctl
        with self.assertRaisesRegex(DeployError, 'invocation changed'):
            self.target.rollback()
        self.assertFalse(any(args[0] == 'kill' for args in self.ex.effects))

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
        with self.assertRaisesRegex(DeployError, 'HTTP publishing requires complete prepared distribution'):
            self.make_target()

    def test_old_ownership_requests_are_recovery_only(self):
        self.request['schema'] = publisher.EXPANDED_SCHEMA
        self.request.pop('serving')
        with self.assertRaisesRegex(DeployError, 'recovery-only'):
            self.make_target()
        recovered = self.make_target(recovery=True)
        with self.assertRaisesRegex(DeployError, 'recovery-only'):
            recovered.plan()

    def test_default_mode_resumes_the_checker_after_deploy_restart_and_rollback(self):
        self.deploy()
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.LOAD)['active_state'], 'active')
        self.target.restart()
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.LOAD)['active_state'], 'active')
        self.target.rollback()
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.LOAD)['active_state'], 'active')

    def stopped_load_target(self, active='inactive'):
        self.ex.systemctl('coordinator', 'stop', publisher.LOAD)
        self.ex.host('coordinator').units[publisher.LOAD]['active'] = active
        self.request['resume_load'] = False
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        self.ex.effects.clear()

    def assert_load_stopped(self):
        state = self.ex.probe_unit('coordinator', publisher.LOAD)
        self.assertIn(state['active_state'], ('inactive', 'failed'))
        self.assertEqual(state['main_pid'], 0)
        self.assertNotIn(('start', publisher.LOAD), self.ex.effects)
        self.assertNotIn(('restart', publisher.LOAD), self.ex.effects)

    def test_stopped_checker_remains_stopped_through_deploy_restart_and_rollback(self):
        for active in ('inactive', 'failed'):
            with self.subTest(active=active):
                self.stopped_load_target(active)
                journal = self.deploy()
                self.assertEqual(journal['phase'], 'committed')
                self.assertEqual(set(journal['checks']), set(publisher.EXPANDED_PRODUCTS))
                self.assert_load_stopped()
                self.target.restart()
                self.assert_load_stopped()
                restored = self.target.rollback()
                self.assertEqual(restored['phase'], 'rolled-back')
                self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.old_publisher)
                self.assert_load_stopped()

    def test_probe_failure_restores_publisher_without_starting_stopped_checker(self):
        self.stopped_load_target('failed')
        self.ex.fail_probe = True
        with self.assertRaisesRegex(DeployError, 'encrypted-answer probe failed'):
            self.deploy()
        self.assert_load_stopped()
        self.ex.fail_probe = False
        self.target.rollback()
        self.assert_load_stopped()

    def test_checker_mode_must_match_the_request(self):
        self.request['resume_load'] = False
        target = self.make_target()
        with self.assertRaisesRegex(DeployError, 'requires.*stopped'):
            target.plan()
        self.ex.systemctl('coordinator', 'stop', publisher.LOAD)
        self.ex.effects.clear()
        with self.assertRaisesRegex(DeployError, 'must be running'):
            self.target.plan()
        self.assertEqual(self.ex.effects, [])

    def test_stopped_checker_with_a_process_or_critical_latch_refuses(self):
        self.stopped_load_target('failed')
        state = self.ex.host('coordinator').units[publisher.LOAD]
        state['pid'] = 42
        with self.assertRaisesRegex(DeployError, 'MainPID 0'):
            self.target.plan()
        state['pid'] = 0
        self.ex.put('coordinator', publisher.LOAD_LATCH, '{"reason":"incorrect encrypted answer"}')
        with self.assertRaisesRegex(DeployError, 'critical latch'):
            self.target.plan()
        self.assertEqual(self.ex.effects, [])

    def test_stopped_checker_restart_refuses_a_new_latch(self):
        self.stopped_load_target()
        self.deploy()
        self.ex.put('coordinator', publisher.LOAD_LATCH, '{"reason":"incorrect encrypted answer"}')
        self.ex.effects.clear()
        with self.assertRaisesRegex(DeployError, 'critical latch'):
            self.target.restart()
        self.assertEqual(self.ex.effects, [])

    def test_resume_load_option_is_boolean_and_only_for_normal_serving_updates(self):
        for value in (None, 0, 1, 'false'):
            with self.subTest(value=value):
                self.request['resume_load'] = value
                with self.assertRaisesRegex(DeployError, 'boolean.*publisher-cutover/3'):
                    self.make_target()
        self.request['resume_load'] = False
        self.request['schema'] = publisher.EXPANDED_SCHEMA
        self.request.pop('serving')
        with self.assertRaisesRegex(DeployError, 'publisher-cutover/3 only'):
            self.make_target(recovery=True)

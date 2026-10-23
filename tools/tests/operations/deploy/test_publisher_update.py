"""Publisher updates preserve product settings and recover after interrupted effects."""
import copy
import json
import unittest
from publisher_fixture import PublisherFixture
from wallet_pir_ops.deploy import publisher
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.remote import RemoteError


class PublisherTests(PublisherFixture, unittest.TestCase):
    def interrupted_stop(self):
        self.request['resume_load'] = False
        self.ex.systemctl('coordinator', 'stop', publisher.LOAD)
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        original = self.ex.systemctl
        def systemctl(host, *args):
            result = original(host, *args)
            if args == ('stop', publisher.UNIT):
                raise RemoteError('connection lost after stop')
            return result
        self.ex.systemctl = systemctl
        with self.assertRaises(RemoteError):
            self.deploy()
        self.ex.systemctl = original
        self.ex.effects.clear()

    def test_interrupted_stop_installs_staged_candidate_without_restarting_predecessor(self):
        self.interrupted_stop()
        self.assertEqual(self.target.continue_installation()['phase'], 'committed')
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'],
                         self.request['release']['binary_sha256'])
        self.assertFalse(any(args[:2] == ('stop', publisher.UNIT) for args in self.ex.effects))

    def test_installation_recovery_refuses_changed_configuration(self):
        self.interrupted_stop()
        self.ex.put('coordinator', publisher.CONFIG, 'changed')
        with self.assertRaisesRegex(DeployError, 'managed file changed'):
            self.target.continue_installation()
        self.assertEqual(self.ex.effects, [])

    def test_installation_recovery_refuses_changed_staged_binary(self):
        self.interrupted_stop()
        journal = self.target.latest()
        self.ex.put('coordinator', journal['plan']['binary'], 'foreign binary')
        with self.assertRaisesRegex(DeployError, 'staged publisher changed'):
            self.target.continue_installation()
        self.assertEqual(self.ex.effects, [])

    def test_installation_recovery_refuses_a_running_publisher(self):
        self.interrupted_stop()
        self.ex.systemctl('coordinator', 'start', publisher.UNIT)
        self.ex.effects.clear()
        with self.assertRaisesRegex(DeployError, 'predecessor is not stopped'):
            self.target.continue_installation()
        self.assertEqual(self.ex.effects, [])

    def test_interrupted_verification_resumes_without_restarting_publisher(self):
        self.ex.fail_probe = True
        with self.assertRaises(DeployError):
            self.deploy()
        before = self.ex.probe_unit('coordinator', publisher.UNIT)['invocation_id']
        self.ex.fail_probe = False
        self.ex.effects.clear()
        self.assertEqual(self.target.verify()['phase'], 'committed')
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['invocation_id'], before)
        self.assertFalse(any(publisher.UNIT in args and args[0] in ('start', 'stop', 'restart')
                             for args in self.ex.effects))

    def test_verification_recovery_refuses_changed_configuration(self):
        self.ex.fail_probe = True
        with self.assertRaises(DeployError):
            self.deploy()
        self.ex.put('coordinator', publisher.CONFIG, 'foreign configuration')
        self.ex.effects.clear()
        with self.assertRaisesRegex(DeployError, 'managed file changed'):
            self.target.verify()
        self.assertEqual(self.ex.effects, [])

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

    def test_stale_managed_file_refuses_before_staging(self):
        plan = self.target.plan()
        observers = json.loads(self.ex.read('coordinator', publisher.OBSERVERS))
        self.ex.put('coordinator', publisher.OBSERVERS, json.dumps({**observers, 'changed': True}))
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

    def test_each_product_check_is_required(self):
        self.request['checks'].pop('status')
        with self.assertRaisesRegex(DeployError, 'independent exact check'):
            self.make_target()

    def test_old_ownership_requests_are_recovery_only(self):
        self.request['schema'] = publisher.EXPANDED_SCHEMA
        self.request.pop('serving')
        with self.assertRaisesRegex(DeployError, 'recovery-only'):
            self.make_target()
        recovered = self.make_target(recovery=True)
        with self.assertRaisesRegex(DeployError, 'recovery-only'):
            recovered.plan()

    def test_update_refuses_an_installed_configuration_it_would_change(self):
        # An installed fleet-era setting is not carried or dropped silently.
        installed = json.loads(self.ex.read('coordinator', publisher.CONFIG))
        installed['transparent_history']['settings']['fleet_command'] = '/opt/history/fleet.py'
        self.ex.put('coordinator', publisher.CONFIG, json.dumps(installed))
        with self.assertRaisesRegex(DeployError, 'preserve every product setting'):
            self.make_target().plan()

    def install_retired_https_settings(self):
        installed = copy.deepcopy(self.request['configuration'])
        installed['transparent_history']['settings'].update({
            'http_only': True, 'distribute_prepared': True, 'ship_runtimes': True,
            'shadow': False, 'fleet_command': '/opt/history/fleet.py', 'fleet_config': '/etc/retired-fleet.json'})
        installed['transaction_details']['settings'].update({
            'http_only': True, 'ship_runtimes': True, 'workers': ['retired-worker'],
            'mode': 'live', 'blocks_per_step': 10, 'step_interval_ms': 100,
            'replay_end': 1000, 'exit_when_idle': False})
        text = json.dumps(installed, indent=3) + '\n'
        self.ex.put('coordinator', publisher.CONFIG, text)
        return installed, text

    def test_https_update_removes_retired_settings_and_rollback_restores_exact_bytes(self):
        _, original = self.install_retired_https_settings()
        plan = self.target.plan()
        self.assertEqual(self.ex.mutations(), [])
        self.assertEqual(plan['plan']['before'][publisher.CONFIG]['text'], original)
        self.assertEqual(self.target.deploy(self.binary, plan['plan_sha256'], self.serving_binary)['phase'], 'committed')
        self.assertEqual(json.loads(self.ex.read('coordinator', publisher.CONFIG)), self.request['configuration'])
        self.target.rollback()
        self.assertEqual(self.ex.read('coordinator', publisher.CONFIG), original)
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.old_publisher)

    def test_retired_settings_do_not_allow_other_product_changes(self):
        self.install_retired_https_settings()
        original = copy.deepcopy(self.request['configuration'])
        changes = [('transparent_history', 'recent_from', 2),
                   ('transparent_history', 'rpc_url', 'http://other-node:8232'),
                   ('transaction_details', 'poll_ms', 501),
                   ('enhance', 'poll_seconds', 20),
                   ('status', 'publisher', {'state_dir': '/srv/other-status'})]
        for product, field, value in changes:
            with self.subTest(product=product, field=field):
                self.request['configuration'] = copy.deepcopy(original)
                self.request['configuration'][product]['settings'][field] = value
                with self.assertRaisesRegex(DeployError, 'preserve every product setting'):
                    self.make_target().plan()
                self.assertEqual(self.ex.mutations(), [])

    def test_retiring_settings_requires_existing_https_and_prepared_runtimes(self):
        changes = [('transparent_history', 'http_only', False),
                   ('transparent_history', 'http_only', 1),
                   ('transparent_history', 'distribute_prepared', False),
                   ('transparent_history', 'ship_runtimes', False),
                   ('transparent_history', 'shadow', True),
                   ('transparent_history', 'shadow', 0),
                   ('transaction_details', 'http_only', False),
                   ('transaction_details', 'ship_runtimes', False),
                   ('transaction_details', 'mode', 'replay'),
                   ('transaction_details', 'exit_when_idle', True)]
        for product, field, value in changes:
            with self.subTest(product=product, field=field, value=value):
                installed, _ = self.install_retired_https_settings()
                installed[product]['settings'][field] = value
                self.ex.put('coordinator', publisher.CONFIG, json.dumps(installed))
                with self.assertRaisesRegex(DeployError, 'preserve every product setting'):
                    self.target.plan()
                self.assertEqual(self.ex.mutations(), [])

    def test_retired_field_drift_invalidates_the_reviewed_plan(self):
        installed, _ = self.install_retired_https_settings()
        plan = self.target.plan()
        installed['transparent_history']['settings']['fleet_config'] = '/etc/changed-fleet.json'
        self.ex.put('coordinator', publisher.CONFIG, json.dumps(installed))
        with self.assertRaisesRegex(DeployError, 'stale'):
            self.target.deploy(self.binary, plan['plan_sha256'], self.serving_binary)
        self.assertEqual(self.ex.mutations(), [])

    def test_new_update_requests_refuse_retired_settings_but_saved_requests_can_recover(self):
        original = copy.deepcopy(self.request['configuration'])
        for product, field in [('transparent_history', 'http_only'), ('transaction_details', 'workers')]:
            with self.subTest(product=product, field=field):
                self.request['configuration'] = copy.deepcopy(original)
                self.request['configuration'][product]['settings'][field] = True
                with self.assertRaisesRegex(DeployError, 'retired Transparent settings'):
                    self.make_target()
                self.make_target(recovery=True)

    def test_default_mode_resumes_the_checker_after_deploy_restart_and_rollback(self):
        self.deploy()
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.LOAD)['active_state'], 'active')
        self.target.restart()
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.LOAD)['active_state'], 'active')
        self.target.rollback()
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.LOAD)['active_state'], 'active')

    def disk_latched_target(self, reasons=None):
        self.stopped_load_target()
        text = json.dumps({'utc': '2026-10-21T00:00:00Z', 'reason': reasons or [
            'transparent-pir-v12-archive: disk headroom below 20%']})
        self.ex.put('coordinator', publisher.LOAD_LATCH, text)
        self.request['expected_disk_latch_sha256'] = publisher.sha(text)
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        return text

    def test_reviewed_disk_latch_survives_repair_and_rollback_with_checker_stopped(self):
        text = self.disk_latched_target()
        self.assertEqual(self.deploy()['phase'], 'committed')
        self.assert_load_stopped()
        self.assertEqual(self.ex.read('coordinator', publisher.LOAD_LATCH), text)
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assert_load_stopped()
        self.assertEqual(self.ex.read('coordinator', publisher.LOAD_LATCH), text)

    def test_other_critical_latches_still_refuse_before_effects(self):
        for reason in ['encrypted answer mismatch', 'transparent-pir-v12-archive: oom increased',
                       'transparent-pir-v12-archive: available memory below 20%']:
            self.disk_latched_target([reason])
            self.ex.effects.clear()
            with self.assertRaisesRegex(DeployError, 'only the reviewed disk-headroom'):
                self.target.plan()
            self.assertEqual(self.ex.effects, [])

    def test_reviewed_latch_change_invalidates_the_repair(self):
        self.disk_latched_target()
        self.ex.put('coordinator', publisher.LOAD_LATCH, 'changed')
        with self.assertRaisesRegex(DeployError, 'latch changed'):
            self.target.plan()
        self.ex.remove('coordinator', publisher.LOAD_LATCH)
        with self.assertRaisesRegex(DeployError, 'latch changed'):
            self.target.plan()

    def test_disk_latch_permission_cannot_resume_the_checker(self):
        self.disk_latched_target()
        self.request['resume_load'] = True
        with self.assertRaisesRegex(DeployError, 'resume_load:false'):
            self.make_target()

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

    def test_resume_load_option_requires_a_boolean_and_a_supported_update(self):
        for value in (None, 0, 1, 'false'):
            with self.subTest(value=value):
                self.request['resume_load'] = value
                with self.assertRaisesRegex(DeployError, 'boolean.*publisher update'):
                    self.make_target()
        self.request['resume_load'] = False
        self.request['schema'] = publisher.EXPANDED_SCHEMA
        self.request.pop('serving')
        with self.assertRaisesRegex(DeployError, 'publisher update or Enhance HTTPS'):
            self.make_target(recovery=True)

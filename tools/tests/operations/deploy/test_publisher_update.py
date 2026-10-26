"""Publisher updates preserve product settings and recover after interrupted effects."""
import copy
import json
import unittest
from publisher_fixture import PublisherFixture
from wallet_pir_ops.deploy import publisher
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.remote import RemoteError


class PublisherTests(PublisherFixture, unittest.TestCase):
    def receiver_request(self):
        from wallet_pir_ops.deploy import publisher_receiver
        self.request['configuration']['receiver'] = {'enabled': True, 'settings': {
            'data_dir': publisher_receiver.STATE, 'bind': '127.0.0.1:18380', 'depth': 2}}
        self.request['receiver_adoption'] = {
            'host': 'receiver-01', 'machine_id': 'b' * 32,
            'config_path': '/etc/receiver-pir-publisher.json', 'data_dir': publisher_receiver.STATE,
            'publisher_origin': 'https://publisher.example.test'}
        self.request['checks']['receiver'] = {'host': 'coordinator', 'argv': ['/bin/check-receiver'], 'timeout': 20}
        fleet = self.request['inventory']
        fleet['services']['publisher']['managed_products'] = [*publisher.PRODUCTS, 'receiver']
        fleet['services']['receiver'] = {'roles': {}}
        fleet['ancillary']['receiver-edge'] = {'host': 'receiver-01', 'unit': 'caddy.service',
                                              'kind': 'routing', 'consumers': ['receiver']}

    def test_receiver_adoption_accepts_fifth_product_and_preserves_existing_settings(self):
        from wallet_pir_ops.deploy import publisher_products
        installed = copy.deepcopy(self.request['configuration'])
        self.receiver_request()
        self.make_target()
        candidate = self.request['configuration']
        self.assertFalse(publisher.configuration_preserves_products(candidate, installed))
        self.assertTrue(publisher.configuration_preserves_products(candidate, installed,
                                                                   self.request['receiver_adoption']))
        states = {publisher.UNIT: self.ex.probe_unit('coordinator', publisher.UNIT)}
        rendered = publisher_products.render(candidate, '/bin/new-publisher', states,
                                             receiver_environments=['-/etc/receiver-pir/near-test.env'])
        self.assertIn('MemoryMax=47G', rendered)
        self.assertIn('"/srv/receiver-pir/index"', rendered)
        self.assertIn('EnvironmentFile=-/etc/receiver-pir/near-test.env', rendered)
        self.assertIn('/srv/receiver-pir/index/publisher.lock', publisher_products.locks(candidate))

    def test_receiver_adoption_refuses_a_second_publishing_owner(self):
        self.receiver_request()
        self.request['inventory']['services']['receiver']['roles']['server'] = [{'host': 'receiver-01'}]
        with self.assertRaisesRegex(ValueError, 'both own publishing'):
            self.make_target()

    def install_product_local_chain(self):
        installed = copy.deepcopy(self.request['configuration'])
        chain = installed.pop('chain')
        for name, url, cookie in [('enhance', 'zakura_rpc_url', 'zakura_cookie'),
                                  ('status', 'rpc_url', 'cookie'),
                                  ('transparent_history', 'rpc_url', 'rpc_cookie'),
                                  ('transaction_details', 'rpc_url', 'rpc_cookie')]:
            installed[name]['settings'].update({url: chain['rpc_url'], cookie: chain['rpc_cookie']})
        installed['enhance']['settings']['poll_seconds'] = 10
        installed['transaction_details']['settings']['poll_ms'] = 500
        self.request['configuration']['chain']['poll_ms'] = 500
        original = json.dumps(installed, indent=4) + '\n'
        self.ex.put('coordinator', publisher.CONFIG, original)
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        return installed, original

    def test_root_chain_conversion_deploys_and_restores_original_bytes(self):
        _, original = self.install_product_local_chain()
        self.deploy()
        self.assertEqual(json.loads(self.ex.read('coordinator', publisher.CONFIG)),
                         self.request['configuration'])
        self.target.rollback()
        self.assertEqual(self.ex.read('coordinator', publisher.CONFIG), original)
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.old_publisher)

    def test_root_chain_conversion_refuses_a_different_node_or_cookie(self):
        installed, _ = self.install_product_local_chain()
        for key in ('rpc_url', 'rpc_cookie'):
            with self.subTest(key=key):
                changed = copy.deepcopy(installed)
                changed['transparent_history']['settings'][key] += '-other'
                self.ex.put('coordinator', publisher.CONFIG, json.dumps(changed))
                with self.assertRaisesRegex(DeployError, 'preserve every product setting'):
                    self.target.plan()
        self.assertEqual(self.ex.effects, [])

    def test_root_chain_conversion_keeps_implicit_one_second_observation(self):
        installed, _ = self.install_product_local_chain()
        installed['enhance']['settings']['poll_seconds'] = 60
        installed['transaction_details']['settings']['poll_ms'] = 30000
        candidate = copy.deepcopy(self.request['configuration'])
        candidate['chain']['poll_ms'] = 30000
        self.assertFalse(publisher.configuration_preserves_products(candidate, installed))
        candidate['chain']['poll_ms'] = 1000
        self.assertTrue(publisher.configuration_preserves_products(candidate, installed))

    def test_root_chain_conversion_refuses_other_setting_or_poll_changes(self):
        for setting in ('root', 'poll_ms'):
            with self.subTest(setting=setting):
                installed, _ = self.install_product_local_chain()
                installed['transaction_details']['settings'][setting] = 2000 if setting == 'poll_ms' else '/other'
                self.ex.put('coordinator', publisher.CONFIG, json.dumps(installed))
                with self.assertRaisesRegex(DeployError, 'preserve every product setting'):
                    self.target.plan()
        self.assertEqual(self.ex.effects, [])

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
        sampler = json.loads(self.ex.read('coordinator', publisher.SAMPLER))
        self.ex.put('coordinator', publisher.SAMPLER, json.dumps({**sampler, 'changed': True}))
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

    def test_contract_major_change_or_unknown_never_stops_the_fleet(self):
        new = self.request['release']['binary_sha256']
        major_two = json.dumps({'schema': 'wallet-pir-contract-info/1', 'executable': 'pir-publisher',
                                'contract_version': '2.0.0', 'contract_major': 2,
                                'products': [], 'backends': []})
        cases = ((major_two, {}, 'differs from the running major 1; use fleet-release'),
                 (None, {}, 'reports no contract identity'),
                 (self.ex.default_contract('pir-publisher'),
                  {'http://127.0.0.1:18080': (200, '{"service": "pir-publisher", "version": "0.9.0"}')},
                  'running publisher contract major is unknown'))
        for report, discovery, refusal in cases:
            with self.subTest(refusal=refusal):
                self.ex.contract_reports[new] = report
                self.ex.discovery = discovery
                with self.assertRaisesRegex(DeployError, refusal):
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

    def test_failed_automatic_rollback_keeps_the_deploy_and_rollback_errors(self):
        self.ex.fail_probe = True
        self.ex.crash_on = ('disable', publisher.UNIT)
        plan = self.target.plan()['plan_sha256']
        with self.assertRaises(DeployError) as raised:
            publisher.deploy_or_restore(self.target, self.binary, plan, self.serving_binary)
        message = str(raised.exception)
        self.assertIn('encrypted-answer probe failed', message)
        self.assertIn('reply lost after completed service mutation', message)
        failure = self.target.latest()['failure']
        self.assertRegex(failure['deploy_error'], '^DeployError: .*encrypted-answer probe failed')
        self.assertRegex(failure['rollback_error'], '^RemoteError: .*reply lost')
        self.ex.fail_probe = False
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertEqual(self.target.latest()['failure'], failure)

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


    def test_update_refuses_an_installed_configuration_it_would_change(self):
        # An installed fleet-era setting is not carried or dropped silently.
        installed = json.loads(self.ex.read('coordinator', publisher.CONFIG))
        installed['transparent_history']['settings']['fleet_command'] = '/opt/history/fleet.py'
        self.ex.put('coordinator', publisher.CONFIG, json.dumps(installed))
        with self.assertRaisesRegex(DeployError, 'preserve every product setting'):
            self.make_target().plan()

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
                self.assertEqual(set(journal['checks']), set(publisher.PRODUCTS))
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

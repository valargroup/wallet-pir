"""A stale diagnostic client can be refreshed without restarting restored services."""
import copy
from types import SimpleNamespace
import unittest

from publisher_fixture import PublisherFixture
from fake_fleet import sha256
from wallet_pir_ops import durable
from wallet_pir_ops.deploy import publisher, publisher_probe_repair as repair
from wallet_pir_ops.deploy.engine import DeployError


class ProbeRepairTests(PublisherFixture, unittest.TestCase):
    def restored(self):
        self.request['resume_load'] = False
        self.ex.systemctl('coordinator', 'stop', publisher.LOAD)
        self.old_probe_sha = sha256(b'old status probe')
        harness = ("if hashlib.sha256(args.client.read_bytes()).hexdigest() != '"
                   + self.old_probe_sha + "':\n    raise ValueError('Status probe executable changed')\n")
        self.request['checks']['status'] = {
            'host': 'coordinator', 'argv': ['python3', '-c', harness,
                '--client', '/bin/old-status-probe', '--seconds', '180', '--clients', '8'],
            'timeout': 270}
        self.request['inventory']['services']['publisher']['exact_check']['products'] = self.request['checks']
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        self.deploy()
        original = self.ex.run
        def expired(host, argv, timeout):
            if '--client' in argv:
                return 1, 'selected session expired during the diagnostic lookup'
            return original(host, argv, timeout)
        self.ex.run = expired
        with self.assertRaisesRegex(DeployError, 'status independent encrypted-answer probe failed'):
            self.target.rollback()
        self.ex.run = original
        self.ex.effects.clear()
        return self.target.latest()

    def arguments(self, operation, digest=None):
        return SimpleNamespace(command='publisher-cutover-repair-probe-' + operation,
                               expect_plan_sha256=digest)

    def reviewed(self):
        return repair.dispatch(self.target, self.arguments('plan'))['plan_sha256']

    def test_refresh_preserves_every_gate_and_finishes_without_service_effects(self):
        previous = self.restored()
        original = copy.deepcopy(self.request)
        digest = self.reviewed()
        self.ex.runs.clear()
        result = repair.dispatch(self.target, self.arguments('verify', digest))
        self.assertEqual(result['phase'], 'rolled-back')
        self.assertEqual(result['request'], original)
        self.assertEqual(self.ex.effects, [])
        self.assertEqual(set(result['checks']), set(publisher.PRODUCTS))
        check = repair.checks_for(self.target, result)['status']
        expected = copy.deepcopy(original['checks']['status'])
        expected['argv'][expected['argv'].index('--client') + 1] = result['plan']['binary']
        expected['argv'][2] = expected['argv'][2].replace(self.old_probe_sha,
                                                       original['release']['binary_sha256'])
        self.assertEqual(check, expected)
        self.assertIn(('coordinator', expected['argv']), self.ex.runs)
        self.assertEqual(previous['plan'], result['plan'])
        self.assertEqual(repair.dispatch(self.target, self.arguments('verify', digest))['phase'],
                         'rolled-back')
        self.assertEqual(self.ex.effects, [])

    def test_stale_plan_refuses_before_recording_the_refresh(self):
        self.restored()
        self.reviewed()
        with self.assertRaisesRegex(DeployError, 'plan is stale'):
            repair.dispatch(self.target, self.arguments('verify', '0' * 64))
        self.assertNotIn('status_probe_refresh', self.target.latest())
        self.assertEqual(self.ex.effects, [])

    def test_changed_probe_executable_refuses_before_any_service_effect(self):
        journal = self.restored()
        self.ex.put('coordinator', journal['plan']['binary'], b'changed executable')
        with self.assertRaisesRegex(DeployError, 'probe executable changed'):
            self.reviewed()
        self.assertEqual(self.ex.effects, [])

    def test_pending_effect_or_changed_files_refuse_verification(self):
        journal = self.restored()
        for kind in ('intent', 'configuration', 'process'):
            with self.subTest(kind=kind):
                if kind == 'intent':
                    changed = copy.deepcopy(journal)
                    changed['intent'] = 'restore start ' + publisher.UNIT
                    self.ex.put('coordinator', publisher.ROOT_DIR + '/transactions/'
                                + journal['id'] + '/journal.json', durable.canonical(changed))
                elif kind == 'configuration':
                    self.ex.put('coordinator', publisher.CONFIG, 'foreign configuration')
                else:
                    self.ex.host('coordinator').units[publisher.UNIT]['exe'] = 'f' * 64
                with self.assertRaises(DeployError):
                    self.reviewed()
                self.assertEqual(self.ex.effects, [])
                if kind == 'intent':
                    self.ex.put('coordinator', publisher.ROOT_DIR + '/transactions/'
                                + journal['id'] + '/journal.json', durable.canonical(journal))
                elif kind == 'configuration':
                    self.ex.put('coordinator', publisher.CONFIG,
                                journal['plan']['before'][publisher.CONFIG]['text'])

    def test_load_failure_keeps_the_restoration_open(self):
        self.restored()
        digest = self.reviewed()
        self.ex.fail_probe = True
        with self.assertRaisesRegex(DeployError, 'independent encrypted-answer probe failed'):
            repair.dispatch(self.target, self.arguments('verify', digest))
        self.assertEqual(self.target.latest()['phase'], 'restoring')
        self.assertEqual(self.ex.effects, [])

    def test_refreshed_status_probe_failure_still_fails_recovery(self):
        self.restored()
        digest = self.reviewed()
        original = self.ex.run
        def failed(host, argv, timeout):
            return (1, 'a lookup failed') if '--client' in argv else original(host, argv, timeout)
        self.ex.run = failed
        with self.assertRaisesRegex(DeployError, 'status independent encrypted-answer probe failed'):
            repair.dispatch(self.target, self.arguments('verify', digest))
        self.assertEqual(self.target.latest()['phase'], 'restoring')
        self.assertEqual(self.ex.effects, [])

    def test_process_change_during_probes_refuses_completion(self):
        self.restored()
        digest = self.reviewed()
        original = self.ex.run
        def changed(host, argv, timeout):
            result = original(host, argv, timeout)
            if '--client' in argv:
                self.ex.host('coordinator').units[publisher.UNIT]['exe'] = 'f' * 64
            return result
        self.ex.run = changed
        with self.assertRaisesRegex(DeployError, 'restored process identity differs'):
            repair.dispatch(self.target, self.arguments('verify', digest))
        self.assertEqual(self.target.latest()['phase'], 'restoring')
        self.assertEqual(self.ex.effects, [])

    def test_recorded_refresh_cannot_change_the_probe_policy(self):
        journal = self.restored()
        change = repair.replacement(self.target)
        change['client'] = '/bin/unreviewed-client'
        journal['status_probe_refresh'] = {'replacement': change, 'plan_sha256': 'a' * 64}
        with self.assertRaisesRegex(DeployError, 'differs from the pinned request'):
            repair.checks_for(self.target, journal)

    def test_harness_refresh_leaves_other_digest_literals_unchanged(self):
        old, new = 'a' * 64, 'b' * 64
        source = ("if hashlib.sha256(args.client.read_bytes()).hexdigest() != '" + old
                  + "':\n    raise ValueError('client changed')\noracle = '" + old + "'\n")
        expected = source.replace("!= '" + old, "!= '" + new)
        self.assertEqual(repair.refreshed_harness(source, new), expected)

    def test_missing_or_ambiguous_client_digest_is_refused(self):
        pin = "if hashlib.sha256(args.client.read_bytes()).hexdigest() != '" + 'a' * 64 + "':\n    pass\n"
        for source in ('pass', pin + pin):
            with self.subTest(source=source):
                with self.assertRaisesRegex(DeployError, 'exactly one client digest'):
                    repair.refreshed_harness(source, 'b' * 64)

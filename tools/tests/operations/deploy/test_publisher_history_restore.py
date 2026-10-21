"""History restart evidence is prepared before the predecessor stops and installed only when unchanged."""
import json
import unittest
from publisher_fixture import PublisherFixture
from wallet_pir_ops.deploy import publisher
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.remote import RemoteError

ROOT = '/srv/transparent-activity/full-v12/publications'
ACTIVE = ROOT + '/active.json'
TARGET = ROOT + '/candidate-7/history-restore.json'
RECORD = '{"schema":"history-restore/1","inventory_sha256":"' + 'c' * 64 + '"}'


class HistoryRestoreTests(PublisherFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.request['configuration']['transparent_history']['settings']['distribute_prepared'] = True
        self.ex.put('coordinator', publisher.CONFIG, json.dumps(self.request['configuration']))
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        self.ex.put('coordinator', ACTIVE, json.dumps({'directory': ROOT + '/candidate-7', 'map_sha256': 'd' * 64}))
        self.prepared = []
        self.fail_prepare = False
        self.during_prepare = None
        original = self.ex.run

        def run(host, argv, timeout):
            if 'prepare-history-restore' in argv:
                # The predecessor is still serving while the record is prepared.
                self.assertEqual(self.ex.probe_unit(host, publisher.UNIT)['exe_sha256'], self.old_publisher)
                self.assertEqual(argv[argv.index('--publication') + 1], ROOT + '/candidate-7')
                self.prepared.append(argv)
                if self.during_prepare:
                    self.during_prepare()
                if self.fail_prepare:
                    return 1, 'artifact digest mismatch'
                self.ex.put(host, argv[argv.index('--output') + 1], RECORD)
                return 0, '{}'
            return original(host, argv, timeout)
        self.ex.run = run
        self.ex.effects.clear()

    def stops(self):
        return [a for a in self.ex.effects if a[:2] == ('stop', publisher.UNIT)]

    def test_record_is_prepared_before_the_stop_and_installed_before_start(self):
        journal = self.deploy()
        self.assertEqual(journal['phase'], 'committed')
        self.assertEqual(len(self.prepared), 1)
        completed = journal['completed']
        self.assertLess(completed.index('prepare history restore'), completed.index('stop ' + publisher.UNIT))
        self.assertLess(completed.index('stop ' + publisher.UNIT), completed.index('install history restore record'))
        self.assertLess(completed.index('install history restore record'), completed.index('start publisher'))
        self.assertEqual(self.ex.read('coordinator', TARGET), RECORD)
        # Readiness and the independent encrypted-answer checks passed; no
        # background-validation result was awaited.
        self.assertTrue(all(c['passed'] for c in journal['checks'].values()))

    def test_failed_preparation_never_stops_the_predecessor(self):
        self.fail_prepare = True
        with self.assertRaisesRegex(DeployError, 'predecessor still serves'):
            self.deploy()
        self.assertEqual(self.stops(), [])
        self.assertIsNone(self.ex.read('coordinator', TARGET))
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.old_publisher)

    def test_publication_change_during_preparation_refuses_before_stopping(self):
        self.during_prepare = lambda: self.ex.put('coordinator', ACTIVE, json.dumps(
            {'directory': ROOT + '/candidate-8', 'map_sha256': 'e' * 64}))
        with self.assertRaisesRegex(DeployError, 'changed while its restore record was prepared'):
            self.deploy()
        self.assertEqual(self.stops(), [])
        self.assertIsNone(self.ex.read('coordinator', TARGET))

    def test_publication_change_after_the_stop_restarts_the_predecessor_and_is_retryable(self):
        original = self.ex.systemctl

        def systemctl(host, *args):
            result = original(host, *args)
            if args == ('stop', publisher.UNIT):
                self.ex.put(host, ACTIVE, json.dumps({'directory': ROOT + '/candidate-8', 'map_sha256': 'e' * 64}))
            return result
        self.ex.systemctl = systemctl
        with self.assertRaisesRegex(DeployError, 'retry the deploy'):
            self.deploy()
        journal = self.target.latest()
        self.assertEqual(journal['phase'], 'rolled-back')
        self.assertNotIn('install history restore record', journal['completed'])
        self.assertIsNone(self.ex.read('coordinator', TARGET))
        state = self.ex.probe_unit('coordinator', publisher.UNIT)
        self.assertEqual(state['active_state'], 'active')
        self.assertEqual(state['exe_sha256'], self.old_publisher)
        # The prepared record was never hashed again while stopped.
        self.assertEqual(len(self.prepared), 1)
        self.target.plan()

    def test_interrupted_stop_continues_with_the_prepared_record(self):
        self.request['resume_load'] = False
        self.ex.systemctl('coordinator', 'stop', publisher.LOAD)
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        self.ex.crash_on = ('stop', publisher.UNIT)
        with self.assertRaises(RemoteError):
            self.deploy()
        self.ex.effects.clear()
        self.assertEqual(self.target.continue_installation()['phase'], 'committed')
        self.assertEqual(len(self.prepared), 1)
        self.assertEqual(self.ex.read('coordinator', TARGET), RECORD)
        self.assertEqual(self.stops(), [])

    def test_continuation_refuses_a_changed_record(self):
        self.request['resume_load'] = False
        self.ex.systemctl('coordinator', 'stop', publisher.LOAD)
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        self.ex.crash_on = ('stop', publisher.UNIT)
        with self.assertRaises(RemoteError):
            self.deploy()
        record = self.target.latest()['history_restore']['record']
        self.ex.put('coordinator', record, RECORD.replace('c', 'f'))
        with self.assertRaisesRegex(DeployError, 'record changed outside its transaction'):
            self.target.continue_installation()
        self.assertIsNone(self.ex.read('coordinator', TARGET))

    def test_rollback_removes_the_installed_record(self):
        self.deploy()
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertIsNone(self.ex.read('coordinator', TARGET))
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.old_publisher)

    def test_failed_exact_check_rolls_back_with_the_record(self):
        self.ex.fail_probe = True
        with self.assertRaisesRegex(DeployError, 'encrypted-answer probe failed'):
            self.deploy()
        self.ex.fail_probe = False
        self.target.rollback()
        self.assertIsNone(self.ex.read('coordinator', TARGET))

    def test_fresh_installation_without_an_active_publication_prepares_in_full(self):
        self.ex.remove('coordinator', ACTIVE)
        journal = self.deploy()
        self.assertEqual(journal['phase'], 'committed')
        self.assertEqual(self.prepared, [])
        self.assertNotIn('history_restore', journal)


if __name__ == '__main__':
    unittest.main()

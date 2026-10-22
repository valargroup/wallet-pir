"""History restart evidence is prepared from a pin before the predecessor stops and installed only when unchanged."""
import json
import unittest
from publisher_fixture import PublisherFixture
from wallet_pir_ops.deploy import publisher, publisher_restore
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.remote import RemoteError

ROOT = '/srv/transparent-activity/full-v12/publications'
ACTIVE = ROOT + '/active.json'
LIVE = ROOT + '/candidate-7'
TARGET = LIVE + '/history-restore.json'
CACHE = ROOT + '/.cutover-preparation'
MAP = 'd' * 64
RECORD = '{"schema":"transparent-history-restore-v1","map_sha256":"' + MAP + '","inventory_sha256":"' + 'c' * 64 + '"}'
PRIOR = '{"schema":"transparent-history-restore-v1","note":"earlier évidence"}\n'
ADVANCED = json.dumps({'directory': ROOT + '/candidate-8', 'map_sha256': 'e' * 64})


class HistoryRestoreTests(PublisherFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.ex.put('coordinator', publisher.CONFIG, json.dumps(self.request['configuration']))
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        self.ex.put('coordinator', ACTIVE, json.dumps({'directory': LIVE, 'map_sha256': MAP}))
        self.ex.put('coordinator', LIVE + '/shards.json', '{"shards":[]}')
        self.ex.put('coordinator', LIVE + '/table.bin', b'large immutable table')
        self.prepared = []
        self.pinned_inputs = []
        self.fail_prepare = False
        self.crash_pin = False
        self.during_prepare = None
        self.record = RECORD
        original = self.ex.run

        def run(host, argv, timeout):
            if len(argv) > 2 and argv[1].endswith('/snapshot-warmer.py') and argv[2] == 'pin':
                self.ex.runs.append((host, argv))
                source, destination = argv[3:]
                for path in list(self.ex.host(host).files):
                    if path.startswith(source + '/') and not path.endswith('.partial'):
                        self.ex.link(host, path, destination + path[len(source):])
                if self.crash_pin:
                    raise RemoteError('reply lost after a partial pin')
                return 0, '{"pinned_files":2}'
            if len(argv) > 2 and argv[1].endswith('/snapshot-warmer.py') and argv[2] == 'same':
                self.ex.runs.append((host, argv))
                return 0, json.dumps(self.compare_pin(host, *argv[3:]))
            if 'prepare-history-restore' in argv:
                # The predecessor is still serving while its pinned publication is verified.
                self.assertEqual(self.ex.probe_unit(host, publisher.UNIT)['exe_sha256'], self.old_publisher)
                pin = argv[argv.index('--publication') + 1]
                self.assertTrue(pin.startswith(CACHE + '/publisher-'))
                self.prepared.append(argv)
                if self.during_prepare:
                    self.during_prepare()
                self.pinned_inputs.append(self.ex.read(host, pin + '/table.bin'))
                if self.fail_prepare:
                    return 1, 'artifact digest mismatch'
                self.ex.put(host, argv[argv.index('--output') + 1], self.record)
                return 0, '{}'
            return original(host, argv, timeout)
        self.ex.run = run
        self.ex.effects.clear()

    def compare_pin(self, host, source, pin):
        """The snapshot helper's `same`, over the fake fleet's inode numbers."""
        pinned = [path for path in self.ex.host(host).files if path.startswith(pin + '/')]
        differs = [path[len(pin) + 1:] for path in pinned
                   if self.ex.stat(host, source + path[len(pin):]) != self.ex.stat(host, path)]
        return {'compared': len(pinned), 'same': not differs, 'differs': sorted(differs)}

    def stops(self):
        return [a for a in self.ex.effects if a[:2] == ('stop', publisher.UNIT)]

    def pins(self):
        return [p for p in self.ex.host('coordinator').files if p.startswith(CACHE + '/')]

    def raw(self, path):
        return self.ex.host('coordinator').files.get(path)

    def interrupt_stop(self):
        self.request['resume_load'] = False
        self.ex.systemctl('coordinator', 'stop', publisher.LOAD)
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        self.ex.crash_on = ('stop', publisher.UNIT)
        with self.assertRaises(RemoteError):
            self.deploy()

    def test_record_is_prepared_from_a_pin_before_the_stop_and_installed_before_start(self):
        hashed, original = [], self.ex.sha256
        self.ex.sha256 = lambda host, path: hashed.append(path) or original(host, path)
        journal = self.deploy()
        self.assertEqual(journal['phase'], 'committed')
        self.assertEqual(len(self.prepared), 1)
        completed = journal['completed']
        for earlier, later in [('pin history restore snapshot', 'prepare history restore'),
                               ('prepare history restore', 'stop ' + publisher.UNIT),
                               ('stop ' + publisher.UNIT, 'remove history restore snapshot'),
                               ('remove history restore snapshot', 'install history restore record'),
                               ('install history restore record', 'start publisher')]:
            self.assertLess(completed.index(earlier), completed.index(later))
        self.assertEqual(self.ex.read('coordinator', TARGET), RECORD)
        self.assertEqual(journal['history_restore']['prior'], {'sha256': None, 'backup': None})
        self.assertEqual(journal['history_restore']['pin_comparison'],
                         {'compared': 2, 'same': True, 'differs': []})
        self.assertEqual(self.pins(), [])
        # The large bodies are hashed only by the preparation command, never by the deploy.
        self.assertFalse(any(path.endswith('/table.bin') for path in hashed))

    def test_commit_does_not_wait_for_background_validation(self):
        original = self.ex.http_get
        requested = []

        def get(host, url, timeout=5):
            requested.append(url)
            status, body = original(host, url, timeout)
            if url.endswith(':18080/ready') and status == 200:
                value = json.loads(body)
                value['integrity_validation'] = {'state': 'pending'}
                value['correctness_validation'] = {'state': 'pending'}
                return status, json.dumps(value)
            return status, body
        self.ex.http_get = get
        journal = self.deploy()
        self.assertEqual(journal['phase'], 'committed')
        self.assertTrue(all(c['passed'] for c in journal['checks'].values()))
        self.assertFalse(any('validation' in url for url in requested))

    def test_failed_preparation_never_stops_the_predecessor(self):
        self.fail_prepare = True
        with self.assertRaisesRegex(DeployError, 'predecessor still serves'):
            self.deploy()
        self.assertEqual(self.stops(), [])
        self.assertIsNone(self.ex.read('coordinator', TARGET))
        self.assertNotEqual(self.pins(), [])
        result = self.target.rollback()
        self.assertTrue(result['staging_only'])
        self.assertEqual(self.pins(), [])
        self.assertEqual(self.stops(), [])
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.old_publisher)

    def test_interrupted_pin_is_removed_by_staging_rollback(self):
        self.crash_pin = True
        invocation = self.ex.probe_unit('coordinator', publisher.UNIT)['invocation_id']
        with self.assertRaises(RemoteError):
            self.deploy()
        journal = self.target.latest()
        self.assertEqual((journal['phase'], journal['intent']), ('staging', 'pin history restore snapshot'))
        self.assertTrue(journal['history_restore']['work'].startswith(CACHE + '/'))
        self.assertNotEqual(self.pins(), [])
        self.assertTrue(self.target.rollback()['staging_only'])
        self.assertEqual(self.pins(), [])
        self.assertEqual(self.stops(), [])
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['invocation_id'], invocation)

    def test_collection_during_preparation_cannot_alter_the_pinned_input(self):
        def collect():
            # The predecessor advances and collects the publication being verified.
            self.ex.put('coordinator', ACTIVE, ADVANCED)
            for path in [p for p in self.ex.host('coordinator').files if p.startswith(LIVE + '/')]:
                self.ex.remove('coordinator', path)
        self.during_prepare = collect
        with self.assertRaisesRegex(DeployError, 'changed while its restore record was prepared'):
            self.deploy()
        self.assertEqual(self.pinned_inputs, ['large immutable table'])
        self.assertEqual(self.stops(), [])
        self.assertIsNone(self.ex.read('coordinator', TARGET))
        self.target.rollback()
        self.assertEqual(self.pins(), [])

    def test_publication_change_during_preparation_refuses_before_stopping(self):
        self.during_prepare = lambda: self.ex.put('coordinator', ACTIVE, ADVANCED)
        with self.assertRaisesRegex(DeployError, 'changed while its restore record was prepared'):
            self.deploy()
        self.assertEqual(self.stops(), [])
        self.assertIsNone(self.ex.read('coordinator', TARGET))
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['active_state'], 'active')

    def test_record_for_another_publication_refuses_before_stopping(self):
        self.record = RECORD.replace(MAP, 'f' * 64)
        with self.assertRaisesRegex(DeployError, 'names another publication'):
            self.deploy()
        self.assertEqual(self.stops(), [])

    def test_publication_change_after_the_stop_rolls_back_and_is_retryable(self):
        self.change_after_the_stop(lambda host: self.ex.put(host, ACTIVE, ADVANCED))
        with self.assertRaisesRegex(DeployError, 'advanced after the predecessor stopped.*retry the deploy'):
            self.deploy()
        journal = self.target.latest()
        self.assertIn('retry', journal)
        # The ordinary transaction rollback restored the predecessor.
        self.assert_restored_predecessor(journal)
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['active_state'], 'active')
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.LOAD)['active_state'], 'active')
        self.assertEqual(len(self.prepared), 1)
        self.target.plan()

    def change_after_the_stop(self, change):
        original = self.ex.systemctl

        def systemctl(host, *args):
            result = original(host, *args)
            if args == ('stop', publisher.UNIT) and not self.stops()[1:]:
                change(host)
            return result
        self.ex.systemctl = systemctl

    def assert_restored_predecessor(self, journal):
        self.assertEqual(journal['phase'], 'rolled-back')
        self.assertNotIn('install history restore record', journal['completed'])
        self.assertIn('restore start ' + publisher.UNIT, journal['completed'])
        self.assertIsNone(self.ex.read('coordinator', TARGET))
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.old_publisher)
        self.assertEqual(self.pins(), [])

    def test_replaced_artifact_after_the_stop_rolls_back_and_is_retryable(self):
        # Same bytes and size under the same name, but another inode than the pinned one.
        self.change_after_the_stop(lambda host: self.ex.put(host, LIVE + '/table.bin', b'large immutable table'))
        with self.assertRaisesRegex(DeployError, 'artifacts differ from their pin.*retry the deploy'):
            self.deploy()
        journal = self.target.latest()
        self.assertEqual(journal['history_restore']['pin_comparison']['differs'], ['table.bin'])
        self.assert_restored_predecessor(journal)
        self.assertEqual(len(self.prepared), 1)
        self.target.plan()

    def test_missing_artifact_after_the_stop_rolls_back_and_is_retryable(self):
        self.change_after_the_stop(lambda host: self.ex.remove(host, LIVE + '/table.bin'))
        with self.assertRaisesRegex(DeployError, 'artifacts differ from their pin'):
            self.deploy()
        self.assert_restored_predecessor(self.target.latest())

    def test_unusable_pin_comparison_refuses_before_installing(self):
        original = self.ex.run
        self.ex.run = lambda host, argv, timeout: ((1, 'snapshot comparison needs regular directories')
                                                   if argv[2:3] == ['same'] else original(host, argv, timeout))
        with self.assertRaisesRegex(DeployError, 'could not compare the History publication with its pin'):
            self.deploy()
        self.assertIsNone(self.ex.read('coordinator', TARGET))
        self.ex.run = original
        self.assert_restored_predecessor(self.target.rollback())

    def test_interrupted_stop_continues_with_the_prepared_record(self):
        self.interrupt_stop()
        self.ex.effects.clear()
        self.assertEqual(self.target.continue_installation()['phase'], 'committed')
        self.assertEqual(len(self.prepared), 1)
        self.assertEqual(self.ex.read('coordinator', TARGET), RECORD)
        self.assertEqual(self.stops(), [])
        self.assertEqual(self.pins(), [])

    def test_continuation_requires_the_exact_staging_effects(self):
        self.interrupt_stop()
        journal = self.target.latest()
        journal['completed'].remove(publisher_restore.PREPARE)
        self.ex.put('coordinator', publisher.ROOT_DIR + '/transactions/' + journal['id'] + '/journal.json',
                    json.dumps(journal))
        with self.assertRaisesRegex(DeployError, 'requires the interrupted predecessor stop'):
            self.target.continue_installation()
        self.assertIsNone(self.ex.read('coordinator', TARGET))

    def test_continuation_refuses_a_changed_record(self):
        self.interrupt_stop()
        record = self.target.latest()['history_restore']['record']
        self.ex.put('coordinator', record, RECORD.replace('c', 'f'))
        with self.assertRaisesRegex(DeployError, 'record changed outside its transaction'):
            self.target.continue_installation()
        self.assertIsNone(self.ex.read('coordinator', TARGET))

    def test_continuation_after_the_publication_advanced_restores_the_predecessor(self):
        self.interrupt_stop()
        self.ex.put('coordinator', ACTIVE, ADVANCED)
        with self.assertRaisesRegex(DeployError, 'retry the deploy'):
            self.target.continue_installation()
        self.assertEqual(self.target.latest()['phase'], 'rolled-back')
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.old_publisher)
        self.assertIsNone(self.ex.read('coordinator', TARGET))

    def test_rollback_removes_a_record_that_was_absent(self):
        self.deploy()
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertIsNone(self.ex.read('coordinator', TARGET))
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.old_publisher)

    def test_rollback_restores_the_exact_replaced_record(self):
        self.ex.put('coordinator', TARGET, PRIOR)
        journal = self.deploy()
        self.assertEqual(self.ex.read('coordinator', TARGET), RECORD)
        self.assertEqual(self.raw(journal['history_restore']['prior']['backup']), PRIOR.encode())
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertEqual(self.raw(TARGET), PRIOR.encode())
        # A repeated rollback is a no-op.
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertEqual(self.raw(TARGET), PRIOR.encode())

    def test_rollback_refuses_a_foreign_record_before_any_effect(self):
        self.deploy()
        self.ex.put('coordinator', TARGET, '{"foreign":true}')
        self.ex.effects.clear()
        with self.assertRaisesRegex(DeployError, 'History restore record changed outside its transaction'):
            self.target.rollback()
        self.assertEqual(self.ex.effects, [])
        self.assertEqual(self.ex.read('coordinator', TARGET), '{"foreign":true}')
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'],
                         self.request['release']['binary_sha256'])

    def test_rollback_refuses_a_changed_preserved_record(self):
        self.ex.put('coordinator', TARGET, PRIOR)
        journal = self.deploy()
        self.ex.put('coordinator', journal['history_restore']['prior']['backup'], 'damaged')
        self.ex.effects.clear()
        with self.assertRaisesRegex(DeployError, 'preserved History restore record changed'):
            self.target.rollback()
        self.assertEqual(self.ex.effects, [])

    def write_failing_on_target(self, after_write):
        original = self.ex.write

        def write(host, path, data, mode):
            if path == TARGET:
                self.ex.write = original
                if after_write:
                    original(host, path, data, mode)
                raise RemoteError('connection lost while installing the record')
            return original(host, path, data, mode)
        self.ex.write = write

    def test_interrupted_install_is_rolled_back_to_the_prior_record(self):
        self.ex.put('coordinator', TARGET, PRIOR)
        self.write_failing_on_target(after_write=True)
        with self.assertRaises(RemoteError):
            self.deploy()
        journal = self.target.latest()
        self.assertEqual(journal['intent'], 'install history restore record')
        self.assertEqual(self.ex.read('coordinator', TARGET), RECORD)
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertEqual(self.raw(TARGET), PRIOR.encode())
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.old_publisher)

    def test_install_interrupted_before_writing_keeps_the_prior_absence(self):
        self.write_failing_on_target(after_write=False)
        with self.assertRaises(RemoteError):
            self.deploy()
        self.assertIsNone(self.ex.read('coordinator', TARGET))
        journal = self.target.rollback()
        self.assertNotIn('restore history restore record', journal['completed'])
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

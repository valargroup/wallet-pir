"""The fleet inventory: durable intent, compare-and-swap, lifecycle rules."""
import importlib.util
import json
from pathlib import Path
import tempfile
import time
import unittest

SPEC = importlib.util.spec_from_file_location('inventory', Path(__file__).resolve().parents[1]/'scripts/transparent-fleet-inventory.py')
I = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(I)


def live_roster():
    roster = [dict(id=f'transparent-pir-recent-0{i}', role='recent-replica', replica_group='recent',
                   ssh_host=f'10.142.0.{i}', upstream=f'10.142.0.{i}:8093', cache_bytes=5368709120,
                   memory_max='7G', build_slots=1) for i in (1, 2, 3, 4)]
    roster += [dict(id=f'transparent-pir-archive-0{i}', role='archive-owner', replica_group=None,
                    ssh_host=f'10.142.1.{i}', upstream=f'10.142.1.{i}:8093', cache_bytes=51539607552,
                    memory_max='56G', build_slots=1) for i in (1, 2)]
    return roster


def live_assignment():
    workers = [dict(id='transparent-pir-archive-01', role='archive-owner', shards=list(range(0, 39))),
               dict(id='transparent-pir-archive-02', role='archive-owner', shards=list(range(39, 77)))]
    workers += [dict(id=f'transparent-pir-recent-0{i}', role='recent-replica', shards=list(range(77, 86))) for i in (1, 2, 3, 4)]
    return {'workers': workers}


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.known = self.root/'known_hosts'
        self.known.write_text('10.142.0.1 ssh-ed25519 AAAAstatic\n')
        self.inv = I.Inventory(self.root/'state', self.root/'roster.json', self.known)
        self.first = self.inv.write(0, lambda _: I.seed(live_roster(), live_assignment()), 'test', archive=True)

    def enroll(self, inv, name='transparent-pir-recent-05', host='10.142.0.20', key='ssh-ed25519 AAAAnew'):
        inv['members'].append(dict(id=name, role='recent-replica', group='recent', origin='elastic', intent='enrolled',
                                   ssh_host=host, upstream=host+':8093', cache_bytes=5368709120, memory_max='7G',
                                   build_slots=1, droplet_id='42', ssh_host_key=key))
        return inv

    def test_seed_pins_the_live_archive_cut_and_keeps_every_host_static(self):
        self.assertEqual(self.first['revision'], 1)
        self.assertEqual(self.first['partition']['ranges'], [{'id': 'a0', 'first': 0, 'last': 38},
                                                              {'id': 'a1', 'first': 39, 'last': 76}])
        self.assertTrue(all(m['origin'] == 'static' and m['intent'] == 'enrolled' for m in self.first['members']))
        roster = json.loads((self.root/'roster.json').read_text())
        owners = {w['id']: w['archive_range'] for w in roster if w['role'] == 'archive-owner'}
        self.assertEqual(owners, {'transparent-pir-archive-01': [0, 38], 'transparent-pir-archive-02': [39, 76]})
        self.assertTrue(all(w['replica_group'] == 'recent' for w in roster if w['role'] == 'recent-replica'))

    def test_a_stale_revision_is_refused(self):
        self.inv.write(1, self.enroll, 'a')
        with self.assertRaisesRegex(I.InventoryError, 'revision 2, not 1'):
            self.inv.write(1, lambda inv: self.enroll(inv, 'transparent-pir-recent-06', '10.142.0.21'), 'b')

    def test_every_revision_is_kept_and_logged(self):
        self.inv.write(1, self.enroll, 'a')
        self.assertEqual(sorted(p.name for p in (self.root/'state'/'inventory.d').iterdir()), ['1.json', '2.json'])
        log = [json.loads(line) for line in (self.root/'state'/'inventory.log.jsonl').read_text().splitlines()]
        self.assertEqual(log[-1]['summary'], {'transparent-pir-recent-05': [None, 'enrolled']})

    def test_elastic_host_keys_follow_membership(self):
        self.inv.write(1, self.enroll, 'a')
        self.assertIn('10.142.0.20 ssh-ed25519 AAAAnew', self.known.read_text())
        self.assertIn('10.142.0.1 ssh-ed25519 AAAAstatic', self.known.read_text())
        self.inv.write(2, lambda inv: I.set_intent(inv, 'transparent-pir-recent-05', 'draining'), 'a')
        self.inv.write(3, lambda inv: I.set_intent(inv, 'transparent-pir-recent-05', 'retired'), 'a')
        # A retired elastic host's address may be reused by another droplet.
        self.assertNotIn('10.142.0.20', self.known.read_text())
        self.assertNotIn('transparent-pir-recent-05', (self.root/'roster.json').read_text())

    def test_lifecycle_steps_cannot_be_skipped_or_reversed(self):
        with self.assertRaisesRegex(I.InventoryError, 'cannot go from enrolled to retired'):
            self.inv.write(1, lambda inv: I.set_intent(inv, 'transparent-pir-recent-04', 'retired'), 'a')
        self.inv.write(1, lambda inv: I.set_intent(inv, 'transparent-pir-recent-04', 'draining'), 'a')
        self.inv.write(2, lambda inv: I.set_intent(inv, 'transparent-pir-recent-04', 'retired'), 'a')
        with self.assertRaisesRegex(I.InventoryError, 'never reused'):
            self.inv.write(3, lambda inv: I.set_intent(inv, 'transparent-pir-recent-04', 'enrolled'), 'a')

    def test_ids_are_never_removed_and_host_facts_never_change(self):
        def drop(inv):
            inv['members'] = [m for m in inv['members'] if m['id'] != 'transparent-pir-recent-04']
            return inv
        with self.assertRaisesRegex(I.InventoryError, 'never removed'):
            self.inv.write(1, drop, 'a')
        def move(inv):
            next(m for m in inv['members'] if m['id'] == 'transparent-pir-recent-04')['ssh_host'] = '10.9.9.9'
            return inv
        with self.assertRaisesRegex(I.InventoryError, 'never changes'):
            self.inv.write(1, move, 'a')

    def test_archive_changes_need_the_archive_flag(self):
        with self.assertRaisesRegex(I.InventoryError, 'archive'):
            self.inv.write(1, lambda inv: I.set_intent(inv, 'transparent-pir-archive-01', 'draining'), 'scaler')
        def repartition(inv):
            inv['partition']['ranges'][0]['last'] = 37
            inv['partition']['ranges'][1]['first'] = 38
            return inv
        with self.assertRaisesRegex(I.InventoryError, 'partition'):
            self.inv.write(1, repartition, 'scaler')

    def test_each_archive_range_keeps_exactly_one_live_owner(self):
        with self.assertRaisesRegex(I.InventoryError, 'exactly one'):
            self.inv.write(1, lambda inv: I.set_intent(inv, 'transparent-pir-archive-01', 'quarantined'), 'op', archive=True)

    def test_two_live_members_cannot_share_a_host(self):
        with self.assertRaisesRegex(I.InventoryError, 'shares a host'):
            self.inv.write(1, lambda inv: self.enroll(inv, host='10.142.0.1'), 'a')

    def test_an_invalid_file_falls_back_to_the_last_good_revision(self):
        self.inv.write(1, self.enroll, 'a')
        (self.root/'state'/'inventory.json').write_text('{"schema": "trunc')
        self.assertEqual(self.inv.last_good()['revision'], 2)

    def test_roster_carries_intent_and_elastic_origin(self):
        self.inv.write(1, self.enroll, 'a')
        self.inv.write(2, lambda inv: I.set_intent(inv, 'transparent-pir-recent-04', 'draining'), 'a')
        roster = {w['id']: w for w in json.loads((self.root/'roster.json').read_text())}
        self.assertEqual(roster['transparent-pir-recent-04']['intent'], 'draining')
        self.assertEqual(roster['transparent-pir-recent-05']['origin'], 'elastic')


class CliTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        state = self.root/'state'
        state.mkdir()
        (self.root/'roster.json').write_text(json.dumps(live_roster()))
        (self.root/'assignment.json').write_text(json.dumps(live_assignment()))
        I.atomic_json(state/'active.json', {'map_sha256': 'a'*64, 'workers': [], 'assignment': str(self.root/'assignment.json')})
        self.config = self.root/'fleet.json'
        self.config.write_text(json.dumps({'state_dir': str(state), 'roster': str(self.root/'roster.json')}))
        self.scaler = self.root/'scaler'
        self.scaler.mkdir()
        self.run_cli('init')
        self.members = {w['id']: {'role': w['role'], 'state': 'serving', 'rendered': True, 'intent': 'enrolled'}
                        for w in live_roster()}
        self.write_membership()

    def write_membership(self, age=0):
        I.atomic_json(self.root/'state'/'membership.json', {'updated_unix': time.time()-age, 'members': self.members})

    def run_cli(self, *args):
        I.main(['--fleet-config', str(self.config), '--scaler-dir', str(self.scaler), *args])

    def intent(self, member):
        inv = json.loads((self.root/'state'/'inventory.json').read_text())
        return next(m['intent'] for m in inv['members'] if m['id'] == member)

    def test_drain_needs_two_other_serving_recent_replicas(self):
        self.run_cli('drain', 'transparent-pir-recent-04')
        self.members['transparent-pir-recent-04']['intent'] = 'draining'
        self.members['transparent-pir-recent-03']['state'] = 'lagging'
        self.write_membership()
        with self.assertRaisesRegex(SystemExit, 'only 1 other'):
            self.run_cli('drain', 'transparent-pir-recent-02')
        self.run_cli('drain', 'transparent-pir-recent-02', '--maintenance')
        self.assertEqual(self.intent('transparent-pir-recent-02'), 'draining')

    def test_drain_refuses_a_stale_membership_record(self):
        self.write_membership(age=60)
        with self.assertRaises(I.InventoryError):
            self.run_cli('drain', 'transparent-pir-recent-04')

    def test_retire_waits_for_the_drained_interval(self):
        self.run_cli('drain', 'transparent-pir-recent-04')
        self.members['transparent-pir-recent-04'].update(intent='draining', rendered=False,
                                                         drained_since_unix=time.time()-30)
        self.write_membership()
        with self.assertRaisesRegex(SystemExit, 'not been drained'):
            self.run_cli('retire', 'transparent-pir-recent-04')
        self.members['transparent-pir-recent-04']['drained_since_unix'] = time.time()-200
        self.write_membership()
        self.run_cli('retire', 'transparent-pir-recent-04')
        self.assertEqual(self.intent('transparent-pir-recent-04'), 'retired')

    def test_archive_owners_need_the_flag_and_a_scaler_outside_act_mode(self):
        with self.assertRaisesRegex(SystemExit, '--archive'):
            self.run_cli('quarantine', 'transparent-pir-archive-01')
        (self.scaler/'policy.json').write_text(json.dumps({'mode': 'act'}))
        with self.assertRaisesRegex(SystemExit, 'pause the scaler'):
            self.run_cli('--archive', 'quarantine', 'transparent-pir-archive-01')

    def test_init_refuses_an_existing_inventory(self):
        with self.assertRaisesRegex(SystemExit, 'already exists'):
            self.run_cli('init')


if __name__ == '__main__':
    unittest.main()

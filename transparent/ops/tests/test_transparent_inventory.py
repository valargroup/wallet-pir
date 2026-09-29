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


NEW_OWNER = 'transparent-pir-archive-03'
OWNER_SPEC = NEW_OWNER + '=10.142.1.3,10.142.1.3:8093,ssh-ed25519 AAAAarchive3,51539607552,56G,1'


class PartitionHistoryTests(unittest.TestCase):
    def inventory(self):
        inv = I.seed(live_roster(), live_assignment())
        inv.update(schema=I.SCHEMA, revision=1)
        return inv

    def test_a_retired_owner_may_name_a_replaced_range(self):
        inv = I.repartition(self.inventory(), I.parse_ranges('a2:0-76'), [I.parse_owner(OWNER_SPEC)], 1.0)
        inv['revision'] = 2
        I.validate(inv)
        self.assertEqual([r['id'] for r in inv['partition_history']], ['a0', 'a1'])

    def test_a_live_or_quarantined_owner_must_name_a_current_range(self):
        for intent in ('enrolled', 'draining', 'quarantined'):
            with self.subTest(intent=intent):
                inv = I.repartition(self.inventory(), I.parse_ranges('a2:0-76'), [I.parse_owner(OWNER_SPEC)], 1.0)
                inv['revision'] = 2
                next(m for m in inv['members'] if m['id'] == 'transparent-pir-archive-01')['intent'] = intent
                with self.assertRaisesRegex(I.InventoryError, 'no current archive range'):
                    I.validate(inv)

    def test_history_ids_are_never_reused(self):
        inv = self.inventory()
        inv['partition_history'] = [{'id': 'a0', 'first': 0, 'last': 76}]
        with self.assertRaisesRegex(I.InventoryError, 'partition_history'):
            I.validate(inv)
        inv['partition_history'] = [{'id': 'old', 'first': 0, 'last': 76}, {'id': 'old', 'first': 0, 'last': 38}]
        with self.assertRaisesRegex(I.InventoryError, 'partition_history'):
            I.validate(inv)
        inv['partition_history'] = {'id': 'old'}
        with self.assertRaisesRegex(I.InventoryError, 'must be a list'):
            I.validate(inv)

    def test_exactly_one_live_owner_per_current_range_remains(self):
        inv = I.repartition(self.inventory(), I.parse_ranges('a2:0-76'), [I.parse_owner(OWNER_SPEC)], 1.0)
        inv['revision'] = 2
        next(m for m in inv['members'] if m['id'] == NEW_OWNER)['intent'] = 'quarantined'
        with self.assertRaisesRegex(I.InventoryError, 'no current archive range|exactly one'):
            I.validate(inv)

    def test_owner_specs_and_host_keys_parse(self):
        owner = I.parse_owner(OWNER_SPEC)
        self.assertEqual((owner['ssh_host_key'], owner['cache_bytes'], owner['memory_max'], owner['build_slots']),
                         ('ssh-ed25519 AAAAarchive3', 51539607552, '56G', 1))
        with tempfile.NamedTemporaryFile('w') as key:
            key.write('10.142.1.3 ssh-ed25519 AAAAfromfile\n')
            key.flush()
            self.assertEqual(I.host_key(key.name), 'ssh-ed25519 AAAAfromfile')
        for bad in ('x=1,2,3', NEW_OWNER + '=h,u,ssh-ed25519 K,lots,56G,1'):
            with self.assertRaises(I.InventoryError):
                I.parse_owner(bad)
        with self.assertRaisesRegex(I.InventoryError, 'ID:FIRST-LAST'):
            I.parse_ranges('a2:0..76')


class RepartitionCliTests(unittest.TestCase):
    ACTIVE = 'a' * 64

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = self.root/'state'
        self.state.mkdir()
        (self.root/'roster.json').write_text(json.dumps(live_roster()))
        (self.root/'assignment.json').write_text(json.dumps(live_assignment()))
        I.atomic_json(self.state/'active.json', {'map_sha256': self.ACTIVE, 'workers': [],
                                                 'assignment': str(self.root/'assignment.json')})
        self.publish(self.ACTIVE, archive='sealed')
        self.known = self.root/'known_hosts'
        self.known.write_text('10.142.1.1 ssh-ed25519 AAAAarchive1\n10.142.1.2 ssh-ed25519 AAAAarchive2\n')
        self.config = self.root/'fleet.json'
        self.config.write_text(json.dumps({'state_dir': str(self.state), 'roster': str(self.root/'roster.json'),
                                           'known_hosts': str(self.known), 'ssh_key': str(self.root/'id')}))
        self.scaler = self.root/'scaler'
        self.scaler.mkdir()
        self.run_cli('init')
        self.members = {w['id']: {'role': w['role'], 'state': 'serving', 'rendered': True, 'intent': 'enrolled'}
                        for w in live_roster()}
        self.write_membership()
        self.status = {'active': {'map_sha256': self.ACTIVE}, 'warm': True}
        self.probed, self.dead = [], {}

    def publish(self, digest, archive, recent='tail'):
        directory = self.root/'pub'/digest
        directory.mkdir(parents=True)
        shards = [{'manifest_digest': f'{archive}-{i}'} for i in range(77)]
        shards += [{'manifest_digest': f'{recent}-{i}'} for i in range(77, 86)]
        (directory/'shards.json').write_text(json.dumps({'shards': shards}))
        I.atomic_json(self.state/f'{digest}.request.json', {'directory': str(directory), 'map_sha256': digest})

    def write_membership(self):
        I.atomic_json(self.state/'membership.json', {'updated_unix': time.time(), 'members': self.members})

    def probe(self, config, owner):
        self.probed.append(owner['id'])
        return {'ready': True, 'map_sha256': self.status['active']['map_sha256']}, self.status

    def alive(self, config, member):
        return self.dead.get(member['id'])

    def run_cli(self, *args, probe=None):
        I.main(['--fleet-config', str(self.config), '--scaler-dir', str(self.scaler), *args],
               probe=probe or self.probe, alive=self.alive)

    def repartition(self, *extra, probe=None):
        self.run_cli('--archive', 'repartition', '--ranges', 'a2:0-76', '--owner', OWNER_SPEC, *extra, probe=probe)

    def inventory(self):
        return json.loads((self.state/'inventory.json').read_text())

    def intents(self):
        return {m['id']: m['intent'] for m in self.inventory()['members'] if m['role'] == 'archive-owner'}

    def roster_owners(self):
        return {w['id']: w['archive_range'] for w in json.loads((self.root/'roster.json').read_text())
                if w['role'] == 'archive-owner'}

    def test_repartition_needs_the_flag_and_a_scaler_outside_act_mode(self):
        with self.assertRaisesRegex(SystemExit, '--archive'):
            self.run_cli('repartition', '--ranges', 'a2:0-76', '--owner', OWNER_SPEC)
        (self.scaler/'policy.json').write_text(json.dumps({'mode': 'act'}))
        with self.assertRaisesRegex(SystemExit, 'pause the scaler'):
            self.repartition()
        self.assertEqual(self.inventory()['revision'], 1)

    def test_repartition_swaps_owners_and_partition_in_one_revision(self):
        self.repartition()
        inv = self.inventory()
        self.assertEqual(inv['revision'], 2)
        self.assertEqual(inv['partition'], {'ranges': [{'id': 'a2', 'first': 0, 'last': 76}]})
        self.assertEqual([r['id'] for r in inv['partition_history']], ['a0', 'a1'])
        self.assertEqual(inv['repartition']['from_revision'], 1)
        self.assertEqual(self.intents(), {'transparent-pir-archive-01': 'retired', 'transparent-pir-archive-02': 'retired',
                                          NEW_OWNER: 'enrolled'})
        old = [m for m in inv['members'] if m['id'] in ('transparent-pir-archive-01', 'transparent-pir-archive-02')]
        self.assertTrue(all(m['retired_reason'] == 'repartition' for m in old))
        new = next(m for m in inv['members'] if m['id'] == NEW_OWNER)
        self.assertEqual((new['group'], new['origin'], new['cache_bytes'], new['memory_max']), ('a2', 'static', 51539607552, '56G'))
        # Only the new owner is routable, so the planner and the fleet quorum see one archive owner.
        self.assertEqual(self.roster_owners(), {NEW_OWNER: [0, 76]})
        self.assertIn('10.142.1.3 ssh-ed25519 AAAAarchive3', self.known.read_text())
        self.assertIn('10.142.1.1 ssh-ed25519 AAAAarchive1', self.known.read_text())
        self.assertEqual(self.probed, [NEW_OWNER])
        log = (self.state/'inventory.log.jsonl').read_text().splitlines()
        self.assertEqual(len(log), 2)

    def test_repartition_refuses_a_standby_that_is_not_warm_on_the_archive(self):
        for status, message in [({'active': {'map_sha256': self.ACTIVE}, 'warm': False}, 'not warm'),
                                ({'active': {'map_sha256': self.ACTIVE}, 'warm': True, 'invalidated': True}, 'not warm'),
                                ({'active': {'map_sha256': 'c' * 64}, 'warm': True}, 'no longer on the coordinator')]:
            with self.subTest(message=message):
                self.status = status
                with self.assertRaisesRegex(I.InventoryError, message):
                    self.repartition()
        def unreachable(config, owner):
            raise OSError('connection refused')
        with self.assertRaisesRegex(SystemExit, 'standby probe'):
            self.repartition(probe=unreachable)
        self.assertEqual(self.inventory()['revision'], 1)

    def test_a_standby_on_an_older_map_with_the_same_archive_is_accepted(self):
        self.publish('b' * 64, archive='other')
        self.status = {'active': {'map_sha256': 'b' * 64}, 'warm': True}
        with self.assertRaisesRegex(I.InventoryError, 'archive shards differ'):
            self.repartition()
        self.publish('d' * 64, archive='sealed', recent='older')
        self.status = {'active': {'map_sha256': 'd' * 64}, 'warm': True}
        self.repartition()
        self.assertEqual(self.intents()[NEW_OWNER], 'enrolled')

    def test_new_ranges_are_fresh_cover_the_archive_and_have_one_owner_each(self):
        for ranges, message in [('a0:0-76', 'fresh'), ('a2:0-70', 'ends at shard 76'),
                                ('a2:0-38,a3:39-76', '2 ranges need 2 owners'), ('a2:1-76', 'start at shard 0')]:
            with self.subTest(ranges=ranges):
                with self.assertRaisesRegex(I.InventoryError, message):
                    self.run_cli('--archive', 'repartition', '--ranges', ranges, '--owner', OWNER_SPEC,
                                 '--skip-standby-check')
        with self.assertRaisesRegex(I.InventoryError, 'already a member'):
            self.run_cli('--archive', 'repartition', '--ranges', 'a2:0-76', '--owner',
                         OWNER_SPEC.replace(NEW_OWNER, 'transparent-pir-recent-01'), '--skip-standby-check')
        with self.assertRaisesRegex(I.InventoryError, 'shares a host'):
            self.run_cli('--archive', 'repartition', '--ranges', 'a2:0-76', '--owner',
                         OWNER_SPEC.replace('10.142.1.3', '10.142.0.1'), '--skip-standby-check')
        self.assertEqual(self.inventory()['revision'], 1)

    def test_a_concurrent_write_during_the_probe_fails_the_repartition(self):
        def racing(config, owner):
            inv = I.Inventory(self.state)
            inv.write(1, lambda i: I.set_intent(i, 'transparent-pir-recent-04', 'draining'), 'other')
            return self.probe(config, owner)
        with self.assertRaisesRegex(I.InventoryError, 'revision 2, not 1'):
            self.repartition(probe=racing)
        self.assertEqual(self.intents()['transparent-pir-archive-01'], 'enrolled')

    def test_restore_returns_the_old_owners_and_retires_the_new_one(self):
        self.repartition()
        self.run_cli('--archive', 'restore', '--revision', '1')
        inv = self.inventory()
        self.assertEqual(inv['revision'], 3)
        self.assertEqual(inv['restored_from']['revision'], 1)
        self.assertEqual(inv['partition'], json.loads((self.state/'inventory.d'/'1.json').read_text())['partition'])
        self.assertEqual([r['id'] for r in inv['partition_history']], ['a2'])
        self.assertEqual(self.intents(), {'transparent-pir-archive-01': 'enrolled', 'transparent-pir-archive-02': 'enrolled',
                                          NEW_OWNER: 'retired'})
        self.assertEqual(self.roster_owners(), {'transparent-pir-archive-01': [0, 38], 'transparent-pir-archive-02': [39, 76]})
        self.assertNotIn('AAAAarchive3', self.known.read_text())
        self.assertIn('AAAAarchive1', self.known.read_text())
        with self.assertRaisesRegex(I.InventoryError, 'no repartition to restore'):
            self.run_cli('--archive', 'restore', '--revision', '1')
        # The retired range id stays reserved.
        with self.assertRaisesRegex(I.InventoryError, 'fresh'):
            self.run_cli('--archive', 'repartition', '--ranges', 'a2:0-76', '--owner',
                         OWNER_SPEC.replace('archive-03', 'archive-04'), '--skip-standby-check')

    def test_restore_takes_only_the_revision_the_repartition_replaced(self):
        with self.assertRaisesRegex(I.InventoryError, 'no repartition'):
            self.run_cli('--archive', 'restore', '--revision', '1')
        self.repartition()
        for revision in ('0', '2'):
            with self.subTest(revision=revision):
                with self.assertRaisesRegex(I.InventoryError, 'only revision 1'):
                    self.run_cli('--archive', 'restore', '--revision', revision)
        (self.scaler/'policy.json').write_text(json.dumps({'mode': 'act'}))
        with self.assertRaisesRegex(SystemExit, 'pause the scaler'):
            self.run_cli('--archive', 'restore', '--revision', '1')
        with self.assertRaisesRegex(SystemExit, '--archive'):
            self.run_cli('restore', '--revision', '1')

    def test_restore_keeps_later_recent_changes_and_refuses_later_archive_changes(self):
        self.repartition()
        self.run_cli('drain', 'transparent-pir-recent-04')
        self.run_cli('--archive', 'restore', '--revision', '1')
        inv = self.inventory()
        self.assertEqual(next(m['intent'] for m in inv['members'] if m['id'] == 'transparent-pir-recent-04'), 'draining')
        self.assertEqual(self.intents()['transparent-pir-archive-01'], 'enrolled')

        # A second cycle, now with an archive change after the repartition.
        self.run_cli('--archive', 'repartition', '--ranges', 'a3:0-76', '--owner',
                     OWNER_SPEC.replace('archive-03', 'archive-04').replace('10.142.1.3', '10.142.1.4'))
        self.run_cli('--archive', 'drain', 'transparent-pir-archive-04')
        with self.assertRaisesRegex(I.InventoryError, 'changed the archive after the repartition'):
            self.run_cli('--archive', 'restore', '--revision', '4')

    def test_restore_is_refused_once_an_old_owner_is_stopped(self):
        self.repartition()
        self.dead['transparent-pir-archive-02'] = 'service not active'
        with self.assertRaisesRegex(SystemExit, 'archive-02 cannot take its range back'):
            self.run_cli('--archive', 'restore', '--revision', '1')
        self.dead.clear()
        self.members['transparent-pir-archive-01']['state'] = 'unreachable'
        self.write_membership()
        with self.assertRaisesRegex(SystemExit, 'archive-01 was observed unreachable'):
            self.run_cli('--archive', 'restore', '--revision', '1')
        self.assertEqual(self.intents()[NEW_OWNER], 'enrolled')
        self.run_cli('--archive', 'restore', '--revision', '1', '--force')
        self.assertEqual(self.intents()[NEW_OWNER], 'retired')


if __name__ == '__main__':
    unittest.main()

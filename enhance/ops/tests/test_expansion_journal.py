"""Crash-boundary checks for the infrastructure writer, without provider calls."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/expansion-journal.py'
spec = importlib.util.spec_from_file_location('expansion_journal', SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
INVENTORY = {'groups': [{'name': 'g1', 'replicas': [
    {'name': 'r1', 'url': 'http://192.0.2.1:8291'},
    {'name': 'r2', 'url': 'http://192.0.2.2:8291'}]}]}
POLICY = {'project_id': 'test-project', 'vpc_id': 'test-vpc'}
REQUEST = {'id': 'successor-5-pair-2', 'target_groups': 2, 'successor_ordinal': 5,
           'registered': False, 'boundary_records': 5406720, 'requested_at': 100}
HEALTH = {'protocol': module.PROTOCOL, 'registered_groups': 1,
          'capacity': {'requested': REQUEST['id'], 'requests': {REQUEST['id']: REQUEST}}}
OLD = {f'digitalocean_droplet.worker[{i}]': str(100 + i) for i in range(2)}
ALL = {f'digitalocean_droplet.worker[{i}]': str(100 + i) for i in range(4)}


class ExpansionJournalTests(unittest.TestCase):
    def test_restart_preserves_pending_request_across_reorg(self):
        with tempfile.TemporaryDirectory() as temp:
            with module.Journal(temp) as journal:
                original = journal.observe(HEALTH, INVENTORY, POLICY)
            changed = copy.deepcopy(HEALTH)
            changed['capacity'] = {'requested': None, 'requests': {}}
            with module.Journal(temp) as journal:
                self.assertEqual(journal.observe(changed, INVENTORY, POLICY), original)
            self.assertEqual((Path(temp) / 'journal.json').stat().st_mode & 0o777, 0o600)

    def test_writer_lock_released_on_close(self):
        with tempfile.TemporaryDirectory() as temp:
            with module.Journal(temp):
                with self.assertRaises(BlockingIOError):
                    with module.Journal(temp):
                        self.fail('second writer acquired the lock')
            with module.Journal(temp) as journal:
                journal.observe(HEALTH, INVENTORY, POLICY)

    def test_apply_crash_cannot_repeat_without_reconciliation(self):
        with tempfile.TemporaryDirectory() as temp:
            with module.Journal(temp) as journal:
                journal.observe(HEALTH, INVENTORY, POLICY)
                with self.assertRaises(ValueError):
                    journal.start_apply('a' * 64)
                journal.record_resources(OLD)
                journal.start_apply('a' * 64)
            with module.Journal(temp) as journal:
                self.assertEqual(journal.state['operation']['phase'], 'applying')
                with self.assertRaises(ValueError):
                    journal.start_apply('b' * 64)
                partial = dict(OLD, **{'digitalocean_droplet.worker[2]': '102'})
                journal.resolve_apply(resources=partial, evidence_sha256='c' * 64, complete=False)
            with module.Journal(temp) as journal:
                self.assertEqual(journal.state['operation']['resources'], partial)
                journal.start_apply('d' * 64)
                with self.assertRaises(ValueError):
                    journal.resolve_apply(resources=partial, evidence_sha256='e' * 64, complete=True)
                journal.resolve_apply(resources=ALL, evidence_sha256='e' * 64, complete=True)
            with module.Journal(temp) as journal:
                operation = journal.state['operation']
                self.assertEqual(operation['phase'], 'provisioned')
                self.assertEqual(operation['resources'], ALL)
                self.assertEqual(len(operation['attempts']), 2)
                with self.assertRaises(ValueError):
                    journal.start_apply('f' * 64)

    def test_identity_replacement_alias_and_extra_slot_rejected_atomically(self):
        with tempfile.TemporaryDirectory() as temp, module.Journal(temp) as journal:
            journal.observe(HEALTH, INVENTORY, POLICY)
            journal.record_resources(OLD)
            before = (Path(temp) / 'journal.json').read_bytes()
            for resources in [
                {'digitalocean_droplet.worker[0]': '999'},
                {'digitalocean_droplet.worker[2]': '100'},
                {'digitalocean_droplet.worker[4]': '104'},
                {'digitalocean_droplet.worker[2]': None},
            ]:
                with self.subTest(resources=resources), self.assertRaises(ValueError):
                    journal.record_resources(resources)
                self.assertEqual((Path(temp) / 'journal.json').read_bytes(), before)
                self.assertEqual(journal.state['operation']['resources'], OLD)

    def test_pending_inputs_are_frozen(self):
        with tempfile.TemporaryDirectory() as temp, module.Journal(temp) as journal:
            journal.observe(HEALTH, INVENTORY, POLICY)
            with self.assertRaises(ValueError):
                journal.observe(HEALTH, INVENTORY, dict(POLICY, project_id='different'))
            inventory = copy.deepcopy(INVENTORY)
            inventory['groups'][0]['replicas'][0]['url'] = 'http://192.0.2.3:8291'
            with self.assertRaises(ValueError):
                journal.observe(HEALTH, inventory, POLICY)

    def test_invalid_or_cross_protocol_demand_never_creates_operation(self):
        mutations = [
            lambda h: h.update(protocol='ironwood-enhance-pir-v3'),
            lambda h: h.update(registered_groups=2),
            lambda h: h['capacity']['requests'][REQUEST['id']].update(target_groups=3),
            lambda h: h['capacity']['requests'][REQUEST['id']].update(registered=True),
            lambda h: h['capacity']['requests'][REQUEST['id']].update(successor_ordinal=6),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutate), tempfile.TemporaryDirectory() as temp, module.Journal(temp) as journal:
                health = copy.deepcopy(HEALTH)
                mutate(health)
                with self.assertRaises(ValueError):
                    journal.observe(health, INVENTORY, POLICY)
                self.assertIsNone(journal.state['operation'])
                self.assertFalse((Path(temp) / 'journal.json').exists())

    def test_lock_released_after_corrupt_journal(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'journal.json'
            path.write_text('broken')
            with self.assertRaises(json.JSONDecodeError):
                with module.Journal(temp):
                    pass
            path.unlink()
            with module.Journal(temp) as journal:
                journal.observe(HEALTH, INVENTORY, POLICY)

"""Test-only inventory handoff survives a restart without changing identities."""
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/v4-isolated-expansion.py'
spec = importlib.util.spec_from_file_location('v4_isolated_expansion', SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

OLD = {'groups': [{'name': 'group-1', 'replicas': [
    {'name': 'r1', 'url': 'http://10.0.0.1:8291'},
    {'name': 'r2', 'url': 'http://10.0.0.2:8291'}]}]}
PAIR = [{'address': f'digitalocean_droplet.worker[{i + 2}]',
         'resource_id': str(102 + i), 'name': f'enhance-pir-v4-g02-r{i + 1}',
         'private_ipv4': f'10.0.0.{i + 3}'} for i in range(2)]
REQUEST = 'successor-5-pair-2'


class NoOpLock:
    def __init__(self, _policy):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass


class RegistrationTests(unittest.TestCase):
    def test_one_pair_is_pinned_even_when_a_later_demand_exists(self):
        first = {'id': REQUEST, 'phase': 'registered', 'target_groups': 2,
                 'registered_inventory_digest': module.journal_module.digest(OLD)}
        current = {'registered_groups': 2, 'capacity': {
            'requested': 'successor-11-pair-3',
            'requests': {REQUEST: {'registered': True},
                         'successor-11-pair-3': {'registered': False}}}}
        self.assertTrue(module.completed_matches(first, OLD, current, REQUEST, 2))
        with self.assertRaisesRegex(ValueError, 'different expansion'):
            module.require_expected({'id': 'successor-11-pair-3', 'target_groups': 3}, REQUEST, 2)

    def test_retries_pending_inventory_and_rejects_changed_pair(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            inventory = directory / 'workers.json'
            config = directory / 'bootstrap.json'
            inventory.write_text(json.dumps(OLD))
            config.write_text(json.dumps({'revision': 'a' * 40, 'manifest_sha256': 'b' * 64}))
            operation = {'id': REQUEST, 'phase': 'bootstrapped', 'before': 1,
                         'target_groups': 2, 'inventory_digest': module.journal_module.digest(OLD),
                         'bootstrap': {'receipts': {p['address']: {'phase': 'bootstrapped'} for p in PAIR}}}
            journal = SimpleNamespace(state={'operation': operation}, directory=directory)
            pending = {'protocol': module.journal_module.PROTOCOL, 'registered_groups': 1,
                       'capacity': {'requested': REQUEST, 'requests': {REQUEST: {'registered': False}}}}
            acknowledged = {'protocol': module.journal_module.PROTOCOL, 'registered_groups': 2,
                            'capacity': {'requested': None, 'requests': {REQUEST: {'registered': True}}}}
            with (patch.object(module.provisioning, 'StateLock', NoOpLock),
                  patch.object(module.provisioning, 'Terraform', return_value=object()),
                  patch.object(module.provisioning, 'DigitalOcean', return_value=object()),
                  patch.object(module.provisioning, 'inspect_fleet', return_value={'workers': {}}),
                  patch.object(module.bootstrap, 'targets', return_value=PAIR),
                  patch.object(module.bootstrap, 'validate_receipt'),
                  patch.object(module.bootstrap.installer, 'verify_bundle', return_value={}),
                  patch.object(module, 'health', return_value=pending)):
                first = module.registration(journal, directory, {}, inventory, config,
                                            directory, 'token', 'http://127.0.0.1:8280')
                self.assertEqual(len(first['groups']), 2)
                self.assertEqual(json.loads(inventory.read_text()), first)
                second = module.registration(journal, directory, {}, inventory, config,
                                             directory, 'token', 'http://127.0.0.1:8280')
                self.assertEqual(second, first)
                inventory.write_text(json.dumps({'groups': [*OLD['groups'],
                    {'name': 'group-2', 'replicas': [{'name': 'impostor', 'url': 'http://10.0.0.9:8291'}]}]}))
                with self.assertRaisesRegex(ValueError, 'pending inventory differs'):
                    module.registration(journal, directory, {}, inventory, config,
                                        directory, 'token', 'http://127.0.0.1:8280')
            inventory.write_text(json.dumps(first))
            with (patch.object(module.provisioning, 'StateLock', NoOpLock),
                  patch.object(module.provisioning, 'Terraform', return_value=object()),
                  patch.object(module.provisioning, 'DigitalOcean', return_value=object()),
                  patch.object(module.provisioning, 'inspect_fleet', return_value={'workers': {}}),
                  patch.object(module.bootstrap, 'targets', return_value=PAIR),
                  patch.object(module.bootstrap, 'validate_receipt'),
                  patch.object(module.bootstrap.installer, 'verify_bundle', return_value={}),
                  patch.object(module, 'health', return_value=acknowledged)):
                self.assertEqual(module.registration(journal, directory, {}, inventory, config,
                                                     directory, 'token', 'http://127.0.0.1:8280'), first)


if __name__ == '__main__':
    unittest.main()

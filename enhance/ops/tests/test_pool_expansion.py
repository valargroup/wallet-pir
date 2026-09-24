import importlib.util
from pathlib import Path
import unittest


def module(name):
    spec = importlib.util.spec_from_file_location(name.replace('-', '_'), Path(__file__).parents[1] / 'scripts' / (name + '.py'))
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod); return mod

journal = module('pool-expansion-journal')
validator = module('pool-plan')

class PoolExpansionTests(unittest.TestCase):
    def test_one_worker_intent_is_stable_and_apply_requires_reconciliation(self):
        inventory = {'workers': [{'name': 'a', 'url': 'http://a'}, {'name': 'b', 'url': 'http://b'}]}
        policy = {'project_id': 'wallet-pir'}
        health = {'protocol': 'ironwood-enhance-pir-v7', 'pool': {}, 'capacity': {
            'requested_worker': 'pool-worker-3', 'worker_requests': {'pool-worker-3': {'target_workers': 3}}}}
        state = journal.observe({'version': 1, 'completed': {}}, health, policy, inventory)
        self.assertEqual(state, journal.observe(state, health, policy, inventory))
        receipt = {'operation_id': 'pool-worker-3', 'policy_sha256': journal.digest(policy), 'plan_sha256': 'a' * 64}
        state = journal.advance(state, 'planned', receipt)
        state = journal.advance(state, 'apply_intent', receipt)
        with self.assertRaises(ValueError): journal.advance(state, 'provisioned', receipt)
        reconciled = {**receipt, 'droplet_id': 123, 'terraform_reconciled': True, 'provider_reconciled': True}
        state = journal.advance(state, 'provisioned', reconciled)
        state = journal.advance(state, 'bootstrapped', reconciled)
        with self.assertRaises(ValueError): journal.advance(state, 'qualified', {**reconciled, 'qualification': 'unqualified'})
        with self.assertRaises(ValueError): journal.observe(state, health, {'project_id': 'other'}, inventory)

    def test_worker_plan_rejects_deletion_and_unexpected_changes(self):
        policy = {'existing_pool_workers': 0, 'region': 'ams3', 'vpc_id': 'vpc', 'ssh_key_ids': ['roman'], 'project_id': 'wallet-pir'}
        plan = {'resource_changes': [
            {'address': 'digitalocean_droplet.enhance_pool_worker[0]', 'change': {'actions': ['create'], 'after': {
                'name': 'enhance-pir-pool-01', 'size': 'c-4', 'image': 'ubuntu-24-04-x64', 'region': 'ams3',
                'vpc_uuid': 'vpc', 'ssh_keys': ['roman'], 'tags': ['enhance-pir-worker']}}},
            {'address': 'digitalocean_project_resources.enhance_pool_worker[0]', 'change': {'actions': ['create'],
                'after': {'project': 'wallet-pir'}, 'after_unknown': {'resources': True}}}]}
        self.assertEqual(validator.validate(plan, policy, {})['target_pool_workers'], 1)
        plan['resource_changes'].append({'address': 'digitalocean_droplet.coordinator', 'change': {'actions': ['delete']}})
        with self.assertRaises(ValueError): validator.validate(plan, policy, {})

if __name__ == '__main__': unittest.main()

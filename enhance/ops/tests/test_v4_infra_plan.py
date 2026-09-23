"""Validate append-only v4 provisioning using an actual Terraform mocked plan."""
import copy
import importlib.util
import json
from pathlib import Path
import unittest

OPS = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('v4_infra_plan', OPS / 'scripts/v4-infra-plan.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
POLICY = {'region': 'ams3', 'vpc_id': '00000000-0000-4000-8000-000000000002',
          'project_id': '00000000-0000-4000-8000-000000000001', 'ssh_key_ids': ['12345'],
          'operator_ssh_cidrs': ['192.0.2.20/32'], 'coordinator_private_ipv4': '192.0.2.10'}


def fixture():
    return json.loads((OPS / 'fixtures/v4-infra-bootstrap.json').read_text())


def resource(plan, address):
    return next(r for r in plan['resource_changes'] if r['address'] == address)


def materialize(plan, index):
    address = f'digitalocean_droplet.worker[{index}]'
    worker = resource(plan, address)['change']
    worker['actions'] = ['no-op']
    worker['after']['id'] = str(1000 + index)
    worker['before'] = copy.deepcopy(worker['after'])
    membership = resource(plan, f'digitalocean_project_resources.worker[{index}]')['change']
    membership['after']['resources'] = [f'do:droplet:{1000 + index}']
    return {address: str(1000 + index)}


class V4PlanTests(unittest.TestCase):
    def test_actual_mocked_bootstrap_plan(self):
        result = module.validate(fixture(), POLICY, 0, {})
        self.assertEqual(result['target_groups'], 1)
        self.assertEqual(len(result['creates']), 6)

    def test_partial_creation_uses_recorded_identity(self):
        plan = fixture()
        existing = materialize(plan, 0)
        result = module.validate(plan, POLICY, 0, existing)
        self.assertNotIn('digitalocean_droplet.worker[0]', result['creates'])
        self.assertIn('digitalocean_droplet.worker[1]', result['creates'])
        with self.assertRaises(ValueError):
            module.validate(plan, POLICY, 0, {'digitalocean_droplet.worker[0]': 'other'})

    def test_append_only_next_pair(self):
        plan = fixture()
        for index in range(2):
            for prefix in ['digitalocean_droplet', 'digitalocean_project_resources']:
                new = copy.deepcopy(resource(plan, f'{prefix}.worker[{index}]'))
                new['address'] = f'{prefix}.worker[{index + 2}]'
                new['index'] = index + 2
                if prefix == 'digitalocean_droplet':
                    new['change']['after']['name'] = f'enhance-pir-v4-g02-r{index + 1}'
                plan['resource_changes'].append(new)
        existing = {}
        for index in range(2):
            existing.update(materialize(plan, index))
            resource(plan, f'digitalocean_project_resources.worker[{index}]')['change']['actions'] = ['no-op']
        for address in ['digitalocean_tag.worker', 'digitalocean_firewall.worker']:
            resource(plan, address)['change']['actions'] = ['no-op']
        self.assertEqual(len(module.validate(plan, POLICY, 1, existing)['creates']), 4)

    def test_mutations_rejected(self):
        def worker(plan):
            return resource(plan, 'digitalocean_droplet.worker[0]')['change']
        mutations = [
            lambda p: worker(p).update(actions=['delete', 'create']),
            lambda p: worker(p).update(actions=['update']),
            lambda p: worker(p)['after'].update(name='enhance-pir-worker-01'),
            lambda p: worker(p)['after'].update(size='s-1vcpu-1gb'),
            lambda p: worker(p)['after'].update(vpc_uuid='other'),
            lambda p: worker(p)['after'].update(ssh_keys=['other']),
            lambda p: resource(p, 'digitalocean_project_resources.worker[0]')['change']['after'].update(project='other'),
            lambda p: resource(p, 'digitalocean_firewall.worker')['change']['after']['inbound_rule'].append({'protocol': 'tcp', 'port_range': '8291', 'source_addresses': ['0.0.0.0/0']}),
            lambda p: p['resource_changes'].append({'address': 'cloudflare_dns_record.production', 'change': {'actions': ['no-op']}}),
            lambda p: p['resource_changes'].pop(),
        ]
        for mutate in mutations:
            plan = fixture()
            mutate(plan)
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                module.validate(plan, POLICY, 0, {})

    def test_fleet_ceiling(self):
        with self.assertRaises(ValueError):
            module.validate(fixture(), POLICY, 4, {})

"""The elastic root's saved-plan validator admits only requested creates and
allowlisted destroys of elastic recent replicas.

The fixtures under transparent/ops/fixtures/elastic-plans/ are synthetic
`terraform show -json` documents in Terraform 1.14's format for the elastic
root; none comes from a live plan.
"""
import copy
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / 'transparent/ops/scripts/transparent-plan.py'
FIXTURES = ROOT / 'transparent/ops/fixtures/elastic-plans'
SPEC = importlib.util.spec_from_file_location('transparent_plan', SCRIPT)
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)

R5, R6, R7 = (f'transparent-pir-recent-0{i}' for i in (5, 6, 7))
PROFILE = {'size': 's-4vcpu-8gb', 'region': 'ams3', 'vpc_uuid': '00000000-0000-4000-8000-00000000a142',
           'tag': 'transparent-pir-worker', 'image': 'ubuntu-24-04-x64',
           'project_id': '85639967-fecb-4c8d-88be-c0e3dee3f86c'}


def fixture(name):
    return json.loads((FIXTURES / name).read_text())


def addresses(*names):
    return sorted(f'{kind}.recent["{name}"]' for name in names
                  for kind in ('digitalocean_droplet', 'digitalocean_project_resources'))


class PlanValidatorTests(unittest.TestCase):
    def refused(self, plan, pattern, **kwargs):
        kwargs.setdefault('profile', PROFILE)
        with self.assertRaisesRegex(M.Refused, pattern):
            M.validate(plan, **kwargs)

    def test_one_create_beside_an_unchanged_member(self):
        self.assertEqual(M.validate(fixture('create-one.json'), allow_create=[R6], profile=PROFILE),
                         {'creates': addresses(R6), 'destroys': []})

    def test_two_creates(self):
        self.assertEqual(M.validate(fixture('create-two.json'), allow_create=[R6, R7], profile=PROFILE),
                         {'creates': addresses(R6, R7), 'destroys': []})
        self.refused(fixture('create-two.json'), R7 + ' is not allowed to be created', allow_create=[R6])

    def test_one_destroy_with_its_recorded_id(self):
        self.assertEqual(M.validate(fixture('destroy-one.json'), allow_destroy=[R5], allow_destroy_ids=['512345678']),
                         {'creates': [], 'destroys': addresses(R5)})

    def test_destroy_with_another_id_is_refused(self):
        plan = fixture('destroy-one.json')
        self.refused(plan, 'droplet 512345678 is not on the destroy allowlist',
                     allow_destroy=[R5], allow_destroy_ids=['512345699'])
        self.refused(plan, 'not on the destroy allowlist', allow_destroy=[R5])
        self.refused(plan, R5 + ' is not allowed to be destroyed', allow_destroy_ids=['512345678'])
        # The project entry must name the same droplet as the member being destroyed.
        plan['resource_changes'][1]['change']['before']['resources'] = ['do:droplet:512345699']
        self.refused(plan, 'different droplets', allow_destroy=[R5], allow_destroy_ids=['512345678', '512345699'])
        plan = fixture('destroy-one.json')
        plan['resource_changes'][0]['change']['before']['name'] = R6
        self.refused(plan, 'name differs', allow_destroy=[R5], allow_destroy_ids=['512345678'])

    def test_update_of_a_droplet_is_refused(self):
        # Even a member the actuator may destroy may not be changed in place.
        self.refused(fixture('update-droplet.json'), 'updates are refused',
                     allow_destroy=[R5], allow_destroy_ids=['512345678'])

    def test_replacement_is_refused(self):
        plan = fixture('replace-droplet.json')
        self.refused(plan, 'replacement is refused', allow_destroy=[R5], allow_destroy_ids=['512345678'])
        plan['resource_changes'][0]['change']['actions'] = ['create', 'delete']
        self.refused(plan, 'replacement is refused', allow_destroy=[R5], allow_destroy_ids=['512345678'])

    def test_unexpected_resource_type_is_refused(self):
        self.refused(fixture('unexpected-type.json'), r'digitalocean_firewall\.recent: not an elastic recent replica',
                     allow_create=[R6])

    def test_name_outside_the_allow_set_is_refused(self):
        self.refused(fixture('create-one.json'), R6 + ' is not allowed to be created', allow_create=[R7])
        self.refused(fixture('create-one.json'), 'not allowed to be created')

    def test_archive_names_are_refused_everywhere(self):
        self.refused(fixture('archive-name.json'), 'archive resources are never planned', allow_create=[R6])
        for field in ('allow_create', 'allow_destroy'):
            with self.subTest(field=field):
                self.refused(fixture('create-one.json'), 'only elastic recent replicas', **{field: ['transparent-pir-archive-01']})
        plan = fixture('create-one.json')
        plan['resource_drift'] = [dict(plan['resource_changes'][0], address='digitalocean_droplet.transparent_archive[0]')]
        self.refused(plan, 'archive', allow_create=[R6])
        noop = fixture('create-one.json')
        noop['resource_changes'][0]['address'] = 'digitalocean_droplet.recent["transparent-pir-archive-01"]'
        self.refused(noop, 'archive', allow_create=[R6])

    def test_image_and_profile_drift_are_refused(self):
        self.refused(fixture('image-drift.json'), "image 'ubuntu-22-04-x64' differs", allow_create=[R6])
        for key, value in [('size', 's-8vcpu-16gb'), ('region', 'nyc3'), ('vpc_uuid', '00000000-0000-4000-8000-000000000999'),
                           ('name', R7)]:
            with self.subTest(key=key):
                plan = fixture('create-one.json')
                plan['resource_changes'][2]['change']['after'][key] = value
                self.refused(plan, key, allow_create=[R6])
        for tags in (['transparent-pir-worker', 'extra'], [], ['transparent-pir-router']):
            with self.subTest(tags=tags):
                plan = fixture('create-one.json')
                plan['resource_changes'][2]['change']['after']['tags'] = tags
                self.refused(plan, 'tags differ', allow_create=[R6])
        plan = fixture('create-one.json')
        plan['resource_changes'][3]['change']['after']['project'] = '00000000-0000-4000-8000-000000000001'
        self.refused(plan, 'different project', allow_create=[R6])
        plan = fixture('create-one.json')
        plan['resource_changes'][3]['change']['after']['resources'] = ['do:droplet:1']
        plan['resource_changes'][3]['change']['after_unknown'] = {}
        self.refused(plan, 'newly created droplet', allow_create=[R6])
        self.refused(fixture('create-one.json'), 'full pinned profile', allow_create=[R6], profile=dict(PROFILE, image=None))

    def test_addresses_outside_this_root_are_refused(self):
        plan = fixture('create-one.json')
        plan['resource_changes'][2]['module_address'] = 'module.production'
        plan['resource_changes'][2]['address'] = 'module.production.' + plan['resource_changes'][2]['address']
        self.refused(plan, 'outside the elastic root', allow_create=[R6])
        for field, value in [('provider_name', 'registry.terraform.io/other/digitalocean'), ('name', 'worker'),
                             ('index', R7), ('mode', 'imported'), ('type', 'digitalocean_project_resources')]:
            with self.subTest(field=field):
                plan = fixture('create-one.json')
                plan['resource_changes'][2][field] = value
                self.refused(plan, 'not an elastic recent replica', allow_create=[R6])
        for address in ['digitalocean_droplet.transparent_recent[0]', 'digitalocean_droplet.recent["transparent-pir-recent-5"]',
                        'digitalocean_droplet.recent["transparent-pir-router-01"]']:
            with self.subTest(address=address):
                plan = fixture('create-one.json')
                plan['resource_changes'][2]['address'] = address
                self.refused(plan, 'not an elastic recent replica', allow_create=[R6])

    def test_data_sources_may_only_be_read(self):
        plan = fixture('create-one.json')
        read = {'address': 'data.digitalocean_vpc.fleet', 'mode': 'data', 'type': 'digitalocean_vpc', 'name': 'fleet',
                'provider_name': M.PROVIDER, 'change': {'actions': ['read'], 'before': None, 'after': {}}}
        plan['resource_changes'].append(read)
        self.assertEqual(M.validate(plan, allow_create=[R6], profile=PROFILE)['creates'], addresses(R6))
        read['change']['actions'] = ['create']
        self.refused(plan, 'data sources may only be read', allow_create=[R6])

    def test_malformed_or_ambiguous_plans_are_refused(self):
        self.refused({'resource_changes': []}, 'not Terraform plan JSON')
        self.refused(dict(fixture('create-one.json'), errored=True), 'errored', allow_create=[R6])
        self.refused(dict(fixture('create-one.json'), deferred_changes=[{'reason': 'provider_config_unknown'}]),
                     'deferred', allow_create=[R6])
        plan = fixture('create-one.json')
        plan['resource_changes'].append(copy.deepcopy(plan['resource_changes'][2]))
        self.refused(plan, 'appears twice', allow_create=[R6])
        plan = fixture('create-one.json')
        plan['resource_changes'][2]['change']['actions'] = ['forget']
        self.refused(plan, 'is not allowed', allow_create=[R6])
        self.refused(fixture('create-one.json'), 'never both created and destroyed', allow_create=[R6], allow_destroy=[R6])
        self.refused(fixture('destroy-one.json'), 'malformed', allow_destroy=[R5], allow_destroy_ids=['0x1'])

    def test_unchanged_members_and_drift_pass_without_permission(self):
        plan = fixture('destroy-one.json')
        plan['resource_changes'] = plan['resource_changes'][2:]
        plan['resource_drift'] = [dict(copy.deepcopy(plan['resource_changes'][0]), change={'actions': ['update']})]
        self.assertEqual(M.validate(plan), {'creates': [], 'destroys': []})


class CommandLineTests(unittest.TestCase):
    def run_cli(self, *arguments, stdin=None):
        return subprocess.run([sys.executable, str(SCRIPT), *arguments], capture_output=True, text=True, input=stdin)

    def test_summary_and_refusal(self):
        profile = [arg for key, value in PROFILE.items() for arg in ('--' + key.replace('_', '-'), value)]
        result = self.run_cli('--plan-json', str(FIXTURES / 'create-two.json'), '--allow-create', R6,
                              '--allow-create', R7, *profile)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), {'creates': addresses(R6, R7), 'destroys': []})
        result = self.run_cli('--plan-json', '-', '--allow-destroy', R5, '--allow-destroy-id', '512345678',
                              stdin=(FIXTURES / 'destroy-one.json').read_text())
        self.assertEqual(json.loads(result.stdout), {'creates': [], 'destroys': addresses(R5)})
        result = self.run_cli('--plan-json', str(FIXTURES / 'update-droplet.json'))
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, '')
        self.assertIn('elastic plan refused: digitalocean_droplet.recent["transparent-pir-recent-05"]: updates are refused', result.stderr)

    def test_secrets_never_reach_the_output(self):
        plan = fixture('create-one.json')
        secret = plan['variables']['host_keys']['value'][R6]['private']
        for arguments in (['--allow-create', R6], []):
            with self.subTest(arguments=arguments), patch.object(sys, 'stdin', io.StringIO(json.dumps(plan))), \
                    patch('sys.stdout', new_callable=io.StringIO) as out:
                profile = [arg for key, value in PROFILE.items() for arg in ('--' + key.replace('_', '-'), value)]
                try:
                    M.main(['--plan-json', '-', *arguments, *profile])
                    message = out.getvalue()
                except SystemExit as refusal:
                    message = str(refusal.code)
                self.assertNotIn('fixture-not-a-token', message)
                self.assertNotIn(secret.strip(), message)
                self.assertNotIn('#cloud-config', message)
        result = self.run_cli('--plan-json', str(FIXTURES / 'missing.json'))
        self.assertIn('cannot read plan JSON', result.stderr)


if __name__ == '__main__':
    unittest.main()

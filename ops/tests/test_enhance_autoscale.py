import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location('enhance_autoscale', Path(__file__).parents[1] / 'scripts/enhance-autoscale.py')
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


def plan():
    return {'resource_changes': [
        {'address': f'digitalocean_droplet.worker[{i}]', 'change': {
            'actions': ['create'], 'after': {'size': 'c-4', 'name': f'enhance-pir-worker-{i+1:02d}', 'region': 'ams3'}}}
        for i in (2, 3)]}


class PlanSafety(unittest.TestCase):
    def test_computed_worker_tag_counts_do_not_block_next_expansion(self):
        candidate = plan()
        candidate['resource_drift'] = [{'address': 'digitalocean_tag.worker', 'change': {
            'actions': ['update'],
            'before': {'name': 'enhance-pir-worker', 'droplets_count': 2, 'total_resource_count': 2},
            'after': {'name': 'enhance-pir-worker', 'droplets_count': 4, 'total_resource_count': 4}}}]
        module.validate_plan(candidate, 1)
        candidate['resource_drift'][0]['change']['after']['name'] = 'unexpected-tag'
        with self.assertRaises(ValueError):
            module.validate_plan(candidate, 1)

    def test_only_next_pair(self):
        module.validate_plan(plan(), 1)

    def test_refuses_replacement_and_unrelated_change(self):
        for address, actions in [('digitalocean_droplet.worker[0]', ['delete', 'create']),
                                 ('digitalocean_droplet.transparent_recent[0]', ['update'])]:
            candidate = plan()
            candidate['resource_changes'].append({'address': address, 'change': {'actions': actions}})
            with self.assertRaises(ValueError):
                module.validate_plan(candidate, 1)

    def test_wrong_size_and_partial_pair(self):
        candidate = plan()
        candidate['resource_changes'][0]['change']['after']['size'] = 's-4vcpu-8gb'
        with self.assertRaises(ValueError):
            module.validate_plan(candidate, 1)

    def test_partial_apply_can_finish_only_the_existing_pair(self):
        candidate = plan()
        candidate['resource_changes'][0]['change']['actions'] = ['no-op']
        module.validate_plan(candidate, 1, ['digitalocean_droplet.worker[2]'])
        with self.assertRaises(ValueError):
            module.validate_plan(candidate, 1, ['digitalocean_droplet.worker[0]'])
        candidate = plan()
        candidate['resource_changes'].pop()
        with self.assertRaises(ValueError):
            module.validate_plan(candidate, 1)

    def test_unknown_or_removing_project_membership_refused(self):
        for after, unknown in [({'project': 'p', 'resources': []}, {}),
                               ({'project': 'p'}, {'resources': True})]:
            candidate = plan()
            candidate['resource_changes'].append({'address': 'digitalocean_project_resources.enhance', 'change': {
                'actions': ['update'], 'before': {'project': 'p', 'resources': ['existing']},
                'after': after, 'after_unknown': unknown}})
            with self.assertRaises(ValueError):
                module.validate_plan(candidate, 1)


class Trigger(unittest.TestCase):
    def setUp(self):
        self.state = {}
        self.topology = {'revision': 0, 'groups': [{}]}
        self.health = {'phase': {'phase': 'serving'}, 'ironwood_tree_size': 950000,
                       'tables': {'enhance': {'workers': 2}}}

    def test_three_fresh_samples_and_cooldown(self):
        self.assertFalse(module.observe(self.state, self.topology, self.health, 10000))
        self.assertFalse(module.observe(self.state, self.topology, self.health, 10300))
        self.assertTrue(module.observe(self.state, self.topology, self.health, 10600))
        self.state['last_success'] = 10600
        self.assertFalse(module.observe(self.state, self.topology, self.health, 10900))

    def test_gaps_reorg_and_unhealthy_reset_trigger(self):
        for now in (10000, 10300):
            module.observe(self.state, self.topology, self.health, now)
        self.assertFalse(module.observe(self.state, self.topology, self.health, 12000))
        self.health['ironwood_tree_size'] -= 1
        self.assertFalse(module.observe(self.state, self.topology, self.health, 12300))
        self.health['phase']['phase'] = 'failed'
        self.assertFalse(module.observe(self.state, self.topology, self.health, 12600))
        self.assertEqual(self.state['trigger_samples'], 0)

    def test_ceiling(self):
        self.topology['groups'] *= 4
        self.health['ironwood_tree_size'] *= 4
        self.health['tables']['enhance']['workers'] = 8
        for now in (10000, 10300, 10600):
            self.assertFalse(module.observe(self.state, self.topology, self.health, now))

    def test_atomic_journal_roundtrip(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'state.json'
            module.atomic(path, {'desired_groups': 2, 'operation': {'id': 'test', 'step': 'bootstrap'}})
            config = {'state_dir': directory}
            self.assertEqual(module.Controller(config).state['operation']['step'], 'bootstrap')
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)


class OperationSafety(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.controller = module.Controller({'state_dir': self.directory.name, 'enabled': True, 'tfvars_file': '/fixture/inputs.tfvars'})
        self.controller.verify_release = Mock(return_value='a' * 40)
        self.controller.notify = Mock(return_value=True)
        self.controller.terraform = Mock()
        self.operation = {'id': 'test', 'step': 'planned', 'release': 'a' * 40, 'old_groups': [{}], 'positions': 950000}

    def test_persisted_count_overrides_initial_tfvars_only_on_plan(self):
        self.controller.config['terraform_dir'] = '/fixture/terraform'
        self.controller.state['desired_groups'] = 2
        with patch.object(module, 'run', return_value='') as command:
            module.Controller.terraform(self.controller, ['plan', '-var-file=initial.tfvars'])
            self.assertEqual(command.call_args.args[0][-1], '-var=enhance_group_count=2')
            module.Controller.terraform(self.controller, ['apply', 'saved.tfplan'])
            self.assertEqual(command.call_args.args[0][-2:], ['apply', 'saved.tfplan'])

    def test_failed_notification_never_provisions(self):
        self.controller.notify.return_value = False
        with self.assertRaisesRegex(RuntimeError, 'notification'):
            self.controller.advance(self.operation, {}, 10000)
        self.controller.terraform.assert_not_called()
        self.assertEqual(self.controller.state['desired_groups'], 1)

    def test_unexpected_plan_is_not_applied(self):
        self.controller.terraform.side_effect = ['', '{"resource_changes": []}']
        with patch.dict(module.os.environ, {'TF_VAR_digitalocean_token': 'fixture-not-a-token'}), patch.object(module, 'http_json', return_value={'droplets': [{}, {}]}):
            with self.assertRaises(ValueError):
                self.controller.advance(self.operation, {}, 10000)
        commands = [call.args[0][0] for call in self.controller.terraform.call_args_list]
        self.assertEqual(commands, ['plan', 'show'])

    def test_release_change_never_bootstraps(self):
        self.controller.verify_release.return_value = 'b' * 40
        self.operation['step'] = 'bootstrap'
        with self.assertRaisesRegex(RuntimeError, 'release changed'):
            self.controller.advance(self.operation, {}, 10000)
        self.controller.terraform.assert_not_called()

    def test_crash_after_commit_skips_bootstrap(self):
        self.controller.state['operation'] = {**self.operation, 'step': 'bootstrap'}
        self.controller.state['desired_groups'] = 2
        self.controller.topology = Mock(return_value={'revision': 1, 'groups': [{}, {}], 'last_operation': {'operation_id': 'test'}, 'error': None})
        self.controller.check_existing = Mock(return_value={'phase': {'phase': 'serving'}, 'ironwood_tree_size': 950000, 'tables': {'enhance': {'workers': 4}}})
        self.controller.advance = Mock()
        self.controller.flush_notifications = Mock()
        self.controller.tick()
        self.assertEqual(self.controller.advance.call_args.args[0]['step'], 'observing')


if __name__ == '__main__':
    unittest.main()

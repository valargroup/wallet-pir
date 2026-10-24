"""The query candidate can be added without adopting or changing the fleet."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'tools/lib'))
sys.path.insert(0, str(ROOT / 'tools/tests/operations'))
from wallet_pir_ops import durable
from wallet_pir_ops.deploy import query_provision as query
from wallet_pir_ops.terraform import file_sha256
from test_query_infra import plan as initial_plan, PROJECT


class Lock:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


class Runtime:
    def __init__(self, document, before):
        self.document, self.before = document, before
        self.applied = False
        self.fail_apply = False
        self.change_existing_identity = False
        self.calls = []

    def terraform(self, root, **options):
        self.calls.append(('root', str(root)))
        return self

    def show_json(self, path):
        return self.document

    def state_pull(self):
        value = copy.deepcopy(self.before)
        if self.applied:
            value['serial'] += 1
            if self.change_existing_identity:
                value['resources'][0]['instances'][0]['attributes']['id'] = 'changed-node'
            value['resources'].append({'mode': 'managed', 'module': 'module.query_serving[0]',
                                       'type': 'digitalocean_droplet', 'name': 'query',
                                       'instances': [{'attributes': {'id': 'new-node'}}]})
        return value

    def run(self, arguments):
        self.calls.append(tuple(arguments))

    def apply(self, path, expected):
        if self.fail_apply:
            raise RuntimeError('simulated transport interruption')
        assert file_sha256(path) == expected
        self.applied = True

    def output_json(self):
        return {'query_serving_node': {'value': {'public_ipv4': '192.0.2.10'}}}


class QueryProvision(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.canonical = self.root / 'installed/production'
        self.source = self.root / 'candidate/fleet'
        self.canonical.mkdir(parents=True)
        self.source.mkdir(parents=True)
        (self.canonical / 'enhance').mkdir()
        (self.source / 'enhance').mkdir()
        (self.canonical / 'enhance/cloud-init.tftpl').write_text('existing template')
        (self.source / 'enhance/cloud-init.tftpl').write_text('existing template')
        for name, text in {'main.tf': 'existing fleet', 'backend.tf': 'existing backend',
                           'deployed.auto.tfvars': 'existing operator inputs'}.items():
            (self.canonical / name).write_text(text)
            (self.source / name).write_text(text)
        (self.source / 'query-serving.tf').write_text('new query module declaration')
        module = self.source.parent / 'query-serving'
        module.mkdir()
        for name in query.MODULE_FILES:
            (module / name).write_text('reviewed ' + name)
        saved = self.root / 'saved.tfplan'
        saved.write_bytes(b'test saved plan')
        self.request = {'machine_id': 'a' * 32, 'source_sha': 'b' * 40,
                        'staged_root': str(self.source), 'saved_plan': str(saved),
                        'saved_plan_sha256': file_sha256(saved), 'project_id': PROJECT,
                        'baseline': query.inputs(self.canonical)}
        self.runtime = Runtime(initial_plan(), {
            'lineage': 'test-lineage', 'serial': 37,
            'resources': [{'mode': 'managed', 'type': 'digitalocean_droplet', 'name': 'existing',
                           'instances': [{'attributes': {'id': 'preserved-node'}}]}]})
        self.receipt = self.root / 'receipt.json'
        self.variables = self.root / 'production.tfvars'
        self.variables.write_text('existing production inputs')
        self.autoscale = self.root / 'autoscale.json'
        self.autoscale.write_text('{"desired_groups":2}')
        for name, value in [('PRODUCTION_VARIABLES', self.variables), ('AUTOSCALE_STATE', self.autoscale)]:
            patcher = patch.object(query, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.request.update(operator_variables_sha256=file_sha256(self.variables), desired_groups=2)
        self.runtime.document['variables'] = {'enhance_group_count': {'value': 2}}

    def prepare(self):
        return query.prepare(self.request, canonical=self.canonical, terraform=self.runtime)

    def provision(self, expected=None):
        expected = expected or durable.digest(self.prepare())
        with patch.object(query, 'environment', return_value={}):
            return query.provision(self.request, expected, self.receipt, canonical=self.canonical,
                                   terraform_factory=self.runtime.terraform, lock=Lock())

    def test_creation_preserves_existing_files_and_resource_identities(self):
        before = query.inputs(self.canonical)
        result = self.provision()
        self.assertEqual(result['phase'], 'committed')
        self.assertEqual(result['serial_after'], 38)
        self.assertTrue(self.runtime.applied)
        after = query.inputs(self.canonical)
        self.assertEqual({k: after[k] for k in before}, before)
        self.assertEqual((self.canonical / 'query-serving.auto.tfvars').read_text(), query.ENABLED)
        self.assertEqual((self.canonical.parent / 'query-serving/main.tf').read_text(), 'reviewed main.tf')
        self.assertIn(('init', '-input=false', '-lockfile=readonly'), self.runtime.calls)
        with self.assertRaisesRegex(ValueError, 'already exists'):
            self.provision(expected=durable.digest(result['plan']))

    def test_changed_inputs_plan_source_and_unrelated_actions_are_refused(self):
        original = (self.source / 'main.tf').read_text()
        for path in (self.source / 'main.tf', self.canonical / 'main.tf',
                     self.source / 'enhance/cloud-init.tftpl',
                     Path(self.request['saved_plan'])):
            value = path.read_bytes()
            path.write_bytes(b'changed')
            with self.subTest(path=path), self.assertRaises(ValueError):
                self.prepare()
            path.write_bytes(value)
        self.assertEqual((self.source / 'main.tf').read_text(), original)
        self.runtime.document['resource_changes'].append(
            {'mode': 'managed', 'address': 'existing.node', 'change': {'actions': ['delete']}})
        with self.assertRaisesRegex(ValueError, 'unrelated'):
            self.prepare()
        self.assertFalse(self.receipt.exists())
        self.assertFalse((self.canonical / 'query-serving.tf').exists())

    def test_legacy_string_counts_preserve_the_same_autoscaling_binding(self):
        self.runtime.document['variables']['enhance_group_count']['value'] = '2'
        self.prepare()
        for value in ('02', '2.0', '5', 0, True, None):
            self.runtime.document['variables']['enhance_group_count']['value'] = value
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'group count'):
                self.prepare()

    def test_changed_production_variables_and_autoscaling_are_refused(self):
        value = self.variables.read_bytes()
        self.variables.write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError, 'production variables'):
            self.prepare()
        self.variables.write_bytes(value)
        self.autoscale.write_text('{"desired_groups":3}')
        with self.assertRaisesRegex(ValueError, 'autoscaling'):
            self.prepare()
        self.autoscale.write_text('{"desired_groups":2}')
        self.runtime.document['variables']['enhance_group_count']['value'] = 3
        with self.assertRaisesRegex(ValueError, 'saved plan differs'):
            self.prepare()
        self.assertFalse(self.receipt.exists())

    def test_pinned_plan_digest_and_module_bytes_are_rechecked(self):
        expected = durable.digest(self.prepare())
        (self.source.parent / 'query-serving/main.tf').write_text('changed')
        with self.assertRaisesRegex(ValueError, 'plan changed'):
            self.provision(expected)
        self.assertFalse(self.receipt.exists())
        self.assertFalse(self.runtime.applied)

    def test_an_interrupted_apply_retains_source_and_a_reconciliation_receipt(self):
        self.runtime.fail_apply = True
        with self.assertRaisesRegex(RuntimeError, 'interruption'):
            self.provision()
        self.assertEqual(json.loads(self.receipt.read_text())['phase'], 'needs-reconcile')
        self.assertTrue((self.canonical / 'query-serving.tf').exists())
        self.assertTrue((self.canonical / 'query-serving.auto.tfvars').exists())

    def test_an_unexpected_existing_identity_change_does_not_report_success(self):
        self.runtime.change_existing_identity = True
        with self.assertRaisesRegex(ValueError, 'identities changed'):
            self.provision()
        self.assertEqual(json.loads(self.receipt.read_text())['phase'], 'needs-reconcile')

    def test_a_busy_production_lock_prevents_source_or_cloud_changes(self):
        class Busy(Lock):
            def __enter__(self):
                raise BlockingIOError('writer already holds production lock')
        expected = durable.digest(self.prepare())
        with self.assertRaises(BlockingIOError):
            query.provision(self.request, expected, self.receipt, canonical=self.canonical,
                            terraform_factory=self.runtime.terraform, lock=Busy())
        self.assertFalse(self.receipt.exists())
        self.assertFalse((self.canonical / 'query-serving.tf').exists())
        self.assertFalse(self.runtime.applied)

    def test_module_redirects_and_existing_destinations_are_refused(self):
        module = self.source.parent / 'query-serving/main.tf'
        module.unlink()
        module.symlink_to(self.source / 'main.tf')
        with self.assertRaisesRegex(ValueError, 'redirected'):
            self.prepare()
        module.unlink()
        module.write_text('reviewed main.tf')
        (self.canonical / 'query-serving.auto.tfvars').write_text('query_serving_enabled = false')
        self.request['baseline'] = query.inputs(self.canonical)
        with self.assertRaises(ValueError):
            self.prepare()


if __name__ == '__main__':
    unittest.main()

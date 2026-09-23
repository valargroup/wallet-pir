"""Provider/TF orchestration failures must preserve the durable apply fence."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

OPS = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('provision', OPS / 'scripts/provision.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
POLICY = {'region': 'ams3', 'vpc_id': '00000000-0000-4000-8000-000000000002',
          'project_id': '00000000-0000-4000-8000-000000000001', 'ssh_key_ids': ['12345'],
          'operator_ssh_cidrs': ['192.0.2.20/32'], 'coordinator_private_ipv4': '192.0.2.10',
          'state_lineage': 'fixture-lineage', 'account_uuid': 'fixture-account'}
INVENTORY = {'groups': [{'name': 'g1', 'replicas': [
    {'name': f'enhance-pir-v4-g01-r{i + 1}', 'url': f'http://10.0.0.{i + 1}:8291'} for i in range(2)]}]}
REQUEST = {'id': 'successor-5-pair-2', 'target_groups': 2, 'successor_ordinal': 5, 'registered': False}
HEALTH = {'protocol': module.journal_module.PROTOCOL, 'registered_groups': 1,
          'capacity': {'requested': REQUEST['id'], 'requests': {REQUEST['id']: REQUEST}}}


def state(count=2):
    return {'lineage': POLICY['state_lineage'], 'resources': [{
        'mode': 'managed', 'type': 'digitalocean_droplet', 'name': 'worker',
        'instances': [{'index_key': i, 'attributes': {'id': str(100 + i)}} for i in range(count)]}]}


def droplets(count=2):
    return [{'id': 100 + i, 'name': f'enhance-pir-v4-g{i // 2 + 1:02d}-r{i % 2 + 1}',
             'size_slug': 'c-4', 'region': {'slug': 'ams3'}, 'vpc_uuid': POLICY['vpc_id'], 'status': 'active',
             'tags': ['enhance-pir-v4-worker'], 'networks': {'v4': [{'type': 'private', 'ip_address': f'10.0.0.{i + 1}'}]}}
            for i in range(count)]


def plan(complete=False):
    value = json.loads((OPS / 'fixtures/infra-bootstrap.json').read_text())
    for resource in list(value['resource_changes']):
        if resource['type'] in ('digitalocean_droplet', 'digitalocean_project_resources'):
            extra = copy.deepcopy(resource)
            index = extra['index'] + 2
            extra.update(index=index, address=f"{extra['type']}.worker[{index}]")
            if extra['type'] == 'digitalocean_droplet':
                extra['change']['after']['name'] = f'enhance-pir-v4-g02-r{index % 2 + 1}'
            value['resource_changes'].append(extra)
    for resource in value['resource_changes']:
        index = resource.get('index', -1)
        if index < 2 or complete:
            resource['change']['actions'] = ['no-op']
            if resource['type'] == 'digitalocean_droplet':
                resource['change']['after']['id'] = str(100 + index)
            elif resource['type'] == 'digitalocean_project_resources':
                resource['change']['after']['resources'] = [f'do:droplet:{100 + index}']
    return value


class Provider:
    def __init__(self, terraform):
        self.terraform = terraform
        self.account = {'uuid': POLICY['account_uuid'], 'status': 'active'}
        self.project = {'id': POLICY['project_id'], 'owner_uuid': POLICY['account_uuid']}
        self.vpc = {'id': POLICY['vpc_id'], 'region': POLICY['region'], 'ip_range': '192.0.2.0/24'}

    def get(self, path):
        if path == '/v2/account':
            return {'account': self.account}
        if path == '/v2/projects/' + POLICY['project_id']:
            return {'project': self.project}
        assert path == '/v2/vpcs/' + POLICY['vpc_id']
        return {'vpc': self.vpc}

    def droplets(self):
        return droplets(self.terraform.count)


class Terraform:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.count = 2
        self.calls = []
        self.failure = None

    def verify(self):
        self.calls.append('verify')

    def state(self):
        return state(self.count)

    def plan(self, label):
        self.calls.append('plan')
        path = self.directory / (label + '.tfplan')
        path.write_bytes(b'mocked-saved-plan')
        return path, plan(self.count == 4)

    def apply(self, path, digest):
        self.calls.append('apply')
        if self.failure:
            self.count = self.failure
            raise RuntimeError('injected apply interruption')
        self.count = 4


class ProvisionTests(unittest.TestCase):
    def test_provisioned_only_after_verified_final_plan(self):
        with tempfile.TemporaryDirectory() as temp, module.journal_module.Journal(temp) as journal:
            journal.observe(HEALTH, INVENTORY, POLICY)
            tf = Terraform(temp)
            module.provision(journal, tf, Provider(tf), POLICY, INVENTORY)
            self.assertEqual(journal.state['operation']['phase'], 'provisioned')
            self.assertEqual(tf.calls.count('apply'), 1)
            evidence = json.loads((Path(temp) / 'reconciliation.json').read_text())
            self.assertEqual(journal.state['operation']['attempts'][0]['resolution'], module.journal_module.digest(evidence))
            module.provision(journal, tf, Provider(tf), POLICY, INVENTORY)
            self.assertEqual(tf.calls.count('apply'), 1)

    def test_restart_recovers_fully_created_fleet_without_second_apply(self):
        with tempfile.TemporaryDirectory() as temp:
            tf = Terraform(temp)
            tf.failure = 4
            with module.journal_module.Journal(temp) as journal:
                journal.observe(HEALTH, INVENTORY, POLICY)
                with self.assertRaises(RuntimeError):
                    module.provision(journal, tf, Provider(tf), POLICY, INVENTORY)
                self.assertEqual(journal.state['operation']['phase'], 'applying')
            with module.journal_module.Journal(temp) as journal:
                module.provision(journal, tf, Provider(tf), POLICY, INVENTORY)
                self.assertEqual(journal.state['operation']['phase'], 'provisioned')
            self.assertEqual(tf.calls.count('apply'), 1)

    def test_partial_interruption_keeps_fence_and_records_identity(self):
        with tempfile.TemporaryDirectory() as temp:
            tf = Terraform(temp)
            tf.failure = 3
            with module.journal_module.Journal(temp) as journal:
                journal.observe(HEALTH, INVENTORY, POLICY)
                with self.assertRaises(RuntimeError):
                    module.provision(journal, tf, Provider(tf), POLICY, INVENTORY)
            with module.journal_module.Journal(temp) as journal:
                with self.assertRaisesRegex(ValueError, 'partial apply'):
                    module.provision(journal, tf, Provider(tf), POLICY, INVENTORY)
                self.assertEqual(journal.state['operation']['phase'], 'applying')
                self.assertEqual(journal.state['operation']['resources']['digitalocean_droplet.worker[2]'], '102')
            self.assertEqual(tf.calls.count('apply'), 1)

    def test_interrupted_membership_only_becomes_retryable_without_apply(self):
        with tempfile.TemporaryDirectory() as temp, module.journal_module.Journal(temp) as journal:
            journal.observe(HEALTH, INVENTORY, POLICY)
            journal.record_resources(module.state_workers(state()))
            journal.start_apply('a' * 64)
            tf = Terraform(temp)
            tf.count = 4
            remaining = plan(True)
            for resource in remaining['resource_changes']:
                if resource['type'] == 'digitalocean_project_resources' and resource['index'] >= 2:
                    resource['change']['actions'] = ['create']
            with patch.object(tf, 'plan', return_value=(Path(temp) / 'unused.tfplan', remaining)):
                module.provision(journal, tf, Provider(tf), POLICY, INVENTORY)
            self.assertEqual(journal.state['operation']['phase'], 'retryable')
            self.assertNotIn('apply', tf.calls)
            self.assertEqual(len(journal.state['operation']['resources']), 4)
            self.assertIsNotNone(journal.state['operation']['attempts'][0]['resolution'])
            # Drift after reconciliation must not widen the authorized retry.
            drift = plan(True)
            for resource in drift['resource_changes']:
                if resource['type'] == 'digitalocean_firewall':
                    resource['change']['actions'] = ['create']
            with patch.object(tf, 'plan', return_value=(Path(temp) / 'unused.tfplan', drift)):
                with self.assertRaises(ValueError):
                    module.provision(journal, tf, Provider(tf), POLICY, INVENTORY)
            self.assertEqual(journal.state['operation']['phase'], 'retryable')
            self.assertNotIn('apply', tf.calls)
            # A fresh invocation verifies the live bindings again before applying.
            module.provision(journal, tf, Provider(tf), POLICY, INVENTORY)
            self.assertEqual(journal.state['operation']['phase'], 'provisioned')
            self.assertEqual(tf.calls.count('apply'), 1)

    def test_unfinished_firewall_keeps_interrupted_apply_fenced(self):
        with tempfile.TemporaryDirectory() as temp, module.journal_module.Journal(temp) as journal:
            journal.observe(HEALTH, INVENTORY, POLICY)
            journal.record_resources(module.state_workers(state()))
            journal.start_apply('a' * 64)
            tf = Terraform(temp)
            tf.count = 4
            remaining = plan(True)
            for resource in remaining['resource_changes']:
                if resource['type'] == 'digitalocean_firewall':
                    resource['change']['actions'] = ['create']
            with patch.object(tf, 'plan', return_value=(Path(temp) / 'unused.tfplan', remaining)):
                with self.assertRaises(ValueError):
                    module.provision(journal, tf, Provider(tf), POLICY, INVENTORY)
            self.assertEqual(journal.state['operation']['phase'], 'applying')
            self.assertNotIn('apply', tf.calls)

    def test_wrong_account_prevents_plan(self):
        with tempfile.TemporaryDirectory() as temp, module.journal_module.Journal(temp) as journal:
            journal.observe(HEALTH, INVENTORY, POLICY)
            tf = Terraform(temp)
            provider = Provider(tf)
            provider.account['uuid'] = 'another-account'
            with self.assertRaises(ValueError):
                module.provision(journal, tf, provider, POLICY, INVENTORY)
            self.assertNotIn('plan', tf.calls)

    def test_orphan_duplicate_profile_and_inventory_mismatch_rejected(self):
        operation = {'before': 1, 'target_groups': 2, 'resources': {}}
        cases = [droplets(3), droplets(2) + [droplets(2)[0]]]
        for field, value in [('size_slug', 's-1vcpu-1gb'), ('vpc_uuid', 'wrong'), ('status', 'new'), ('id', 999)]:
            changed = droplets()
            changed[0][field] = value
            cases.append(changed)
        changed = droplets()
        changed[0]['networks']['v4'][0]['ip_address'] = '10.0.0.99'
        cases.append(changed)
        for items in cases:
            with self.subTest(items=items), self.assertRaises(ValueError):
                module.reconcile_workers(state(), items, POLICY, operation, INVENTORY)

    def test_foreign_lineage_tainted_state_and_replaced_journal_identity(self):
        operation = {'before': 1, 'target_groups': 2, 'resources': {}}
        wrong = state()
        wrong['lineage'] = 'other'
        with self.assertRaises(ValueError):
            module.reconcile_workers(wrong, droplets(), POLICY, operation, INVENTORY)
        wrong = state()
        wrong['resources'][0]['instances'][0]['status'] = 'tainted'
        with self.assertRaises(ValueError):
            module.state_workers(wrong)
        operation['resources'] = {'digitalocean_droplet.worker[0]': '999'}
        with self.assertRaises(ValueError):
            module.reconcile_workers(state(), droplets(), POLICY, operation, INVENTORY)

    def test_unsafe_plan_never_records_apply_intent(self):
        with tempfile.TemporaryDirectory() as temp, module.journal_module.Journal(temp) as journal:
            journal.observe(HEALTH, INVENTORY, POLICY)
            tf = Terraform(temp)
            unsafe = plan()
            unsafe['resource_changes'][0]['change']['actions'] = ['delete', 'create']
            with patch.object(tf, 'plan', return_value=(Path(temp) / 'unused', unsafe)), self.assertRaises(ValueError):
                module.provision(journal, tf, Provider(tf), POLICY, INVENTORY)
            self.assertEqual(journal.state['operation']['phase'], 'requested')
            self.assertNotIn('apply', tf.calls)

    def test_pagination_must_stay_on_provider_origin(self):
        provider = module.DigitalOcean('fixture-token')
        with patch.object(provider, 'get', return_value={'droplets': [], 'links': {'pages': {'next': 'https://elsewhere.invalid/v2/droplets'}}}):
            with self.assertRaises(ValueError):
                provider.droplets()

    def test_pagination_and_duplicate_detection(self):
        provider = module.DigitalOcean('fixture-token')
        pages = [{'droplets': droplets(1), 'links': {'pages': {'next': 'https://api.digitalocean.com/v2/droplets?page=2'}}},
                 {'droplets': droplets(2)[1:]}]
        with patch.object(provider, 'get', side_effect=pages):
            self.assertEqual(len(provider.droplets()), 2)
        pages[1]['droplets'] = droplets(1)
        with patch.object(provider, 'get', side_effect=pages), self.assertRaises(ValueError):
            provider.droplets()

    def test_module_rejects_extra_inputs_and_symlink(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name in module.MODULE_FILES:
                (root / name).write_text('fixture')
            digest = module.module_digest(root)
            self.assertEqual(len(digest), 64)
            (root / 'override.tf').write_text('')
            with self.assertRaises(ValueError):
                module.module_digest(root)
            (root / 'override.tf').unlink()
            (root / 'main.tf').unlink()
            (root / 'main.tf').symlink_to(root / 'variables.tf')
            with self.assertRaises(ValueError):
                module.module_digest(root)

    def test_saved_plan_tamper_blocks_subprocess_apply(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)
            terraform = module.Terraform(path, path, POLICY, 2, 'fixture-token')
            plan_path = path / 'saved.tfplan'
            plan_path.write_bytes(b'changed-after-review')
            with patch.object(terraform, 'verify'), patch.object(terraform, 'run') as run:
                with self.assertRaisesRegex(ValueError, 'saved plan changed'):
                    terraform.apply(plan_path, 'a' * 64)
                run.assert_not_called()

    def test_runtime_overrides_are_removed_and_token_stays_out_of_inputs(self):
        with tempfile.TemporaryDirectory() as temp, patch.dict(module.os.environ, {
                'TF_CLI_ARGS_apply': '-lock=false', 'TF_WORKSPACE': 'production',
                'DIGITALOCEAN_API_URL': 'https://elsewhere.invalid'}, clear=True):
            terraform = module.Terraform(Path(temp), Path(temp), POLICY, 2, 'fixture-token')
            for key in ('TF_CLI_ARGS_apply', 'TF_WORKSPACE', 'DIGITALOCEAN_API_URL'):
                self.assertNotIn(key, terraform.env)
            self.assertEqual(terraform.env['TF_VAR_digitalocean_token'], 'fixture-token')
            self.assertNotIn('fixture-token', terraform.variables.read_text())
            self.assertEqual(terraform.variables.stat().st_mode & 0o777, 0o600)

    def test_backend_identity_and_locking_are_required(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'root'
            work = Path(temp) / 'operation'
            root.mkdir()
            work.mkdir()
            for name in module.MODULE_FILES:
                (root / name).write_text('fixture')
            policy = dict(POLICY, module_sha256=module.module_digest(root),
                          backend={'bucket': 'fixture', 'key': 'isolated-v4', 'region': 'fixture-region'})
            metadata = root / '.terraform/terraform.tfstate'
            metadata.parent.mkdir()
            backend = {'type': 's3', 'config': dict(policy['backend'], use_lockfile=True)}
            metadata.write_text(json.dumps({'backend': backend}))
            terraform = module.Terraform(root, work, policy, 2, 'fixture-token')
            with patch.object(terraform, 'run', return_value=b'default\n'):
                terraform.verify()
                backend['config']['use_lockfile'] = False
                metadata.write_text(json.dumps({'backend': backend}))
                with self.assertRaisesRegex(ValueError, 'state locking'):
                    terraform.verify()
                backend['config'].update(use_lockfile=True, key='legacy-production')
                metadata.write_text(json.dumps({'backend': backend}))
                with self.assertRaisesRegex(ValueError, 'backend differs'):
                    terraform.verify()

    def test_wrong_project_or_vpc_prevents_plan(self):
        for field, value in [('owner_uuid', 'wrong-account'), ('region', 'nyc1'), ('ip_range', '10.0.0.0/24')]:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temp, module.journal_module.Journal(temp) as journal:
                journal.observe(HEALTH, INVENTORY, POLICY)
                tf = Terraform(temp)
                provider = Provider(tf)
                if field == 'owner_uuid':
                    provider.project[field] = value
                else:
                    provider.vpc[field] = value
                with self.assertRaises(ValueError):
                    module.provision(journal, tf, provider, POLICY, INVENTORY)
                self.assertNotIn('plan', tf.calls)


class StateLockTests(unittest.TestCase):
    def test_spaces_cannot_claim_native_s3_locking(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name in module.MODULE_FILES:
                (root / name).write_text('fixture')
            policy = dict(POLICY, module_sha256=module.module_digest(root), backend={
                'bucket': 'fixture', 'key': 'isolated-v4', 'region': 'us-east-1',
                'endpoints': {'s3': 'https://ams3.digitaloceanspaces.com'}})
            metadata = root / '.terraform/terraform.tfstate'
            metadata.parent.mkdir()
            metadata.write_text(json.dumps({'backend': {'type': 's3', 'config': dict(policy['backend'], use_lockfile=True)}}))
            tf = module.Terraform(root, root, policy, 2, 'fixture')
            with self.assertRaisesRegex(ValueError, 'Spaces requires'):
                tf.verify()
            policy['state_lock'] = {'type': 'pinned_host', 'machine_id': 'a' * 32}
            metadata.write_text(json.dumps({'backend': {'type': 's3', 'config': dict(policy['backend'], use_lockfile=False)}}))
            with self.assertRaisesRegex(ValueError, 'not held'):
                tf.verify()
            with patch.object(module.subprocess, 'run') as run:
                with self.assertRaisesRegex(ValueError, 'not held'):
                    tf.run(['apply', 'fixture.tfplan'])
                run.assert_not_called()

    @unittest.skipUnless(module.os.geteuid() == 0, 'real root-owned lock contract runs on Linux/root')
    def test_real_lock_contention_inheritance_and_replacement(self):
        import subprocess
        import sys
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            identity = root / 'machine-id'
            identity.write_text('a' * 32)
            policy = dict(POLICY, state_lock={'type': 'pinned_host', 'machine_id': 'a' * 32})
            with patch.object(module.StateLock, 'PATH', root / 'writer.lock'), patch.object(module.StateLock, 'MACHINE_ID', identity):
                with module.StateLock(policy) as held:
                    with self.assertRaises(BlockingIOError):
                        with module.StateLock(policy):
                            pass
                    # Child retains the descriptor after the controller closes it.
                    child = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.read()'],
                                             stdin=subprocess.PIPE, pass_fds=(held.fd,))
                try:
                    with self.assertRaises(BlockingIOError):
                        with module.StateLock(policy):
                            pass
                finally:
                    child.communicate(timeout=10)
                with module.StateLock(policy) as held:
                    tf = module.Terraform(root, root, policy, 2, 'fixture', held)
                    with patch.object(module.subprocess, 'run', return_value=type('Result', (), {'returncode': 0, 'stdout': b'ok'})()) as run:
                        self.assertEqual(tf.run(['workspace', 'show']), b'ok')
                        self.assertEqual(run.call_args.kwargs['pass_fds'], (held.fd,))
                    module.StateLock.PATH.unlink()
                    module.StateLock.PATH.touch(mode=0o600)
                    with self.assertRaisesRegex(ValueError, 'replaced'):
                        held.verify()
                wrong = dict(policy, state_lock={'type': 'pinned_host', 'machine_id': 'b' * 32})
                with self.assertRaisesRegex(ValueError, 'pinned host'):
                    with module.StateLock(wrong):
                        pass
                module.StateLock.PATH.unlink()
                module.StateLock.PATH.symlink_to(identity)
                with self.assertRaises(OSError):
                    with module.StateLock(policy):
                        pass

"""Pair bootstrap resumes per-host installation without treating it as qualification."""
import copy
import importlib.util
import json
from pathlib import Path
import shlex
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

OPS = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


module = load('v4_pair', OPS / 'scripts/v4-bootstrap-pair.py')
fixtures = load('bootstrap_fixtures', OPS / 'tests/test_v4_bootstrap.py')
PAIR = [{'address': f'digitalocean_droplet.worker[{i + 2}]', 'resource_id': str(102 + i),
         'name': f'enhance-pir-v4-g02-r{i + 1}', 'private_ipv4': f'10.0.0.{i + 3}'} for i in range(2)]


def prepare(journal, directory):
    bundle = directory / 'bundle'
    digest = fixtures.bundle_at(bundle)
    config = {'revision': fixtures.SHA, 'manifest_sha256': digest, 'known_hosts_sha256': 'd' * 64,
              'limits': {t['name']: copy.deepcopy(fixtures.LIMITS) for t in PAIR}}
    journal.state['operation'] = {'id': 'successor-5-pair-2', 'phase': 'provisioned', 'before': 1, 'target_groups': 2,
                                 'resources': {f'digitalocean_droplet.worker[{i}]': str(100 + i) for i in range(4)}}
    journal.save()
    return bundle, config


def receipt(target, bundle, config):
    identity = module.installer.verify_bundle(bundle, config['revision'], config['manifest_sha256'])
    limits = config['limits'][target['name']]
    return {**identity, 'worker_name': target['name'], 'private_ipv4': target['private_ipv4'],
            'phase': 'bootstrapped', 'qualification': 'unqualified', 'limits': limits,
            'placement_policy': {'sealed_shards': config.get('sealed_shards', 6)},
            'unit_sha256': module.installer.sha256(module.installer.unit(identity['binary_sha256'], target['private_ipv4'], limits, config.get('sealed_shards', 6))),
            'host': dict(fixtures.FACTS, hostname=target['name']),
            'health': dict(fixtures.HEALTH, placement_policy={'sealed_shards': config.get('sealed_shards', 6)}, incarnation='process-' + target['resource_id']),
            'runtime': {'main_pid': 123, 'cgroup': '/system.slice/' + module.installer.SERVICE}}


class FakeRemote:
    def __init__(self):
        self.calls = []
        self.fail = None

    def bootstrap(self, target, bundle, config):
        self.calls.append(target['resource_id'])
        if target['resource_id'] == self.fail:
            raise RuntimeError('injected SSH interruption after first worker')
        return receipt(target, bundle, config)


class PairBootstrapTests(unittest.TestCase):
    def test_seven_shard_receipt_binds_unit_and_worker_health(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            # The fixture supplies the same frozen artifact used by the installer.
            bundle = root / 'bundle'
            digest = fixtures.bundle_at(bundle)
            config = {'revision': fixtures.SHA, 'manifest_sha256': digest, 'sealed_shards': 7,
                      'limits': {t['name']: copy.deepcopy(fixtures.LIMITS) for t in PAIR}}
            value = receipt(PAIR[0], bundle, config)
            identity = module.installer.verify_bundle(bundle, config['revision'], digest)
            module.validate_receipt(value, PAIR[0], config, identity)
            value['health']['placement_policy']['sealed_shards'] = 6
            with self.assertRaises(ValueError):
                module.validate_receipt(value, PAIR[0], config, identity)

    def test_partial_pair_survives_restart_and_remains_unqualified(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            remote = FakeRemote()
            remote.fail = '103'
            with module.journal_module.Journal(directory / 'journal') as journal:
                bundle, config = prepare(journal, directory)
                with self.assertRaises(RuntimeError):
                    module.bootstrap_pair(journal, PAIR, config, bundle, lambda _: remote)
                self.assertEqual(journal.state['operation']['phase'], 'bootstrapping')
            with module.journal_module.Journal(directory / 'journal') as journal:
                self.assertEqual(set(journal.state['operation']['bootstrap']['receipts']), {PAIR[0]['address']})
                remote.fail = None
                module.bootstrap_pair(journal, PAIR, config, bundle, lambda _: remote)
                operation = journal.state['operation']
                self.assertEqual(operation['phase'], 'bootstrapped')
                self.assertEqual(len(operation['bootstrap']['receipts']), 2)
                self.assertTrue(all(r['qualification'] == 'unqualified' for r in operation['bootstrap']['receipts'].values()))
                self.assertNotIn('registered', operation)
            self.assertEqual(remote.calls, ['102', '103', '102', '103'])

    def test_changed_configuration_or_target_rejected_before_remote_work(self):
        with tempfile.TemporaryDirectory() as temp, module.journal_module.Journal(Path(temp) / 'journal') as journal:
            bundle, config = prepare(journal, Path(temp))
            remote = FakeRemote()
            remote.fail = '103'
            with self.assertRaises(RuntimeError):
                module.bootstrap_pair(journal, PAIR, config, bundle, lambda _: remote)
            previous = remote.calls.copy()
            changed = copy.deepcopy(config)
            changed['known_hosts_sha256'] = 'e' * 64
            with self.assertRaises(ValueError):
                module.bootstrap_pair(journal, PAIR, changed, bundle, lambda _: remote)
            targets = copy.deepcopy(PAIR)
            targets[0]['resource_id'] = '999'
            with self.assertRaises(ValueError):
                module.bootstrap_pair(journal, targets, config, bundle, lambda _: remote)
            self.assertEqual(remote.calls, previous)

    def test_receipt_must_bind_artifact_limits_and_fresh_process(self):
        with tempfile.TemporaryDirectory() as temp, module.journal_module.Journal(Path(temp) / 'journal') as journal:
            bundle, config = prepare(journal, Path(temp))
            original = receipt(PAIR[0], bundle, config)
            identity = module.installer.verify_bundle(bundle, config['revision'], config['manifest_sha256'])
            mutations = [lambda r: r.update(qualification='passed'), lambda r: r.update(binary_sha256='f' * 64),
                         lambda r: r.update(private_ipv4='10.0.0.99'), lambda r: r['health'].update(epoch=1),
                         lambda r: r['runtime'].update(cgroup='/another/service'), lambda r: r['host'].update(cpus=8)]
            for mutate in mutations:
                changed = copy.deepcopy(original)
                mutate(changed)
                with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                    module.validate_receipt(changed, PAIR[0], config, identity)

    def test_same_process_on_both_replicas_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp, module.journal_module.Journal(Path(temp) / 'journal') as journal:
            bundle, config = prepare(journal, Path(temp))
            def identical(target, bundle, config):
                result = receipt(target, bundle, config)
                result['health']['incarnation'] = 'same-process'
                return result
            with self.assertRaises(ValueError):
                module.bootstrap_pair(journal, PAIR, config, bundle, lambda _: SimpleNamespace(bootstrap=identical))
            self.assertEqual(journal.state['operation']['phase'], 'bootstrapping')
            self.assertEqual(len(journal.state['operation']['bootstrap']['receipts']), 1)

    def test_live_target_selection_uses_recorded_ids_and_distinct_private_addresses(self):
        operation = {'before': 1, 'target_groups': 2, 'resources': {p['address']: p['resource_id'] for p in PAIR}}
        observed = {'workers': operation['resources'].copy(), 'droplets': [
            {'id': int(p['resource_id']), 'name': p['name'], 'networks': {'v4': [{'type': 'private', 'ip_address': p['private_ipv4']}]}}
            for p in PAIR]}
        self.assertEqual(module.targets(operation, observed), PAIR)
        observed['droplets'][1]['networks']['v4'][0]['ip_address'] = PAIR[0]['private_ipv4']
        with self.assertRaises(ValueError):
            module.targets(operation, observed)

    def test_ssh_pins_host_keys_and_quotes_remote_arguments(self):
        with tempfile.TemporaryDirectory() as temp:
            known = Path(temp) / 'known_hosts'
            known.write_text('fixture-host-key')
            digest = module.installer.sha256(known.read_bytes())
            remote = module.Remote(PAIR[0], Path(temp) / 'key with spaces', known, digest)
            arguments = ['printf', '%s', 'literal;$(must-not-run)']
            with patch.object(module.subprocess, 'run', return_value=SimpleNamespace(returncode=0, stdout=b'ok')) as run:
                self.assertEqual(remote.command(arguments), b'ok')
                call = run.call_args.args[0]
                self.assertEqual(shlex.split(call[-1]), arguments)
                self.assertIn('StrictHostKeyChecking=yes', call)
                self.assertIn('ForwardAgent=no', call)
            known.write_text('changed-host-key')
            with self.assertRaises(ValueError):
                remote.command(arguments)
            with self.assertRaises(ValueError):
                module.Remote(PAIR[0], Path(temp) / 'key', known, digest)

    def test_transfer_must_be_verified_before_executing_bundle_code(self):
        with tempfile.TemporaryDirectory() as temp:
            known = Path(temp) / 'known_hosts'
            known.write_text('fixture-host-key')
            remote = module.Remote(PAIR[0], Path(temp) / 'key', known, module.installer.sha256(known.read_bytes()))
            with patch.object(remote, 'copy'), patch.object(remote, 'command', return_value=b'wrong-digest') as commands:
                with self.assertRaises(ValueError):
                    remote.bootstrap(PAIR[0], Path(temp), {'manifest_sha256': 'a' * 64})
                self.assertFalse(any(call.args[0][0] == 'python3' for call in commands.call_args_list))

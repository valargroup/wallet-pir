#!/usr/bin/env python3
"""Run one live v4 expansion in a disposable synthetic coordinator environment.

This is a test driver, not a production capacity controller or qualification
receipt. It joins the existing demand, provisioning and bootstrap adapters with
the deliberately separate inventory-registration step. All inputs and evidence
stay on the isolated coordinator; credentials are runtime environment variables.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.parse
import urllib.request


def sibling(name):
    path = Path(__file__).with_name(name + '.py')
    spec = importlib.util.spec_from_file_location(name.replace('-', '_'), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


provisioning = sibling('v4-provision')
bootstrap = sibling('v4-bootstrap-pair')
journal_module = provisioning.journal_module


def health(origin):
    with urllib.request.urlopen(origin.rstrip('/') + '/v1/health', timeout=15) as response:
        data = response.read(1024 * 1024 + 1)
    if len(data) > 1024 * 1024:
        raise ValueError('oversized coordinator health response')
    return json.loads(data)


def run_adapter(name, args):
    result = subprocess.run(['python3', str(Path(__file__).with_name(name + '.py')), *args],
                            capture_output=True, timeout=3600)
    if result.returncode:
        # Provider and Terraform stderr can include private operation data.
        raise RuntimeError(name + ' stopped; inspect its private journal and provider state')


def require_expected(operation, operation_id, target_groups):
    if (operation.get('id') != operation_id
            or operation.get('target_groups') != target_groups):
        raise ValueError('different expansion requested; this test is pinned to one pair')


def completed_matches(completed, inventory, current, operation_id, target_groups):
    return (completed is not None and completed.get('id') == operation_id
            and completed.get('phase') == 'registered'
            and completed.get('target_groups') == target_groups
            and completed.get('registered_inventory_digest') == journal_module.digest(inventory)
            and current.get('registered_groups') == target_groups
            and current.get('capacity', {}).get('requests', {}).get(operation_id, {}).get('registered') is True)


def registration(journal, terraform_dir, policy, inventory_path, bootstrap_policy,
                 bundle, token, origin):
    operation = journal.state['operation']
    if operation is None or operation['phase'] != 'bootstrapped':
        raise ValueError('registration requires a fully bootstrapped pair')
    before = operation['before']
    existing = json.loads(inventory_path.read_text())
    old_inventory = {'groups': existing['groups'][:before]}
    if journal_module.digest(old_inventory) != operation['inventory_digest']:
        raise ValueError('established inventory differs from the expansion request')
    if len(existing['groups']) not in (before, before + 1) or before + 1 != operation['target_groups']:
        raise ValueError('unexpected registration target')
    config = json.loads(bootstrap_policy.read_text())
    identity = bootstrap.installer.verify_bundle(bundle, config['revision'], config['manifest_sha256'])
    with provisioning.StateLock(policy) as state_lock:
        terraform = provisioning.Terraform(terraform_dir, journal.directory / operation['id'],
                                           policy, operation['target_groups'], token, state_lock)
        observed = provisioning.inspect_fleet(terraform, provisioning.DigitalOcean(token),
                                              policy, operation, old_inventory)
        pair = bootstrap.targets(operation, observed)
        replicas = []
        for target in pair:
            receipt = operation['bootstrap']['receipts'][target['address']]
            bootstrap.validate_receipt(receipt, target, config, identity)
            replicas.append({'name': target['name'],
                             'url': 'http://' + target['private_ipv4'] + ':8291'})
        current = health(origin)
        registered = current.get('registered_groups') == operation['target_groups']
        request = current.get('capacity', {}).get('requests', {}).get(operation['id'], {})
        if (current.get('protocol') != journal_module.PROTOCOL
                or current.get('registered_groups') not in (before, operation['target_groups'])
                or (registered and request.get('registered') is not True)
                or (not registered and current.get('capacity', {}).get('requested') != operation['id'])):
            raise ValueError('coordinator demand changed before registration')
        added = {'name': 'group-' + str(before + 1), 'replicas': replicas}
        if any(group['name'] == added['name'] for group in old_inventory['groups']):
            raise ValueError('duplicate group name')
        updated = {'groups': [*old_inventory['groups'], added]}
        if len(existing['groups']) == before:
            if registered:
                raise ValueError('coordinator registered capacity absent from inventory')
            journal_module.atomic(inventory_path, updated)
        elif existing != updated:
            raise ValueError('pending inventory differs from the verified pair')
        return updated


def wait_registered(origin, operation, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = health(origin)
        request = value.get('capacity', {}).get('requests', {}).get(operation['id'], {})
        if value.get('registered_groups') == operation['target_groups'] and request.get('registered') is True:
            return value
        time.sleep(2)
    raise RuntimeError('coordinator did not register the provisioned pair')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('state-dir', 'terraform-dir', 'policy', 'inventory', 'bootstrap-policy',
                 'bundle', 'ssh-key', 'known-hosts', 'fixture-marker'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--coordinator-url', default='http://127.0.0.1:8280')
    parser.add_argument('--stage', choices=('provision', 'complete'), default='complete')
    parser.add_argument('--expected-operation-id', required=True)
    parser.add_argument('--expected-target-groups', type=int, required=True)
    parser.add_argument('--token-env', default='DIGITALOCEAN_ACCESS_TOKEN')
    parser.add_argument('--acknowledge-unqualified-test', action='store_true', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    if (args.expected_target_groups not in (2, 3, 4)
            or not re.fullmatch(r'successor-[0-9]+-pair-' + str(args.expected_target_groups),
                                args.expected_operation_id)):
        parser.error('pin one valid successor request and target group count')
    endpoint = urllib.parse.urlsplit(args.coordinator_url)
    if endpoint.scheme != 'http' or endpoint.hostname not in ('127.0.0.1', '::1') or endpoint.username or endpoint.password:
        parser.error('test coordinator URL must be loopback HTTP')
    if args.fixture_marker.read_text().strip() != 'synthetic-fixture':
        parser.error('test-only registration requires a synthetic coordinator data directory')
    token = os.environ.get(args.token_env)
    if not token:
        parser.error('runtime DigitalOcean token is missing')
    policy = json.loads(args.policy.read_text())
    common = ['--state-dir', str(args.state_dir), '--terraform-dir', str(args.terraform_dir),
              '--policy', str(args.policy), '--inventory', str(args.inventory),
              '--token-env', args.token_env]
    with journal_module.Journal(args.state_dir) as journal:
        current = health(args.coordinator_url)
        inventory = json.loads(args.inventory.read_text())
        completed = journal.state['completed'].get(args.expected_operation_id)
        if completed_matches(completed, inventory, current,
                             args.expected_operation_id, args.expected_target_groups):
            print(json.dumps({'operation': completed['id'], 'phase': 'registered',
                              'groups': completed['target_groups'],
                              'qualification': 'unqualified'}, sort_keys=True))
            return
        operation = journal.state['operation']
        if operation is None:
            operation = journal.observe(current, inventory, policy)
        if operation is None:
            raise SystemExit('coordinator has not requested expansion')
        require_expected(operation, args.expected_operation_id, args.expected_target_groups)
        operation_id = operation['id']
    # Each adapter owns both locks for its entire operation. A rerun reconciles
    # their journals before doing anything to an existing resource.
    run_adapter('v4-provision', common)
    if args.stage == 'provision':
        print(json.dumps({'operation': operation_id, 'phase': 'provisioned',
                          'qualification': 'unqualified'}, sort_keys=True))
        return
    run_adapter('v4-bootstrap-pair', [*common, '--bootstrap-policy', str(args.bootstrap_policy),
                '--bundle', str(args.bundle), '--ssh-key', str(args.ssh_key),
                '--known-hosts', str(args.known_hosts)])
    with journal_module.Journal(args.state_dir) as journal:
        operation = journal.state['operation']
        if operation is None or operation['id'] != operation_id:
            raise ValueError('expansion identity changed')
        registration(journal, args.terraform_dir, policy, args.inventory,
                     args.bootstrap_policy, args.bundle, token, args.coordinator_url)
        current = wait_registered(args.coordinator_url, operation)
        result = {'operation': operation_id, 'phase': 'registered',
                  'groups': current['registered_groups'],
                  'inventory_sha256': hashlib.sha256(args.inventory.read_bytes()).hexdigest(),
                  'qualification': 'unqualified'}
        journal.finish_registration(current, json.loads(args.inventory.read_text()))
        print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Durable, serialized expansion demand and infrastructure identities.

The CLI consumes live or captured health and a local inventory, persisting one
operation. It does not provision or register workers. The deployment adapter must
hold this journal's lock across provider operations and use the apply fence.
"""
import argparse
import copy
import fcntl
import json
import os
from pathlib import Path
import re
import sys
import urllib.request

# The shared primitives live in the same checkout; this script is never shipped alone.
LIB = str(Path(__file__).resolve().parents[3] / 'ops/lib')
if LIB not in sys.path:
    sys.path.insert(0, LIB)
from wallet_pir_ops import durable  # noqa: E402

PROTOCOL = 'ironwood-enhance-pir-v7'

# Byte-compatible with journals, digests and evidence written before the extraction.
digest = durable.digest


def atomic(path, value):
    durable.atomic_json(path, value, prefix='.journal-')


def inventory_count(inventory):
    groups = inventory['groups']
    if not isinstance(groups, list) or not 1 <= len(groups) <= 4:
        raise ValueError('invalid inventory')
    names, replicas, urls = set(), set(), set()
    for group in groups:
        if set(group) != {'name', 'replicas'} or not isinstance(group['name'], str) or not group['name'] or group['name'] in names:
            raise ValueError('invalid group identity')
        names.add(group['name'])
        if len(group['replicas']) != 2:
            raise ValueError('a group requires a replica pair')
        for replica in group['replicas']:
            if set(replica) != {'name', 'url'}:
                raise ValueError('invalid replica identity')
            name, url = replica['name'], replica['url']
            if not isinstance(name, str) or not name or name in replicas or not isinstance(url, str) or not url or url.rstrip('/') in urls:
                raise ValueError('replica identities must be distinct')
            replicas.add(name)
            urls.add(url.rstrip('/'))
    return len(groups)


class Journal:
    """One lock spans observation, external work and durable acknowledgement.

    An interrupted apply is deliberately ambiguous: a provider may have created
    a Droplet before Terraform saved its ID. Recovery must resolve this from both
    remote state and provider inventory before permitting another plan/apply.
    """
    def __init__(self, directory):
        self.directory = Path(directory)
        self.lock = None
        self.state = None

    def __enter__(self):
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.lock = os.open(self.directory / 'journal.lock', os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.path = self.directory / 'journal.json'
            self.state = json.loads(self.path.read_text()) if self.path.exists() else {
                'version': 1, 'operation': None, 'completed': {}}
            if self.state.get('version') != 1:
                raise ValueError('unsupported expansion journal')
            return self
        except BaseException:
            os.close(self.lock)
            self.lock = None
            raise

    def __exit__(self, *_):
        if self.lock is not None:
            os.close(self.lock)
            self.lock = None

    def save(self):
        if self.lock is None:
            raise RuntimeError('expansion journal is not locked')
        atomic(self.path, self.state)

    def observe(self, health, inventory, policy):
        """Adopt one coordinator request; reorgs cannot replace pending demand."""
        count = inventory_count(inventory)
        if health.get('protocol') != PROTOCOL or type(health.get('registered_groups')) is not int or health['registered_groups'] != count:
            raise ValueError('coordinator and inventory disagree')
        operation = self.state['operation']
        if operation is not None:
            if operation['inventory_digest'] != digest(inventory) or operation['policy_digest'] != digest(policy):
                raise ValueError('pending operation inputs changed')
            return copy.deepcopy(operation)
        capacity = health['capacity']
        request_id = capacity.get('requested')
        if request_id is None:
            return None
        request = capacity['requests'][request_id]
        target = request.get('target_groups')
        ordinal = request.get('successor_ordinal')
        if (type(target) is not int or target != count + 1 or target > 4
                or type(ordinal) is not int or ordinal < 0
                or request_id != f'successor-{ordinal}-pair-{target}'
                or request.get('id') != request_id or request.get('registered') is not False):
            raise ValueError('invalid expansion request')
        if request_id in self.state['completed']:
            raise ValueError('completed expansion request reappeared')
        operation = {
            'id': request_id, 'request': copy.deepcopy(request), 'before': count,
            'target_groups': target, 'inventory_digest': digest(inventory),
            'policy_digest': digest(policy), 'phase': 'requested', 'resources': {},
            'attempts': [],
        }
        self.state['operation'] = operation
        self.save()
        return copy.deepcopy(operation)

    def record_resources(self, resources):
        """Merge trusted provider/state identities, preserving every recorded ID."""
        operation = self.state['operation']
        if operation is None:
            raise ValueError('no expansion operation')
        allowed = {f'digitalocean_droplet.worker[{i}]' for i in range(operation['target_groups'] * 2)}
        merged = dict(operation['resources'])
        for address, resource_id in resources.items():
            if address not in allowed or not re.fullmatch('[1-9][0-9]*', str(resource_id)):
                raise ValueError('unexpected infrastructure identity')
            resource_id = str(resource_id)
            if address in merged and merged[address] != resource_id:
                raise ValueError('recorded infrastructure identity changed')
            merged[address] = resource_id
        if len(set(merged.values())) != len(merged):
            raise ValueError('one resource cannot fill multiple replica slots')
        operation['resources'] = merged
        self.save()

    def start_apply(self, plan_sha256):
        """Persist intent before calling Terraform; never repeat an ambiguous apply."""
        operation = self.state['operation']
        if operation is None or operation['phase'] not in ('requested', 'retryable'):
            raise ValueError('apply requires a new or reconciled operation')
        if not re.fullmatch('[0-9a-f]{64}', plan_sha256):
            raise ValueError('apply requires a saved-plan digest')
        required = {f'digitalocean_droplet.worker[{i}]' for i in range(operation['before'] * 2)}
        if not required <= set(operation['resources']):
            raise ValueError('record established resource identities before apply')
        operation['attempts'].append({'plan_sha256': plan_sha256, 'resolution': None})
        operation['phase'] = 'applying'
        self.save()

    def resolve_apply(self, *, resources, evidence_sha256, complete):
        """Record adapter reconciliation, including provider-side orphan checks.

        The evidence digest must bind the adapter's state/provider observations.
        This method cannot establish their truth; the live adapter is responsible
        for collision/import checks and verifying every required resource.
        """
        operation = self.state['operation']
        if operation is None or operation['phase'] != 'applying':
            raise ValueError('no unresolved apply')
        if type(complete) is not bool or not re.fullmatch('[0-9a-f]{64}', evidence_sha256):
            raise ValueError('reconciliation evidence is required')
        required = {f'digitalocean_droplet.worker[{i}]' for i in range(operation['target_groups'] * 2)}
        if complete and set(resources) != required:
            raise ValueError('complete reconciliation requires the entire fleet')
        self.record_resources(resources)
        operation['attempts'][-1]['resolution'] = evidence_sha256
        operation['phase'] = 'provisioned' if complete else 'retryable'
        self.save()

    def finish_registration(self, health, inventory):
        """Close an expansion only after the coordinator acknowledges its pair."""
        operation = self.state['operation']
        if operation is None or operation['phase'] != 'bootstrapped':
            raise ValueError('registration requires a bootstrapped operation')
        request = health.get('capacity', {}).get('requests', {}).get(operation['id'], {})
        if (health.get('protocol') != PROTOCOL
                or health.get('registered_groups') != operation['target_groups']
                or request.get('registered') is not True
                or inventory_count(inventory) != operation['target_groups']
                or len(operation['resources']) != operation['target_groups'] * 2):
            raise ValueError('coordinator has not acknowledged the complete pair')
        result = copy.deepcopy(operation)
        result['phase'] = 'registered'
        result['registered_inventory_digest'] = digest(inventory)
        self.state['completed'][operation['id']] = result
        self.state['operation'] = None
        self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=Path, required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--health', type=Path, help='Captured coordinator health response')
    source.add_argument('--coordinator-url', help='Read current health from the coordinator')
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--policy', type=Path, required=True)
    args = parser.parse_args()
    try:
        with Journal(args.state_dir) as journal:
            if args.health is not None:
                health = json.loads(args.health.read_text())
            else:
                with urllib.request.urlopen(args.coordinator_url.rstrip('/') + '/v1/health', timeout=10) as response:
                    data = response.read(1024 * 1024 + 1)
                if len(data) > 1024 * 1024:
                    raise ValueError('oversized health response')
                health = json.loads(data)
            operation = journal.observe(health, json.loads(args.inventory.read_text()),
                                        json.loads(args.policy.read_text()))
            summary = {'operation': operation['id'], 'phase': operation['phase'], 'target_groups': operation['target_groups']} if operation else {'operation': None}
    except (ValueError, KeyError, TypeError, OSError):
        raise SystemExit('expansion observation rejected; inspect private inputs or journal lock') from None
    print(json.dumps(summary, sort_keys=True))


if __name__ == '__main__':
    main()

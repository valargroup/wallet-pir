#!/usr/bin/env python3
"""Validate an append-only plan for the dedicated Enhance v4 Terraform root.

Plan JSON can contain credentials. Only an allowlisted summary is printed.
This command never applies a plan or reads cloud credentials.
"""
import argparse
import json
from pathlib import Path
import re


def validate(plan, policy, before, existing):
    if type(before) is not int or not 0 <= before < 4:
        raise ValueError('expansion requires zero to three existing groups')
    target = before + 1
    count = target * 2
    workers = {f'digitalocean_droplet.worker[{i}]': i for i in range(count)}
    members = {f'digitalocean_project_resources.worker[{i}]': i for i in range(count)}
    shared = {'digitalocean_tag.worker', 'digitalocean_firewall.worker'}
    allowed = set(workers) | set(members) | shared
    old_workers = {f'digitalocean_droplet.worker[{i}]' for i in range(before * 2)}
    if not old_workers <= set(existing) <= set(workers) or any(not re.fullmatch('[1-9][0-9]*', str(v)) for v in existing.values()):
        raise ValueError('recorded worker identities do not match the expansion')
    for drift in plan.get('resource_drift', []):
        if drift['change']['actions'] != ['no-op']:
            old = drift['change'].get('before') or {}
            new = drift['change'].get('after') or {}
            changed = {k for k in old.keys() | new.keys() if old.get(k) != new.get(k)}
            if (drift.get('address') == 'digitalocean_tag.worker'
                    and old.get('name') == new.get('name') == 'enhance-pir-v4-worker'
                    and changed <= {'total_resource_count', 'droplets_count'}):
                continue
            raise ValueError('reconcile infrastructure drift before expansion')
    seen = set()
    created = []
    for resource in plan.get('resource_changes', []):
        address = resource['address']
        change = resource['change']
        actions = change['actions']
        after = change.get('after') or {}
        if address not in allowed or address in seen or resource.get('mode', 'managed') != 'managed':
            raise ValueError('plan contains an unexpected resource')
        seen.add(address)
        if actions not in (['no-op'], ['create']):
            raise ValueError('only existing resources and additive creates are permitted')
        if actions == ['create']:
            if address in old_workers or address in existing or (before > 0 and address in shared):
                raise ValueError('plan would recreate an established resource')
            if address in members and members[address] < before * 2:
                raise ValueError('plan would recreate established project membership')
            created.append(address)
        if address in workers:
            index = workers[address]
            expected = {'name': f'enhance-pir-v4-g{index // 2 + 1:02d}-r{index % 2 + 1}',
                        'size': 'c-4', 'region': policy['region'], 'vpc_uuid': policy['vpc_id'],
                        'image': 'ubuntu-24-04-x64'}
            # The provider refreshes a created Droplet's image slug into an image ID.
            if actions == ['no-op']:
                expected.pop('image')
                if str(after.get('id')) != str(existing.get(address)) or address not in existing:
                    raise ValueError('existing worker identity differs from the operation journal')
            if any(after.get(k) != v for k, v in expected.items()):
                raise ValueError('worker plan differs from the pinned profile')
            if set(after.get('tags') or []) != {'enhance-pir-v4-worker'}:
                raise ValueError('worker tag differs from the isolated fleet')
            if actions == ['create'] and set(after.get('ssh_keys') or []) != set(policy['ssh_key_ids']):
                raise ValueError('worker SSH authorization differs from the pinned keys')
        elif address in members:
            if after.get('project') != policy['project_id']:
                raise ValueError('project membership targets a different project')
            worker = f'digitalocean_droplet.worker[{members[address]}]'
            resources = after.get('resources')
            if worker in existing:
                if resources != [f'do:droplet:{existing[worker]}']:
                    raise ValueError('project membership differs from the recorded worker')
            else:
                unknown = change.get('after_unknown', {}).get('resources')
                if not ((resources is None and unknown is True) or (resources == [None] and unknown == [True])):
                    raise ValueError('new membership must refer to the newly allocated worker URN')
        elif address == 'digitalocean_tag.worker':
            if after.get('name') != 'enhance-pir-v4-worker':
                raise ValueError('unexpected tag')
        else:
            if after.get('name') != 'enhance-pir-v4-workers' or set(after.get('tags') or []) != {'enhance-pir-v4-worker'}:
                raise ValueError('unexpected firewall identity')
            inbound = after.get('inbound_rule') or []
            expected = {'22': set(policy['operator_ssh_cidrs']) | {policy['coordinator_private_ipv4'] + '/32'},
                        '8291': {policy['coordinator_private_ipv4'] + '/32'}}
            ports = set()
            for rule in inbound:
                port = rule.get('port_range')
                if (port not in expected or port in ports or rule.get('protocol') != 'tcp'
                        or set(rule.get('source_addresses') or []) != expected[port]
                        or any(rule.get(k) for k in ('source_tags', 'source_droplet_ids', 'source_load_balancer_uids', 'source_kubernetes_ids'))):
                    raise ValueError('unexpected worker ingress')
                ports.add(port)
            if ports != set(expected):
                raise ValueError('worker ingress is incomplete')
    if seen != allowed:
        raise ValueError('plan does not describe the complete intended fleet')
    return {'target_groups': target, 'creates': sorted(created), 'recorded_workers': len(existing)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan-json', type=Path, required=True)
    parser.add_argument('--policy', type=Path, required=True)
    parser.add_argument('--existing', type=Path, required=True)
    parser.add_argument('--before', type=int, required=True)
    args = parser.parse_args()
    try:
        result = validate(json.loads(args.plan_json.read_text()), json.loads(args.policy.read_text()),
                          args.before, json.loads(args.existing.read_text()))
    except (ValueError, KeyError, TypeError, OSError):
        raise SystemExit('infrastructure plan rejected; inspect private operation inputs') from None
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()

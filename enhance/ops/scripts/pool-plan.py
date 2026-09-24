#!/usr/bin/env python3
"""Validate one additive production worker expansion. Never prints plan values."""
import argparse
import json
import re
from pathlib import Path


def validate(plan, policy, existing):
    count = policy['existing_pool_workers']
    if type(count) is not int or not 0 <= count < 14:
        raise ValueError('invalid existing worker count')
    droplet = f'digitalocean_droplet.enhance_pool_worker[{count}]'
    member = f'digitalocean_project_resources.enhance_pool_worker[{count}]'
    creates = set()
    for drift in plan.get('resource_drift', []):
        if drift['change']['actions'] != ['no-op']:
            raise ValueError('reconcile infrastructure drift before expansion')
    seen = set()
    for resource in plan.get('resource_changes', []):
        address, change = resource['address'], resource['change']
        if address in seen:
            raise ValueError('duplicate resource')
        seen.add(address)
        if resource.get('mode') == 'data':
            if change['actions'] not in (['no-op'], ['read']):
                raise ValueError('unexpected data action')
            continue
        if change['actions'] == ['no-op']:
            if address in existing and str((change.get('after') or {}).get('id')) != str(existing[address]):
                raise ValueError('existing identity changed')
            continue
        if change['actions'] != ['create'] or address not in (droplet, member):
            raise ValueError('only the next individual worker and membership may be created')
        if address in existing:
            raise ValueError('cannot recreate a recorded resource')
        creates.add(address)
        after = change.get('after') or {}
        if address == droplet:
            expected = {'name': f'enhance-pir-pool-{count+1:02d}', 'size': 'c-4',
                        'image': 'ubuntu-24-04-x64', 'region': policy['region'], 'vpc_uuid': policy['vpc_id']}
            if any(after.get(key) != value for key, value in expected.items()):
                raise ValueError('worker profile differs from frozen policy')
            if set(after.get('ssh_keys') or []) != set(policy['ssh_key_ids']):
                raise ValueError('SSH public keys differ')
            if after.get('tags') != ['enhance-pir-worker']:
                raise ValueError('worker firewall tag differs')
        else:
            if after.get('project') != policy['project_id']:
                raise ValueError('project membership differs')
            unknown = change.get('after_unknown', {}).get('resources')
            if not ((after.get('resources') is None and unknown is True)
                    or (after.get('resources') == [None] and unknown == [True])):
                raise ValueError('membership must refer to the newly allocated worker')
    if creates != {droplet, member} or not set(existing) <= seen:
        raise ValueError('incomplete expansion or missing existing identities')
    return {'creates': sorted(creates), 'target_pool_workers': count + 1}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('plan', 'policy', 'existing'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    try:
        result = validate(*(json.loads(getattr(args, key).read_text()) for key in ('plan', 'policy', 'existing')))
    except (ValueError, KeyError, TypeError, OSError):
        raise SystemExit('pool expansion plan rejected; inspect private inputs') from None
    print(json.dumps(result, sort_keys=True))

if __name__ == '__main__':
    main()

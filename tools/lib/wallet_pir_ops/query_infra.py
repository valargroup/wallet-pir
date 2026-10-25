"""Refuse a query-node provisioning plan that changes the existing fleet.

The checked JSON comes from Terraform's saved plan, not a hand-written request.
Only the six resources of one approved node may be created; replacements and
later capacity changes need a separately reviewed plan.
"""
RESOURCES = {
    'digitalocean_tag.query', 'digitalocean_droplet.query',
    'digitalocean_volume.data', 'digitalocean_volume_attachment.data',
    'digitalocean_project_resources.query', 'cloudflare_dns_record.query',
}
# Each node's module address and its approved capacity and destination.
NODES = {
    '01': {'module': 'query_serving', 'size': 'm-24vcpu-192gb', 'volume_gib': 1024},
    '02': {'module': 'query_serving_02', 'size': 'g5-16vcpu-96gb-80gb', 'volume_gib': 512},
}


def prefix(node):
    return 'module.%s[0].' % NODES[node]['module']


def initial_plan(plan, project_id, node='01'):
    """Return created addresses only for one approved node's complete creation.

    Refuse unknown critical inputs, drift, deferred work, changes outside this
    node and any delete/update. Values and credentials are never returned.
    """
    spec, start = NODES[node], prefix(node)
    name = 'wallet-pir-query-' + node
    if plan.get('complete') is not True or plan.get('resource_drift') or plan.get('deferred_changes'):
        raise ValueError('candidate provisioning requires a complete plan without drift')
    created = {}
    for resource in plan.get('resource_changes', []):
        if resource.get('mode') != 'managed':
            continue
        change = resource['change']
        if resource.get('previous_address') or change.get('importing'):
            raise ValueError('activate existing fleet source separately before provisioning the query node')
        if change['actions'] == ['no-op']:
            continue
        address = resource['address']
        if not address.startswith(start) or address[len(start):] not in RESOURCES:
            raise ValueError('candidate plan changes an unrelated resource')
        if change['actions'] != ['create'] or address in created:
            raise ValueError('candidate plan must contain unique creations only')
        created[address] = change.get('after') or {}
    if set(created) != {start + resource for resource in RESOURCES}:
        raise ValueError('candidate plan must create exactly the six declared resources')
    expected = {
        'digitalocean_droplet.query': {'size': spec['size'], 'region': 'ams3', 'name': name},
        'digitalocean_volume.data': {'size': spec['volume_gib'], 'name': name + '-data', 'region': 'ams3'},
        'digitalocean_project_resources.query': {'project': project_id},
        'cloudflare_dns_record.query': {'name': name + '.valargroup.dev', 'type': 'A', 'proxied': False},
    }
    for resource, fields in expected.items():
        actual = created[start + resource]
        if any(type(actual.get(key)) is not type(value) or actual[key] != value
               for key, value in fields.items()):
            raise ValueError('candidate plan differs from the approved capacity or destination')
    return sorted(created)

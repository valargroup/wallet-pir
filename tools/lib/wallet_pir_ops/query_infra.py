"""Refuse a candidate provisioning plan that changes the existing fleet.

The checked JSON comes from Terraform's saved plan, not a hand-written request.
Only the initial six-resource candidate is accepted; replacements and later
capacity changes need a separately reviewed plan.
"""
PREFIX = 'module.query_serving[0].'
RESOURCES = {
    'digitalocean_tag.query', 'digitalocean_droplet.query',
    'digitalocean_volume.data', 'digitalocean_volume_attachment.data',
    'digitalocean_project_resources.query', 'cloudflare_dns_record.query',
}
HOSTNAME = 'wallet-pir-query-01.valargroup.dev'
SIZE = 'm-24vcpu-192gb'


def initial_plan(plan, project_id):
    """Return created addresses only for the approved, complete initial plan.

    Refuse unknown critical inputs, drift, deferred work, changes outside this
    candidate and any delete/update. Values and credentials are never returned.
    """
    if plan.get('complete') is not True or plan.get('resource_drift') or plan.get('deferred_changes'):
        raise ValueError('candidate provisioning requires a complete plan without drift')
    created = {}
    for resource in plan.get('resource_changes', []):
        if resource.get('mode') != 'managed':
            continue
        change = resource['change']
        if change['actions'] == ['no-op']:
            continue
        address = resource['address']
        if not address.startswith(PREFIX) or address[len(PREFIX):] not in RESOURCES:
            raise ValueError('candidate plan changes an unrelated resource')
        if change['actions'] != ['create'] or address in created:
            raise ValueError('candidate plan must contain unique creations only')
        created[address] = change.get('after') or {}
    if set(created) != {PREFIX + name for name in RESOURCES}:
        raise ValueError('candidate plan must create exactly the six declared resources')
    expected = {
        'digitalocean_droplet.query': {'size': SIZE, 'region': 'ams3', 'name': 'wallet-pir-query-01'},
        'digitalocean_volume.data': {'size': 1024, 'name': 'wallet-pir-query-01-data', 'region': 'ams3'},
        'digitalocean_project_resources.query': {'project': project_id},
        'cloudflare_dns_record.query': {'name': HOSTNAME, 'type': 'A', 'proxied': False},
    }
    for name, fields in expected.items():
        actual = created[PREFIX + name]
        if any(type(actual.get(key)) is not type(value) or actual[key] != value
               for key, value in fields.items()):
            raise ValueError('candidate plan differs from the approved capacity or destination')
    return sorted(created)

#!/usr/bin/env python3
"""Validate a saved plan of the transparent elastic Terraform root.

Reads `terraform show -json <saved plan>` for
ops/infra/digitalocean/transparent-elastic/ and passes it only if every change
is one the actuator asked for:

- create `digitalocean_droplet.recent["<name>"]` or
  `digitalocean_project_resources.recent["<name>"]` for a name in
  --allow-create, with the pinned size, region, VPC, tag, image and project;
- delete those addresses for a name in --allow-destroy whose recorded droplet
  id is in --allow-destroy-id;
- no-op for the root's own members, no-op or read for its data sources.

Everything else is refused: another address, resource type or module, an
update, a replacement, and any archive name. Plan JSON holds secrets (input
variables, user data with host keys); only addresses are printed.
"""
import argparse
import json
from pathlib import Path
import re
import sys

PROVIDER = 'registry.terraform.io/digitalocean/digitalocean'
NAME = re.compile('transparent-pir-recent-[0-9]{2,3}')
ADDRESS = re.compile(r'(digitalocean_droplet|digitalocean_project_resources)\.recent\["(transparent-pir-recent-[0-9]{2,3})"\]')
DROPLET_ID = re.compile('[1-9][0-9]*')
PROFILE = ('size', 'region', 'vpc_uuid', 'tag', 'image', 'project_id')


class Refused(ValueError):
    """The plan, or the request to validate it, is not one the actuator may apply."""


def check_names(names, what):
    names = set(names)
    for name in names:
        if not isinstance(name, str) or 'archive' in name or not NAME.fullmatch(name):
            raise Refused(f'{what} names only elastic recent replicas, not {name!r}')
    return names


def identify(resource):
    """(type, member name) for a resource of this root, None for a data source; refuse the rest."""
    address = resource.get('address')
    if not isinstance(address, str):
        raise Refused('a resource change has no address')
    # Archive owners belong to the production root and are never elastic.
    if 'archive' in address.lower():
        raise Refused(f'{address}: archive resources are never planned by the elastic root')
    if resource.get('module_address') or address.startswith('module.'):
        raise Refused(f'{address}: outside the elastic root')
    if resource.get('mode') == 'data':
        return None
    match = ADDRESS.fullmatch(address)
    if (match is None or resource.get('mode') != 'managed' or resource.get('type') != match[1]
            or resource.get('name') != 'recent' or resource.get('index') != match[2]
            or resource.get('provider_name') != PROVIDER):
        raise Refused(f'{address}: not an elastic recent replica resource')
    return match[1], match[2]


def droplet_id(value, address):
    value = str(value) if isinstance(value, (int, str)) and not isinstance(value, bool) else ''
    if not DROPLET_ID.fullmatch(value):
        raise Refused(f'{address}: recorded droplet id is missing or malformed')
    return value


def check_create(kind, name, change, profile, address):
    after = change.get('after') or {}
    if kind == 'digitalocean_droplet':
        expected = {'name': name, 'size': profile['size'], 'region': profile['region'],
                    'vpc_uuid': profile['vpc_uuid'], 'image': profile['image']}
        for key, value in expected.items():
            if after.get(key) != value:
                raise Refused(f'{address}: {key} {after.get(key)!r} differs from the pinned {value!r}')
        if sorted(after.get('tags') or []) != [profile['tag']]:
            raise Refused(f'{address}: tags differ from the worker tag {profile["tag"]!r}')
    else:
        if after.get('project') != profile['project_id']:
            raise Refused(f'{address}: joins a different project')
        # A new entry names the droplet created in the same apply, whose URN is not known yet.
        resources, unknown = after.get('resources'), (change.get('after_unknown') or {}).get('resources')
        if not ((resources is None and unknown is True) or (resources == [None] and unknown == [True])):
            raise Refused(f'{address}: must name only its newly created droplet')


def check_delete(kind, name, change, allowed_ids, address):
    before = change.get('before') or {}
    if kind == 'digitalocean_droplet':
        if before.get('name') != name:
            raise Refused(f'{address}: recorded droplet name differs from its member')
        identity = droplet_id(before.get('id'), address)
    else:
        resources = before.get('resources')
        if not isinstance(resources, list) or len(resources) != 1 or not str(resources[0]).startswith('do:droplet:'):
            raise Refused(f'{address}: recorded project entry does not name one droplet')
        identity = droplet_id(str(resources[0])[len('do:droplet:'):], address)
    if identity not in allowed_ids:
        raise Refused(f'{address}: droplet {identity} is not on the destroy allowlist')
    return identity


def validate(plan, allow_create=(), allow_destroy=(), allow_destroy_ids=(), profile=None):
    """Return {"creates": [...], "destroys": [...]} addresses, or raise Refused."""
    allow_create = check_names(allow_create, 'allow-create')
    allow_destroy = check_names(allow_destroy, 'allow-destroy')
    if allow_create & allow_destroy:
        raise Refused('a name is never both created and destroyed: names are not reused')
    allowed_ids = set()
    for value in allow_destroy_ids:
        allowed_ids.add(droplet_id(value, 'allow-destroy-id'))
    if allow_create and (not isinstance(profile, dict) or any(not isinstance(profile.get(k), str) or not profile[k] for k in PROFILE)):
        raise Refused('creates require the full pinned profile: ' + ', '.join(PROFILE))
    if not isinstance(plan, dict) or not str(plan.get('format_version', '')).startswith('1.'):
        raise Refused('input is not Terraform plan JSON')
    if plan.get('errored') is True:
        raise Refused('Terraform reported an errored plan')
    if plan.get('deferred_changes'):
        raise Refused('deferred changes cannot be validated')
    for resource in plan.get('resource_drift') or []:
        # Drift is informational: resource_changes decide what apply does.
        identify(resource)
    seen, creates, destroys, destroyed_ids = set(), [], [], {}
    for resource in plan.get('resource_changes') or []:
        member = identify(resource)
        address = resource['address']
        if address in seen:
            raise Refused(f'{address}: appears twice')
        seen.add(address)
        change = resource.get('change') or {}
        actions = change.get('actions')
        if member is None:
            if actions not in (['no-op'], ['read']):
                raise Refused(f'{address}: data sources may only be read')
            continue
        kind, name = member
        if actions == ['no-op']:
            continue
        if actions == ['create']:
            if name not in allow_create:
                raise Refused(f'{address}: {name} is not allowed to be created')
            check_create(kind, name, change, profile, address)
            creates.append(address)
        elif actions == ['delete']:
            if name not in allow_destroy:
                raise Refused(f'{address}: {name} is not allowed to be destroyed')
            destroyed_ids.setdefault(name, set()).add(check_delete(kind, name, change, allowed_ids, address))
            destroys.append(address)
        elif actions == ['update']:
            raise Refused(f'{address}: updates are refused; a member never changes during its life')
        elif sorted(actions or []) == ['create', 'delete']:
            raise Refused(f'{address}: replacement is refused; create a new member instead')
        else:
            raise Refused(f'{address}: action {actions!r} is not allowed')
    for name, identities in destroyed_ids.items():
        if len(identities) != 1:
            raise Refused(f'{name}: droplet and project entry name different droplets')
    return {'creates': sorted(creates), 'destroys': sorted(destroys)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--plan-json', required=True, help='terraform show -json output, or - for stdin')
    parser.add_argument('--allow-create', action='append', default=[], metavar='NAME')
    parser.add_argument('--allow-destroy', action='append', default=[], metavar='NAME')
    parser.add_argument('--allow-destroy-id', action='append', default=[], metavar='DROPLET_ID')
    for field in PROFILE:
        parser.add_argument('--' + field.replace('_', '-'), dest=field, help='pinned for creates')
    args = parser.parse_args(argv)
    try:
        text = sys.stdin.read() if args.plan_json == '-' else Path(args.plan_json).read_text()
        plan = json.loads(text)
    except (OSError, ValueError):
        raise SystemExit('elastic plan refused: cannot read plan JSON') from None
    try:
        result = validate(plan, args.allow_create, args.allow_destroy, args.allow_destroy_id,
                          {field: getattr(args, field) for field in PROFILE})
    except (Refused, KeyError, TypeError, AttributeError) as error:
        reason = str(error) if isinstance(error, Refused) else 'malformed plan JSON'
        raise SystemExit('elastic plan refused: ' + reason) from None
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()

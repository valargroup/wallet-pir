"""Read-only comparison of plans for the fleet's service-module extraction."""
import re


def declared_moves(root):
    moves = {}
    for path in root.glob('*.tf'):
        for block in re.findall(r'^moved \{.*?^\}', path.read_text(), re.M | re.S):
            match = re.search(r'from\s*=\s*(\S+)\s+to\s*=\s*(\S+)', block)
            if not match or match[1] in moves:
                raise ValueError('invalid or duplicate move mapping')
            moves[match[1]] = match[2]
    return moves


def destination(address, moves):
    seen = set()
    while True:
        if address in seen:
            raise ValueError('cyclic move mapping')
        seen.add(address)
        match = next((old for old in sorted(moves, key=len, reverse=True)
                      if address == old or address.startswith(old + '[')), None)
        if match is None:
            return address
        address = moves[match] + address[len(match):]


def resources(module):
    result = {}
    for resource in module.get('resources', []):
        address = resource['address']
        if address in result or resource.get('mode') != 'managed':
            raise ValueError('duplicate or unsupported resource')
        result[address] = resource['values']
    for child in module.get('child_modules', []):
        for address, values in resources(child).items():
            if address in result:
                raise ValueError('duplicate resource')
            result[address] = values
    return result


def compare(before, after, moves):
    """Require the same complete IDs, values and root outputs, with only declared moves.

    Both inputs are `terraform show -json` of freshly saved plans against one
    protected snapshot and the same variables. The baseline must also be clean;
    this comparison never accepts unrelated drift just because both plans show it.
    """
    if before.get('variables') != after.get('variables'):
        raise ValueError('root input values differ')
    for plan in (before, after):
        if plan.get('errored') or plan.get('complete') is False:
            raise ValueError('incomplete or errored plan')
        if plan.get('resource_drift') or plan.get('deferred_changes'):
            raise ValueError('reconcile drift or deferred changes first')
        for resource in plan.get('resource_changes', []):
            change = resource['change']
            if resource.get('mode') != 'managed' or change['actions'] != ['no-op']:
                raise ValueError('provider resource action is forbidden')
            if (change.get('before') != change.get('after')
                    or any_unknown(change.get('after_unknown', {}))):
                raise ValueError('resource values changed or remain unknown')
            previous = resource.get('previous_address')
            if previous and destination(previous, moves) != resource['address']:
                raise ValueError('undeclared state address move')
        if any(change['actions'] != ['no-op'] for change in plan.get('output_changes', {}).values()):
            raise ValueError('root output change is forbidden')
    old = resources(before['planned_values']['root_module'])
    new = resources(after['planned_values']['root_module'])
    baseline_changes = before.get('resource_changes', [])
    if (len({r['address'] for r in baseline_changes}) != len(baseline_changes)
            or {r['address'] for r in baseline_changes} != set(old)
            or any(r.get('previous_address') for r in baseline_changes)):
        raise ValueError('baseline inventory is incomplete or already contains moves')
    changes = {r['address']: r for r in after.get('resource_changes', [])}
    if len(changes) != len(after.get('resource_changes', [])) or set(changes) != set(new):
        raise ValueError('missing or duplicate resource changes')
    remapped = {}
    moved = 0
    for address, values in old.items():
        target = destination(address, moves)
        if target in remapped or not values.get('id'):
            raise ValueError('duplicate destination or missing resource identity')
        remapped[target] = values
        if target != address:
            if changes.get(target, {}).get('previous_address') != address:
                raise ValueError('expected state move is missing')
            moved += 1
        elif changes.get(target, {}).get('previous_address'):
            raise ValueError('unexpected move of an unchanged address')
    if remapped != new:
        raise ValueError('resource inventory, identity or values differ')
    if before['planned_values'].get('outputs') != after['planned_values'].get('outputs'):
        raise ValueError('root outputs differ')
    return {'resources': len(new), 'moved_instances': moved, 'provider_actions': 0}


def any_unknown(value):
    if isinstance(value, dict):
        return any(any_unknown(child) for child in value.values())
    if isinstance(value, list):
        return any(any_unknown(child) for child in value)
    return value is True

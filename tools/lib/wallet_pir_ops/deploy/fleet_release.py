"""Plan a fleet release across publishers and every consumer, whatever the topology.

A `fleet-release/1` request names the candidate executables (one per build
variant, each with its `--contract-info` report), the processes to change, the
public publisher origins and the process serving each, and one independent
encrypted-answer probe per product. Planning reads the shared role registry and
the reviewed inventory; it never uses a product-specific role list.

Every inventory target becomes one *process*, identified by its verified host
(the inventory address, so two names for one machine are one host) and unit. A
unified query node is one process with several enabled products; a
distributed worker is one process with one. Two declarations of one process
refuse, as does a consumer whose publisher origin no reviewed process serves.

Compatibility follows `wallet_pir_ops.contract`: a release is *breaking* when a
publisher origin's contract major changes, and then every consumer of that
origin, selected or not, standby and CUDA included, must change to a candidate
of the new major. In a compatible release every changed consumer must match the
major of every publisher it reads, including unchanged publishers; unchanged
consumers keep relationships they already validated at startup. Unknown
majors, omitted participants and missing recovery inputs are refusals, listed
before anything is stopped.

`plan` is pure: preflight gathers observations through the executor and passes
them in, so tests and production share the same decisions. The plan's digest
covers the request, the inventory and every observation it relied on; a
reviewed plan whose digest no longer matches a fresh plan is stale.
"""
import hashlib
import json
from urllib.parse import urlsplit

from .. import contract
from . import descriptors

SCHEMA = 'fleet-release/1'
PLAN_SCHEMA = 'fleet-release-plan/1'
UNIFIED = ('query-server', 'combined')
QUERY_PRODUCTS = ('enhance', 'status', 'transparent_history', 'transaction_details', 'receiver')
SERVICE_PRODUCTS = {'enhance': ('enhance',), 'status': ('status',),
                    'transparent-history': ('transparent_history',),
                    'transparent-display': ('transaction_details',), 'receiver': ('receiver',)}
# Roles that forward to other query processes and start after them.
DEPENDENT_ROLES = ('packing-router', 'query-ingress', 'router')
PUBLISHER_KEYS = ('publisher', 'publisher_origin')
REQUEST_KEYS = {'schema', 'source', 'inventory_sha256', 'executables', 'select', 'publishers', 'probes'}


def digest(document):
    return hashlib.sha256(json.dumps(document, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def origin(url):
    """`https://host:port` for an HTTPS publisher URL, or None for anything else."""
    if not isinstance(url, str):
        return None
    parts = urlsplit(url)
    if parts.scheme != 'https' or not parts.hostname:
        return None
    return 'https://%s:%d' % (parts.hostname.lower(), parts.port or 443)


def publisher_origins(settings):
    """Every HTTPS publisher origin named anywhere in a role's settings."""
    found = set()
    if isinstance(settings, dict):
        for key, value in settings.items():
            if key in PUBLISHER_KEYS and origin(value):
                found.add(origin(value))
            else:
                found |= publisher_origins(value)
    elif isinstance(settings, list):
        for value in settings:
            found |= publisher_origins(value)
    return found


def validate_request(request):
    """Shape checks only; facts about the fleet are checked by `plan`."""
    problems = []
    if not isinstance(request, dict) or request.get('schema') != SCHEMA:
        return ['request schema must be %s' % SCHEMA]
    unknown = set(request) - REQUEST_KEYS
    if unknown:
        problems.append('request has unknown fields %s' % sorted(unknown))
    source = request.get('source')
    if not (isinstance(source, dict) and isinstance(source.get('revision'), str)
            and isinstance(source.get('submitted_unix'), (int, float))):
        problems.append('source needs revision and submitted_unix (the start of the release clock)')
    if not (isinstance(request.get('inventory_sha256'), str) and len(request['inventory_sha256']) == 64):
        problems.append('inventory_sha256 must pin the reviewed inventory')
    executables = request.get('executables')
    if not isinstance(executables, dict):
        problems.append('executables maps build variants to {sha256, contract}')
        executables = {}
    for variant, entry in executables.items():
        if not (isinstance(entry, dict) and isinstance(entry.get('sha256'), str) and len(entry['sha256']) == 64
                and isinstance(entry.get('contract'), dict)):
            problems.append('executable %s needs sha256 and its contract-info report' % variant)
    select = request.get('select')
    if not (isinstance(select, list) and select and all(isinstance(item, str) for item in select)
            and len(set(select)) == len(select)):
        problems.append('select lists distinct process keys (host/unit) to change')
    publishers = request.get('publishers')
    if not (isinstance(publishers, dict) and all(origin(name) == name and isinstance(key, str)
                                                 for name, key in publishers.items())):
        problems.append('publishers maps normalized https://host:port origins to process keys')
    probes = request.get('probes')
    if not (isinstance(probes, dict) and all(isinstance(probe, dict) and isinstance(probe.get('argv'), list)
                                             and isinstance(probe.get('host'), str)
                                             for probe in probes.values())):
        problems.append('probes maps each product to an independent {host, argv} encrypted-answer probe')
    return problems


def processes(registry, inventory):
    """Inventory targets grouped into processes, plus declaration conflicts.

    Returns `(by_key, problems)`. A process records every (service, role) that
    declares it; more than one declaration is a conflict, never merged.
    """
    identity = {name: entry.get('address', name) for name, entry in inventory.hosts.items()}
    by_identity, problems = {}, []
    for name in sorted(inventory.services):
        if name not in registry:
            problems.append('inventory names unknown service %s' % name)
            continue
        for target in descriptors.targets(registry[name], inventory):
            key = (identity[target.host], target.unit)
            by_identity.setdefault(key, []).append((registry[name], target))
    by_key = {}
    for (_, unit), declared in sorted(by_identity.items()):
        hosts = sorted({target.host for _, target in declared})
        key = '%s/%s' % (hosts[0], unit)
        if len(declared) > 1:
            problems.append('%s is declared %d times (%s); one process has one owner'
                            % (key, len(declared), ', '.join('%s.%s@%s' % (s.name, t.role.name, t.host)
                                                             for s, t in declared)))
            continue
        service, target = declared[0]
        executable = target.role.binary or service.binary
        if (service.name, target.role.name) == UNIFIED:
            products = sorted(set(target.settings) & set(QUERY_PRODUCTS))
        elif service.name == 'publisher':
            products = sorted(inventory.services['publisher'].get('managed_products', []))
        else:
            products = list(SERVICE_PRODUCTS.get(service.name, ()))
        by_key[key] = {
            'key': key, 'host': target.host, 'unit': target.unit, 'service': service.name,
            'role': target.role.name, 'executable': executable,
            'variant': target.artifact_kind or executable, 'products': products,
            'publisher': executable == 'pir-publisher',
            'reads': sorted(publisher_origins(target.settings)) if executable == 'pir-query-server' else [],
            'rank': 1 if target.role.name in DEPENDENT_ROLES else 0,
            'install': target.install,
        }
    return by_key, problems


def candidate_problems(process, entry):
    """Refusals for one changing process against its candidate executable."""
    if entry is None:
        return ['%s: no candidate executable for variant %s' % (process['key'], process['variant'])]
    try:
        info = contract.parse_contract_info(json.dumps(entry['contract']), executable=process['executable'],
                                            sha256=entry['sha256'])
    except contract.ContractError as error:
        return ['%s: %s' % (process['key'], error)]
    problems = []
    missing = contract.missing_products(info, process['products'])
    if missing:
        problems.append('%s: candidate %s does not compile enabled products %s'
                        % (process['key'], entry['sha256'][:12], missing))
    if 'cuda' in process['variant'] and 'cuda' not in info.backends:
        problems.append('%s: CUDA variant candidate %s has no CUDA backend' % (process['key'], entry['sha256'][:12]))
    return problems


def plan(registry, inventory, inventory_sha256, request, observations):
    """The fleet release plan, with every refusal in `problems`.

    `observations['processes'][key]` holds `exe_sha256`, `contract_major` (None
    when the running release cannot say) and `previous_staged`;
    `observations['publishers'][origin]` holds the advertised `major` or None.
    Activation is allowed only when `problems` is empty.
    """
    problems = validate_request(request)
    if problems:
        return {'schema': PLAN_SCHEMA, 'problems': problems}
    if request['inventory_sha256'] != inventory_sha256:
        problems.append('inventory changed since review (%s, reviewed %s)'
                        % (inventory_sha256[:12], request['inventory_sha256'][:12]))
    found, declared = processes(registry, inventory)
    problems += declared
    selected = set(request['select'])
    problems += ['selected process %s is not in the inventory' % key for key in sorted(selected - set(found))]
    observed = observations.get('processes', {})
    candidates = {}
    for key in sorted(selected & set(found)):
        process = found[key]
        entry = request['executables'].get(process['variant'])
        refusals = candidate_problems(process, entry)
        problems += refusals
        if not refusals:
            candidates[key] = entry
        state = observed.get(key, {})
        if state.get('exe_sha256') is None and not process['install']:
            problems.append('%s: no running executable observed to restore' % key)
        elif state.get('exe_sha256') is not None and not state.get('previous_staged'):
            problems.append('%s: previous release %s is not staged for recovery'
                            % (key, str(state.get('exe_sha256'))[:12]))

    publishers = {}
    for name, key in sorted(request['publishers'].items()):
        process = found.get(key)
        if process is None or not process['publisher']:
            problems.append('publisher origin %s names %s, which is not a publisher process' % (name, key))
            continue
        current = observations.get('publishers', {}).get(name, {}).get('major')
        final = candidates[key]['contract']['contract_major'] if key in candidates else current
        if key in selected and current is None:
            problems.append('%s: current contract major is unknown; refusing to change its publisher' % name)
        publishers[name] = {'process': key, 'current_major': current, 'final_major': final,
                            'breaking': None not in (current, final) and current != final}
    for process in found.values():
        for name in process['reads']:
            if name not in publishers:
                problems.append('%s reads publisher %s, which no reviewed publisher process serves'
                                % (process['key'], name))
                continue
            relation = publishers[name]
            changed = process['key'] in candidates
            major = candidates[process['key']]['contract']['contract_major'] if changed else None
            if relation['breaking'] and not changed:
                problems.append('%s reads %s, whose contract major changes %s -> %s, but is not part of the release'
                                % (process['key'], name, relation['current_major'], relation['final_major']))
            elif changed and major != relation['final_major']:
                problems.append('%s candidate major %s would not match publisher %s major %s'
                                % (process['key'], major, name, relation['final_major']))
    for product in sorted({p for key in candidates for p in found[key]['products']}):
        if product not in request['probes']:
            problems.append('no independent encrypted-answer probe for %s' % product)

    changing = [found[key] for key in sorted(candidates)]
    consumers = [p for p in changing if not p['publisher']]
    waves = {
        'stop': [[p['key'] for p in consumers], [p['key'] for p in changing if p['publisher']]],
        'start': [[p['key'] for p in changing if p['publisher']],
                  [p['key'] for p in consumers if p['rank'] == 0],
                  [p['key'] for p in consumers if p['rank'] == 1]],
    }
    waves = {phase: [wave for wave in steps if wave] for phase, steps in waves.items()}
    entries = [{**process, 'action': 'change' if process['key'] in candidates else 'keep',
                'previous': observed.get(process['key'], {}),
                'proposed': ({'sha256': candidates[process['key']]['sha256'],
                              'contract_major': candidates[process['key']]['contract']['contract_major']}
                             if process['key'] in candidates else None)}
               for process in sorted(found.values(), key=lambda p: p['key'])]
    document = {
        'schema': PLAN_SCHEMA, 'request_sha256': digest(request), 'inventory_sha256': inventory_sha256,
        'source': request['source'],
        'kind': 'breaking' if any(r['breaking'] for r in publishers.values()) else 'compatible',
        'processes': entries, 'publishers': publishers, 'waves': waves,
        'probes': {product: request['probes'][product]
                   for product in sorted({p for c in changing for p in c['products']}) if product in request['probes']},
        'problems': sorted(set(problems)),
    }
    document['plan_sha256'] = digest(document)
    return document


def stale(reviewed_sha256, fresh):
    """Refusal text when a fresh plan no longer matches the reviewed digest."""
    if fresh.get('plan_sha256') != reviewed_sha256:
        return 'plan is stale: reviewed %s, fresh %s' % (reviewed_sha256[:12], str(fresh.get('plan_sha256'))[:12])
    return None


# ------------------------------------------------------------ observation

OBSERVE_SECONDS = 60
# Independent host operations in flight at once across the fleet.
HOST_CONCURRENCY = 4


def observe_process(registry, executor, process):
    """Running executable, its staged release and that release's contract major.

    Read-only. A release older than `--contract-info` reports no major.
    """
    state = executor.probe_unit(process['host'], process['unit'])
    running = state.get('exe_sha256') if state.get('active_state') == 'active' else None
    result = {'exe_sha256': running, 'contract_major': None, 'previous_staged': False}
    if running is None:
        return result
    path = registry[process['service']].release_binary(running, process['executable'])
    result['previous_staged'] = executor.sha256(process['host'], path) == running
    if result['previous_staged']:
        code, output = executor.run(process['host'], [path, *contract.CONTRACT_INFO_ARGV], OBSERVE_SECONDS)
        if code == 0:
            try:
                result['contract_major'] = contract.parse_contract_info(
                    output, executable=process['executable'], sha256=running).contract_major
            except contract.ContractError:
                pass
    return result


def observe_publisher(executor, host, name):
    """The major a publisher origin advertises, observed from the host serving it."""
    try:
        status, body = executor.http_get(host, name + contract.DISCOVERY_PATH, OBSERVE_SECONDS)
        return {'major': contract.publisher_document_major(status, body)}
    except Exception as error:  # noqa: BLE001 - an unobserved origin is unknown, never compatible
        return {'major': None, 'error': str(error)}


def observe(registry, inventory, executor, request):
    """Observations for `plan`, at most `HOST_CONCURRENCY` hosts at a time.

    Operations on one host stay sequential so a host never sees overlapping
    probes from this command.
    """
    from concurrent.futures import ThreadPoolExecutor
    found, _ = processes(registry, inventory)
    by_host = {}
    for process in found.values():
        by_host.setdefault(process['host'], []).append(process)

    def host_observations(host):
        return {process['key']: observe_process(registry, executor, process) for process in by_host[host]}

    with ThreadPoolExecutor(max_workers=HOST_CONCURRENCY) as pool:
        results = list(pool.map(host_observations, sorted(by_host)))
    publishers = {}
    for name, key in sorted(request.get('publishers', {}).items()):
        if key in found:
            publishers[name] = observe_publisher(executor, found[key]['host'], name)
    return {'processes': {key: value for result in results for key, value in result.items()},
            'publishers': publishers}


# ------------------------------------------------------------ command line

def register(commands):
    """Read-only planning commands. Activation is not offered by this module yet."""
    for operation in ('plan', 'preflight'):
        command = commands.add_parser('fleet-release-' + operation, allow_abbrev=False)
        command.add_argument('--request', required=True)
        command.add_argument('--request-sha256', required=True)
        if operation == 'preflight':
            command.add_argument('--expect-plan-sha256', required=True)


def load_request(path, expected_sha256):
    from pathlib import Path
    data = Path(path).read_bytes()
    if hashlib.sha256(data).hexdigest() != expected_sha256:
        raise ValueError('fleet release request checksum differs')
    return json.loads(data)


def dispatch(args, executor=None, registry=None):
    """`plan` prints the reviewed plan; `preflight` refuses a stale or refused plan.

    Both only read: no unit, file or process changes.
    """
    from pathlib import Path
    from .engine import DeployError
    request = load_request(args.request, args.request_sha256)
    inventory_bytes = Path(args.inventory).read_bytes()
    inventory = descriptors.load_inventory(args.inventory)
    registry = registry or descriptors.load_descriptors(args.descriptors)
    if executor is None:
        from .remote import SSHExecutor
        executor = SSHExecutor(inventory)
    fresh = plan(registry, inventory, hashlib.sha256(inventory_bytes).hexdigest(), request,
                 observe(registry, inventory, executor, request))
    if args.command == 'fleet-release-preflight':
        refusal = stale(args.expect_plan_sha256, fresh)
        if refusal or fresh['problems']:
            raise DeployError('fleet release refused before any change:\n  '
                              + '\n  '.join(([refusal] if refusal else []) + fresh['problems']))
    return fresh

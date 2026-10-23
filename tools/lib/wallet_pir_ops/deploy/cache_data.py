"""Move the existing Details cache onto its approved, already-attached volume."""
import hashlib
import json
from pathlib import Path

from .. import durable
from . import cache_data_host, cache_storage_host, descriptors
from .engine import DeployError, mutation_fence
from .remote import SSHExecutor, production_lock
from .transaction import new_id


def register(commands):
    for operation in ('plan', 'prepare', 'activate', 'status'):
        command = commands.add_parser('cache-data-' + operation, allow_abbrev=False)
        command.add_argument('--request', type=Path, required=True)
        command.add_argument('--request-sha256', required=True)
        if operation == 'prepare':
            command.add_argument('--expect-plan-sha256', required=True)
        if operation in ('activate', 'status'):
            command.add_argument('--transaction', required=True)


def dispatch(args, executor=None):
    raw = args.request.read_bytes()
    if hashlib.sha256(raw).hexdigest() != args.request_sha256:
        raise DeployError('cache storage request digest differs')
    request = json.loads(raw)
    if set(request) != {'host', 'unit', 'configuration', 'volume_id', 'volume_name', 'size_gib'}:
        raise DeployError('cache storage request requires explicit host, unit, configuration and volume identity')
    inventory = descriptors.load_inventory(args.inventory)
    entries = inventory.services.get('transparent-display', {}).get('roles', {})
    if request['host'] not in {entry['host'] for role in ('worker', 'external-worker') for entry in entries.get(role, [])}:
        raise DeployError('storage host must be an installed transaction-detail query worker')
    owned = []
    for receipt in Path(args.state_dir).glob('volume-*.json'):
        candidate = json.loads(receipt.read_text())
        approved = candidate.get('plan', {}).get('request', {})
        if (candidate.get('phase') == 'attached' and candidate.get('volume_id') == request['volume_id']
                and all(approved.get(key) == request[value] for key, value in
                        [('host', 'host'), ('name', 'volume_name'), ('size_gib', 'size_gib')])):
            owned.append(candidate)
    if len(owned) != 1:
        raise DeployError('cache volume lacks its unique approved provisioning receipt')
    executor = executor or SSHExecutor(inventory)
    operation = args.command.removeprefix('cache-data-')
    transaction = getattr(args, 'transaction', None) or new_id('cache-data', durable.digest(request))
    source = ('import types\ncache=types.ModuleType("_cache")\nexec(' + repr(Path(cache_storage_host.__file__).read_text())
              + ',cache.__dict__)\n' + Path(cache_data_host.__file__).read_text().replace('from . import cache_storage_host as cache\n', ''))
    local = Path(args.state_dir) / (transaction + '.json')
    local.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with production_lock(inventory, executor) as lock:
        mutation_fence(inventory, executor, lock)
        lock.verify()
        if operation not in ('plan', 'status'):
            durable.atomic_json(local, {'phase': operation, 'request': request, 'transaction': transaction}, mode=0o600)
        code, output = executor.run(request['host'], ['python3', '-c', source, operation, json.dumps(request),
                                                     getattr(args, 'expect_plan_sha256', ''), transaction],
                                    3000 if operation in ('prepare', 'activate') else 90)
        if code:
            raise DeployError('cache storage ' + operation + ' failed; retained transaction ' + transaction + ': ' + output[-1500:])
        try:
            result = json.loads(output)
        except ValueError:
            if operation == 'plan':
                raise
            # A child tool's diagnostic output must not cause a completed copy
            # to be repeated. Its durable host receipt is the authoritative result.
            recorded = json.loads(executor.read(request['host'], str(cache_data_host.receipt_path(transaction))))
            if recorded['plan']['request'] != request:
                raise DeployError('storage receipt differs from the reviewed request')
            result = cache_data_host.summary(recorded)
        if operation == 'plan':
            return {'plan': result, 'plan_sha256': durable.digest(result)}
        durable.atomic_json(local, result, mode=0o600)
        return result

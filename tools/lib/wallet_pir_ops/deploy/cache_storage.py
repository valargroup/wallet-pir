"""Plan and retire one demonstrably unused transaction-detail query cache.

Both operations hold the existing production lock. The host rechecks the exact
plan before deleting anything and retains its intent/result in a private receipt.
"""
import json
from pathlib import Path

from .. import durable
from . import cache_storage_host, descriptors
from .engine import DeployError, mutation_fence
from .remote import SSHExecutor, production_lock
from .transaction import new_id


def register(commands):
    for operation in ('plan', 'prune'):
        command = commands.add_parser('cache-storage-' + operation, allow_abbrev=False)
        command.add_argument('--host', required=True)
        command.add_argument('--unit', default='transparent-txid-display-worker.service')
        command.add_argument('--configuration', required=True)
        command.add_argument('--path', required=True)
        if operation == 'prune':
            command.add_argument('--expect-plan-sha256', required=True)


def dispatch(args, executor=None):
    inventory = descriptors.load_inventory(args.inventory)
    entries = inventory.services.get('transparent-display', {}).get('roles', {})
    allowed = {entry['host'] for role in ('worker', 'external-worker') for entry in entries.get(role, [])}
    if args.host not in allowed:
        raise DeployError('cleanup host must be an existing transaction-detail query worker')
    request = {'unit': args.unit, 'configuration': args.configuration, 'path': args.path}
    executor = executor or SSHExecutor(inventory)
    executor.check_identity(args.host)
    identifier = new_id('cache-prune', durable.digest(request))
    operation = args.command.removeprefix('cache-storage-')
    with production_lock(inventory, executor) as lock:
        mutation_fence(inventory, executor, lock)
        lock.verify()
        receipt = Path(args.state_dir) / (identifier + '.json')
        remote_receipt = '/opt/wallet-pir/maintenance/' + identifier + '/state.json'
        if operation == 'prune':
            receipt.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            intent = {'transaction': identifier, 'phase': 'deleting', 'host': args.host,
                      'request': request, 'expected_plan_sha256': args.expect_plan_sha256}
            durable.atomic_json(receipt, intent, mode=0o600)
            executor.mkdir(inventory.lock['host'], str(Path(remote_receipt).parent), 0o700)
            executor.write(inventory.lock['host'], remote_receipt, durable.canonical(intent), 0o600)
        code, output = executor.run(args.host, ['python3', '-c', Path(cache_storage_host.__file__).read_text(),
                                               'plan' if operation == 'plan' else 'prune',
                                               json.dumps(request), getattr(args, 'expect_plan_sha256', ''), identifier], 180)
        if code:
            raise DeployError('cache ' + operation + ' refused: ' + output[-1500:])
        result = json.loads(output)
        if operation == 'plan':
            return {'plan': result, 'plan_sha256': durable.digest(result)}
        durable.atomic_json(receipt, result, mode=0o600)
        executor.write(inventory.lock['host'], remote_receipt, durable.canonical(result), 0o600)
        return {'transaction': identifier, **result}

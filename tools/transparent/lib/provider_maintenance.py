"""Pinned recent-provider containment and offline collection through the deploy lock.

Plan/preflight read the unit, its configuration and archive readiness. Quiesce
stops only the pinned recent invocation; collect deletes only an approved exact
manifest while both provider locks are held; resume starts the unchanged unit.
Every effect is journaled. Deleted cache bytes have no rollback; a failed action
leaves the recent worker stopped for inspection rather than restarting retries.
"""
import hashlib
import json
from pathlib import Path
import re
import shlex

from wallet_pir_ops import durable
from wallet_pir_ops.deploy import descriptors, units
from wallet_pir_ops.deploy.engine import DeployError, mutation_fence
from wallet_pir_ops.deploy.remote import production_lock
from wallet_pir_ops.deploy.transaction import FINAL, Journal
from tools.transparent.lib import binary_upgrade

SCHEMA = 'transparent-provider-maintenance/1'
SERVICE = 'transparent-provider-maintenance'
UNIT = 'transparent-v12-recent.service'
SHA = re.compile('[0-9a-f]{64}')


def load_request(path, expected):
    raw = Path(path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected:
        raise DeployError('maintenance request digest differs')
    request = json.loads(raw)
    keys = {'schema', 'git_sha', 'worker', 'action', 'expected', 'retained_publications'}
    pin_keys = {'unit_sha256', 'config_sha256', 'binary_sha256', 'invocation_id'}
    if (set(request) != keys or request['schema'] != SCHEMA
            or not isinstance(request['git_sha'], str) or not re.fullmatch('[0-9a-f]{40}', request['git_sha'])
            or not isinstance(request['worker'], str) or request['action'] not in ('quiesce', 'collect', 'resume')
            or not isinstance(request['expected'], dict) or set(request['expected']) != pin_keys
            or not all(isinstance(request['expected'][key], str) and SHA.fullmatch(request['expected'][key])
                       for key in pin_keys - {'invocation_id'})
            or not (request['expected']['invocation_id'] is None or
                    isinstance(request['expected']['invocation_id'], str) and
                    re.fullmatch('[0-9a-f]{32}', request['expected']['invocation_id']))
            or request['action'] == 'quiesce' and request['expected']['invocation_id'] is None
            or not isinstance(request['retained_publications'], list)
            or not all(isinstance(path, str) and Path(path).is_absolute() for path in request['retained_publications'])):
        raise DeployError('invalid pinned recent-provider maintenance request')
    return request


def unit_config(executor, target):
    state = executor.probe_unit(target.host, target.unit)
    effective = units.effective([state['fragment_text'] or ''] + [d['text'] for d in state['drop_ins']])
    _, binary, tail = units.split_exec(units.exec_start(effective))
    args = shlex.split(tail)
    if args[:1] != ['--config'] or len(args) != 2 or not args[1].startswith('/'):
        raise DeployError('recent provider must run pir-query-server --config FILE')
    raw = executor.read(target.host, args[1])
    section = json.loads(raw)['transparent_history']
    if section.get('enabled') is not True:
        raise DeployError('recent provider History is not enabled')
    settings = section['settings']
    if not settings.get('publisher') or settings.get('full_holdings') is True or not Path(settings['shard_dir']).is_absolute():
        raise DeployError('maintenance supports only the autonomous recent History provider')
    return state, binary, args[1], settings['shard_dir']


class Maintenance:
    def __init__(self, request, request_sha256, executor, inventory, services, state_dir, out, lock=None):
        self.request, self.request_sha256, self.ex = request, request_sha256, executor
        self.inventory, self.state_dir, self.out = inventory, state_dir, out
        workers = descriptors.targets(services['transparent-history'], inventory)
        recent = [t for t in workers if t.key == request['worker'] and t.unit == UNIT]
        archives = [t for t in workers if t.unit == 'transparent-v12-archive.service']
        if len(recent) != 1 or len(archives) != 1:
            raise DeployError('maintenance requires the installed recent and archive worker pair')
        self.target, self.archive = recent[0], archives[0]
        self.lock_factory = lock or (lambda: production_lock(inventory, executor))

    def assess(self):
        upgrade = Journal.load(self.state_dir, 'transparent-history')
        if upgrade and upgrade.status not in FINAL:
            raise DeployError('unfinished History binary transaction ' + upgrade.id)
        target, pin = self.target, self.request['expected']
        state, binary, config, root = unit_config(self.ex, target)
        if (state.get('need_daemon_reload') or binary_upgrade.unit_digest(state) != pin['unit_sha256']
                or self.ex.sha256(target.host, config) != pin['config_sha256']
                or self.ex.sha256(target.host, binary) != pin['binary_sha256']
                or state.get('invocation_id') != pin['invocation_id']):
            raise DeployError('recent unit, configuration, binary or invocation changed')
        archive_state = self.ex.probe_unit(self.archive.host, self.archive.unit)
        status, body = self.ex.http_get(self.archive.host, self.archive.url(self.archive.role.ready_url or self.archive.role.health))
        ready = json.loads(body) if status == 200 else {}
        if (archive_state['active_state'] != 'active' or ready.get('ready') is not True
                or ready.get('prewarm_failed', 0) or ready.get('warm_runtimes') != ready.get('target_runtimes')):
            raise DeployError('archive is not ready; maintenance refused')
        action = self.request['action']
        if action == 'quiesce':
            if state['active_state'] != 'active' or state['exe_sha256'] != pin['binary_sha256']:
                raise DeployError('quiesce requires the exact running predecessor')
            from urllib.parse import urlsplit, urlunsplit
            address = urlsplit(target.url(target.role.ready_url or target.role.health))
            status, body = self.ex.http_get(target.host, urlunsplit((address.scheme, address.netloc, '/v2/shards/init', '', '')))
            if status != 503 or json.loads(body).get('error') != 'publication invalidated by reorg':
                raise DeployError('recent worker is not refusing reorganized history')
        elif state['active_state'] not in ('inactive', 'failed') or state['main_pid']:
            raise DeployError('collection and resume require the recent worker to be stopped')
        plan = {'request_sha256': self.request_sha256, 'worker': target.key, 'action': action, 'root': root,
                'archive_invocation': archive_state.get('invocation_id'), 'expected': pin}
        if action == 'collect':
            plan['collection'] = self.ex.provider_collection(target.host, root, self.request['retained_publications'])
            # Free space is an observation, not an identity that can invalidate a plan.
            plan['collection'].pop('available_bytes', None)
        return plan

    def deploy(self, expected):
        for host in {self.target.host, self.archive.host, self.inventory.lock['host']}:
            self.ex.check_identity(host)
        with self.lock_factory() as lock:
            mutation_fence(self.inventory, self.ex, lock)
            previous = Journal.load(self.state_dir, SERVICE)
            if previous and previous.status not in FINAL:
                raise DeployError('unfinished provider maintenance transaction ' + previous.id)
            plan = self.assess()
            if durable.digest(plan) != expected:
                raise DeployError('maintenance plan changed; nothing was changed')
            record = {'key': self.target.key, 'host': self.target.host, 'unit': UNIT, 'role': 'worker', 'phase': 'pending'}
            journal = Journal.create(self.state_dir, SERVICE, self.request['git_sha'],
                                     {'request_sha256': self.request_sha256, 'plan_sha256': expected}, [record], None)
            journal.data['plan'] = plan
            journal.event('maintenance planned', action=self.request['action'])
            try:
                lock.verify()
                journal.set_phase(0, 'installing')
                if self.request['action'] == 'collect':
                    result = self.ex.provider_collection(self.target.host, plan['root'], self.request['retained_publications'],
                                                         plan['collection']['manifest_sha256'])
                    journal.event('cache collection completed; deleted bytes have no rollback',
                                  available_bytes=result['available_bytes'])
                else:
                    action = 'stop' if self.request['action'] == 'quiesce' else 'start'
                    self.ex.systemctl(self.target.host, action, UNIT)
                    state = self.ex.probe_unit(self.target.host, UNIT)
                    if action == 'stop' and (state['main_pid'] or state['active_state'] not in ('inactive', 'failed')):
                        raise DeployError('recent worker did not stop; inspect the transaction')
                    if action == 'start' and (state['active_state'] != 'active' or state['exe_sha256'] != self.request['expected']['binary_sha256']):
                        raise DeployError('unchanged recent predecessor did not start')
                lock.verify()
                archive = self.ex.probe_unit(self.archive.host, self.archive.unit)
                if archive.get('invocation_id') != plan['archive_invocation']:
                    raise DeployError('archive invocation changed during maintenance')
                journal.set_phase(0, 'verified')
                journal.set_status('committed')
            except Exception:
                journal.set_status('failed')
                raise
            self.out('committed %s: %s; cache deletion has no rollback; resume does not establish readiness'
                     % (journal.id, self.request['action']))


def run(args, executor, out, inventory, services, lock=None, **_options):
    request = load_request(args.request, args.request_sha256)
    operation = Maintenance(request, args.request_sha256, executor, inventory, services, args.state_dir, out, lock)
    action = args.command.removeprefix('transparent-provider-maintenance-')
    if action == 'status':
        latest = Journal.load(args.state_dir, SERVICE)
        out(json.dumps({'transaction': latest.id if latest else None, 'status': latest.status if latest else None,
                        'worker': executor.probe_unit(operation.target.host, UNIT)}, sort_keys=True))
    elif action == 'deploy':
        operation.deploy(args.expect_plan_sha256)
    else:
        if action == 'preflight':
            for host in {operation.target.host, operation.archive.host}:
                executor.check_identity(host)
            mutation_fence(inventory, executor)
        plan = operation.assess()
        if args.output:
            durable.atomic_json(args.output, plan, mode=0o600)
        out('plan_sha256: ' + durable.digest(plan))
        collection = plan.get('collection', {}).get('manifest')
        if collection:
            out('collection: %d paths, %d allocated bytes reclaimable; retained %s; deletion has no rollback'
                % (len(collection['delete']), collection['reclaimable_bytes'], ', '.join(collection['retained'])))
        else:
            out('%s: %s only; archive stays running' % (request['action'], request['worker']))
    return 0

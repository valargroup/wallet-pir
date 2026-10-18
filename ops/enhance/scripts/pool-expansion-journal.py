#!/usr/bin/env python3
"""Persist individual-worker expansion intent; provisioning never implies readiness.

Run under the existing production infrastructure lock. An interrupted apply stays
ambiguous until provider inventory and Terraform state are reconciled. This tool
records reviewed receipts and never dispatches CI or modifies a worker inventory.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def atomic(path, value):
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix='.pool-')
    try:
        with os.fdopen(fd, 'w') as file:
            json.dump(value, file, sort_keys=True, indent=2)
            file.write('\n'); file.flush(); os.fsync(file.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(directory)
        finally: os.close(directory)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)


def observe(state, health, policy, inventory):
    if health.get('protocol') != 'ironwood-enhance-pir-v7' or health.get('pool') is None:
        raise ValueError('requires pooled coordinator health')
    capacity = health['capacity']
    request_id = capacity.get('requested_worker')
    if request_id is None:
        return state
    request = capacity['worker_requests'][request_id]
    workers = [r for g in inventory.get('groups', []) for r in g['replicas']] + inventory.get('workers', [])
    if len({r['name'] for r in workers}) != len(workers) or len({r['url'] for r in workers}) != len(workers):
        raise ValueError('duplicate inventory worker')
    if request['target_workers'] != len(workers) + 1:
        raise ValueError('demand must add exactly one worker')
    operation = state.get('operation')
    identity = {'id': request_id, 'target_workers': request['target_workers'],
                'policy_sha256': digest(policy), 'inventory_sha256': digest(inventory)}
    if operation:
        if any(operation[key] != value for key, value in identity.items()):
            raise ValueError('pending operation identity changed')
        return state
    if request_id in state.get('completed', {}):
        return state
    return {**state, 'operation': {**identity, 'phase': 'observed'}}


def advance(state, phase, receipt):
    op = state.get('operation')
    if not op:
        raise ValueError('no pending demand')
    transitions = {'planned': 'observed', 'apply_intent': 'planned', 'provisioned': 'apply_intent',
                   'bootstrapped': 'provisioned', 'qualified': 'bootstrapped', 'registered': 'qualified'}
    if phase not in transitions or op['phase'] not in (transitions[phase], phase):
        raise ValueError('invalid or ambiguous provisioning transition')
    if receipt.get('operation_id') != op['id'] or receipt.get('policy_sha256') != op['policy_sha256']:
        raise ValueError('receipt identity differs')
    if phase in ('planned', 'apply_intent'):
        sha = receipt.get('plan_sha256', '')
        if len(sha) != 64 or any(c not in '0123456789abcdef' for c in sha):
            raise ValueError('missing reviewed plan digest')
        if phase == 'apply_intent' and sha != op['receipts']['planned']['plan_sha256']:
            raise ValueError('saved plan changed before apply')
    if phase == 'provisioned' and not (receipt.get('terraform_reconciled') is True and receipt.get('provider_reconciled') is True):
        raise ValueError('reconcile both state and provider before resuming')
    if phase in ('provisioned', 'bootstrapped', 'qualified', 'registered'):
        if type(receipt.get('droplet_id')) is not int or receipt['droplet_id'] <= 0:
            raise ValueError('missing provider identity')
        if phase != 'provisioned' and receipt['droplet_id'] != op['receipts']['provisioned']['droplet_id']:
            raise ValueError('worker was replaced')
    if phase == 'qualified' and receipt.get('qualification') != 'qualified':
        raise ValueError('bootstrap and short-test receipts cannot authorize registration')
    receipts = dict(op.get('receipts', {}))
    if phase in receipts and receipts[phase] != receipt:
        raise ValueError('conflicting phase receipt')
    receipts[phase] = receipt
    updated = {**op, 'phase': phase, 'receipts': receipts}
    if phase == 'registered':
        return {**state, 'operation': None, 'completed': {**state.get('completed', {}), op['id']: updated}}
    return {**state, 'operation': updated}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=Path, required=True)
    sub = parser.add_subparsers(dest='command', required=True)
    observer = sub.add_parser('observe')
    for name in ('health', 'policy', 'inventory'): observer.add_argument('--' + name, type=Path, required=True)
    update = sub.add_parser('advance')
    update.add_argument('--phase', required=True)
    update.add_argument('--receipt', type=Path, required=True)
    args = parser.parse_args()
    args.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (args.state_dir / 'journal.lock').open('a+') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        path = args.state_dir / 'journal.json'
        state = json.loads(path.read_text()) if path.exists() else {'version': 1, 'operation': None, 'completed': {}}
        if state.get('version') != 1: raise ValueError('unknown journal version')
        if args.command == 'observe':
            state = observe(state, *(json.loads(getattr(args, key).read_text()) for key in ('health', 'policy', 'inventory')))
        else: state = advance(state, args.phase, json.loads(args.receipt.read_text()))
        atomic(path, state)
        print(json.dumps({'operation': (state.get('operation') or {}).get('id'), 'phase': (state.get('operation') or {}).get('phase')}))

if __name__ == '__main__':
    main()

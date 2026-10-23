"""Repair an interrupted publisher's storage sandbox without changing its release.

The plan binds the exact old and new unit bytes. Application records the repair
in the existing transaction before stopping the candidate, installing the unit,
and starting the same executable. Failures remain available for forward recovery;
this operation never automatically rolls back the publication roots.
"""
from .. import durable
from .engine import mutation_fence
from .publisher import SERVING_SCHEMA, UNIT, UNIT_PATH, require
from .publisher_products import storage_sandbox
from .remote import production_lock


def plan(owner, journal):
    require(journal is not None and journal['request'] == owner.request
            and journal['phase'] == 'verifying' and owner.request['schema'] == SERVING_SCHEMA
            and not owner.resume_load, 'sandbox repair requires this interrupted verification with checker stopped')
    owner.resources()
    owner.require_stopped_load()
    owner.serving.restore_guard(journal)
    saved = journal.get('sandbox_repair')
    require(journal['intent'] is None or saved and journal['intent'].startswith('sandbox repair '),
            'another transaction effect is pending')
    before = saved['before'] if saved else journal['plan']['desired'][UNIT_PATH]
    state = owner.ex.probe_unit(owner.host, UNIT)
    drop_ins = state['drop_ins']
    expected_drop_ins = [dict(entry, text=journal['plan']['desired'].get(entry['path'], entry['text']))
                         for entry in journal['plan']['units'][UNIT]['drop_ins']]
    require(drop_ins == expected_drop_ins, 'publisher drop-ins changed before sandbox repair')
    after = storage_sandbox(before, owner.request['configuration'], [d['text'] for d in drop_ins])
    require(before != after, 'publisher already has the required storage write permissions')
    repair = {'before': before, 'after': after, 'drop_ins': drop_ins,
              'request_sha256': durable.digest(owner.request)}
    require(saved is None or saved == repair, 'recorded sandbox repair differs')
    for path, text in journal['plan']['desired'].items():
        allowed = (before, after) if saved and path == UNIT_PATH else (text,)
        require(owner.read(path) in allowed, 'managed file changed before sandbox repair: ' + path)
    require(not state['main_pid'] and saved is not None
            or state['active_state'] == 'active' and state['exe_sha256'] == owner.request['release']['binary_sha256'],
            'sandbox repair requires the recorded candidate executable')
    return repair


def dispatch(owner, args):
    with production_lock(owner.inventory, owner.ex) as owner.lock:
        journal = owner.latest()
        repair = plan(owner, journal)
        digest = durable.digest(repair)
        if args.command.endswith('-plan'):
            return {'plan_sha256': digest, 'repair': repair}
        require(digest == args.expect_plan_sha256, 'sandbox repair plan is stale')
        mutation_fence(owner.inventory, owner.ex, owner.lock)
        journal['sandbox_repair'] = repair
        journal['plan']['desired'][UNIT_PATH] = repair['after']
        owner.save(journal)
        def step(name, action):
            name = 'sandbox repair ' + name
            if name not in journal['completed']:
                owner.effect(journal, name, action)
        step('stop publisher', lambda: owner.stop(UNIT))
        starting = journal['intent'] == 'sandbox repair start publisher'
        if ('sandbox repair start publisher' not in journal['completed']
                and not (starting and owner.ex.probe_unit(owner.host, UNIT)['main_pid'])):
            owner.unlocked()
        step('install unit', lambda: owner.write(UNIT_PATH, repair['after'], 0o644))
        step('reload services', lambda: owner.ex.systemctl(owner.host, 'daemon-reload'))
        step('start publisher', lambda: owner.ex.systemctl(owner.host, 'start', UNIT))
    return owner.verify()

"""Cross-operation fence for an interrupted coordinator schema transaction.

Read under the production lock before any unrelated mutation. This durable
pointer survives SSH loss; a live descriptor alone cannot fence remote owners
once their coordinator has died. Only schema recovery may use an unfinished ID.
The module is stdlib-only so source bootstrap can carry the exact same check.
"""
import json
import hashlib
from pathlib import Path
import re

SCHEMA_STATE = Path('/srv/transparent-activity/ops/schema')
SCHEMA_POINTER = 'latest-transparent-schema.json'
HOST_ACTIONS = Path('/srv/transparent-activity/ops/host-actions')
INPUT_STAGING = Path('/srv/transparent-activity/ops/input-staging')


def schema_mutation_fence(read, state=SCHEMA_STATE, *, skip_input=None, recovery=None):
    raw = read(str(Path(state)/SCHEMA_POINTER))
    if raw is not None:
        pointer = json.loads(raw)
        identifier = pointer.get('id')
        if set(pointer) != {'id'} or not isinstance(identifier, str) or not re.fullmatch(r'transparent-schema-[A-Za-z0-9-]+', identifier):
            raise ValueError('invalid schema ownership pointer; reconcile before mutation')
        raw = read(str(Path(state)/(identifier+'.json')))
        if raw is None:
            raise ValueError('missing schema ownership record; reconcile before mutation')
        record = json.loads(raw)
        reconciled = (record.get('status') == 'reconciled-v10' and
                      record.get('v10_reconciliation', {}).get('status') == 'passed' and
                      record.get('events') and len(record['events']) >= 5 and
                      [e.get('name') for e in record['events'][-5:]] ==
                      ['withdraw-origins','restore-v10','verify-rollback','reopen-v10','verify-service'] and
                      all(e.get('group') == 'rollback' and e.get('status') == 'passed' for e in record['events'][-5:]))
        recovering = (isinstance(recovery, dict) and set(recovery) == {'transaction', 'recipe_sha256'} and
                      recovery['transaction'] == identifier and
                      record.get('recipe_sha256') == recovery['recipe_sha256'] and
                      isinstance(record.get('recipe'), dict) and
                      hashlib.sha256(json.dumps(record['recipe'], sort_keys=True, separators=(',', ':')).encode()).hexdigest() == recovery['recipe_sha256'] and
                      record.get('status') in ('interrupted', 'rollback-failed') and
                      not any(e.get('status') == 'running' for e in record.get('events', [])))
        if recovery is not None and not recovering:
            raise ValueError('recovery source request differs from the failed transaction')
        if (record.get('journal_version') != 1 or record.get('id') != identifier or
                record.get('status') not in ('committed', 'rolled-back') and not recovering and not reconciled):
            raise ValueError('unfinished schema transaction; reconcile its remote owners and recover before mutation')
    elif recovery is not None:
        raise ValueError('recovery source staging requires an existing transaction')
    raw = read(str(HOST_ACTIONS/'latest.json'))
    if raw is not None:
        pointer = json.loads(raw)
        if (set(pointer) != {'transaction', 'request_id'} or
                not isinstance(pointer['transaction'], str) or not re.fullmatch(r'transparent-schema-[A-Za-z0-9-]+', pointer['transaction']) or
                not isinstance(pointer['request_id'], str) or not re.fullmatch('[a-z0-9-]{1,64}', pointer['request_id'])):
            raise ValueError('invalid remote host ownership pointer')
        raw = read(str(HOST_ACTIONS/pointer['transaction']/(pointer['request_id']+'.json')))
        if raw is None or json.loads(raw).get('status') not in ('passed', 'failed', 'reconciled'):
            raise ValueError('unfinished remote host owner; reconcile before mutation')

    raw = read(str(INPUT_STAGING/'latest.json'))
    if raw is not None:
        pointer = json.loads(raw)
        identifier = pointer.get('request_sha256')
        if set(pointer) != {'request_sha256'} or not isinstance(identifier,str) or not re.fullmatch('[0-9a-f]{64}',identifier):
            raise ValueError('invalid input staging ownership pointer')
        raw = read(str(INPUT_STAGING/(identifier+'.json')))
        if raw is None:
            raise ValueError('missing input staging owner; reconcile before mutation')
        record = json.loads(raw)
        if record.get('request_sha256') != identifier:
            raise ValueError('input staging owner identity changed')
        if record.get('status') not in ('staged','reconciled') and skip_input != identifier:
            raise ValueError('unfinished input staging owner; reconcile before mutation')


def local_schema_fence(state=SCHEMA_STATE, *, skip_input=None, recovery=None):
    def read(path):
        target = Path(path)
        if target.is_symlink():
            raise ValueError('schema ownership record cannot be a symlink')
        if not target.exists():
            return None
        if target.stat().st_size > 1024*1024:
            raise ValueError('schema ownership record exceeds bound')
        return target.read_text()
    schema_mutation_fence(read, state, skip_input=skip_input, recovery=recovery)

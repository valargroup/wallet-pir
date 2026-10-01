"""Locked, journaled execution of the reviewed Transparent schema-cutover recipe.

This coordinates existing deployment programs; it does not infer their success
from startup or authorize a cutover without their publication/correctness gates.
Recipes contain paths and public identities, never credentials. Run through
ops/scripts/wallet-pir-deploy.py on the inventory's pinned coordinator.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time

from wallet_pir_ops import durable, schema_fence
from wallet_pir_ops.deploy.remote import ProductionLock
from wallet_pir_ops.deploy.transaction import new_id

FORWARD = ('preserve-v10', 'maintenance', 'stage-v11', 'activate-prewarm',
           'align-origins', 'verify-canonical', 'resume-load', 'verify-service')
ROLLBACK = ('withdraw-origins', 'restore-v10', 'verify-rollback', 'reopen-v10', 'verify-service')
SERVICE = 'transparent-schema'
HEX = re.compile(r'^[0-9a-f]+$')
IDENTIFIER = re.compile(r'^transparent-schema-[A-Za-z0-9-]+$')
MAX_RECIPE = 256 * 1024


class OperationError(ValueError):
    pass


def require(ok, message):
    if not ok:
        raise OperationError(message)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def file_hash(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def validate(recipe):
    fields = {'version', 'source_sha', 'publication_sha256', 'inputs', 'rollback_inputs',
              'preflight', 'steps', 'rollback'}
    require(isinstance(recipe, dict) and set(recipe) == fields, 'invalid recipe fields')
    require(type(recipe['version']) is int and recipe['version'] == 1, 'unsupported recipe version')
    for field, length in [('source_sha', 40), ('publication_sha256', 64)]:
        value = recipe[field]
        require(isinstance(value, str) and len(value) == length and HEX.fullmatch(value), 'invalid '+field)
    input_paths = {}
    for group in ('inputs', 'rollback_inputs'):
        entries = recipe[group]
        require(isinstance(entries, list) and 1 <= len(entries) <= 128, 'invalid '+group)
        paths = set()
        for entry in entries:
            require(isinstance(entry, dict) and set(entry) == {'path', 'sha256'}, 'invalid input entry')
            path, sha = entry['path'], entry['sha256']
            require(isinstance(path, str) and Path(path).is_absolute() and '\x00' not in path, 'invalid input path')
            require(isinstance(sha, str) and len(sha) == 64 and HEX.fullmatch(sha), 'invalid input checksum')
            require(path not in paths, 'duplicate input path')
            paths.add(path)
        input_paths[group] = paths
    for group in ('preflight', 'steps', 'rollback'):
        commands = recipe[group]
        require(isinstance(commands, list) and 1 <= len(commands) <= 32, 'invalid '+group)
        names = []
        allowed = input_paths['rollback_inputs' if group == 'rollback' else 'inputs']
        for command in commands:
            require(isinstance(command, dict) and set(command) == {'name', 'argv', 'timeout', 'read_only'}, 'invalid command')
            name, argv = command['name'], command['argv']
            require(isinstance(name, str) and re.fullmatch('[a-z][a-z0-9-]{0,63}', name), 'invalid phase name')
            require(isinstance(argv, list) and 1 <= len(argv) <= 64 and all(
                isinstance(a, str) and len(a) <= 8192 and '\x00' not in a for a in argv), 'invalid argv')
            require(Path(argv[0]).is_absolute(), 'command executable must be absolute')
            # No inline shell/Python: the invoked program must be a checksum-bound file.
            if argv[0] in ('/usr/bin/python3', '/usr/bin/bash', '/bin/bash'):
                require(len(argv) >= 2 and argv[1] in allowed, 'interpreter needs a checksum-bound script')
            else:
                require(argv[0] in allowed, 'command needs a checksum-bound executable')
            require(type(command['timeout']) is int and 1 <= command['timeout'] <= 1800, 'invalid timeout')
            require(type(command['read_only']) is bool, 'read_only must be boolean')
            if group == 'preflight':
                require(command['read_only'], 'preflight commands must be read-only')
            names.append(name)
        require(len(set(names)) == len(names), 'duplicate phase name')
        if group == 'steps':
            require(tuple(names) == FORWARD, 'cutover must contain every forward phase in order')
        if group == 'rollback':
            require(tuple(names) == ROLLBACK, 'rollback must contain every recovery phase in order')
            require(sum(c['timeout'] for c in commands) <= 900, 'rollback deadlines exceed 15 minutes')
    for group, name in [('steps', 'verify-canonical'), ('steps', 'verify-service'),
                        ('rollback', 'verify-rollback'), ('rollback', 'verify-service')]:
        require(next(c for c in recipe[group] if c['name'] == name)['read_only'], name+' must be read-only')
    return recipe


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate JSON key')
        result[key] = value
    return result


def load_recipe(path):
    with Path(path).open('rb') as stream:
        data = stream.read(MAX_RECIPE + 1)
    require(len(data) <= MAX_RECIPE, 'recipe exceeds size bound')
    return validate(json.loads(data, object_pairs_hook=unique_object))


def verify_inputs(entries):
    for entry in entries:
        path = Path(entry['path'])
        require(path.is_file() and not path.is_symlink(), 'input is missing or a symlink: '+str(path))
        require(file_hash(path) == entry['sha256'], 'input checksum changed: '+str(path))


def run_command(command, log, pass_fds):
    result = subprocess.run(command['argv'], timeout=command['timeout'],
                            stdout=log, stderr=subprocess.STDOUT, pass_fds=pass_fds,
                            env=dict(os.environ, PYTHONDONTWRITEBYTECODE='1',
                                     WALLET_PIR_PRODUCTION_LOCK_FDS=','.join(map(str, pass_fds))))
    return result.returncode


class Runner:
    def __init__(self, inventory, state_dir, run=run_command, lock_factory=None, out=print):
        self.inventory = inventory
        self.state_dir = Path(state_dir)
        self.run = run
        self.lock_factory = lock_factory or (lambda: ProductionLock(inventory.lock))
        self.out = out
        self.active_id = None

    def coordinator(self):
        require(self.state_dir == Path('/srv/transparent-activity/ops/schema'), 'production schema journal must use the shared fenced state directory')
        require(self.inventory.lock.get('type') == 'pinned_host', 'schema operations must run on the pinned coordinator')
        require(os.geteuid() == 0 and ProductionLock.MACHINE_ID.read_text().strip() == self.inventory.lock['machine_id'],
                'schema operation is not on the pinned root coordinator')

    def plan(self, recipe):
        validate(recipe)
        self.out('recipe '+digest(recipe))
        self.out('source '+recipe['source_sha']+' publication '+recipe['publication_sha256'])
        for group in ('preflight', 'steps', 'rollback'):
            self.out(group+': '+', '.join(c['name'] for c in recipe[group]))
        self.out('rollback timeout budget: '+str(sum(c['timeout'] for c in recipe['rollback']))+' seconds')

    def preflight(self, recipe, lock=None):
        validate(recipe)
        self.coordinator()
        verify_inputs(recipe['inputs'])
        verify_inputs(recipe['rollback_inputs'])
        for command in recipe['preflight']:
            if lock:
                lock.verify()
            with tempfile.TemporaryFile() as log:
                code = self.run(command, log, lock.descriptors() if lock else ())
            require(code == 0, 'preflight failed: '+command['name'])
        self.out('preflight passed for recipe '+digest(recipe))

    def save(self, record):
        durable.atomic_json(self.state_dir/(record['id']+'.json'), record, mode=0o600)

    def load(self, identifier=None):
        if identifier is None:
            pointer = self.state_dir/('latest-'+SERVICE+'.json')
            if not pointer.exists():
                return None
            identifier = json.loads(pointer.read_text())['id']
        require(isinstance(identifier, str) and IDENTIFIER.fullmatch(identifier), 'invalid transaction identifier')
        record = json.loads((self.state_dir/(identifier+'.json')).read_text())
        require(record.get('journal_version') == 1, 'unsupported schema journal version')
        require(record['id'] == identifier and digest(validate(record['recipe'])) == record['recipe_sha256'], 'transaction recipe changed')
        return record

    def execute(self, record, group, lock):
        entries = record['recipe']['rollback_inputs' if group == 'rollback' else 'inputs']
        directory = self.state_dir/record['id']/group
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        started = time.monotonic()
        budget = sum(c['timeout'] for c in record['recipe'][group])
        for index, original in enumerate(record['recipe'][group]):
            lock.verify()
            verify_inputs(entries)
            command = dict(original)
            command['argv'] = [a.replace('{transaction}', record['id']).replace(
                '{journal}', str(self.state_dir/(record['id']+'.json'))) for a in command['argv']]
            if group == 'rollback':
                command['timeout'] = min(command['timeout'], int(budget-(time.monotonic()-started)))
                require(command['timeout'] > 0, 'rollback exceeded its time budget')
            phase = {'group': group, 'name': command['name'], 'status': 'running', 'started': time.time()}
            record['events'].append(phase)
            self.save(record)  # Durable intent before every side effect.
            log_path = directory/('%02d-%s-%d.log' % (index, command['name'], len(record['events'])))
            fd = os.open(log_path, os.O_WRONLY|os.O_CREAT|os.O_EXCL, 0o600)
            phase_started = time.monotonic()
            try:
                with os.fdopen(fd, 'wb') as log:
                    code = self.run(command, log, lock.descriptors())
                if code == 75:
                    # The child has a durable uncertain remote outcome. A normal
                    # nonzero path would race it with automatic recovery.
                    raise subprocess.TimeoutExpired('schema descendant requires reconciliation', command['timeout'])
                phase.update(exit_code=code, status='passed' if code == 0 else 'failed')
            except BaseException as error:
                phase['status'] = 'failed' if isinstance(error, Exception) else 'interrupted'
                phase['error_type'] = type(error).__name__
                raise
            finally:
                phase.update(seconds=time.monotonic()-phase_started, log=str(log_path))
                self.save(record)
            require(code == 0, 'phase failed: '+command['name'])
            lock.verify()

    def recover(self, record, lock):
        record['status'] = 'rolling-back'
        self.save(record)
        try:
            self.execute(record, 'rollback', lock)
        except BaseException:
            record['status'] = 'rollback-failed'
            self.save(record)
            raise
        record['status'] = 'rolled-back'
        self.save(record)

    def deploy(self, recipe, expected):
        require(digest(validate(recipe)) == expected, 'recipe differs from reviewed plan digest')
        self.coordinator()
        with self.lock_factory() as lock:
            lock.verify()
            schema_fence.local_schema_fence()
            previous = self.load()
            require(previous is None or previous['status'] in ('committed', 'rolled-back'),
                    'unfinished schema transaction; recover it before deploying')
            self.preflight(recipe, lock)
            identifier = new_id(SERVICE, expected)
            self.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
            record = {'journal_version': 1, 'id': identifier, 'recipe': recipe, 'recipe_sha256': expected,
                      'driver_sha256': file_hash(Path(__file__)),
                      'status': 'applying', 'created': time.time(), 'events': []}
            self.save(record)
            self.active_id = identifier
            durable.atomic_json(self.state_dir/('latest-'+SERVICE+'.json'), {'id': identifier}, mode=0o600)
            self.out('transaction '+identifier)
            try:
                self.execute(record, 'steps', lock)
            except subprocess.TimeoutExpired:
                # A descendant may still hold the inherited lock or be completing
                # remote work. Do not race it with automatic rollback.
                record['status'] = 'interrupted'
                self.save(record)
                raise OperationError('schema phase timed out; reconcile and recover the recorded transaction') from None
            except Exception:
                # A lost/replaced lock forbids further mutations, including rollback.
                lock.verify()
                self.recover(record, lock)
                raise
            except BaseException:
                record['status'] = 'interrupted'
                self.save(record)
                raise
            record['status'] = 'committed'
            self.save(record)
            self.out('committed '+identifier)
            return record

    def rollback(self, identifier=None):
        self.coordinator()
        with self.lock_factory() as lock:
            lock.verify()
            record = self.load(identifier)
            require(record is not None, 'no schema transaction recorded')
            latest = self.load()
            require(latest['id'] == record['id'], 'recover the latest schema transaction first')
            if record['status'] != 'rolled-back':
                self.recover(record, lock)
            self.out('rolled-back '+record['id'])
            return record

    def status(self, identifier=None):
        record = self.load(identifier)
        self.out('no schema transaction recorded' if record is None else record['id']+': '+record['status'])
        self.out('status is the journal state; live canonical checks remain required')

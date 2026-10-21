"""Locked repair of the stopped History continuous exact-answer load.

`transparent-v2-5qps-continuous.service` runs `transparent-quality-load.py`
from an immutable source tree and does not restart itself. When that script is
at fault, this operation installs the reviewed script and starts only that unit.
It writes nothing else:

- The request pins the unit fragment (no drop-ins), the script it runs now, the
  load's `pins.json`, `rate-query` and `fixture.json`, and each pinned worker. Every
  pinned worker must be running its pinned binary.
- The script comes from this operations release (`check_installed` binds the
  release to `script.git_sha`). It is copied to the immutable
  `<SCRIPTS>/<sha256>/transparent-quality-load.py`. The new unit is the old
  fragment with only the script path changed. Its arguments, resource limits,
  pins, permit, incident history and thresholds stay as they are.
- Under the production lock: refuse a critical latch, a pinned binary that
  changed, or an unfinished transaction. Stage the script, then run the request's
  independent encrypted-answer check. Only then write the unit and start it, and
  wait until the load reports `running`.
- Every effect is journaled together with the previous unit bytes. The old script
  stays where it was. A failure, or `rollback`, stops the unit and restores the
  previous fragment, which leaves the load stopped as it was before. A latch or
  incident record is never removed.
"""
import hashlib
import json
import os
import re
import shlex
import sys
import time

from wallet_pir_ops import durable
from wallet_pir_ops.deploy import units
from wallet_pir_ops.deploy.engine import DeployError, mutation_fence, text_sha256
from wallet_pir_ops.deploy.remote import production_lock
from wallet_pir_ops.deploy.transaction import FINAL, Journal
from wallet_pir_ops.paths import ROOT

SCHEMA = 'transparent-load-repair/1'
SERVICE = 'transparent-load-repair'
UNIT = 'transparent-v2-5qps-continuous.service'
FRAGMENT = '/etc/systemd/system/' + UNIT
LOAD_ROOT = '/srv/transparent-activity/canonical-load/v12'
SCRIPTS = '/srv/transparent-activity/ops/load-scripts'
SCRIPT_NAME = 'transparent-quality-load.py'
SOURCE = ROOT / 'tools/transparent/scripts' / SCRIPT_NAME
PYTHON = '/usr/bin/python3'
SHA256 = re.compile('^[0-9a-f]{64}$')
# Two healthy 15-second ticks after rate-query reports ready.
VERIFY_SECONDS = 300
REQUEST_KEYS = {'schema', 'host', 'previous', 'script', 'load', 'workers', 'exact_check'}


def require(condition, message):
    if not condition:
        raise DeployError('invalid load repair request: ' + message)


def is_sha256(value):
    return isinstance(value, str) and SHA256.fullmatch(value) is not None


def load_request(path, expected_sha256):
    with open(path, 'rb') as handle:
        data = handle.read()
    if hashlib.sha256(data).hexdigest() != expected_sha256:
        raise DeployError('request checksum differs from --request-sha256')
    return validate(json.loads(data))


def validate(request):
    require(isinstance(request, dict) and set(request) == REQUEST_KEYS, 'keys are %s' % sorted(REQUEST_KEYS))
    require(request['schema'] == SCHEMA, 'schema must be ' + SCHEMA)
    require(isinstance(request['host'], str) and request['host'], 'host is the coordinator inventory host')
    previous, script, load = request['previous'], request['script'], request['load']
    require(isinstance(previous, dict) and set(previous) == {'unit_sha256', 'script_path', 'script_sha256'}
            and is_sha256(previous['unit_sha256']) and is_sha256(previous['script_sha256'])
            and isinstance(previous['script_path'], str) and previous['script_path'].startswith('/')
            and os.path.basename(previous['script_path']) == SCRIPT_NAME,
            'previous is {unit_sha256, script_path (absolute, %s), script_sha256}' % SCRIPT_NAME)
    require(isinstance(script, dict) and set(script) == {'git_sha', 'sha256'} and is_sha256(script['sha256'])
            and isinstance(script['git_sha'], str) and re.fullmatch('[0-9a-f]{40}', script['git_sha']) is not None,
            'script is {git_sha (40 lowercase hex), sha256}')
    require(script['sha256'] != previous['script_sha256'], 'script.sha256 is the script already installed')
    require(isinstance(load, dict) and set(load) == {'pins_sha256', 'rate_query_sha256', 'fixture_sha256'}
            and all(is_sha256(v) for v in load.values()), 'load pins pins_sha256, rate_query_sha256, fixture_sha256')
    workers = request['workers']
    require(isinstance(workers, dict) and workers and all(
        isinstance(w, dict) and set(w) == {'host', 'unit'} and all(isinstance(v, str) and v for v in w.values())
        for w in workers.values()), 'workers maps each pinned worker id to {host, unit}')
    check = request['exact_check']
    require(isinstance(check, dict) and set(check) == {'host', 'argv', 'timeout'}
            and isinstance(check['host'], str) and check['host'] and isinstance(check['argv'], list) and check['argv']
            and all(isinstance(a, str) and a for a in check['argv'])
            and type(check['timeout']) is int and 1 <= check['timeout'] <= 900,
            'exact_check is {host, argv, timeout (1..900)}: the independent encrypted-answer probe')
    return request


def script_path(sha):
    return '%s/%s/%s' % (SCRIPTS, sha, SCRIPT_NAME)


def repaired_text(text, old_path, new_path):
    """The fragment with only its ExecStart script replaced; raises if it is not `python3 -B <old_path> ...`."""
    _, binary, tail = units.split_exec(units.exec_start(units.effective([text])))
    words = tail.split()
    if binary != PYTHON or words[:2] != ['-B', old_path]:
        raise DeployError('ExecStart is not %s -B %s ...' % (PYTHON, old_path))
    if text.count(old_path) != 1:
        raise DeployError('the unit names the script path more than once')
    return text.replace(old_path, new_path)


class Repair:
    def __init__(self, request, request_sha256, ex, inventory, state_dir, out=print, lock=None,
                 clock=time.monotonic, sleep=time.sleep, poll_seconds=5.0):
        self.request, self.request_sha256, self.ex, self.inventory = request, request_sha256, ex, inventory
        self.state_dir, self.out, self.clock, self.sleep, self.poll_seconds = state_dir, out, clock, sleep, poll_seconds
        self.host = request['host']
        self.lock_factory = lock or (lambda: production_lock(inventory, ex))
        self.lock = None

    def hosts(self):
        return list(dict.fromkeys([self.host, self.request['exact_check']['host'], self.inventory.lock.get('host')]
                                  + [w['host'] for w in self.request['workers'].values()]))

    def check_identities(self):
        for host in self.hosts():
            if host:
                try:
                    self.ex.check_identity(host)
                except Exception as error:
                    raise DeployError('deploy identity is not accepted by %s; nothing was changed (%s)' % (host, error)) from error

    def guard_problems(self):
        """No critical latch, and the load's inputs and every pinned worker binary unchanged."""
        load, problems = self.request['load'], []
        if self.ex.sha256(self.host, LOAD_ROOT + '/latched.json') is not None:
            problems.append('continuous load has an unresolved critical latch (%s/latched.json)' % LOAD_ROOT)
        for name, key in (('pins.json', 'pins_sha256'), ('rate-query', 'rate_query_sha256'), ('fixture.json', 'fixture_sha256')):
            if self.ex.sha256(self.host, '%s/%s' % (LOAD_ROOT, name)) != load[key]:
                problems.append('%s/%s differs from load.%s' % (LOAD_ROOT, name, key))
        try:
            pins = json.loads(self.ex.read(self.host, LOAD_ROOT + '/pins.json') or 'null')
            if not isinstance(pins, dict) or set(pins) != set(self.request['workers']):
                return problems + ['load pins %s, request workers are %s'
                                   % (sorted(pins) if isinstance(pins, dict) else pins, sorted(self.request['workers']))]
        except ValueError as error:
            return problems + ['load pins.json is unreadable: %s' % error]
        for worker_id, target in sorted(self.request['workers'].items()):
            state = self.ex.probe_unit(target['host'], target['unit'])
            if state['active_state'] != 'active' or state['exe_sha256'] != pins[worker_id]:
                problems.append('%s (%s on %s) is %s running %s; the load pins %s' % (
                    worker_id, target['unit'], target['host'], state['active_state'], state['exe_sha256'], pins[worker_id]))
        return problems

    def assess(self):
        """`(plan, new unit text, unit state, problems)`; the plan names the bytes a deploy installs."""
        previous, script = self.request['previous'], self.request['script']
        state = self.ex.probe_unit(self.host, UNIT)
        problems, new_text = [], None
        if state['load_state'] != 'loaded' or state['fragment_path'] != FRAGMENT or state['drop_ins']:
            problems.append('%s must be loaded from %s alone (no drop-ins)' % (UNIT, FRAGMENT))
        elif text_sha256(state['fragment_text']) != previous['unit_sha256']:
            problems.append('%s differs from previous.unit_sha256' % FRAGMENT)
        else:
            try:
                new_text = repaired_text(state['fragment_text'], previous['script_path'], script_path(script['sha256']))
            except (DeployError, units.UnitError) as error:
                problems.append(str(error))
        if state['active_state'] == 'active':
            problems.append('%s is running; this repairs only a stopped load' % UNIT)
        if self.ex.sha256(self.host, previous['script_path']) != previous['script_sha256']:
            problems.append('%s differs from previous.script_sha256 (it is kept for rollback)' % previous['script_path'])
        if hashlib.sha256(SOURCE.read_bytes()).hexdigest() != script['sha256']:
            problems.append('this operations release\'s %s does not hash to script.sha256' % SCRIPT_NAME)
        if self.ex.sha256(self.host, script_path(script['sha256'])) not in (None, script['sha256']):
            problems.append('immutable %s holds different bytes' % script_path(script['sha256']))
        problems += self.guard_problems()
        plan = {'request_sha256': self.request_sha256, 'host': self.host, 'unit': UNIT,
                'previous_unit_sha256': previous['unit_sha256'], 'new_unit_sha256': text_sha256(new_text),
                'script_path': script_path(script['sha256']), 'script_sha256': script['sha256'],
                'exact_check': self.request['exact_check']}
        return plan, new_text, state, problems

    def describe(self, plan, state):
        self.out('%s on %s: %s; ExecStart script %s -> %s'
                 % (UNIT, self.host, state['active_state'], self.request['previous']['script_path'], plan['script_path']))
        self.out('unit %s -> %s; script %s from git %s'
                 % (plan['previous_unit_sha256'], plan['new_unit_sha256'], plan['script_sha256'], self.request['script']['git_sha']))
        self.out('plan_sha256: ' + durable.digest(plan))

    def exact_check(self, journal=None):
        check = self.request['exact_check']
        if journal:
            journal.event('exact check', host=check['host'], argv=check['argv'])
        code, output = self.ex.run(check['host'], check['argv'], check['timeout'])
        if journal:
            journal.event('exact check finished', returncode=code, output=output[-2000:])
        if code:
            raise DeployError('independent encrypted-answer check failed (exit %d): %s' % (code, output[-1000:]))
        self.out('independent encrypted-answer check passed')

    def locked(self, action):
        self.check_identities()
        with self.lock_factory() as lock:
            self.lock = lock
            try:
                mutation_fence(self.inventory, self.ex, lock)
                return action()
            finally:
                self.lock = None

    def deploy(self, expect_plan_sha256):
        return self.locked(lambda: self._deploy(expect_plan_sha256))

    def _deploy(self, expect_plan_sha256):
        latest = Journal.load(self.state_dir, SERVICE)
        if latest is not None and latest.status not in FINAL:
            raise DeployError('transaction %s is %s; finish it with rollback first' % (latest.id, latest.status))
        plan, new_text, state, problems = self.assess()
        self.describe(plan, state)
        if problems:
            raise DeployError('refused under the lock; nothing was changed:\n  ' + '\n  '.join(problems))
        if durable.digest(plan) != expect_plan_sha256:
            raise DeployError('plan differs from --expect-plan-sha256; nothing was changed')
        sha = self.request['script']['sha256']
        record = {'key': 'load', 'role': 'load', 'host': self.host, 'unit': UNIT, 'phase': 'pending',
                  'changes': [{'op': 'write', 'path': FRAGMENT, 'previous': state['fragment_text'], 'new': new_text,
                               'previous_sha256': plan['previous_unit_sha256'], 'new_sha256': plan['new_unit_sha256']}],
                  'previous_active_state': state['active_state'], 'script': {'path': plan['script_path'], 'sha256': sha}}
        journal = Journal.create(self.state_dir, SERVICE, sha, {'request_sha256': self.request_sha256,
                                 'git_sha': self.request['script']['git_sha'], 'plan_sha256': expect_plan_sha256}, [record], None)
        self.out('transaction %s (%s)' % (journal.id, journal.path))
        try:
            self.stage(journal)
            self.exact_check(journal)
            self.lock.verify()
            problems = self.guard_problems()
            if problems:
                raise DeployError('refused after the exact check; the unit was not changed:\n  ' + '\n  '.join(problems))
            journal.set_status('activating')
            self.apply(journal, new=True)
            journal.set_status('committed')
        except BaseException as error:
            journal.event('failure', error='%s: %s' % (type(error).__name__, error))
            if journal.touched():
                self.out('repair failed (%s); restoring the previous unit, load left stopped' % error)
                try:
                    self.restore(journal)
                except Exception as failure:
                    raise DeployError('repair failed (%s) and its rollback did not complete: %s' % (error, failure)) from error
            else:
                journal.set_status('failed')
            raise
        self.out('committed %s: %s runs %s' % (journal.id, UNIT, plan['script_path']))
        return journal

    def stage(self, journal):
        """The reviewed script at its immutable content path; an existing copy must already match."""
        sha, path = self.request['script']['sha256'], script_path(self.request['script']['sha256'])
        self.lock.verify()
        if self.ex.sha256(self.host, path) is None:
            journal.event('staging', host=self.host, path=path)
            partial = os.path.dirname(path) + '.partial-' + journal.id
            self.ex.mkdir(self.host, partial, 0o755)
            self.ex.upload(self.host, str(SOURCE), partial + '/' + SCRIPT_NAME, 0o644)
            if self.ex.sha256(self.host, partial + '/' + SCRIPT_NAME) != sha:
                raise DeployError('uploaded script does not hash to script.sha256')
            self.lock.verify()
            self.ex.rename(self.host, partial, os.path.dirname(path))
        if self.ex.sha256(self.host, path) != sha:
            raise DeployError('immutable %s holds different bytes' % path)
        self.out('%s: staged %s' % (self.host, path))

    def apply(self, journal, new):
        """Write the new unit and start it, or stop the unit and restore the previous fragment."""
        record, change = journal.hosts[0], journal.hosts[0]['changes'][0]
        self.lock.verify()
        if not new:
            journal.set_phase(0, 'restoring')
            self.ex.systemctl(self.host, 'stop', UNIT)
            self.ex.write(self.host, FRAGMENT, change['previous'].encode(), 0o644)
            self.ex.systemctl(self.host, 'daemon-reload')
            journal.set_phase(0, 'restored')
            self.out('%s: previous unit restored and left stopped; latch and incidents untouched' % UNIT)
            return
        journal.set_phase(0, 'installing')
        self.ex.write(self.host, FRAGMENT, change['new'].encode(), 0o644)
        self.ex.systemctl(self.host, 'daemon-reload')
        before = {'invocation_id': self.ex.probe_unit(self.host, UNIT).get('invocation_id'),
                  'status_sha256': self.ex.sha256(self.host, LOAD_ROOT + '/status.json')}
        journal.set_phase(0, 'restarting', **before)
        # restart starts the stopped unit only; nothing else is started or restarted.
        self.ex.systemctl(self.host, 'restart', UNIT)
        journal.set_phase(0, 'verifying')
        deadline = self.clock() + VERIFY_SECONDS
        while (reason := self.verified(record, before)) is not None:
            if self.clock() >= deadline:
                raise DeployError('%s not verified within %ss: %s' % (UNIT, VERIFY_SECONDS, reason))
            self.sleep(self.poll_seconds)
        journal.set_phase(0, 'verified')

    def verified(self, record, before):
        state = self.ex.probe_unit(self.host, UNIT)
        if state['active_state'] != 'active' or state.get('need_daemon_reload') \
                or state.get('invocation_id') == before['invocation_id']:
            return 'unit is %s and has not started with the written unit' % state['active_state']
        if self.ex.sha256(self.host, LOAD_ROOT + '/latched.json') is not None:
            return 'the load latched a critical incident'
        if self.ex.sha256(self.host, LOAD_ROOT + '/status.json') == before['status_sha256']:
            return 'the load has not written status.json since it started'
        try:
            mode = json.loads(self.ex.read(self.host, LOAD_ROOT + '/status.json') or 'null')['mode']
        except (ValueError, TypeError, KeyError):
            return 'status.json is unreadable'
        return None if mode == 'running' else 'load mode is %r, not running' % mode

    def restore(self, journal, force=False):
        journal.set_status('rolling-back')
        change = journal.hosts[0]['changes'][0]
        current = self.ex.sha256(self.host, FRAGMENT)
        if current not in (change['previous_sha256'], change['new_sha256']) and not force:
            journal.set_status('rollback-refused', current_sha256=current)
            raise DeployError('rollback refused; %s changed outside the transaction (review, then --force)' % FRAGMENT)
        try:
            self.apply(journal, new=False)
        except BaseException:
            journal.set_phase(0, 'restore-failed')
            journal.set_status('rollback-failed')
            raise
        journal.set_status('rolled-back')
        self.out('rolled back %s' % journal.id)

    def rollback(self, identifier=None, force=False):
        def run():
            journal = Journal.load(self.state_dir, SERVICE, identifier)
            if journal is None:
                raise DeployError('no %s transaction to roll back' % SERVICE)
            if journal.status == 'rolled-back':
                self.out('%s is already rolled back' % journal.id)
            elif not journal.touched():
                journal.set_status('failed')
                self.out('%s touched nothing; marked failed' % journal.id)
            else:
                self.restore(journal, force)
            return journal
        return self.locked(run)

    def observed(self):
        state = self.ex.probe_unit(self.host, UNIT)
        try:
            mode = json.loads(self.ex.read(self.host, LOAD_ROOT + '/status.json') or 'null')['mode']
        except (ValueError, TypeError, KeyError):
            mode = 'unknown'
        latch = self.ex.sha256(self.host, LOAD_ROOT + '/latched.json') is not None
        return '%s: %s, unit %s, load mode %s, latch %s' % (UNIT, state['active_state'], text_sha256(state['fragment_text']),
                                                           mode, 'PRESENT' if latch else 'absent')


def run(args, executor, out, inventory, lock=None, clock=time.monotonic, sleep=time.sleep, poll_seconds=5.0, **_options):
    """`transparent-load-repair-*` entry point; returns the exit code."""
    action = args.command.removeprefix('transparent-load-repair-')
    request = load_request(args.request, args.request_sha256)
    repair = Repair(request, args.request_sha256, executor, inventory, args.state_dir, out, lock, clock, sleep, poll_seconds)
    problems = []
    if action in ('plan', 'preflight', 'status'):
        if action != 'plan':
            repair.check_identities()
            mutation_fence(inventory, executor)
        latest = Journal.load(args.state_dir, SERVICE)
        if latest is not None:
            out('latest transaction %s: %s' % (latest.id, latest.status))
            if latest.status not in FINAL:
                problems.append('transaction %s is %s; roll it back first' % (latest.id, latest.status))
        if action == 'status' and latest is not None and latest.status == 'committed' \
                and latest.data['source']['request_sha256'] == args.request_sha256:
            # After the repair the request's pins describe the earlier unit.
            if executor.sha256(request['host'], FRAGMENT) != latest.hosts[0]['changes'][0]['new_sha256']:
                problems.append('%s is no longer the unit transaction %s installed' % (FRAGMENT, latest.id))
        else:
            plan, _, state, found = repair.assess()
            repair.describe(plan, state)
            problems += found
            if action == 'preflight' and not problems:
                try:
                    repair.exact_check()
                except DeployError as error:
                    problems.append(str(error))
        out(repair.observed())
    elif action == 'deploy':
        repair.deploy(args.expect_plan_sha256)
        out(repair.observed())
    elif action == 'rollback':
        repair.rollback(args.transaction, args.force)
        out(repair.observed())
    for problem in problems:
        out('problem: ' + problem)
    if action in ('plan', 'preflight', 'deploy'):
        out('rollback: ' + shlex.join([sys.executable, str(ROOT / 'tools/scripts/wallet-pir-deploy.py'), '--inventory',
                                       str(args.inventory), '--state-dir', str(args.state_dir), 'transparent-load-repair-rollback',
                                       '--request', str(args.request), '--request-sha256', args.request_sha256]))
    return 1 if problems else 0

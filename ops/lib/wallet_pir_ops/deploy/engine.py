"""Plan, preflight, deploy, rollback, status and baseline capture for one service.

A deploy changes one unit at a time, in descriptor order, and only where the
running executable or the effective unit differs from the target. Each unit
change is the managed drop-in `zz-wallet-pir-release.conf` (which clears and
sets ExecStart, keeping the argument tail) or, for template roles, the whole
unit rendered from its `.service.in`. Every side effect is journaled first; a
failure restores the touched units in reverse order and checks that the
previous executable is running again.
"""
from dataclasses import dataclass, field
import difflib
import hashlib
import json
import os
from pathlib import Path
import time

from .. import durable
from . import descriptors, units
from .remote import production_lock
from .. import schema_fence
from .transaction import FINAL, Journal

SYSTEM = '/etc/systemd/system'
SELF_CHECK_SECONDS = 60


class DeployError(RuntimeError):
    pass


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def text_sha256(text):
    return None if text is None else hashlib.sha256(text.encode()).hexdigest()


def unit_files(state):
    """`(path, text)` for the fragment and each drop-in, in the order systemd applies them."""
    return [(state['fragment_path'], state['fragment_text'])] + [(d['path'], d['text']) for d in state['drop_ins']]


@dataclass
class TargetPlan:
    target: descriptors.Target
    state: dict
    action: str = 'refuse'
    changes: list = field(default_factory=list)
    drop_ins: list = field(default_factory=list)
    drift: list = field(default_factory=list)
    refusals: list = field(default_factory=list)
    exec_start: str = None
    exec_binary: str = None


class Deployer:
    def __init__(self, service, inventory, executor, state_dir, baseline_path=None, lock=None,
                 out=print, sleep=time.sleep, clock=time.monotonic, poll_seconds=2.0, only=None):
        self.service = service
        self.inventory = inventory
        self.ex = executor
        self.state_dir = Path(state_dir)
        self.baseline_path = Path(baseline_path) if baseline_path else self.state_dir / 'baselines' / (service.name + '.json')
        self.lock_factory = lock or (lambda: production_lock(inventory, executor))
        self.lock = None
        self.out = out
        self.sleep = sleep
        self.clock = clock
        self.poll_seconds = poll_seconds
        self.targets = descriptors.select(descriptors.targets(service, inventory), only)

    # ------------------------------------------------------------ inspection

    def check_identities(self, extra=()):
        """Every host must accept the deploy identity before anything is read or changed."""
        hosts = list(dict.fromkeys([t.host for t in self.targets] + [h for h in extra if h]))
        for host in hosts:
            try:
                self.ex.check_identity(host)
            except Exception as error:
                raise DeployError('deploy identity is not accepted by %s; nothing was changed (%s)' % (host, error)) from error

    def probe(self):
        return {t.key: self.ex.probe_unit(t.host, t.unit) for t in self.targets}

    def plan_target(self, target, state, sha, retire_historical=False):
        plan = TargetPlan(target, state)
        role = target.role
        if state['load_state'] != 'loaded':
            plan.refusals.append('unit is %s; the CLI changes installed units only' % (state['load_state'] or 'unknown'))
            return plan
        if state['active_state'] != 'active' or not state['main_pid'] or not state['exe_sha256']:
            plan.refusals.append('unit is %s, not running; start or reconcile it by hand first' % state['active_state'])
            return plan
        if state['need_daemon_reload']:
            plan.refusals.append('unit files changed on disk without a daemon-reload')
            return plan
        if any(text is None for _, text in unit_files(state)):
            plan.refusals.append('a loaded unit file is no longer readable')
            return plan
        try:
            current = units.effective(text for _, text in unit_files(state))
            plan.exec_start = units.exec_start(current)
            prefix, plan.exec_binary, tail = units.split_exec(plan.exec_start)
        except units.UnitError as error:
            plan.refusals.append(str(error))
            return plan
        if self.ex.sha256(target.host, plan.exec_binary) != state['exe_sha256']:
            plan.refusals.append('running executable differs from ExecStart binary %s, so a rollback could not restore it'
                                 % plan.exec_binary)
        release = self.service.release_binary(sha)
        managed = '%s/%s.d/%s' % (SYSTEM, target.unit, units.MANAGED_DROP_IN)
        kept = {}
        for path, text in unit_files(state)[1:]:
            name = os.path.basename(path)
            if role.mode == 'exec-drop-in' and path == managed:
                disposition = 'managed'
            elif not units.sets_exec_start(text) and role.owns_unit:
                disposition = 'refuse'
                plan.refusals.append('role %s owns its whole unit; remove or fold in %s' % (role.name, path))
            elif not units.sets_exec_start(text):
                disposition = 'keep'
            elif units.adoptable(path, role.adoptable_drop_ins) or name == units.MANAGED_DROP_IN:
                if units.exec_start_only(text) and (retire_historical or name == units.MANAGED_DROP_IN
                                                     or role.mode != 'exec-drop-in'):
                    disposition = 'retire'
                elif role.mode == 'exec-drop-in' and name < units.MANAGED_DROP_IN:
                    disposition = 'supersede'
                else:
                    disposition = 'refuse'
                    plan.refusals.append('historical drop-in %s sets more than ExecStart and cannot be superseded' % path)
            elif role.mode == 'exec-drop-in' and name < units.MANAGED_DROP_IN:
                # A layer from an earlier manual rollout: left in place and
                # shadowed by the managed drop-in, which sorts after it.
                disposition = 'supersede'
            else:
                disposition = 'refuse'
                plan.refusals.append('unmanaged drop-in %s overrides ExecStart; reconcile it first' % path)
            plan.drop_ins.append((path, disposition))
            if disposition in ('keep', 'supersede'):
                kept[name] = text
            elif disposition == 'retire':
                plan.changes.append({'op': 'retire', 'path': path, 'previous': text, 'new': None})
        try:
            if role.mode == 'exec-drop-in':
                fragment = state['fragment_text']
                kept[units.MANAGED_DROP_IN] = units.managed_drop_in(prefix, release, tail)
                previous = next((text for path, text in unit_files(state) if path == managed), None)
                if previous != kept[units.MANAGED_DROP_IN]:
                    plan.changes.insert(0, {'op': 'write', 'path': managed, 'previous': previous,
                                            'new': kept[units.MANAGED_DROP_IN]})
            else:
                path = '%s/%s' % (SYSTEM, target.unit)
                if state['fragment_path'] != path:
                    plan.refusals.append('template roles replace %s, but systemd loads %s' % (path, state['fragment_path']))
                fragment = units.render(role.template.read_text(),
                                        descriptors.template_values(self.service, self.inventory, sha))
                if fragment != state['fragment_text']:
                    plan.changes.insert(0, {'op': 'write', 'path': path, 'previous': state['fragment_text'], 'new': fragment})
            desired = units.effective([fragment] + [kept[name] for name in sorted(kept)])
            if units.split_exec(units.exec_start(desired))[1] != release:
                plan.refusals.append('the new unit would not run %s' % release)
            plan.drift = units.differences(units.normalized(current), units.normalized(desired))
        except (units.UnitError, descriptors.DescriptorError) as error:
            plan.refusals.append(str(error))
            return plan
        unchanged = state['exe_sha256'] == sha and not plan.drift
        plan.action = 'skip' if unchanged else 'restart'
        if unchanged:
            plan.changes = []
        return plan

    def assess(self, sha, binary=None, allow_drift=False, retire_historical=False, require_baseline=True):
        """Plans for every target and the reasons, if any, a deploy must not start. Read-only."""
        states = self.probe()
        problems = self.baseline_problems(states, required=require_baseline)
        plans = [self.plan_target(t, states[t.key], sha, retire_historical) for t in self.targets]
        for plan in plans:
            key = plan.target.key
            problems += ['%s: %s' % (key, reason) for reason in plan.refusals]
            if plan.action == 'restart' and plan.drift and not allow_drift:
                problems.append('%s: the new unit differs from the live one beyond the binary (see drift); '
                                'reconcile the template with the live unit, or pass --allow-unit-drift' % key)
            if plan.action == 'restart' and any(c['op'] == 'retire' for c in plan.changes) and not retire_historical:
                problems.append('%s: historical ExecStart drop-ins would be retired into the transaction '
                                'directory; pass --retire-historical to accept' % key)
        size = os.path.getsize(binary) if binary else 0
        for host in dict.fromkeys(p.target.host for p in plans if p.action == 'restart'):
            if self.ex.sha256(host, self.service.release_binary(sha)) == sha:
                continue
            if not binary:
                problems.append('%s: release %s is not staged and no binary was given' % (host, sha[:12]))
            elif self.ex.free_bytes(host, self.service.root) < size + self.service.min_free_bytes:
                problems.append('%s: less than %d bytes free for the release under %s'
                                % (host, size + self.service.min_free_bytes, self.service.root))
        return plans, problems

    def describe(self, plans, sha):
        self.out('%s: target sha256 %s' % (self.service.name, sha))
        for plan in plans:
            target, state = plan.target, plan.state
            self.out('%s %s: %s' % (target.key, target.unit, plan.action))
            if state.get('exe_sha256'):
                self.out('  running %s from %s' % (state['exe_sha256'], plan.exec_binary))
            for path, disposition in plan.drop_ins:
                note = {'managed': 'managed; replaced', 'keep': 'kept (does not set ExecStart)',
                        'retire': 'retire: moved into the host transaction directory, content journaled',
                        'supersede': 'left in place; its ExecStart is overridden by the managed drop-in',
                        'refuse': 'REFUSED'}[disposition]
                self.out('  drop-in %s: %s' % (path, note))
            for change in plan.changes:
                self.out('  %s %s' % (change['op'], change['path']))
            for line in plan.drift:
                self.out('  drift %s' % line)
            for reason in plan.refusals:
                self.out('  refused: %s' % reason)

    # -------------------------------------------------------------- baseline

    def snapshot(self, states):
        entries = {}
        for target in self.targets:
            state = states[target.key]
            entries[target.key] = {
                'host': target.host, 'role': target.role.name, 'unit': target.unit,
                'load_state': state['load_state'], 'active_state': state['active_state'],
                'exe_sha256': state['exe_sha256'], 'fragment_path': state['fragment_path'],
                'fragment_text': state['fragment_text'], 'drop_ins': state['drop_ins'],
            }
        return {'service': self.service.name, 'captured_unix': int(time.time()), 'targets': entries}

    def capture_baseline(self, path=None):
        """Record every target's running executable and complete unit text. Read-only on hosts."""
        path = Path(path) if path else self.baseline_path
        self.check_identities()
        baseline = self.snapshot(self.probe())
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        durable.atomic_json(path, baseline, mode=0o600)
        for key, entry in baseline['targets'].items():
            self.out('%s %s: %s %s, %d drop-in(s)' % (key, entry['unit'], entry['active_state'],
                                                      entry['exe_sha256'], len(entry['drop_ins'])))
        self.out('baseline written to %s' % path)
        return baseline

    def baseline_problems(self, states, required):
        if not self.baseline_path.exists():
            return ['no baseline at %s; run capture-baseline and review it first' % self.baseline_path] if required else []
        baseline = json.loads(self.baseline_path.read_text())
        live = self.snapshot(states)['targets']
        problems = []
        if baseline.get('service') != self.service.name:
            return ['baseline %s is for service %r' % (self.baseline_path, baseline.get('service'))]
        # A deploy limited with --only needs its own targets in the baseline.
        if not set(live) <= set(baseline['targets']):
            problems.append('baseline targets %s do not cover %s'
                            % (sorted(baseline['targets']), sorted(live)))
        for key in sorted(set(baseline['targets']) & set(live)):
            old, new = baseline['targets'][key], live[key]
            if old['exe_sha256'] != new['exe_sha256']:
                problems.append('%s: running executable %s differs from the baseline %s'
                                % (key, new['exe_sha256'], old['exe_sha256']))
            before = ''.join('# %s\n%s' % (p, t) for p, t in unit_files(old))
            after = ''.join('# %s\n%s' % (p, t) for p, t in unit_files(new))
            if before != after:
                diff = difflib.unified_diff(before.splitlines(), after.splitlines(), 'baseline', 'live', lineterm='', n=1)
                problems.append('%s: unit files changed since the baseline:\n    %s' % (key, '\n    '.join(list(diff)[:40])))
        return problems

    def refresh_baseline(self):
        """After a transaction ends, the state it produced is the new known state."""
        baseline = self.snapshot(self.probe())
        # Targets outside an --only selection keep their recorded state.
        if self.baseline_path.exists():
            previous = json.loads(self.baseline_path.read_text())
            if previous.get('service') == self.service.name:
                baseline['targets'] = {**previous.get('targets', {}), **baseline['targets']}
        self.baseline_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        durable.atomic_json(self.baseline_path, baseline, mode=0o600)

    # ---------------------------------------------------------- verification

    def check(self, record, expected):
        state = self.ex.probe_unit(record['host'], record['unit'])
        if state['active_state'] != 'active':
            return False, 'unit is %s' % state['active_state']
        if state['exe_sha256'] != expected:
            return False, 'running executable is %s, expected %s' % (state['exe_sha256'], expected)
        verify = record['verify']
        status, body = self.ex.http_get(record['host'], verify['health'])
        if status != 200:
            return False, 'health answered %s' % status
        try:
            document = json.loads(body)
        except ValueError:
            document = None
        if isinstance(document, dict) and 'binary_sha256' in document and document['binary_sha256'] != expected:
            return False, 'health reports binary_sha256 %s' % document['binary_sha256']
        for field, expected_value in verify.get('health_equals', {}).items():
            value = document
            for part in field.split('.'):
                value = value.get(part) if isinstance(value, dict) else None
            if type(value) is not type(expected_value) or value != expected_value:
                return False, 'health %s is %r, expected %r' % (field, value, expected_value)
        ready = verify.get('ready')
        if ready:
            value = document.get(ready['field']) if isinstance(document, dict) else None
            if ('equals' in ready and value != ready['equals']) or (ready.get('nonempty') and not value):
                return False, 'health %s is %r' % (ready['field'], value)
        if verify.get('ready_url'):
            status, _ = self.ex.http_get(record['host'], verify['ready_url'])
            if not 200 <= status < 300:
                return False, 'ready answered %s' % status
        return True, 'ready'

    def wait_verified(self, record, expected):
        deadline = self.clock() + record['verify']['timeout']
        while True:
            ok, reason = self.check(record, expected)
            if ok:
                return
            if self.clock() >= deadline:
                raise DeployError('%s: not verified within %ss: %s' % (record['key'], record['verify']['timeout'], reason))
            self.sleep(self.poll_seconds)

    # ---------------------------------------------------------------- deploy

    def record(self, plan):
        target = plan.target
        return {
            'key': target.key, 'role': target.role.name, 'host': target.host, 'unit': target.unit,
            'action': plan.action, 'phase': 'pending', 'drift': plan.drift,
            'previous': {'exe_sha256': plan.state['exe_sha256'], 'exec_start': plan.exec_start,
                         'exec_binary': plan.exec_binary},
            'changes': [{**change, 'previous_sha256': text_sha256(change['previous']),
                         'new_sha256': text_sha256(change['new'])} for change in plan.changes],
            'verify': {'health': target.url(target.role.health), 'ready': target.role.ready,
                       'ready_url': target.url(target.role.ready_url), 'timeout': target.role.ready_timeout,
                       'health_equals': target.health_equals},
        }

    def stage(self, hosts, sha, binary, journal=None):
        """Install the binary as `/opt/<svc>/releases/<sha256>/<binary>` and run its self-check there.

        Adds an immutable release directory only; no unit or process changes.
        """
        path = self.service.release_binary(sha)
        for host in hosts:
            self.lock.verify()
            current = self.ex.sha256(host, path)
            if current is None:
                if binary is None:
                    raise DeployError('%s: release %s is not staged and no binary was given' % (host, sha[:12]))
                partial = '%s/releases/.partial-%s' % (self.service.root, journal.id if journal else sha)
                if journal:
                    journal.event('staging', host=host)
                self.ex.mkdir(host, partial, 0o755)
                self.ex.upload(host, binary, '%s/%s' % (partial, self.service.binary), 0o755)
                if self.ex.sha256(host, '%s/%s' % (partial, self.service.binary)) != sha:
                    raise DeployError('%s: uploaded binary does not match %s' % (host, sha))
                self.ex.rename(host, partial, self.service.release_dir(sha))
            elif current != sha:
                raise DeployError('%s: immutable release %s holds different bytes (%s)' % (host, path, current))
            code, output = self.ex.run(host, [path, *self.service.self_check], SELF_CHECK_SECONDS)
            if code:
                raise DeployError('%s: staged binary failed its self-check (exit %d): %s' % (host, code, output[-500:]))
            self.out('%s: staged %s and it runs' % (host, path))

    def activate(self, journal, index, sha):
        record = journal.hosts[index]
        host, unit = record['host'], record['unit']
        directory = '%s/%s' % (self.service.transaction_dir(journal.id), unit)
        self.lock.verify()
        journal.set_phase(index, 'backing-up')
        self.ex.mkdir(host, directory + '/files', 0o700)
        for number, change in enumerate(record['changes']):
            if change['previous'] is not None:
                self.ex.write(host, '%s/files/%d-%s' % (directory, number, os.path.basename(change['path'])),
                              change['previous'].encode(), 0o600)
        self.ex.write(host, directory + '/record.json', json.dumps(record, indent=2, sort_keys=True).encode(), 0o600)
        journal.set_phase(index, 'installing')
        for change in record['changes']:
            if change['op'] == 'write':
                self.ex.mkdir(host, os.path.dirname(change['path']), 0o755)
                self.ex.write(host, change['path'], change['new'].encode(), 0o644)
            else:
                self.ex.mkdir(host, directory + '/retired', 0o700)
                self.ex.rename(host, change['path'], '%s/retired/%s' % (directory, os.path.basename(change['path'])))
        self.ex.systemctl(host, 'daemon-reload')
        record['restart_issued'] = True
        journal.set_phase(index, 'restarting')
        self.ex.systemctl(host, 'restart', unit)
        journal.set_phase(index, 'verifying')
        self.wait_verified(record, sha)
        journal.set_phase(index, 'verified')
        self.out('%s: running %s and ready' % (record['key'], sha))

    def run_exact_check(self, check, journal, sha):
        argv = [argument.replace('{release_dir}', self.service.release_dir(sha)).replace('{transaction}', journal.id)
                for argument in check['argv']]
        journal.event('exact check', host=check['host'], argv=argv)
        code, output = self.ex.run(check['host'], argv, int(check.get('timeout', 900)))
        journal.event('exact check finished', returncode=code, output=output[-2000:])
        if code:
            raise DeployError('exact-answer check failed (exit %d): %s' % (code, output[-1000:]))

    def deploy(self, sha, binary=None, source=None, allow_drift=False, retire_historical=False, skip_exact_check=False, verify_noop=False):
        """Returns the committed journal, or None when every target already matches."""
        check = descriptors.exact_check(self.service, self.inventory)
        if check is None and not skip_exact_check:
            raise DeployError('inventory has no %s.exact_check; configure one or pass --skip-exact-check '
                              'and run the exact-answer check by hand' % self.service.name)
        self.check_identities([self.inventory.lock.get('host'), check and check['host']])
        with self.lock_factory() as lock:
            self.lock = lock
            try:
                self.schema_fence()
                return self._deploy(sha, binary, source, allow_drift, retire_historical, check, verify_noop)
            finally:
                self.lock = None

    def schema_fence(self):
        self.lock.verify()
        host = self.inventory.lock.get('host')
        if self.inventory.lock.get('type') == 'pinned_host':
            schema_fence.local_schema_fence()
        elif host:
            schema_fence.schema_mutation_fence(lambda path: self.ex.read(host, path))

    def _deploy(self, sha, binary, source, allow_drift, retire_historical, check, verify_noop=False):
        latest = Journal.load(self.state_dir, self.service.name)
        if latest is not None and latest.status not in FINAL:
            raise DeployError('transaction %s is %s; finish it with rollback before deploying again'
                              % (latest.id, latest.status))
        plans, problems = self.assess(sha, binary, allow_drift, retire_historical)
        self.describe(plans, sha)
        if problems:
            raise DeployError('refused before any change:\n  ' + '\n  '.join(problems))
        restart = [plan for plan in plans if plan.action == 'restart']
        if not restart and not verify_noop:
            self.out('no-op: every target already runs %s with the same effective unit' % sha)
            return None
        baseline = json.loads(self.baseline_path.read_text())
        journal = Journal.create(self.state_dir, self.service.name, sha, source,
                                 [self.record(plan) for plan in plans], baseline)
        self.out('transaction %s (%s)' % (journal.id, journal.path))
        try:
            self.stage(list(dict.fromkeys(p.target.host for p in restart)), sha, binary, journal)
            journal.set_status('activating')
            for index, record in enumerate(journal.hosts):
                if record['action'] == 'restart':
                    self.activate(journal, index, sha)
            # Later roles can disturb earlier ones (a restarted coordinator
            # re-fences routers), so every changed target is checked again.
            journal.set_status('verifying')
            for record in journal.hosts:
                if record['action'] == 'restart':
                    self.wait_verified(record, sha)
            if check:
                self.run_exact_check(check, journal, sha)
            journal.set_status('committed')
        except BaseException as error:
            journal.event('failure', error='%s: %s' % (type(error).__name__, error))
            if journal.touched():
                self.out('deploy failed (%s); rolling back %d touched target(s)' % (error, len(journal.touched())))
                try:
                    self.rollback_journal(journal)
                except Exception as failure:
                    raise DeployError('deploy failed (%s) and its rollback did not complete: %s'
                                      % (error, failure)) from error
            else:
                journal.set_status('failed')
            raise
        self.refresh_baseline()
        self.out('committed %s' % journal.id)
        return journal

    # -------------------------------------------------------------- rollback

    def restore_conflicts(self, record):
        conflicts = []
        for change in record['changes']:
            current = self.ex.sha256(record['host'], change['path'])
            if current not in (change['previous_sha256'], change['new_sha256']):
                conflicts.append('%s: %s changed since the transaction' % (record['key'], change['path']))
        return conflicts

    def restore(self, journal, index):
        record = journal.hosts[index]
        host, unit = record['host'], record['unit']
        self.lock.verify()
        journal.set_phase(index, 'restoring')
        for change in reversed(record['changes']):
            if change['previous'] is None:
                self.ex.remove(host, change['path'])
            else:
                self.ex.mkdir(host, os.path.dirname(change['path']), 0o755)
                self.ex.write(host, change['path'], change['previous'].encode(), 0o644)
        self.ex.systemctl(host, 'daemon-reload')
        expected = record['previous']['exe_sha256']
        state = self.ex.probe_unit(host, unit)
        if record.get('restart_issued') or state['active_state'] != 'active' or state['exe_sha256'] != expected:
            self.ex.systemctl(host, 'restart', unit)
        self.wait_verified(record, expected)
        journal.set_phase(index, 'restored')
        self.out('%s: restored %s' % (record['key'], expected))

    def rollback_journal(self, journal, force=False):
        pending = [index for index in reversed(journal.touched()) if journal.hosts[index]['phase'] != 'restored']
        conflicts = [c for index in pending for c in self.restore_conflicts(journal.hosts[index])]
        if conflicts and not force:
            journal.event('rollback refused', conflicts=conflicts)
            raise DeployError('rollback refused; unit files changed after the transaction:\n  ' + '\n  '.join(conflicts))
        journal.set_status('rolling-back')
        failures = []
        for index in pending:
            try:
                self.restore(journal, index)
            except Exception as error:
                journal.set_phase(index, 'restore-failed', error=str(error))
                failures.append('%s: %s' % (journal.hosts[index]['key'], error))
        journal.set_status('rollback-failed' if failures else 'rolled-back')
        if failures:
            raise DeployError('rollback incomplete; re-run rollback after fixing:\n  ' + '\n  '.join(failures))

    def rollback(self, identifier=None, force=False):
        journal = Journal.load(self.state_dir, self.service.name, identifier)
        if journal is None:
            raise DeployError('no %s transaction is recorded in %s' % (self.service.name, self.state_dir))
        latest = Journal.load(self.state_dir, self.service.name)
        if latest.id != journal.id and latest.status not in ('failed', 'rolled-back'):
            raise DeployError('later transaction %s is %s; roll it back first' % (latest.id, latest.status))
        if journal.status == 'rolled-back':
            self.out('%s is already rolled back' % journal.id)
            return journal
        self.check_identities([self.inventory.lock.get('host')] + [r['host'] for r in journal.hosts])
        with self.lock_factory() as lock:
            self.lock = lock
            try:
                self.schema_fence()
                self.rollback_journal(journal, force)
            finally:
                self.lock = None
        self.refresh_baseline()
        self.out('rolled back %s' % journal.id)
        return journal

    # ---------------------------------------------------------------- status

    def status(self):
        journal = Journal.load(self.state_dir, self.service.name)
        if journal:
            self.out('latest transaction %s: %s (target %s)' % (journal.id, journal.status, journal.data['binary_sha256']))
            for record in journal.hosts:
                self.out('  %s: %s %s' % (record['key'], record['action'], record['phase']))
        else:
            self.out('no %s transaction recorded' % self.service.name)
        for target in self.targets:
            state = self.ex.probe_unit(target.host, target.unit)
            self.out('%s %s: %s %s' % (target.key, target.unit, state['active_state'], state['exe_sha256']))

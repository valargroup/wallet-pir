"""Plan, preflight, deploy, rollback, status and baseline capture for one service.

A deploy changes one unit at a time, in descriptor order, and only where the
running executable or the effective unit differs from the target. An
exec-drop-in role changes the managed drop-in `zz-wallet-pir-release.conf`
(which clears and sets ExecStart, keeping the argument tail). A template or
role-mode role replaces its whole unit. One transaction stages one binary.
Every side effect is journaled first; a failure restores the touched units in
reverse order and checks that the previous executable is running again.
"""
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
import difflib
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import time

from .. import contract, durable
from . import descriptors, timing, units
from .remote import production_lock
from .. import schema_fence
from .transaction import FINAL, FLEET_ROOT, Journal

SYSTEM = '/etc/systemd/system'
SELF_CHECK_SECONDS = 60
PRODUCT_CHECK_SECONDS = 900
# Plan actions that change a target: an installed unit restarted on the
# release, or a role-mode unit installed where it was absent.
CHANGING = ('restart', 'install')


class DeployError(RuntimeError):
    pass


def mutation_fence(inventory, executor, lock=None, *, fleet_id=None):
    """Refuse while a Transparent schema transaction is unfinished; read-only.

    Under the held production lock (`lock`) before every mutation; without one
    it is the same check for a read-only preflight.
    """
    if lock is not None:
        lock.verify()
    host = inventory.lock.get('host')
    if inventory.lock.get('type') == 'pinned_host':
        schema_fence.local_schema_fence()
    elif host:
        schema_fence.schema_mutation_fence(lambda path: executor.read(host, path))
    def read(path):
        if host:
            return executor.read(host, path)
        local = Path(path)
        return local.read_text() if local.exists() else None
    pointer = read(FLEET_ROOT + '/latest.json')
    if pointer is not None:
        identifier = json.loads(pointer).get('id')
        if not isinstance(identifier, str) or not re.fullmatch('fleet-[A-Za-z0-9-]+', identifier):
            raise DeployError('invalid fleet transaction pointer')
        latest = json.loads(read(FLEET_ROOT + '/transactions/' + identifier + '.json') or '{}')
        if latest.get('id') != identifier or latest.get('schema') != 'fleet-release-journal/1':
            raise DeployError('fleet transaction journal is missing or invalid')
        if latest.get('phase') not in FINAL and identifier != fleet_id:
            raise DeployError('unfinished fleet release %s; verify or reconcile it before another mutation' % identifier)


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
    # A role-mode target's writable directories and product check arguments.
    directories: tuple = ()
    check: tuple = ()


class Deployer:
    coordinated = False

    def __init__(self, service, inventory, executor, state_dir, baseline_path=None, lock=None,
                 out=print, sleep=time.sleep, clock=time.monotonic, poll_seconds=2.0, only=None,
                 hold_on_failure=False):
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
        self.hold_on_failure = hold_on_failure
        self.targets = descriptors.select(descriptors.targets(service, inventory), only)
        self.coordinated = False

    def transaction_binary(self):
        """The one executable file this transaction stages.

        A host that runs two binaries gets two release directories, each named
        by the sha256 of the file it runs. Callers split targets first.
        """
        names = {target.role.binary or self.service.binary for target in self.targets}
        if len(names) != 1:
            raise DeployError('one transaction runs one binary, but these targets run %s'
                              % ', '.join(sorted(names)))
        return next(iter(names))

    def release_binary(self, sha):
        return self.service.release_binary(sha, self.transaction_binary())

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
        for path, digest in role.launcher:
            if self.ex.sha256(target.host, path) != digest:
                plan.refusals.append('credential launcher changed: ' + path)
        launcher = tuple(path for path, _ in role.launcher)
        if not role.shared_lifecycle:
            plan.refusals.append('operator-managed role: stop and preflight as the rendered unit user; '
                                 'generic deployment cannot check its exclusive state')
            return plan
        if state['load_state'] != 'loaded':
            if role.mode == 'role' and target.install and state['load_state'] == 'not-found':
                return self.plan_install(plan, sha)
            hint = ('; set "install": true on its inventory entry to install it'
                    if role.mode == 'role' and state['load_state'] == 'not-found' else '')
            plan.refusals.append('unit is %s; the CLI changes installed units only%s'
                                 % (state['load_state'] or 'unknown', hint))
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
            prefix, plan.exec_binary, tail = units.split_exec(plan.exec_start, launcher)
        except units.UnitError as error:
            plan.refusals.append(str(error))
            return plan
        if self.ex.sha256(target.host, plan.exec_binary) != state['exe_sha256']:
            plan.refusals.append('running executable differs from ExecStart binary %s, so a rollback could not restore it'
                                 % plan.exec_binary)
        release = self.release_binary(sha)
        managed = '%s/%s.d/%s' % (SYSTEM, target.unit, units.MANAGED_DROP_IN)
        kept = {}
        for path, text in unit_files(state)[1:]:
            name = os.path.basename(path)
            if role.mode == 'exec-drop-in' and path == managed:
                disposition = 'managed'
            elif (role.owns_unit and role.adopt_command_write_paths and retire_historical
                  and units.adoptable(path, role.adoptable_drop_ins)
                  and units.command_and_write_paths_only(text)):
                # The reviewed complete role owns its command and writable paths.
                # Save each recognized old overlay in the rollback transaction.
                disposition = 'retire'
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
                previous = next((text for path, text in unit_files(state) if path == managed), None)
                kept[units.MANAGED_DROP_IN] = units.managed_drop_in(prefix, release, tail, previous)
                if previous != kept[units.MANAGED_DROP_IN]:
                    plan.changes.insert(0, {'op': 'write', 'path': managed, 'previous': previous,
                                            'new': kept[units.MANAGED_DROP_IN]})
            else:
                path = '%s/%s' % (SYSTEM, target.unit)
                if state['fragment_path'] != path:
                    plan.refusals.append('template roles replace %s, but systemd loads %s' % (path, state['fragment_path']))
                if role.mode == 'role':
                    rendered = role.render(target.settings, self.service.release_dir(sha), target.unit)
                    guard = role.product_guard()
                    if guard is not None and hasattr(guard, 'rendered_role'):
                        rendered = guard.rendered_role(target, state, rendered)
                    fragment = rendered.unit
                    plan.directories, plan.check = rendered.directories, rendered.check
                    plan.changes += self.file_changes(plan, rendered.files, fresh=False)
                else:
                    fragment = units.render(role.template.read_text(),
                                            descriptors.template_values(self.service, self.inventory, sha))
                if fragment != state['fragment_text']:
                    plan.changes.insert(0, {'op': 'write', 'path': path, 'previous': state['fragment_text'], 'new': fragment})
            desired = units.effective([fragment] + [kept[name] for name in sorted(kept)])
            if units.split_exec(units.exec_start(desired), launcher)[1] != release:
                plan.refusals.append('the new unit would not run %s' % release)
            plan.drift = units.differences(units.normalized(current, launcher), units.normalized(desired, launcher))
        except (units.UnitError, descriptors.DescriptorError, ValueError) as error:
            plan.refusals.append(str(error))
            return plan
        # A role's configuration files change what it serves without changing the unit.
        configured = any(not c['path'].startswith(SYSTEM + '/') for c in plan.changes if c['op'] == 'write')
        unchanged = state['exe_sha256'] == sha and not plan.drift and not configured
        plan.action = 'skip' if unchanged else 'restart'
        if unchanged:
            plan.changes = []
        return self.guard_plan(plan)

    def guard_plan(self, plan):
        """A guarded role's own read-only refusals for a changing plan.

        Its writable state is the product's: the engine creates no directory
        for it, so missing or foreign-owned state is a refusal instead.
        """
        guard = plan.target.role.product_guard()
        if guard is not None and plan.action in CHANGING:
            if not getattr(guard, 'manages_directories', False):
                plan.directories = ()
            plan.refusals += guard.plan(self, plan)
        return plan

    def file_changes(self, plan, files, fresh):
        """Writes of a role's rendered configuration files.

        A fresh install never overwrites a file it did not render: an existing
        file with other bytes is the operator's and is refused.
        """
        changes = []
        for path, text in sorted(files.items()):
            current = self.ex.read(plan.target.host, path)
            if current == text:
                continue
            if fresh and current is not None:
                plan.refusals.append('%s already exists with other contents; a fresh install does not '
                                     'overwrite it' % path)
                continue
            changes.append({'op': 'write', 'path': path, 'previous': current, 'new': text})
        return changes

    def plan_install(self, plan, sha):
        """A role-mode unit where none is installed: render it whole, from the inventory's settings."""
        target = plan.target
        path = '%s/%s' % (SYSTEM, target.unit)
        try:
            rendered = target.role.render(target.settings, self.service.release_dir(sha), target.unit)
            if units.split_exec(units.exec_start(units.effective([rendered.unit])))[1] != self.release_binary(sha):
                plan.refusals.append('the new unit would not run %s' % self.release_binary(sha))
        except (units.UnitError, descriptors.DescriptorError, ValueError) as error:
            plan.refusals.append(str(error))
            return plan
        if self.ex.read(target.host, path) is not None:
            plan.refusals.append('%s exists but systemd has not loaded it; reconcile it first' % path)
        plan.changes = [{'op': 'write', 'path': path, 'previous': None, 'new': rendered.unit},
                        *self.file_changes(plan, rendered.files, fresh=True)]
        plan.directories, plan.check = rendered.directories, rendered.check
        plan.action = 'install'
        return self.guard_plan(plan)

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
        for host in dict.fromkeys(p.target.host for p in plans if p.action in CHANGING):
            if self.ex.sha256(host, self.release_binary(sha)) == sha:
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
        verify = record['verify']
        observation = (self.ex.observe_service(record['host'], record['unit'], expected,
                       [verify['health'], verify.get('ready_url')]) if self.coordinated else None)
        state = observation['unit'] if observation is not None else self.ex.probe_unit(record['host'], record['unit'])
        def http_get(url):
            if observation is None:
                return self.ex.http_get(record['host'], url)
            reply = observation['http'][url]
            return reply['status'], reply['body']
        if state['active_state'] != 'active':
            return False, 'unit is %s' % state['active_state']
        if state['exe_sha256'] != expected:
            return False, 'running executable is %s, expected %s' % (state['exe_sha256'], expected)
        status, body = http_get(verify['health'])
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
        guard = self.service.roles[record['role']].product_guard()
        ready = verify.get('ready')
        if ready:
            value = document.get(ready['field']) if isinstance(document, dict) else None
            if ('equals' in ready and value != ready['equals']) or (ready.get('nonempty') and not value):
                restored_ready = getattr(guard, 'restored_ready', None)
                if restored_ready is None or not restored_ready(record, expected, document):
                    return False, 'health %s is %r' % (ready['field'], value)
        if verify.get('ready_url'):
            status, _ = http_get(verify['ready_url'])
            if not 200 <= status < 300:
                return False, 'ready answered %s' % status
        if guard is not None:
            return guard.ready(self, record, expected, document)
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
            'directories': list(plan.directories), 'check': list(plan.check),
            'launcher': [{'path': path, 'sha256': digest} for path, digest in target.role.launcher],
        }

    def stage(self, hosts, sha, binary, journal=None):
        """Install the binary as `/opt/<svc>/releases/<sha256>/<binary>` and run its self-check there.

        Upload to independent hosts together; finish every upload before any
        activation or lock release, including when one host fails. Journal
        writes stay on the caller thread. No unit or process changes here.
        """
        hosts = list(dict.fromkeys(hosts))
        if not hosts:
            return {}
        for host in hosts:
            self.lock.verify()
            if journal:
                journal.event('staging', host=host)
        partial = '%s/releases/.partial-%s' % (self.service.root, journal.id if journal else sha)
        with ThreadPoolExecutor(max_workers=min(4, len(hosts))) as pool:
            futures = [pool.submit(self.stage_host, host, sha, binary, partial) for host in hosts]
            identities = [future.result() for future in futures]
        if journal:
            for host, info in zip(hosts, identities):
                journal.event('staged', host=host,
                              contract=info.to_json() if info is not None else 'unknown')
        return dict(zip(hosts, identities))

    def stage_host(self, host, sha, binary, partial):
        """Verify or upload one immutable release under the caller's lock."""
        name = self.transaction_binary()
        path = self.service.release_binary(sha, name)
        self.lock.verify()
        current = self.ex.sha256(host, path)
        if current is None:
            if binary is None:
                raise DeployError('%s: release %s is not staged and no binary was given' % (host, sha[:12]))
            self.ex.mkdir(host, partial, 0o755)
            self.lock.verify()
            self.ex.upload(host, binary, '%s/%s' % (partial, name), 0o755)
            if self.ex.sha256(host, '%s/%s' % (partial, name)) != sha:
                raise DeployError('%s: uploaded binary does not match %s' % (host, sha))
            self.lock.verify()
            self.ex.rename(host, partial, self.service.release_dir(sha))
        elif current != sha:
            raise DeployError('%s: immutable release %s holds different bytes (%s)' % (host, path, current))
        code, output = self.ex.run(host, [path, *self.service.self_check], SELF_CHECK_SECONDS)
        if code:
            raise DeployError('%s: staged binary failed its self-check (exit %d): %s' % (host, code, output[-500:]))
        info = self.contract_info(host, path, name, sha)
        if self.service.name == 'enhance':
            for target in self.targets:
                if target.host != host or target.artifact_kind != 'enhance-pir-native-cuda':
                    continue
                if name != 'pir-query-server':
                    raise DeployError('%s: CUDA check-backend runs on pir-query-server, not %s' % (host, path))
                device = target.health_equals.get('matvec.cuda_device', 0)
                # NVRTC may live outside ldconfig's search path. Preserve only
                # the unit's explicit loader path, never its credentials.
                state = self.ex.probe_unit(host, target.unit)
                config = units.effective(text for _, text in unit_files(state))
                library_path = config.get('Service', {}).get('Environment', {}).get('LD_LIBRARY_PATH')
                command = (["env", "LD_LIBRARY_PATH=" + library_path] if library_path is not None else [])
                command += [path, 'check-backend', '--matvec-backend', 'cuda', '--cuda-device', str(device)]
                code, output = self.ex.run(host, command, SELF_CHECK_SECONDS)
                if code:
                    raise DeployError('%s: staged CUDA evaluator check failed (exit %d): %s'
                                      % (host, code, output[-500:]))
        self.out('%s: staged %s and it runs' % (host, path))
        return info

    def contract_guard(self, plans, identities, journal):
        """Refuse a forward deploy that would leave a consumer on another contract major.

        Every changing query process that reads an HTTPS publisher, CUDA
        workers and unified nodes included, must run a candidate whose
        compiled major equals what each of its publishers advertises now,
        observed from that consumer's host. Unknown on either side refuses:
        a contract-major change goes through `fleet-release-*`, which moves
        publishers and every consumer together. Runs after staging and before
        any unit changes.
        """
        from .fleet_release import observe_publisher, publisher_origins
        refusals = []
        for plan in plans:
            target = plan.target
            executable = target.role.binary or self.service.binary
            if executable == 'pir-publisher' and self.service.name == 'publisher':
                info = identities.get(target.host)
                if info is None:
                    refusals.append(target.key + ': publisher candidate contract is unknown')
                    continue
                management = target.url(target.role.health).rsplit('/', 1)[0]
                status, body = self.ex.http_get(target.host, management + contract.DISCOVERY_PATH)
                try:
                    major = contract.publisher_document_major(status, body)
                except contract.ContractError:
                    major = None
                if major != info.contract_major:
                    refusals.append(target.key + ': publisher contract changes or is unknown; use fleet-release')
            if executable != 'pir-query-server':
                continue
            origins = sorted(publisher_origins(target.settings))
            if not origins:
                from .fleet_transaction import configuration
                try:
                    document = configuration(self.ex, {'host': target.host, 'key': target.key}, plan.state)[2]
                    origins = sorted(publisher_origins(document))
                    if not origins and target.role.product_guard() is not None:
                        # Retained local-publication workers do not consume
                        # discovery. Their product guard validates their pinned
                        # local authority and predecessor-readable state.
                        continue
                except (DeployError, ValueError):
                    pass
            if not origins:
                refusals.append(target.key + ': query publisher dependency is unknown; use fleet-release')
                continue
            info = identities.get(target.host)
            if info is None:
                refusals.append('%s: candidate reports no contract identity, so compatibility with %s is unknown'
                                % (target.key, ', '.join(origins)))
                continue
            for origin in origins:
                observed = observe_publisher(self.ex, target.host, origin)
                journal.event('publisher contract', target=target.key, origin=origin, major=observed['major'])
                if observed['major'] != info.contract_major:
                    refusals.append('%s: candidate contract major %d, publisher %s advertises %s; use fleet-release'
                                    % (target.key, info.contract_major, origin,
                                       'an unknown major' if observed['major'] is None else observed['major']))
        if refusals:
            raise DeployError('refused before any restart:\n  ' + '\n  '.join(refusals))

    def contract_info(self, host, path, name, sha):
        """The staged executable's compiled contract identity, or None for an older release.

        Releases built before `--contract-info` existed refuse the flag; they
        stay deployable and recorded as unknown. A report that is present but
        malformed or names another executable refuses staging.
        """
        if name not in contract.EXECUTABLES:
            return None
        code, output = self.ex.run(host, [path, *contract.CONTRACT_INFO_ARGV], SELF_CHECK_SECONDS)
        if code:
            return None
        try:
            return contract.parse_contract_info(output, executable=name, sha256=sha)
        except contract.ContractError as error:
            raise DeployError('%s: %s' % (host, error))

    def activate(self, journal, index, sha, *, verify=True, start=True, reload=True):
        """Install a reviewed role, optionally leaving it stopped for a fleet barrier."""
        record = journal.hosts[index]
        host, unit = record['host'], record['unit']
        directory = '%s/%s' % (self.service.transaction_dir(journal.id), unit)
        self.lock.verify()
        for item in record.get('launcher', []):
            if self.ex.sha256(host, item['path']) != item['sha256']:
                raise DeployError('credential launcher changed before restart: ' + item['path'])
        guard = self.service.roles[record['role']].product_guard()
        if guard is not None:
            # Observed again under the lock, immediately before any effect.
            refusals = guard.before_restart(self, record)
            if refusals:
                raise DeployError('%s: refused before restart: %s' % (record['key'], '; '.join(refusals)))
        journal.set_phase(index, 'backing-up')
        self.ex.mkdir(host, directory + '/files', 0o700)
        for number, change in enumerate(record['changes']):
            if change['previous'] is not None:
                self.ex.write(host, '%s/files/%d-%s' % (directory, number, os.path.basename(change['path'])),
                              change['previous'].encode(), 0o600)
        self.ex.write(host, directory + '/record.json', json.dumps(record, indent=2, sort_keys=True).encode(), 0o600)
        journal.set_phase(index, 'installing')
        if guard is not None and hasattr(guard, 'prepare_directories'):
            guard.prepare_directories(self, journal, record)
        else:
            for path in record.get('directories', []):
                self.ex.mkdir(host, path, 0o750)
        for change in record['changes']:
            if change['op'] == 'write':
                self.ex.mkdir(host, os.path.dirname(change['path']), 0o755)
                self.ex.write(host, change['path'], change['new'].encode(), 0o644)
            else:
                self.ex.mkdir(host, directory + '/retired', 0o700)
                self.ex.rename(host, change['path'], '%s/retired/%s' % (directory, os.path.basename(change['path'])))
        if reload:
            self.ex.systemctl(host, 'daemon-reload')
        if record['action'] == 'install':
            self.ex.systemctl(host, 'enable', unit)
        if not start:
            return
        self.start(journal, index, sha, verify=verify)

    def start(self, journal, index, sha, *, verify=True):
        """Start already installed files after the publisher dependency is ready."""
        record = journal.hosts[index]
        host, unit = record['host'], record['unit']
        self.lock.verify()
        record['restart_issued'] = True
        journal.set_phase(index, 'restarting')
        self.ex.systemctl(host, 'restart', unit)
        journal.set_phase(index, 'verifying')
        if not verify:
            # A coordinated publisher cutover verifies after its matching owner starts.
            return
        self.wait_verified(record, sha)
        journal.set_phase(index, 'verified')
        self.out('%s: running %s and ready' % (record['key'], sha))

    def product_checks(self, journal, sha):
        """Run each changing role's product check against the staged release and staged configuration.

        The check is the service's own loader, exiting without serving. Rendered
        configuration is staged in the host transaction directory first and the
        check reads that copy, so a refused configuration or input is found
        before any unit is replaced.
        """
        for record in journal.hosts:
            if not self.service.roles[record['role']].shared_lifecycle:
                raise DeployError('operator-managed role: generic deployment cannot run its stateful preflight')
            if record['action'] not in CHANGING or not record.get('check'):
                continue
            guard = self.service.roles[record['role']].product_guard()
            if guard is not None:
                # Never against live state: the guard checks in disposable state.
                guard.preflight(self, journal, record, sha)
                self.out('%s: product check passed in disposable state' % record['key'])
                continue
            self.configuration_check(journal, record, sha)

    def configuration_check(self, journal, record, sha):
        """Check staged configuration without opening the role's writable state."""
        host = record['host']
        self.lock.verify()
        staged = '%s/%s/check' % (self.service.transaction_dir(journal.id), record['unit'])
        argv = [self.release_binary(sha), *record['check']]
        renamed = {}
        for number, change in enumerate(record['changes']):
            if change['op'] == 'write' and not change['path'].startswith(SYSTEM + '/'):
                copy = '%s/%d-%s' % (staged, number, os.path.basename(change['path']))
                self.ex.mkdir(host, staged, 0o700)
                self.ex.write(host, copy, change['new'].encode(), 0o600)
                renamed[change['path']] = copy
        argv = [renamed.get(argument, argument) for argument in argv]
        journal.event('product check', host=host, unit=record['unit'], argv=argv)
        code, output = self.ex.run(host, argv, PRODUCT_CHECK_SECONDS)
        journal.event('product check finished', host=host, returncode=code, output=output[-2000:])
        if code:
            raise DeployError('%s: the product check refused the staged configuration or input (exit %d): %s'
                              % (record['key'], code, output[-1000:]))
        self.out('%s: product check passed' % record['key'])

    def run_exact_check(self, check, journal, sha):
        if 'products' in check:
            for product, command in check['products'].items():
                journal.event('product exact check', product=product)
                self.run_exact_check(command, journal, sha)
            return
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
        self.check_identities([self.inventory.lock.get('host'), *descriptors.exact_check_hosts(check)])
        with self.lock_factory() as lock:
            self.lock = lock
            try:
                self.schema_fence()
                return self._deploy(sha, binary, source, allow_drift, retire_historical, check, verify_noop)
            finally:
                self.lock = None

    def schema_fence(self):
        mutation_fence(self.inventory, self.ex, self.lock)

    def can_replace_incomplete(self, journal):
        """Product recovery may prove a stopped transaction has restored its processes."""
        return False

    def _deploy(self, sha, binary, source, allow_drift, retire_historical, check, verify_noop=False):
        latest = Journal.load(self.state_dir, self.service.name)
        if latest is not None and latest.status not in FINAL and not self.can_replace_incomplete(latest):
            raise DeployError('transaction %s is %s; finish it with rollback before deploying again'
                              % (latest.id, latest.status))
        plans, problems = self.assess(sha, binary, allow_drift, retire_historical)
        self.describe(plans, sha)
        if problems:
            raise DeployError('refused before any change:\n  ' + '\n  '.join(problems))
        restart = [plan for plan in plans if plan.action in CHANGING]
        if not restart and not verify_noop:
            self.out('no-op: every target already runs %s with the same effective unit' % sha)
            return None
        baseline = json.loads(self.baseline_path.read_text())
        journal = Journal.create(self.state_dir, self.service.name, sha, source,
                                 [self.record(plan) for plan in plans], baseline)
        if latest is not None and latest.status not in FINAL:
            journal.event('replaces incomplete transaction', transaction=latest.id, status=latest.status)
        self.out('transaction %s (%s)' % (journal.id, journal.path))
        try:
            identities = self.stage(list(dict.fromkeys(p.target.host for p in restart)), sha, binary, journal)
            self.contract_guard(restart, identities, journal)
            self.product_checks(journal, sha)
            journal.set_status('activating')
            for index, record in enumerate(journal.hosts):
                if record['action'] in CHANGING:
                    self.activate(journal, index, sha)
            # Later roles can disturb earlier ones (a restarted coordinator
            # re-fences routers), so every changed target is checked again.
            journal.set_status('verifying')
            for record in journal.hosts:
                if record['action'] in CHANGING:
                    self.wait_verified(record, sha)
            if check:
                self.run_exact_check(check, journal, sha)
            journal.set_status('committed')
        except BaseException as error:
            from .deployment_alerts import failed
            failed()
            journal.event('failure', error='%s: %s' % (type(error).__name__, error))
            if self.hold_on_failure and journal.touched():
                journal.set_status('held')
                self.out('deploy failed (%s); holding changed targets for operator repair' % error)
                raise
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
        for item in record.get('launcher', []):
            if self.ex.sha256(record['host'], item['path']) != item['sha256']:
                conflicts.append('credential launcher changed: ' + item['path'])
        for change in record['changes']:
            current = self.ex.sha256(record['host'], change['path'])
            if current not in (change['previous_sha256'], change['new_sha256']):
                conflicts.append('%s: %s changed since the transaction' % (record['key'], change['path']))
        return conflicts

    def restored_process_running(self, journal, record, expected):
        """Only an acknowledged restoration restart can be reused after readiness times out."""
        receipt = next((event for event in reversed(journal.data['events'])
                        if event['message'] in ('restore process started', 'restore process reset')
                        and event.get('target') == record['key']), None)
        if receipt is None or receipt['message'] != 'restore process started':
            return False
        state = self.ex.probe_unit(record['host'], record['unit'])
        if (state['active_state'] != 'active' or state['exe_sha256'] != expected
                or state.get('need_daemon_reload') or not state.get('invocation_id')
                or state['invocation_id'] != receipt.get('invocation_id')
                or receipt.get('exe_sha256') != expected):
            return False
        # Observe before rewriting: restored bytes alone cannot prove which
        # configuration the currently running process started with.
        return all(self.ex.sha256(record['host'], change['path']) == change['previous_sha256']
                   for change in record['changes'])

    def wait_restored(self, record, expected):
        """Verify the restored entrypoint rather than the replacement's listener.

        The retired combined Receiver runtime has no root management server.
        Its wallet inventory proves serving readiness at the captured bind socket.
        Executable, unit and configuration restoration are still checked normally.
        """
        if self.service.name == 'receiver':
            previous = record['previous']
            _prefix, binary, tail = units.split_exec(previous['exec_start'])
            if os.path.basename(binary) == 'receiver-directory':
                arguments = shlex.split(tail)
                bind = arguments[arguments.index('--bind') + 1] if '--bind' in arguments else '127.0.0.1:18380'
                address, port = bind.rsplit(':', 1)
                if not ipaddress.ip_address(address.strip('[]')).is_private or not 1 <= int(port) <= 65535:
                    raise DeployError('legacy Receiver rollback requires its captured private listener')
                record = {**record, 'verify': {**record['verify'], 'health': 'http://' + bind + '/v1/receiver/inventory',
                                              'health_equals': {}, 'ready': None, 'ready_url': None}}
        self.wait_verified(record, expected)

    def restore(self, journal, index, *, verify=True, start=True, reload=True):
        record = journal.hosts[index]
        host, unit = record['host'], record['unit']
        self.lock.verify()
        journal.set_phase(index, 'restoring')
        if record['action'] == 'install':
            return self.uninstall(journal, index)
        expected = record['previous']['exe_sha256']
        if not self.restored_process_running(journal, record, expected):
            # A lost restart reply must remain uncertain. Invalidate any older
            # receipt before effects, and persist completion before readiness.
            journal.event('restore process reset', target=record['key'])
            for change in reversed(record['changes']):
                self.lock.verify()
                if change['previous'] is None:
                    self.ex.remove(host, change['path'])
                else:
                    self.ex.mkdir(host, os.path.dirname(change['path']), 0o755)
                    self.lock.verify()
                    self.ex.write(host, change['path'], change['previous'].encode(), 0o644)
            self.lock.verify()
            if reload:
                self.ex.systemctl(host, 'daemon-reload')
            if start:
                self.start_restored(journal, index)
        if not verify or not start:
            return
        self.wait_restored(record, expected)
        self.lock.verify()
        journal.set_phase(index, 'restored')
        self.out('%s: restored %s' % (record['key'], expected))

    def start_restored(self, journal, index):
        """Restart previously restored files after a fleet dependency barrier."""
        record = journal.hosts[index]
        host, unit = record['host'], record['unit']
        expected = record['previous']['exe_sha256']
        state = self.ex.probe_unit(host, unit)
        if record.get('restart_issued') or state['active_state'] != 'active' or state['exe_sha256'] != expected:
            self.lock.verify()
            self.ex.systemctl(host, 'restart', unit)
            state = self.ex.probe_unit(host, unit)
            if (state['active_state'] == 'active' and state['exe_sha256'] == expected and state.get('invocation_id')):
                journal.event('restore process started', target=record['key'],
                              exe_sha256=expected, invocation_id=state['invocation_id'])

    def uninstall(self, journal, index):
        """Roll a fresh install back to absence: stop, disable and remove what it wrote.

        Created state directories stay: they hold no authority, and a restart
        verifies everything from its inputs again.
        """
        record = journal.hosts[index]
        host, unit = record['host'], record['unit']
        if record.get('restart_issued'):
            self.ex.systemctl(host, 'stop', unit)
            self.ex.systemctl(host, 'disable', unit)
        for change in reversed(record['changes']):
            if change['previous'] is None:
                self.ex.remove(host, change['path'])
            else:
                self.ex.write(host, change['path'], change['previous'].encode(), 0o644)
        self.ex.systemctl(host, 'daemon-reload')
        state = self.ex.probe_unit(host, unit)
        if state['load_state'] == 'loaded' or state['active_state'] == 'active':
            raise DeployError('%s: still %s %s after removal' % (record['key'], state['load_state'], state['active_state']))
        journal.set_phase(index, 'restored')
        self.out('%s: removed; the role is absent again' % record['key'])

    def rollback_journal(self, journal, force=False):
        pending = [index for index in reversed(journal.touched()) if journal.hosts[index]['phase'] != 'restored']
        conflicts = [c for index in pending for c in self.restore_conflicts(journal.hosts[index])]
        if conflicts and not force:
            journal.event('rollback refused', conflicts=conflicts)
            raise DeployError('rollback refused; unit files changed after the transaction:\n  ' + '\n  '.join(conflicts))
        # A guarded role's restore must be safe with its current durable state;
        # --force overrides unit-file conflicts only, never these gates.
        gates = []
        for index in pending:
            record = journal.hosts[index]
            guard = self.service.roles[record['role']].product_guard()
            if guard is not None:
                gates += ['%s: %s' % (record['key'], g) for g in guard.before_restore(self, journal, record)]
        if gates:
            journal.event('rollback refused', gates=gates)
            raise DeployError('rollback refused before any change:\n  ' + '\n  '.join(gates))
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
        if any(not self.service.roles[record['role']].shared_lifecycle for record in journal.hosts):
            raise DeployError('operator-managed role: generic rollback cannot mutate its unit or exclusive state')
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
            elapsed = timing.journal_timing(journal.data)
            for line in timing.describe(elapsed):
                self.out(line)
        else:
            self.out('no %s transaction recorded' % self.service.name)
        observed = []
        for target in self.targets:
            # An unreachable role is reported as such, never left out.
            try:
                state = self.ex.probe_unit(target.host, target.unit)
            except Exception as error:  # noqa: BLE001 - each host's failure is its own result
                self.out('%s %s: unobserved (%s)' % (target.key, target.unit, error))
                observed.append({'role': target.role.name, 'host': target.host, 'unit': target.unit,
                                 'active_state': None, 'binary_sha256': None, 'error': str(error)})
                continue
            self.out('%s %s: %s %s' % (target.key, target.unit, state['active_state'], state['exe_sha256']))
            observed.append({'role': target.role.name, 'host': target.host, 'unit': target.unit,
                             'active_state': state['active_state'], 'binary_sha256': state['exe_sha256']})
        return {'targets': observed, 'transaction': journal.id if journal else None,
                'phase': journal.status if journal else None,
                'timing': timing.journal_timing(journal.data) if journal else None}

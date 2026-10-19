"""Locked activation of Transparent alert delivery: pir-apm and the external pir-monitor.

One reviewed request (bound by `--request-sha256`) names two components:

- `apm`: the coordinator's `pir-apm.service`. Its `ExecStart` runs
  `python3 <credential-exec.py> <pir-apm>`; the wrapper execs the binary, so the
  running executable is pir-apm. A transaction may replace only that final binary
  argument (through the managed ExecStart drop-in, with the wrapper, its digest and
  every other unit setting unchanged) and only the `alert_mode` of the named services
  in pir-apm's Transparent configuration.
- `monitor` (optional): the external `pir-monitor.service`. Only its service-probe
  family mode changes, through a final nonsecret `EnvironmentFile` drop-in, so the
  global `PIR_MONITOR_ALERT_MODE` and the credential drop-ins stay as they are. That
  family covers the Receiver, Status, Transparent and txid probes together.

Nothing else is written: no credential, incident, history or outbox database. Every
pinned digest (unit files, wrapper, running executable, configuration) is checked in
preflight and again under the production lock. The runner journal records every
previous byte before an effect; a failure after any effect restores every touched
component and its running process. A verified component reports its *running* modes
from its own incident database (each family records the mode it last evaluated in),
not from the configuration on disk, which a process reads only at start.

`test` queues one labelled FIRED/RECOVERED pair into a component's existing outbox
with the checksum-qualified pir-apm binary (`--enqueue-test-alert`, which exits
without starting a daemon); the running service's own delivery worker sends it, and
the operation waits until that worker reports the queue drained.
"""
import hashlib
import json
import os
import shlex
import time

from wallet_pir_ops import durable
from wallet_pir_ops.deploy import descriptors, units
from wallet_pir_ops.deploy.engine import DeployError, file_sha256, mutation_fence
from wallet_pir_ops.deploy.remote import production_lock
from wallet_pir_ops.deploy.transaction import FINAL, Journal

SCHEMA = 'transparent-alert-activation/1'
SERVICE = 'transparent-alerts'
SYSTEM = '/etc/systemd/system'
APM_ROOT = '/opt/pir-quality'
WRAPPER = 'credential-exec.py'
MONITOR_ENV = '/etc/pir-monitor/service-alert-mode.env'
MONITOR_DROP_IN = SYSTEM + '/pir-monitor.service.d/95-service-alert-mode.conf'
MONITOR_TOOLS = '/opt/pir-monitor/tools'
MODES = ('shadow', 'active')
SHA256 = '[0-9a-f]{64}'
COMPONENT_KEYS = {'host', 'unit', 'unit_files_sha256', 'wrapper_sha256', 'running_sha256', 'state_path', 'listen',
                  'environment'}
VERIFY_SECONDS = 180
TEST_SECONDS = 300
# Read-only: each family's last evaluated mode, written by every evaluation.
MODE_QUERY = ("import json,sqlite3,sys;d=sqlite3.connect('file:'+sys.argv[1]+'?mode=ro',uri=True);"
              "print(json.dumps(dict(d.execute(\"SELECT key,value FROM metadata WHERE key LIKE 'mode%'\"))))")


def require(condition, message):
    if not condition:
        raise DeployError('invalid alert activation request: ' + message)


def is_sha256(value):
    import re
    return isinstance(value, str) and re.fullmatch(SHA256, value) is not None


def load_request(path, expected_sha256):
    with open(path, 'rb') as handle:
        data = handle.read()
    if hashlib.sha256(data).hexdigest() != expected_sha256:
        raise DeployError('request checksum differs from --request-sha256')
    return validate(json.loads(data))


def validate(request):
    require(isinstance(request, dict) and set(request) == {'schema', 'apm', 'monitor'}, 'keys are schema, apm, monitor')
    require(request['schema'] == SCHEMA, 'schema must be ' + SCHEMA)
    apm, monitor = request['apm'], request['monitor']
    require(isinstance(apm, dict) and set(apm) == COMPONENT_KEYS | {'release', 'config'},
            'apm is %s' % sorted(COMPONENT_KEYS | {'release', 'config'}))
    require(monitor is None or isinstance(monitor, dict) and set(monitor) == COMPONENT_KEYS | {'service_alert_mode'},
            'monitor is null or %s' % sorted(COMPONENT_KEYS | {'service_alert_mode'}))
    for name, component in (('apm', apm), ('monitor', monitor)):
        if component is None:
            continue
        require(all(is_sha256(component[k]) for k in ('unit_files_sha256', 'wrapper_sha256', 'running_sha256')),
                '%s digests are 64 lowercase hex digits' % name)
        require(isinstance(component['unit'], str) and descriptors.UNIT.match(component['unit']),
                '%s.unit is a service' % name)
        require(isinstance(component['host'], str) and component['host'], '%s.host is an inventory host' % name)
        require(isinstance(component['state_path'], str) and component['state_path'].startswith('/'),
                '%s.state_path is absolute' % name)
        require(isinstance(component['listen'], str) and component['listen'].startswith('127.0.0.1:'),
                '%s.listen is its loopback address' % name)
        require(isinstance(component['environment'], str) and component['environment'],
                '%s.environment is its incident label' % name)
    release = apm['release']
    require(isinstance(release, dict) and set(release) == {'git_sha', 'binary_sha256'}
            and is_sha256(release['binary_sha256']) and isinstance(release['git_sha'], str)
            and len(release['git_sha']) == 40, 'apm.release is {git_sha (40 hex), binary_sha256}')
    config = apm['config']
    require(isinstance(config, dict) and set(config) == {'path', 'previous_sha256', 'alert_modes'}
            and config['path'].startswith('/') and is_sha256(config['previous_sha256']),
            'apm.config is {path, previous_sha256, alert_modes}')
    modes = config['alert_modes']
    require(isinstance(modes, dict) and modes and all(m in MODES for m in modes.values()),
            'apm.config.alert_modes maps service names to shadow or active')
    require(monitor is None or monitor['service_alert_mode'] in MODES, 'monitor.service_alert_mode is shadow or active')
    return request


def unit_files_digest(state):
    """The exact loaded unit: fragment and drop-ins, by path and bytes."""
    files = [[state['fragment_path'], state['fragment_text']]] + [[d['path'], d['text']] for d in state['drop_ins']]
    return hashlib.sha256(json.dumps(files).encode()).hexdigest()


def unit_texts(state):
    return [state['fragment_text'] or ''] + [d['text'] or '' for d in state['drop_ins']]


def wrapped_exec(texts):
    """`(prefix, interpreter, wrapper, binary, rest)` of a `python3 credential-exec.py <binary>` ExecStart."""
    prefix, interpreter, tail = units.split_exec(units.exec_start(units.effective(texts)))
    words = tail.split()
    if not os.path.basename(interpreter).startswith('python3') or len(words) < 2 \
            or os.path.basename(words[0]) != WRAPPER or not words[1].startswith('/'):
        raise DeployError('ExecStart is not python3 %s <absolute binary>: %r' % (WRAPPER, interpreter + ' ' + tail))
    return prefix, interpreter, words[0], words[1], words[2:]


def without_exec(config):
    service = {k: v for k, v in config.get('Service', {}).items() if k != 'ExecStart'}
    return {**config, 'Service': service}


def without_env_file(config):
    service = dict(config.get('Service', {}))
    service['EnvironmentFile'] = [f for f in service.get('EnvironmentFile', []) if f != MONITOR_ENV] or None
    return {**config, 'Service': {k: v for k, v in service.items() if v is not None}}


def apm_binary(sha):
    return '%s/releases/%s/pir-apm' % (APM_ROOT, sha)


def monitor_tool(sha):
    return '%s/%s/pir-apm' % (MONITOR_TOOLS, sha)


def running_modes(ex, host, state_path):
    """Each family's last evaluated mode from a service's own incident database, or None."""
    try:
        code, output = ex.run(host, ['/usr/bin/python3', '-c', MODE_QUERY, state_path], 30)
        modes = json.loads(output.strip().splitlines()[-1]) if code == 0 else None
    except Exception:  # an unreachable host or unreadable database is reported as unknown
        modes = None
    return modes if isinstance(modes, dict) else None


def monitor_status(ex, host, listen):
    """The running service's `/monitor-status` body, or None when unreachable or invalid."""
    try:
        status, body = ex.http_get(host, 'http://%s/monitor-status' % listen)
        document = json.loads(body) if status == 200 else None
    except Exception:  # unreachable or invalid is reported as unknown, never as healthy
        document = None
    return document if isinstance(document, dict) else None


def delivery_line(label, status):
    if status is None:
        return '%s delivery: unknown (monitor-status unreachable or invalid)' % label
    delivery = status.get('delivery') if isinstance(status.get('delivery'), dict) else {}
    return ('%s delivery: webhook configured=%s pending=%s last_success=%s failure=%s progress_ok=%s'
            % (label, delivery.get('configured'), delivery.get('pending'), delivery.get('last_success'),
               delivery.get('failure'), status.get('progress_ok')))


class Activation:
    """The request against live state. Every method here only reads."""

    def __init__(self, request, ex, out=print, clock=time.monotonic, sleep=time.sleep, poll_seconds=2.0):
        self.request = request
        self.ex = ex
        self.out = out
        self.clock = clock
        self.sleep = sleep
        self.poll_seconds = poll_seconds

    def components(self):
        return [(name, self.request[name]) for name in ('apm', 'monitor') if self.request[name] is not None]

    # ------------------------------------------------------------ runtime

    def running_modes(self, name):
        component = self.request[name]
        return running_modes(self.ex, component['host'], component['state_path'])

    def monitor_status(self, name):
        component = self.request[name]
        return monitor_status(self.ex, component['host'], component['listen'])

    def expected_modes(self, name, new):
        """Family mode keys a verified process must have recorded."""
        if name == 'apm':
            modes = self.request['apm']['config']['alert_modes']
            return {'mode:quality-' + service: (mode if new else None) for service, mode in modes.items()}
        return {'mode:service-quality': self.request['monitor']['service_alert_mode'] if new else None}

    def report(self, name):
        """Lines naming what is running, never inferred from configuration alone."""
        modes = self.running_modes(name)
        status = self.monitor_status(name)
        lines = []
        if modes is None:
            lines.append('%s running modes: unknown (incident database unreadable)' % name)
        else:
            keys = sorted(k for k in modes if k == 'mode' or k.startswith(('mode:quality-', 'mode:service-quality')))
            lines.append('%s running modes: %s' % (name, ', '.join('%s=%s' % (k, modes[k]) for k in keys) or 'none recorded'))
        lines.append(delivery_line(name, status))
        return lines

    # ------------------------------------------------------------- planning

    def probe(self, name):
        component = self.request[name]
        state = self.ex.probe_unit(component['host'], component['unit'])
        problems = []
        if state['load_state'] != 'loaded' or state['active_state'] != 'active' or not state['exe_sha256']:
            return state, ['%s: %s is %s/%s, not running' % (name, component['unit'], state['load_state'], state['active_state'])]
        if state.get('need_daemon_reload'):
            problems.append('%s: unit files changed on disk without a daemon-reload' % name)
        if unit_files_digest(state) != component['unit_files_sha256']:
            problems.append('%s: unit files differ from the pinned unit_files_sha256' % name)
        if state['exe_sha256'] != component['running_sha256']:
            problems.append('%s: running executable %s, pinned %s' % (name, state['exe_sha256'], component['running_sha256']))
        try:
            _, _, wrapper, binary, _ = wrapped_exec(unit_texts(state))
        except (DeployError, units.UnitError) as error:
            return state, problems + ['%s: %s' % (name, error)]
        if self.ex.sha256(component['host'], wrapper) != component['wrapper_sha256']:
            problems.append('%s: credential wrapper %s differs from wrapper_sha256' % (name, wrapper))
        if self.ex.sha256(component['host'], binary) != state['exe_sha256']:
            problems.append('%s: running executable differs from the wrapped binary %s, so rollback could not '
                            'restore it' % (name, binary))
        return state, problems

    def plan(self, name):
        """`(record, problems)`: the files this component's transaction writes, with their previous bytes."""
        component = self.request[name]
        state, problems = self.probe(name)
        record = {'key': name, 'host': component['host'], 'unit': component['unit'], 'phase': 'pending',
                  'previous_exe_sha256': state.get('exe_sha256'), 'expected_exe_sha256': state.get('exe_sha256'),
                  'previous_invocation_id': state.get('invocation_id'), 'changes': []}
        if problems:
            return record, problems
        if name == 'apm':
            changes, expected, more = self.apm_changes(state)
        else:
            changes, expected, more = self.monitor_changes(state)
        problems += more
        record['changes'], record['expected_exe_sha256'] = changes, expected
        return record, problems

    def change(self, host, path, new):
        previous = self.ex.read(host, path)
        if previous == new:
            return None
        return {'path': path, 'previous': previous, 'new': new,
                'previous_sha256': None if previous is None else hashlib.sha256(previous.encode()).hexdigest(),
                'new_sha256': None if new is None else hashlib.sha256(new.encode()).hexdigest()}

    def apm_changes(self, state):
        apm = self.request['apm']
        host, problems, changes = apm['host'], [], []
        prefix, interpreter, wrapper, binary, rest = wrapped_exec(unit_texts(state))
        sha = apm['release']['binary_sha256']
        managed = '%s/%s.d/%s' % (SYSTEM, apm['unit'], units.MANAGED_DROP_IN)
        later = [d['path'] for d in state['drop_ins'] if os.path.basename(d['path']) > units.MANAGED_DROP_IN
                 and units.sets_exec_start(d['text'] or '')]
        if later:
            problems.append('apm: drop-ins after the managed one set ExecStart: %s' % later)
        if sha != state['exe_sha256']:
            new = units.managed_drop_in(prefix, interpreter, ' '.join([wrapper, apm_binary(sha)] + rest))
            texts = [state['fragment_text'] or ''] + [d['text'] for d in state['drop_ins'] if d['path'] != managed] + [new]
            desired = units.effective(texts)
            current = units.effective(unit_texts(state))
            if units.normalized(without_exec(desired)) != units.normalized(without_exec(current)):
                problems.append('apm: replacing the binary would change other unit settings')
            if wrapped_exec(texts)[1:] != (interpreter, wrapper, apm_binary(sha), rest):
                problems.append('apm: the new unit would not run %s through %s' % (apm_binary(sha), wrapper))
            changes.append(self.change(host, managed, new))
        config = apm['config']
        text = self.ex.read(host, config['path'])
        if text is None or hashlib.sha256(text.encode()).hexdigest() != config['previous_sha256']:
            problems.append('apm: %s differs from config.previous_sha256' % config['path'])
        else:
            document = json.loads(text)
            services = {s.get('service'): s for s in document.get('services', [])}
            missing = sorted(set(config['alert_modes']) - set(services))
            if missing:
                problems.append('apm: configuration has no services %s' % missing)
            elif any(services[n].get('alert_mode', 'shadow') != m for n, m in config['alert_modes'].items()):
                for service, mode in config['alert_modes'].items():
                    services[service]['alert_mode'] = mode
                changes.append(self.change(host, config['path'], json.dumps(document, indent=2) + '\n'))
        return [c for c in changes if c], sha, problems

    def monitor_changes(self, state):
        monitor = self.request['monitor']
        host = monitor['host']
        env = 'PIR_MONITOR_SERVICE_ALERT_MODE=%s\n' % monitor['service_alert_mode']
        drop_in = '[Service]\nEnvironmentFile=%s\n' % MONITOR_ENV
        changes = [self.change(host, MONITOR_ENV, env), self.change(host, MONITOR_DROP_IN, drop_in)]
        # systemd reads drop-ins in basename order; EnvironmentFile values apply over
        # Environment=, and a later file over an earlier one.
        drop_ins = {os.path.basename(d['path']): d['text'] or '' for d in state['drop_ins']}
        drop_ins[os.path.basename(MONITOR_DROP_IN)] = drop_in
        desired = units.effective([state['fragment_text'] or ''] + [drop_ins[k] for k in sorted(drop_ins)])
        problems = []
        files = desired.get('Service', {}).get('EnvironmentFile', [])
        if not files or files[-1] != MONITOR_ENV:
            problems.append('monitor: %s would not be the last EnvironmentFile' % MONITOR_ENV)
        if units.normalized(without_env_file(desired)) != units.normalized(without_env_file(units.effective(unit_texts(state)))):
            problems.append('monitor: the drop-in would change unit settings beyond its EnvironmentFile')
        return [c for c in changes if c], state['exe_sha256'], problems

    def assess(self):
        records, problems = [], []
        for name, _ in self.components():
            record, more = self.plan(name)
            records.append(record)
            problems += more
        sha = self.request['apm']['release']['binary_sha256']
        if any(r['key'] == 'apm' and r['expected_exe_sha256'] != r['previous_exe_sha256'] for r in records):
            if self.ex.sha256(self.request['apm']['host'], apm_binary(sha)) not in (None, sha):
                problems.append('apm: immutable release %s holds different bytes' % apm_binary(sha))
        return records, problems

    def describe(self, records):
        for record in records:
            action = 'restart' if record['changes'] else 'unchanged'
            self.out('%s %s@%s: %s; running %s, after %s' % (record['key'], record['unit'], record['host'], action,
                                                           record['previous_exe_sha256'], record['expected_exe_sha256']))
            for change in record['changes']:
                self.out('  write %s (%s -> %s)' % (change['path'], change['previous_sha256'], change['new_sha256']))


class Activator:
    """The locked transaction: stage, write, restart, verify; restore on any failure."""

    def __init__(self, activation, inventory, state_dir, lock=None):
        self.a = activation
        self.ex = activation.ex
        self.out = activation.out
        self.inventory = inventory
        self.state_dir = state_dir
        self.lock_factory = lock or (lambda: production_lock(inventory, self.ex))
        self.lock = None

    def hosts(self):
        return list(dict.fromkeys([c['host'] for _, c in self.a.components()] + [self.inventory.lock.get('host')]))

    def check_identities(self):
        for host in self.hosts():
            if host:
                try:
                    self.ex.check_identity(host)
                except Exception as error:
                    raise DeployError('deploy identity is not accepted by %s; nothing was changed (%s)' % (host, error)) from error

    def locked(self, action):
        self.check_identities()
        with self.lock_factory() as lock:
            self.lock = lock
            try:
                mutation_fence(self.inventory, self.ex, lock)
                return action()
            finally:
                self.lock = None

    def deploy(self, binary, source):
        return self.locked(lambda: self._deploy(binary, source))

    def _deploy(self, binary, source):
        latest = Journal.load(self.state_dir, SERVICE)
        if latest is not None and latest.status not in FINAL:
            raise DeployError('transaction %s is %s; finish it with rollback before deploying again' % (latest.id, latest.status))
        records, problems = self.a.assess()
        self.a.describe(records)
        if problems:
            raise DeployError('refused under the lock before any change:\n  ' + '\n  '.join(problems))
        records = [r for r in records if r['changes']]
        if not records:
            self.out('no-op: every component already runs the requested binary and modes')
            return None
        sha = self.a.request['apm']['release']['binary_sha256']
        journal = Journal.create(self.state_dir, SERVICE, sha, source, records, None)
        self.out('transaction %s (%s)' % (journal.id, journal.path))
        try:
            if any(r['key'] == 'apm' and r['expected_exe_sha256'] != r['previous_exe_sha256'] for r in records):
                self.stage(journal, self.a.request['apm']['host'], apm_binary(sha), sha, binary)
            journal.set_status('activating')
            for index, record in enumerate(journal.hosts):
                self.apply(journal, index, new=True)
            journal.set_status('committed')
        except BaseException as error:
            journal.event('failure', error='%s: %s' % (type(error).__name__, error))
            if journal.touched():
                self.out('deploy failed (%s); restoring %d touched component(s)' % (error, len(journal.touched())))
                try:
                    self.restore(journal)
                except Exception as failure:
                    raise DeployError('deploy failed (%s) and its rollback did not complete: %s' % (error, failure)) from error
            else:
                journal.set_status('failed')
            raise
        self.out('committed %s' % journal.id)
        for name, _ in self.a.components():
            for line in self.a.report(name):
                self.out(line)
        return journal

    def stage(self, journal, host, path, sha, binary):
        """Upload the immutable release under the lock and run its self-check there."""
        self.lock.verify()
        current = self.ex.sha256(host, path)
        if current is None:
            if binary is None:
                raise DeployError('%s: release %s is not staged and no --binary was given' % (host, sha[:12]))
            if file_sha256(binary) != sha:
                raise DeployError('--binary does not hash to apm.release.binary_sha256')
            journal.event('staging', host=host, path=path)
            partial = os.path.dirname(path) + '.partial-' + journal.id
            self.ex.mkdir(host, partial, 0o755)
            self.ex.upload(host, binary, partial + '/' + os.path.basename(path), 0o755)
            if self.ex.sha256(host, partial + '/' + os.path.basename(path)) != sha:
                raise DeployError('%s: uploaded binary does not match %s' % (host, sha))
            self.lock.verify()
            self.ex.rename(host, partial, os.path.dirname(path))
        elif current != sha:
            raise DeployError('%s: immutable release %s holds different bytes' % (host, path))
        code, output = self.ex.run(host, [path, '--help'], 60)
        if code:
            raise DeployError('%s: staged binary failed its self-check (exit %d): %s' % (host, code, output[-500:]))
        self.out('%s: staged %s and it runs' % (host, path))

    def apply(self, journal, index, new):
        """Write one component's files (new or previous bytes), restart it and verify it."""
        record = journal.hosts[index]
        host = record['host']
        self.lock.verify()
        journal.set_phase(index, 'installing' if new else 'restoring')
        for change in record['changes']:
            text = change['new'] if new else change['previous']
            if text is None:
                self.ex.remove(host, change['path'])
            else:
                self.ex.mkdir(host, os.path.dirname(change['path']), 0o755)
                self.ex.write(host, change['path'], text.encode(), 0o644)
        self.ex.systemctl(host, 'daemon-reload')
        before = self.ex.probe_unit(host, record['unit']).get('invocation_id')
        journal.event('restart issued', target=record['key'], invocation_id=before)
        if new:
            journal.set_phase(index, 'restarting')
        self.ex.systemctl(host, 'restart', record['unit'])
        if new:
            journal.set_phase(index, 'verifying')
        self.wait_verified(record, new, before)
        journal.set_phase(index, 'verified' if new else 'restored')
        self.out('%s: running %s and verified' % (record['key'],
                                                  record['expected_exe_sha256' if new else 'previous_exe_sha256']))

    def verified(self, record, new, before):
        state = self.ex.probe_unit(record['host'], record['unit'])
        expected = record['expected_exe_sha256' if new else 'previous_exe_sha256']
        if state['active_state'] != 'active' or state['exe_sha256'] != expected:
            return 'unit is %s running %s, expected %s' % (state['active_state'], state['exe_sha256'], expected)
        if state.get('need_daemon_reload') or state.get('invocation_id') == before:
            return 'unit has not restarted with the written files'
        if self.a.monitor_status(record['key']) is None:
            return 'monitor-status is unreachable'
        modes = self.a.running_modes(record['key'])
        if modes is None:
            return 'incident database is unreadable'
        if new:
            wrong = {k: modes.get(k) for k, v in self.a.expected_modes(record['key'], True).items() if modes.get(k) != v}
            if wrong:
                return 'running modes %s, expected %s' % (wrong, self.a.expected_modes(record['key'], True))
        return None

    def wait_verified(self, record, new, before):
        deadline = self.a.clock() + VERIFY_SECONDS
        while True:
            reason = self.verified(record, new, before)
            if reason is None:
                return
            if self.a.clock() >= deadline:
                raise DeployError('%s not verified within %ss: %s' % (record['key'], VERIFY_SECONDS, reason))
            self.a.sleep(self.a.poll_seconds)

    def conflicts(self, record):
        return ['%s: %s changed since the transaction' % (record['key'], c['path']) for c in record['changes']
                if self.ex.sha256(record['host'], c['path']) not in (c['previous_sha256'], c['new_sha256'])]

    def restore(self, journal, force=False):
        """Restore every touched component, newest first, to its journalled bytes and process."""
        journal.set_status('rolling-back')
        touched = journal.touched()
        problems = [p for i in touched for p in self.conflicts(journal.hosts[i])]
        if problems and not force:
            journal.set_status('rollback-refused', problems=problems)
            raise DeployError('rollback refused; files changed outside the transaction (review, then --force):\n  '
                              + '\n  '.join(problems))
        for index in reversed(touched):
            try:
                self.apply(journal, index, new=False)
            except BaseException:
                journal.set_phase(index, 'restore-failed')
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
                return journal
            if not journal.touched():
                journal.set_status('failed')
                self.out('%s touched nothing; marked failed' % journal.id)
                return journal
            self.restore(journal, force)
            return journal
        return self.locked(run)

    def test(self, name, binary):
        """Queue one labelled FIRED/RECOVERED pair into a component's outbox and wait for its worker to deliver it."""
        return self.locked(lambda: self._test(name, binary))

    def _test(self, name, binary):
        component = self.a.request[name]
        if component is None:
            raise DeployError('the request has no %s component' % name)
        state, problems = self.a.probe(name)
        if problems:
            raise DeployError('refused before any change:\n  ' + '\n  '.join(problems))
        sha = self.a.request['apm']['release']['binary_sha256']
        host = component['host']
        tool = apm_binary(sha) if name == 'apm' else monitor_tool(sha)
        journal = Journal.create(self.state_dir, SERVICE + '-test', sha, {'component': name}, [], None)
        try:
            if name == 'monitor' or self.ex.sha256(host, tool) is None:
                self.stage(journal, host, tool, sha, binary)
            elif self.ex.sha256(host, tool) != sha:
                raise DeployError('%s holds different bytes' % tool)
            before = self.a.monitor_status(name)
            if before is None or not (before.get('delivery') or {}).get('configured'):
                raise DeployError('%s delivery is not configured or monitor-status is unreachable; nothing was queued' % name)
            user = units.effective(unit_texts(state)).get('Service', {}).get('User')
            argv = (['/usr/sbin/runuser', '-u', user, '--'] if user else []) + [
                '/usr/bin/env', 'PIR_APM_STATE_PATH=' + component['state_path'],
                'PIR_APM_ENVIRONMENT=' + component['environment'], tool, '--enqueue-test-alert']
            self.lock.verify()
            journal.event('enqueue test alert', host=host, argv=argv)
            code, output = self.ex.run(host, argv, 60)
            journal.event('enqueue test alert finished', returncode=code, output=output[-1000:])
            if code:
                raise DeployError('%s: enqueue failed (exit %d): %s' % (name, code, output[-500:]))
            previous = (before.get('delivery') or {}).get('last_success') or 0
            deadline = self.a.clock() + TEST_SECONDS
            while True:
                status = self.a.monitor_status(name) or {}
                delivery = status.get('delivery') or {}
                if (delivery.get('last_success') or 0) > previous and delivery.get('pending') == 0:
                    break
                if self.a.clock() >= deadline:
                    raise DeployError('%s: test messages not delivered within %ss (pending %s, failure %s); they stay '
                                      'queued in the durable outbox' % (name, TEST_SECONDS, delivery.get('pending'),
                                                                        delivery.get('failure')))
                self.a.sleep(self.a.poll_seconds)
            journal.set_status('committed', delivered_unix=delivery.get('last_success'))
            self.out('%s: labelled test FIRED/RECOVERED delivered by the running worker (last_success %s)'
                     % (name, delivery.get('last_success')))
        except BaseException as error:
            journal.event('failure', error='%s: %s' % (type(error).__name__, error))
            journal.set_status('failed')
            raise
        return journal


def run(args, executor, out, inventory, lock=None, clock=time.monotonic, sleep=time.sleep, poll_seconds=2.0):
    """`transparent-alerts-*` entry point; returns the exit code."""
    action = args.command.removeprefix('transparent-alerts-')
    request = load_request(args.request, args.request_sha256)
    activation = Activation(request, executor, out, clock, sleep, poll_seconds)
    activator = Activator(activation, inventory, args.state_dir, lock)
    binary = getattr(args, 'binary', None)
    source = {'request_sha256': args.request_sha256, 'git_sha': request['apm']['release']['git_sha']}
    problems = []
    if action in ('plan', 'preflight', 'status'):
        if action != 'plan':
            activator.check_identities()
            mutation_fence(inventory, executor)
        records, problems = activation.assess()
        activation.describe(records)
        if action == 'preflight' and binary and file_sha256(binary) != request['apm']['release']['binary_sha256']:
            problems.append('--binary does not hash to apm.release.binary_sha256')
        latest = Journal.load(args.state_dir, SERVICE)
        if latest is not None:
            out('latest transaction %s: %s' % (latest.id, latest.status))
            if latest.status not in FINAL:
                problems.append('transaction %s is %s; roll it back first' % (latest.id, latest.status))
        for name, _ in activation.components():
            for line in activation.report(name):
                out(line)
    elif action == 'deploy':
        activator.deploy(binary, source)
    elif action == 'rollback':
        activator.rollback(args.transaction, args.force)
    elif action == 'test':
        activator.test(args.component, binary)
    for problem in problems:
        out('problem: ' + problem)
    if action in ('plan', 'preflight', 'deploy'):
        out('rollback: ' + shlex.join(['tools/scripts/wallet-pir-deploy.py', '--inventory', str(args.inventory),
                                       '--state-dir', str(args.state_dir), 'transparent-alerts-rollback',
                                       '--request', args.request, '--request-sha256', args.request_sha256]))
    return 1 if problems else 0

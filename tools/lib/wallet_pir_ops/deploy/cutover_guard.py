"""Durable arbitration between a forward cutover and its executing watchdog.

The coordinator stores the first interruption time once. A separate systemd
service survives the invoking shell and reboot. At the deadline it fences
forward progress, stops only the transaction's runner, then invokes the ordinary
locked rollback. Recovery retries retain both the deadline and the decision.
"""
import contextlib
import fcntl
import json
import math
from pathlib import Path
import re
import time

from .. import durable
from .engine import DeployError

SECONDS = 3600


def boot_clock():
    return time.clock_gettime(time.CLOCK_BOOTTIME)


class Decision:
    def __init__(self, directory, identifier, *, clock=time.time, boot_id=None, elapsed=None):
        if not re.fullmatch('publisher-[A-Za-z0-9-]+', identifier):
            raise DeployError('invalid supervised publisher transaction')
        self.directory = Path(directory)
        self.identifier, self.clock = identifier, clock
        self.path = self.directory / 'decision.json'
        boot = Path('/proc/sys/kernel/random/boot_id')
        self.boot_id = boot_id if boot_id is not None else boot.read_text().strip() if boot.is_file() else None
        self.elapsed = elapsed or (boot_clock if self.boot_id else None)

    @contextlib.contextmanager
    def locked(self):
        # Callers create the private transaction directory while holding the
        # production lock. No lock here is held across a remote operation.
        with (self.directory / 'decision.lock').open('a') as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            state = json.loads(self.path.read_text())
            if state['id'] != self.identifier or state['version'] != 1:
                raise DeployError('supervised transaction identity changed')
            yield state

    def create(self, request_sha256):
        if not re.fullmatch('[a-f0-9]{64}', request_sha256):
            raise DeployError('supervised request requires its full checksum')
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        with (self.directory / 'decision.lock').open('a') as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            if self.path.exists():
                raise DeployError('supervised transaction already exists')
            durable.atomic_json(self.path, {'version': 1, 'id': self.identifier,
                'request_sha256': request_sha256, 'phase': 'staging',
                'runner_started': False,
                'interrupted_unix': None, 'deadline_unix': None}, mode=0o600)

    def started(self, invocation, pid):
        with self.locked() as state:
            self.check(state)
            if state['runner_started']:
                raise DeployError('forward runner cannot restart an interrupted cutover')
            state.update(runner_started=True, runner_invocation=invocation, runner_pid=pid)
            durable.atomic_json(self.path, state, mode=0o600)

    def cancel_before_start(self):
        with self.locked() as state:
            if not state['runner_started'] and state['phase'] == 'staging':
                state['phase'] = 'rollback'
                durable.atomic_json(self.path, state, mode=0o600)

    def arm(self):
        with self.locked() as state:
            if state['phase'] not in ('staging', 'armed'):
                raise DeployError('transaction is fenced against forward progress')
            if state['interrupted_unix'] is None:
                now = self.clock()
                if not math.isfinite(now) or now <= 0:
                    raise DeployError('invalid coordinator wall clock')
                # Start conservatively at the beginning of this second, before
                # the first stop. Calendar scheduling never extends the hour.
                now = int(now)
                state.update(phase='armed', interrupted_unix=now, deadline_unix=now + SECONDS)
                if self.boot_id:
                    state.update(boot_id=self.boot_id, deadline_elapsed=self.elapsed() + SECONDS)
                durable.atomic_json(self.path, state, mode=0o600)
            self.check(state)
            return state['deadline_unix']

    def check(self, state):
        if state['phase'] not in ('staging', 'armed'):
            raise DeployError('watchdog has fenced forward progress')
        if self.due(state):
            raise DeployError('one-hour cutover deadline reached; rollback is required')

    def due(self, state):
        wall = state['deadline_unix'] is not None and self.clock() >= state['deadline_unix']
        elapsed = (self.boot_id is not None and self.boot_id == state.get('boot_id')
                   and self.elapsed() >= state.get('deadline_elapsed', float('inf')))
        rebooted = (state['interrupted_unix'] is not None and self.boot_id is not None
                    and state.get('boot_id') is not None and self.boot_id != state['boot_id'])
        return wall or elapsed or rebooted

    def guard(self):
        with self.locked() as state:
            self.check(state)

    def accept(self, evidence):
        """Commit only complete product evidence before the original deadline."""
        with self.locked() as state:
            self.check(state)
            products = evidence.get('products')
            if (state['phase'] != 'armed' or not isinstance(products, dict) or not products
                    or any(value != {'ready': True, 'advanced': True, 'exact': True}
                           for value in products.values())):
                raise DeployError('cutover requires readiness, advancement and exact answers for every product')
            state.update(phase='accepted', accepted_unix=self.clock(), evidence=evidence)
            durable.atomic_json(self.path, state, mode=0o600)

    def claim_rollback(self, *, runner_active):
        """Fence before stopping the runner; never undo a timely acceptance."""
        with self.locked() as state:
            if state['phase'] in ('accepted', 'restored'):
                return False
            if not state['runner_started'] and state['phase'] == 'staging':
                return False
            due = self.due(state)
            if state['phase'] != 'rollback' and runner_active and not due:
                return False
            state['phase'] = 'rollback'
            state.setdefault('rollback_started_unix', self.clock())
            durable.atomic_json(self.path, state, mode=0o600)
            return True

    def restored(self):
        with self.locked() as state:
            if state['phase'] != 'rollback':
                raise DeployError('restoration has no rollback decision')
            state.update(phase='restored', restored_unix=self.clock())
            durable.atomic_json(self.path, state, mode=0o600)


def unit_name(identifier):
    if not re.fullmatch('publisher-[A-Za-z0-9-]+', identifier):
        raise DeployError('invalid supervised publisher transaction')
    return 'wallet-pir-cutover-' + identifier


def command(arguments):
    # systemd expands percent specifiers and dollars independently of shell
    # quoting. Encode those too; no shell interprets the resulting ExecStart.
    return ' '.join('"' + value.replace('\\', '\\\\').replace('"', '\\"')
                    .replace('%', '%%').replace('$', '$$') + '"' for value in map(str, arguments))


def services(identifier, forward, watchdog):
    name = unit_name(identifier)
    common = ('[Unit]\nAfter=network-online.target\nWants=network-online.target\n'
              'StartLimitIntervalSec=0\n[Service]\nType=exec\nUser=root\n'
              'UMask=0077\nKillMode=control-group\nTimeoutStopSec=30\n')
    return {
        name + '.service': common.replace('[Service]', 'OnFailure=' + name + '-rollback.service\n[Service]')
            + 'ExecStart=' + command(forward) + '\n',
        name + '-rollback.service': common + 'ExecStart=' + command(watchdog)
            + '\nRestart=on-failure\nRestartSec=15\n[Install]\nWantedBy=multi-user.target\n',
    }

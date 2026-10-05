"""Supervised SSH control masters that carry restricted forwards.

One foreground `ssh -N` master per configured session carries that session's
TCP local forwards, TCP reverse forwards and Unix-socket local forwards. The
supervisor restarts a master that exits, with bounded exponential backoff, and
publishes each session's state in an atomically replaced status file. It owns
only the processes it started: a control or forward path held by any other live
process blocks that session instead of being taken over.

Clients that multiplex over a master use `client_args`: `ControlMaster=no`
never starts a master and `ProxyCommand=false` makes the fallback OpenSSH takes
when the master is missing (a fresh login) fail instead. Artifact transfers
must not use a control master. They open their own connection, because a bulk
transfer on the shared TCP connection delays control traffic past its budget
(transparent-live-fleet.py, `transfer_ssh_args`).

The restricted forwarding account is generated from the same configuration:
`authorized_keys_line` and `sshd_match_block`. OpenSSH cannot restrict
Unix-socket forwards per destination: `permitopen` and `PermitOpen` govern TCP
only, and while any TCP destination list is in force every direct Unix-socket
open is refused. A restricted account therefore forwards loopback TCP only, and
both generators refuse a session with Unix-socket forwards. See
docs/control-sessions.md.

The pure argument and socket-name helpers are shared with Transparent's own
supervisor in transparent-live-fleet.py, whose arguments they reproduce byte
for byte. Standard library only.
"""
import argparse
import dataclasses
import fcntl
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import threading
import time

from . import durable, pinned_ssh

SCHEMA = 'wallet-pir-control-sessions-v1'
STATUS_SCHEMA = 'wallet-pir-control-sessions-status-v1'
DEFAULT_CONFIG = '/etc/wallet-pir/control-sessions.json'
# The modules a copy of this library needs, for installers that ship it beside
# a script instead of running from a checkout.
SHIPPED_MODULES = ('__init__.py', 'control_sessions.py', 'durable.py', 'pinned_ssh.py')
TERMINATE_SECONDS = 5
# A sun_path holds 104 (macOS) or 108 (Linux) bytes; OpenSSH appends a
# temporary suffix of up to 17 bytes while it binds a control socket.
MAX_SOCKET_PATH = 86


# ---------------------------------------------------------------------------
# Pure helpers, shared with transparent-live-fleet.py.

def socket_name(prefix, identity):
    """`prefix` plus a 32-hex digest that binds a private socket to `identity`.

    `identity` is JSON-serialisable: the destination and the authentication
    configuration, so a changed host, key or known-hosts file never reuses a
    master authenticated under the old one. The short digest leaves room for
    OpenSSH's temporary socket suffix.
    """
    return prefix + hashlib.sha256(json.dumps(identity).encode()).hexdigest()[:32]


def client_args(base, control_path):
    """Arguments for a client that may only use the master at `control_path`.

    `ProxyCommand=false` prevents OpenSSH's silent fresh-login fallback when
    the master is missing.
    """
    return [*base, '-oControlMaster=no', '-oProxyCommand=false', '-oControlPath=' + str(control_path)]


def master_args(base, control_path, destination, alive_interval, alive_count_max, forwards=()):
    """A foreground master (`ControlMaster=yes` is `-M`) with no remote command.

    No `-f` and no `ControlPersist`: the caller owns the process until it
    exits. `forwards` (from `forward_args`) go between `-N` and the destination.
    """
    return [*base, '-oControlMaster=yes', '-oControlPersist=no', '-oControlPath=' + str(control_path),
            f'-oServerAliveInterval={alive_interval}', f'-oServerAliveCountMax={alive_count_max}', '-N',
            *forwards, destination]


def forward_args(local=(), reverse=(), unix=()):
    """`-L`/`-R` arguments. A master whose forward fails exits instead of
    running without it; a local Unix socket is private to its owner."""
    if not (local or reverse or unix):
        return []
    args = (['-oStreamLocalBindMask=0177'] if unix else []) + ['-oExitOnForwardFailure=yes']
    for listen, target in local:
        args += ['-L', f'{listen}:{target}']
    for listen, target in reverse:
        args += ['-R', f'{listen}:{target}']
    for path, remote in unix:
        args += ['-L', f'{path}:{remote}']
    return args


class Backoff:
    """Restart delays: `initial`, doubling to `maximum`, back to `initial`
    after a master that stayed up for `stable` seconds."""

    def __init__(self, initial, maximum, stable):
        self.initial, self.maximum, self.stable = initial, maximum, stable
        self.delay = initial

    def after_exit(self, ran_seconds):
        """The delay before the next start, given how long the last one ran."""
        if ran_seconds >= self.stable:
            self.delay = self.initial
        delay = self.delay
        self.delay = min(self.delay * 2, self.maximum)
        return delay


# ---------------------------------------------------------------------------
# Configuration.

_NAME = re.compile(r'[a-z0-9][a-z0-9-]{0,31}')
_USER = re.compile(r'[a-z_][a-z0-9_-]{0,31}')
_HOST = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,252}')
_SHA256 = re.compile(r'[0-9a-f]{64}')
_SESSION_KEYS = {'name', 'destination', 'port', 'identity_file', 'known_hosts', 'known_hosts_sha256',
                 'local_forwards', 'reverse_forwards', 'unix_forwards', 'server_alive_interval',
                 'server_alive_count_max', 'connect_timeout', 'compression'}
_CONFIG_KEYS = {'schema', 'runtime_dir', 'status_file', 'restart_initial_seconds',
                'restart_max_seconds', 'stable_seconds', 'sessions'}


def endpoint(text, loopback=False):
    """A literal `IPv4:port`, as `permitopen` and `permitlisten` require."""
    if not isinstance(text, str) or text.count(':') != 1:
        raise ValueError(f'forward endpoint must be IPv4:port: {text!r}')
    host, port = text.split(':')
    address = ipaddress.IPv4Address(host)
    if not port.isdigit() or not 0 < int(port) < 65536 or str(int(port)) != port:
        raise ValueError(f'forward port out of range: {text!r}')
    if loopback and not address.is_loopback:
        raise ValueError(f'forward listen address must be loopback: {text!r}')
    return f'{address}:{port}'


def unix_path(text, local=False):
    if (not isinstance(text, str) or not text.startswith('/')
            or any(c in text for c in ':\n\r\0') or (local and len(text.encode()) > MAX_SOCKET_PATH)):
        raise ValueError(f'Unix forward path must be absolute, short and without colons or newlines: {text!r}')
    return text


def _pairs(value, field):
    if not isinstance(value, list) or any(not isinstance(p, list) or len(p) != 2 for p in value):
        raise ValueError(f'{field} must be a list of [listen, target] pairs')
    return value


def _number(value, field, minimum, integer=False):
    kinds = (int,) if integer else (int, float)
    if isinstance(value, bool) or not isinstance(value, kinds) or value < minimum:
        raise ValueError(f'{field} must be a number of at least {minimum}')
    return value


@dataclasses.dataclass(frozen=True)
class Session:
    name: str
    destination: str
    identity_file: str
    known_hosts: str
    known_hosts_sha256: str = None
    port: int = 22
    local_forwards: tuple = ()
    reverse_forwards: tuple = ()
    unix_forwards: tuple = ()
    server_alive_interval: int = 2
    server_alive_count_max: int = 3
    connect_timeout: int = 10
    compression: bool = False

    @classmethod
    def parse(cls, value):
        if not isinstance(value, dict):
            raise ValueError('each session must be an object')
        if set(value) - _SESSION_KEYS:
            raise ValueError(f'unknown session fields: {sorted(set(value) - _SESSION_KEYS)}')
        for field in ('name', 'destination', 'identity_file', 'known_hosts'):
            if not isinstance(value.get(field), str):
                raise ValueError(f'session {field} is required')
        if not _NAME.fullmatch(value['name']):
            raise ValueError(f'invalid session name {value["name"]!r}')
        user, _, host = value['destination'].partition('@')
        if not _USER.fullmatch(user) or not _HOST.fullmatch(host):
            raise ValueError(f'destination must be user@host: {value["destination"]!r}')
        for field in ('identity_file', 'known_hosts'):
            if not value[field].startswith('/') or '\n' in value[field]:
                raise ValueError(f'{field} must be an absolute path')
        pin = value.get('known_hosts_sha256')
        if pin is not None and (not isinstance(pin, str) or not _SHA256.fullmatch(pin)):
            raise ValueError('known_hosts_sha256 must be a lowercase SHA-256 hex digest')
        compression = value.get('compression', False)
        if not isinstance(compression, bool):
            raise ValueError('compression must be a boolean')
        port = _number(value.get('port', 22), 'port', 1, True)
        if port > 65535:
            raise ValueError('port out of range')
        return cls(
            name=value['name'], destination=value['destination'], identity_file=value['identity_file'],
            known_hosts=value['known_hosts'], known_hosts_sha256=pin,
            port=port,
            local_forwards=tuple((endpoint(a, loopback=True), endpoint(b))
                                 for a, b in _pairs(value.get('local_forwards', []), 'local_forwards')),
            reverse_forwards=tuple((endpoint(a, loopback=True), endpoint(b))
                                   for a, b in _pairs(value.get('reverse_forwards', []), 'reverse_forwards')),
            unix_forwards=tuple((unix_path(a, local=True), unix_path(b))
                                for a, b in _pairs(value.get('unix_forwards', []), 'unix_forwards')),
            server_alive_interval=_number(value.get('server_alive_interval', 2), 'server_alive_interval', 1, True),
            server_alive_count_max=_number(value.get('server_alive_count_max', 3), 'server_alive_count_max', 1, True),
            connect_timeout=_number(value.get('connect_timeout', 10), 'connect_timeout', 1, True),
            compression=compression)

    @property
    def user(self):
        return self.destination.partition('@')[0]

    def base_args(self):
        return ['ssh', *pinned_ssh.ssh_options(self.identity_file, self.known_hosts, self.connect_timeout),
                *([f'-oPort={self.port}'] if self.port != 22 else []),
                *(['-oCompression=yes'] if self.compression else [])]

    def control_path(self, runtime_dir):
        identity = [self.name, self.destination, self.port, self.identity_file, self.known_hosts]
        return Path(runtime_dir) / socket_name('c-', identity)

    def master_args(self, runtime_dir):
        return master_args(self.base_args(), self.control_path(runtime_dir), self.destination,
                           self.server_alive_interval, self.server_alive_count_max,
                           forward_args(self.local_forwards, self.reverse_forwards, self.unix_forwards))

    def client_args(self, runtime_dir):
        return client_args(self.base_args(), self.control_path(runtime_dir))


@dataclasses.dataclass(frozen=True)
class Config:
    runtime_dir: Path
    status_file: Path
    sessions: tuple
    restart_initial_seconds: float = 1
    restart_max_seconds: float = 30
    stable_seconds: float = 30

    @classmethod
    def parse(cls, value):
        if not isinstance(value, dict) or value.get('schema') != SCHEMA:
            raise ValueError(f'control-session config must declare schema {SCHEMA}')
        if set(value) - _CONFIG_KEYS:
            raise ValueError(f'unknown config fields: {sorted(set(value) - _CONFIG_KEYS)}')
        runtime_dir = value.get('runtime_dir', '/run/wallet-pir-control-sessions')
        if not isinstance(runtime_dir, str) or not runtime_dir.startswith('/'):
            raise ValueError('runtime_dir must be an absolute path')
        status_file = value.get('status_file', runtime_dir + '/status.json')
        if not isinstance(status_file, str) or not status_file.startswith('/'):
            raise ValueError('status_file must be an absolute path')
        if not isinstance(value.get('sessions'), list) or not value['sessions']:
            raise ValueError('at least one session is required')
        sessions = tuple(Session.parse(s) for s in value['sessions'])
        names = [s.name for s in sessions]
        listens = [a for s in sessions for a, _ in s.local_forwards]
        paths = [a for s in sessions for a, _ in s.unix_forwards]
        for kind, items in (('session name', names), ('local listen address', listens), ('local socket path', paths)):
            if len(items) != len(set(items)):
                raise ValueError(f'duplicate {kind}')
        initial = _number(value.get('restart_initial_seconds', 1), 'restart_initial_seconds', 0.01)
        maximum = _number(value.get('restart_max_seconds', 30), 'restart_max_seconds', initial)
        config = cls(Path(runtime_dir), Path(status_file), sessions, initial, maximum,
                     _number(value.get('stable_seconds', 30), 'stable_seconds', 0))
        for session in sessions:
            if len(str(session.control_path(config.runtime_dir)).encode()) > MAX_SOCKET_PATH:
                raise ValueError('runtime_dir is too long for a control socket path')
        return config

    @classmethod
    def load(cls, path):
        return cls.parse(json.loads(Path(path).read_text()))


# ---------------------------------------------------------------------------
# The restricted forwarding account.

_PUBLIC_KEY = re.compile(r'(ssh-ed25519|sk-ssh-ed25519@openssh\.com|ecdsa-sha2-nistp(256|384|521)'
                         r'|sk-ecdsa-sha2-nistp256@openssh\.com|ssh-rsa) [A-Za-z0-9+/]+={0,2}( [\x21-\x7e ]*)?')


def _unique(items):
    return list(dict.fromkeys(items))


def _grants(sessions):
    sessions = list(sessions)
    if any(s.unix_forwards for s in sessions):
        raise ValueError('a restricted account cannot be limited to one Unix socket; '
                         'expose the socket on a loopback TCP port instead')
    opens = _unique(target for s in sessions for _, target in s.local_forwards)
    listens = _unique(listen for s in sessions for listen, _ in s.reverse_forwards)
    return opens, listens


def authorized_keys_line(sessions, public_key, command='/bin/false'):
    """The forwarding-only authorized_keys line for `sessions`' destination account.

    `restrict` disables every forwarding and the terminal; `port-forwarding`
    re-enables TCP forwarding, which `permitopen` (local forwards) and
    `permitlisten` (reverse forwards) confine to exactly the configured
    endpoints. The forced command runs nothing. An empty `permitopen` list
    would permit every destination, so a key needs at least one local forward.
    Install it together with `sshd_match_block`, which repeats the limits
    server side and disables Unix-socket forwarding, which `permitlisten` does
    not govern.
    """
    opens, listens = _grants(sessions)
    if not opens:
        raise ValueError('a forwarding key needs at least one local forward: permitopen cannot express "none"')
    if not isinstance(public_key, str) or not _PUBLIC_KEY.fullmatch(public_key.strip()) or '"' in public_key:
        raise ValueError('public key must be one OpenSSH public key line')
    if not command.startswith('/') or any(c in command for c in '"\\\n'):
        raise ValueError('forced command must be an absolute path without quotes')
    options = ['restrict', 'port-forwarding', f'command="{command}"']
    options += [f'permitopen="{target}"' for target in opens]
    options += [f'permitlisten="{listen}"' for listen in listens]
    return ','.join(options) + ' ' + public_key.strip()


def sshd_match_block(user, sessions, authorized_keys_file):
    """The sshd_config Match block for the forwarding-only account `user`.

    It repeats the key's TCP limits server side (a key without them would
    otherwise be unrestricted) and disables Unix-socket forwarding: remote
    Unix-socket listeners are not subject to `permitlisten`.
    """
    if not _USER.fullmatch(user):
        raise ValueError(f'invalid account name {user!r}')
    if not authorized_keys_file.startswith('/') or any(c.isspace() for c in authorized_keys_file):
        raise ValueError('authorized_keys_file must be an absolute path without spaces')
    opens, listens = _grants(sessions)
    if not opens and not listens:
        raise ValueError('a forwarding account needs at least one forward')
    tcp = 'yes' if opens and listens else ('local' if opens else 'remote')
    return '\n'.join([
        f'Match User {user}',
        f'    AuthorizedKeysFile {authorized_keys_file}',
        f'    AllowTcpForwarding {tcp}',
        '    AllowStreamLocalForwarding no',
        f'    PermitOpen {" ".join(opens) or "none"}',
        f'    PermitListen {" ".join(listens) or "none"}',
        '    AllowAgentForwarding no',
        '    X11Forwarding no',
        '    PermitTTY no',
        '    PermitTunnel no',
        '    GatewayPorts no',
        '    ForceCommand /bin/false',
        '    # Drop a dead forwarding session within ~6 s so a restarted master',
        '    # can bind its reverse listener again.',
        '    ClientAliveInterval 2',
        '    ClientAliveCountMax 3',
    ]) + '\n'


# ---------------------------------------------------------------------------
# The supervisor.

def _utc(seconds):
    return None if seconds is None else time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(seconds))


def socket_listening(path, timeout=1.0):
    """True when a process accepts connections on the Unix socket `path`,
    False when it is absent or refused (a dead socket).

    On Linux a refusal means no listener. macOS also refuses a live listener
    whose backlog is full, so there a busy socket can look dead.
    """
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    probe.settimeout(timeout)
    try:
        probe.connect(str(path))
        return True
    except (ConnectionRefusedError, FileNotFoundError):
        return False
    finally:
        probe.close()


class Blocked(RuntimeError):
    """A session cannot start without touching something it does not own."""


def clear_stale_socket(path, what):
    """Remove a dead Unix socket at `path`; refuse anything else.

    A regular file, a symlink or a socket that still accepts connections
    belongs to someone else and is left untouched.
    """
    path = Path(path)
    if not path.exists() and not path.is_symlink():
        return
    if path.is_symlink() or not path.is_socket():
        raise Blocked(f'{what} {path} exists and is not a socket')
    if socket_listening(path):
        raise Blocked(f'{what} {path} is held by a live process that this supervisor did not start')
    path.unlink(missing_ok=True)


class Supervisor:
    """Runs one master per session until `stop` is set.

    `popen` and the clocks are injectable for tests. `poll_interval` bounds how
    quickly an exit, a connection or a stop request is noticed.
    """

    def __init__(self, config, popen=subprocess.Popen, clock=time.time, monotonic=time.monotonic,
                 log=None, poll_interval=0.2):
        self.config = config
        self.popen, self.clock, self.monotonic = popen, clock, monotonic
        self.log_stream = log or sys.stderr
        self.poll_interval = poll_interval
        self.stop = threading.Event()
        self._lock = threading.Lock()
        self.sessions = {s.name: dict(destination=s.destination, state='starting', pid=None, started_at=None,
                                      connected_since=None, restarts=0, last_exit_code=None, last_error=None,
                                      next_start_in_seconds=None)
                         for s in config.sessions}

    def log(self, event, **fields):
        with self._lock:
            print(json.dumps({'event': event, **fields}), file=self.log_stream, flush=True)

    def _update(self, name, **fields):
        with self._lock:
            self.sessions[name].update(fields)
            self._write_status()

    def _write_status(self):
        value = {'schema': STATUS_SCHEMA, 'supervisor_pid': os.getpid(), 'updated_at': _utc(self.clock()),
                 'sessions': {name: dict(state) for name, state in self.sessions.items()}}
        durable.atomic_json(self.config.status_file, value, prefix='.status-', mode=0o640)

    def run(self):
        """Supervise every session; return once `stop` is set and all masters are reaped."""
        self.config.runtime_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        with (self.config.runtime_dir / 'supervisor.lock').open('a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError('another control-session supervisor owns ' + str(self.config.runtime_dir)) from None
            with self._lock:
                self._write_status()
            threads = [threading.Thread(target=self._supervise, args=(s,), name='session-' + s.name, daemon=True)
                       for s in self.config.sessions]
            for thread in threads:
                thread.start()
            # Short joins keep the main thread responsive to SIGTERM.
            for thread in threads:
                while thread.is_alive():
                    thread.join(0.5)

    def _prepare(self, session):
        if session.known_hosts_sha256 is not None:
            try:
                pinned = pinned_ssh.known_hosts_matches(session.known_hosts, session.known_hosts_sha256)
            except OSError as error:
                raise Blocked(f'known_hosts unreadable: {error.strerror}') from None
            if not pinned:
                raise Blocked('known_hosts differs from its pinned digest')
        clear_stale_socket(session.control_path(self.config.runtime_dir), 'control path')
        for path, _ in session.unix_forwards:
            clear_stale_socket(path, 'forward path')

    def _supervise(self, session):
        backoff = Backoff(self.config.restart_initial_seconds, self.config.restart_max_seconds,
                          self.config.stable_seconds)
        while not self.stop.is_set():
            started = self.monotonic()
            state, code, error = self._attempt(session)
            if self.stop.is_set():
                self._update(session.name, state='stopped', pid=None, connected_since=None,
                             next_start_in_seconds=None)
                return
            delay = backoff.after_exit(self.monotonic() - started)
            with self._lock:
                restarts = self.sessions[session.name]['restarts'] + 1
            self._update(session.name, state=state, pid=None, connected_since=None, restarts=restarts,
                         last_exit_code=code, last_error=error, next_start_in_seconds=delay)
            self.log('control_session_' + ('blocked' if state == 'blocked' else 'exited'), session=session.name,
                     exit_code=code, error=error, restart_in_seconds=delay)
            if self.stop.wait(delay):
                self._update(session.name, state='stopped', next_start_in_seconds=None)
                return

    def _attempt(self, session):
        """Start and watch one master: (next state, exit code, last error)."""
        try:
            self._prepare(session)
        except Blocked as error:
            return 'blocked', None, str(error)
        control_path = session.control_path(self.config.runtime_dir)
        try:
            process = self.popen(session.master_args(self.config.runtime_dir), stdin=subprocess.DEVNULL,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, close_fds=True)
        except OSError as error:
            return 'backoff', None, f'ssh could not start: {error.strerror}'
        last = []
        reader = threading.Thread(target=self._relay, args=(session.name, process.stderr, last), daemon=True)
        reader.start()
        self._update(session.name, state='connecting', pid=process.pid, started_at=_utc(self.clock()),
                     connected_since=None, next_start_in_seconds=None)
        self.log('control_session_started', session=session.name, pid=process.pid)
        connected = False
        while process.poll() is None:
            if not connected and control_path.is_socket():
                connected = True
                self._update(session.name, state='connected', connected_since=_utc(self.clock()))
            if self.stop.wait(self.poll_interval):
                self._reap(process)
                break
        reader.join(1)
        return 'backoff', process.returncode, (last[-1] if last else None)

    def _relay(self, name, stream, last):
        """Copy the master's stderr to the journal, keeping its last line."""
        for raw in iter(stream.readline, b''):
            line = raw.decode(errors='replace').strip()[:500]
            if line:
                last[:] = [line]
                self.log('control_session_ssh', session=name, message=line)
        stream.close()

    def _reap(self, process):
        """Stop a master this supervisor started, and only that one."""
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(TERMINATE_SECONDS)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


# ---------------------------------------------------------------------------
# Verification.

def _tcp_listening(address, timeout):
    host, port = address.split(':')
    try:
        with socket.create_connection((host, int(port)), timeout):
            return True
    except OSError:
        return False


def check(config, run=subprocess.run, timeout=2.0):
    """Whether each master answers on its control socket and each local
    forward accepts a connection. A reverse forward listens on the remote host;
    `ExitOnForwardFailure` ends its master if the host refuses it, so it is
    reported with its master. Nothing here contacts a remote host."""
    try:
        status = json.loads(config.status_file.read_text())['sessions']
    except (OSError, ValueError, KeyError, TypeError):
        status = {}
    report = []
    for session in config.sessions:
        try:
            master = run(session.client_args(config.runtime_dir) + ['-O', 'check', session.destination],
                         stdin=subprocess.DEVNULL, capture_output=True, timeout=timeout).returncode == 0
        except (OSError, subprocess.TimeoutExpired):
            master = False
        forwards = [dict(kind='local', listen=a, target=b, listening=_tcp_listening(a, timeout))
                    for a, b in session.local_forwards]
        forwards += [dict(kind='unix', listen=a, target=b, listening=socket_listening(a, timeout))
                     for a, b in session.unix_forwards]
        forwards += [dict(kind='reverse', listen=a, target=b, listening=master)
                     for a, b in session.reverse_forwards]
        report.append(dict(session=session.name, destination=session.destination, master=master,
                           known_hosts_pinned=session.known_hosts_sha256 is not None,
                           forwards=forwards, status=status.get(session.name)))
    ok = all(entry['master'] and all(f['listening'] for f in entry['forwards']) for entry in report)
    return ok, report


# ---------------------------------------------------------------------------
# Command line (ops/scripts/wallet-pir-control-sessions.py).

def main(argv=None):
    parser = argparse.ArgumentParser(description='Supervised restricted SSH control forwards.')
    parser.add_argument('--config', default=DEFAULT_CONFIG)
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('run', help='supervise every configured master until SIGTERM')
    commands.add_parser('check', help='verify every master and local forward; exit 1 if any is down')
    commands.add_parser('validate', help='parse the configuration and print each master command')
    keys = commands.add_parser('authorized-keys', help="print the forwarding-only key line for a destination account")
    keys.add_argument('--user', required=True, help='destination account; its sessions are combined')
    keys.add_argument('--public-key', required=True, type=Path)
    match = commands.add_parser('sshd-match', help='print the sshd_config Match block for a destination account')
    match.add_argument('--user', required=True)
    match.add_argument('--authorized-keys-file', required=True)
    args = parser.parse_args(argv)
    config = Config.load(args.config)
    if args.command == 'validate':
        print(json.dumps({s.name: s.master_args(config.runtime_dir) for s in config.sessions}, indent=2))
        return 0
    if args.command in ('authorized-keys', 'sshd-match'):
        sessions = [s for s in config.sessions if s.user == args.user]
        if not sessions:
            raise SystemExit(f'no session connects as {args.user}')
        if args.command == 'authorized-keys':
            print(authorized_keys_line(sessions, args.public_key.read_text()))
        else:
            print(sshd_match_block(args.user, sessions, args.authorized_keys_file), end='')
        return 0
    if args.command == 'check':
        ok, report = check(config)
        print(json.dumps({'ok': ok, 'sessions': report}, indent=2))
        return 0 if ok else 1
    supervisor = Supervisor(config)
    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, lambda *_: supervisor.stop.set())
    supervisor.run()
    return 0

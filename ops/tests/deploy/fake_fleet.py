"""An in-memory fleet behind the deploy `Executor` interface.

Each host has files, directories and systemd units. Units run what their
*loaded* configuration names, so a test catches a forgotten daemon-reload;
the running executable's digest is that binary file's digest at start time.
Every mutating call is logged, and an optional observer sees it first.
"""
import contextlib
import hashlib
import json
import os

from wallet_pir_ops.deploy import units
from wallet_pir_ops.deploy.remote import Executor, LockHeld

MUTATING = ('write', 'upload', 'mkdir', 'rename', 'remove', 'systemctl')


def sha256(data):
    return hashlib.sha256(data).hexdigest()


class FakeHost:
    def __init__(self):
        self.files = {}
        self.dirs = {'/'}
        self.units = {}
        self.free = 1 << 40


class FakeLock:
    def __init__(self):
        self.lost = False

    def verify(self):
        if self.lost:
            raise LockHeld('lost')


class FakeFleet(Executor):
    def __init__(self):
        self.hosts = {}
        self.log = []
        self.runs = []
        self.observer = None
        self.refuse_identity = set()
        self.unhealthy = set()        # (host, exe sha256) whose health is never ready
        self.failing_restart = set()  # (host, unit) that fail to start
        self.identity_override = {}   # (host, unit) -> binary_sha256 the health reports
        self.health_shapes = {}       # (host, unit) -> f(ready, exe sha256) giving its own health body
        self.self_check_fails = set()
        self.exact_result = (0, 'exact answers ok')
        self.endpoints = {}
        self.held = set()

    # ------------------------------------------------------------- set-up

    def host(self, name):
        return self.hosts.setdefault(name, FakeHost())

    def put(self, host, path, data):
        h = self.host(host)
        parent = os.path.dirname(path)
        while parent not in h.dirs:
            h.dirs.add(parent)
            parent = os.path.dirname(parent)
        h.files[path] = data.encode() if isinstance(data, str) else data

    def install(self, host, unit, fragment, drop_ins=(), files=None):
        """Install and start a unit. `drop_ins` are `(path, text)`; `files` map paths to bytes."""
        self.put(host, '/etc/systemd/system/' + unit, fragment)
        for path, text in drop_ins:
            self.put(host, path, text)
        for path, data in (files or {}).items():
            self.put(host, path, data)
        self.host(host).units.setdefault(unit, {'active': 'inactive', 'pid': 0, 'exe': None, 'loaded': []})
        self._reload(host)
        self._start(host, unit)

    def edit(self, host, path, text, reload=True):
        self.put(host, path, text)
        if reload:
            self._reload(host)

    # ------------------------------------------------------------ systemd

    def unit_paths(self, host, unit):
        h = self.host(host)
        fragment = '/etc/systemd/system/' + unit
        drop_ins = [path for path in h.files if path.endswith('.conf') and os.path.dirname(path) in (
            '/etc/systemd/system/%s.d' % unit, '/etc/systemd/system.control/%s.d' % unit)]
        return ([fragment] if fragment in h.files else []) + sorted(drop_ins, key=os.path.basename)

    def _disk(self, host, unit):
        return [(path, self.host(host).files[path].decode()) for path in self.unit_paths(host, unit)]

    def _reload(self, host):
        for unit, state in self.host(host).units.items():
            state['loaded'] = self._disk(host, unit)

    def _start(self, host, unit):
        h = self.host(host)
        state = h.units[unit]
        binary = units.split_exec(units.exec_start(units.effective(t for _, t in state['loaded'])))[1]
        if (host, unit) in self.failing_restart or binary not in h.files:
            state.update(active='failed', pid=0, exe=None)
        else:
            state.update(active='active', pid=state['pid'] + 1000, exe=sha256(h.files[binary]))

    # ----------------------------------------------------------- executor

    def _mutate(self, host, op, detail):
        if self.observer:
            self.observer(host, op, detail)
        self.log.append((host, op, detail))

    def check_identity(self, host):
        if host in self.refuse_identity:
            raise RuntimeError('Permission denied (publickey)')

    def probe_unit(self, host, unit):
        state = self.host(host).units.get(unit)
        if state is None:
            return {'load_state': 'not-found', 'active_state': 'inactive', 'sub_state': 'dead', 'main_pid': 0,
                    'need_daemon_reload': False, 'fragment_path': None, 'fragment_text': None, 'drop_ins': [],
                    'exe_sha256': None}
        paths = [path for path, _ in state['loaded']]
        return {'load_state': 'loaded', 'active_state': state['active'], 'sub_state': 'running',
                'main_pid': state['pid'], 'need_daemon_reload': self._disk(host, unit) != state['loaded'],
                'fragment_path': paths[0], 'fragment_text': self.read(host, paths[0]),
                'drop_ins': [{'path': path, 'text': self.read(host, path)} for path in paths[1:]],
                'exe_sha256': state['exe']}

    def sha256(self, host, path):
        data = self.host(host).files.get(path)
        return None if data is None else sha256(data)

    def read(self, host, path):
        data = self.host(host).files.get(path)
        return None if data is None else data.decode()

    def write(self, host, path, data, mode):
        self._mutate(host, 'write', path)
        if os.path.dirname(path) not in self.host(host).dirs:
            raise FileNotFoundError(os.path.dirname(path))
        self.host(host).files[path] = bytes(data)

    def upload(self, host, local_path, path, mode):
        self._mutate(host, 'upload', path)
        if os.path.dirname(path) not in self.host(host).dirs:
            raise FileNotFoundError(os.path.dirname(path))
        with open(local_path, 'rb') as handle:
            self.host(host).files[path] = handle.read()

    def mkdir(self, host, path, mode):
        self._mutate(host, 'mkdir', path)
        h = self.host(host)
        while path not in h.dirs:
            h.dirs.add(path)
            path = os.path.dirname(path)

    def rename(self, host, source, destination):
        self._mutate(host, 'rename', (source, destination))
        h = self.host(host)
        if destination in h.files or destination in h.dirs:
            raise FileExistsError(destination)
        if source in h.files:
            h.files[destination] = h.files.pop(source)
            return
        if source not in h.dirs:
            raise FileNotFoundError(source)
        for old in [p for p in h.files if p.startswith(source + '/')]:
            h.files[destination + old[len(source):]] = h.files.pop(old)
        h.dirs = {destination + d[len(source):] if d == source or d.startswith(source + '/') else d for d in h.dirs}

    def remove(self, host, path):
        self._mutate(host, 'remove', path)
        self.host(host).files.pop(path, None)

    def free_bytes(self, host, path):
        return self.host(host).free

    def systemctl(self, host, *args):
        self._mutate(host, 'systemctl', args)
        if args[0] == 'daemon-reload':
            self._reload(host)
        elif args[0] == 'restart':
            self._start(host, args[1])
        else:
            raise AssertionError('unexpected systemctl %s' % (args,))

    def http_get(self, host, url, timeout=5):
        unit = self.endpoints[(host, url)]
        state = self.host(host).units[unit]
        if state['active'] != 'active':
            return 0, 'connection refused'
        ready = (host, state['exe']) not in self.unhealthy
        shape = self.health_shapes.get((host, unit))
        body = shape(ready, state['exe']) if shape else {'ready': ready, 'published': [7] if ready else []}
        if (host, unit) in self.identity_override:
            body['binary_sha256'] = self.identity_override[(host, unit)]
        return 200, json.dumps(body)

    def run(self, host, argv, timeout):
        self.runs.append((host, list(argv)))
        data = self.host(host).files.get(argv[0])
        if data is not None:
            return (1, 'illegal instruction') if sha256(data) in self.self_check_fails else (0, 'Usage: ...')
        return self.exact_result

    @contextlib.contextmanager
    def hold_lock(self, host, path):
        if (host, path) in self.held:
            raise LockHeld('%s is held on %s' % (path, host))
        self.held.add((host, path))
        self.lock = FakeLock()
        try:
            yield self.lock
        finally:
            self.held.discard((host, path))

    def mutations(self, host=None):
        return [entry for entry in self.log if host is None or entry[0] == host]

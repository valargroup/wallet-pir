"""An in-memory fleet behind the deploy `Executor` interface.

Each host has files, directories and systemd units. Units run what their
*loaded* configuration names, so a test catches a forgotten daemon-reload;
the running executable's digest is that binary file's digest at start time,
and `exec_start` the command line it was started with. As in systemd, a
missing `EnvironmentFile` without the `-` prefix fails the start.
Every mutating call is logged, and an optional observer sees it first.
Each file has an inode number: writing allocates a new one, renaming keeps
it and `link` shares it, as hard links do.
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
        self.inodes = {}
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
        self.invocations = 0
        self.log = []
        self.runs = []
        self.observer = None
        self.refuse_identity = set()
        self.unhealthy = set()        # (host, exe sha256) whose health is never ready
        self.failing_restart = set()  # (host, unit) that fail to start
        self.identity_override = {}   # (host, unit) -> binary_sha256 the health reports
        self.health_shapes = {}       # (host, unit) -> f(ready, exe sha256) giving its own health body
        self.self_check_fails = set()
        # Executable digest to its `--contract-info` report; absent digests refuse the flag.
        self.contract_reports = {}
        self.exact_result = (0, 'exact answers ok')  # or an exception the transport raises
        self.endpoints = {}
        self.held = set()
        self.product_check = None     # f(host, argv) -> (code, output) for a release's own check
        self.next_inode = 0
        self.service_users = {'root', 'enhance-pir', 'enhance-pir-v4'}
        self.cache_owners = {}

    # ------------------------------------------------------------- set-up

    def host(self, name):
        return self.hosts.setdefault(name, FakeHost())

    def put(self, host, path, data):
        h = self.host(host)
        parent = os.path.dirname(path)
        while parent not in h.dirs:
            h.dirs.add(parent)
            parent = os.path.dirname(parent)
        self._store(host, path, data.encode() if isinstance(data, str) else data)

    def _store(self, host, path, data):
        self.next_inode += 1
        self.host(host).files[path] = bytes(data)
        self.host(host).inodes[path] = self.next_inode

    def link(self, host, source, destination):
        """A hard link: the destination shares the source's bytes and inode."""
        h = self.host(host)
        self.put(host, destination, h.files[source])
        h.inodes[destination] = h.inodes[source]

    def stat(self, host, path):
        """`(inode, size)` of a file, or None."""
        h = self.host(host)
        return None if path not in h.files else (h.inodes.get(path), len(h.files[path]))

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
        h = self.host(host)
        # A unit file written since the last reload is loaded, inactive; one
        # removed while stopped is forgotten, as systemd does.
        for path in list(h.files):
            name = os.path.basename(path)
            if os.path.dirname(path) == '/etc/systemd/system' and name.endswith('.service'):
                h.units.setdefault(name, {'active': 'inactive', 'pid': 0, 'exe': None, 'loaded': []})
        for unit, state in list(h.units.items()):
            if '/etc/systemd/system/' + unit not in h.files and state['active'] != 'active':
                del h.units[unit]
                continue
            state['loaded'] = self._disk(host, unit)

    def _start(self, host, unit):
        h = self.host(host)
        state = h.units[unit]
        config = units.effective(t for _, t in state['loaded'])
        command = units.exec_start(config)
        binary = units.split_exec(command)[1]
        required = [path for path in config['Service'].get('EnvironmentFile', []) if not path.startswith('-')]
        if (host, unit) in self.failing_restart or binary not in h.files or any(p not in h.files for p in required):
            state.update(active='failed', pid=0, exe=None, exec_start=None)
        else:
            self.invocations += 1
            state.update(active='active', pid=state['pid'] + 1000, exe=sha256(h.files[binary]),
                         exec_start=command, invocation_id='%032x' % self.invocations)

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
                'exe_sha256': state['exe'], 'invocation_id': state.get('invocation_id')}

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
        self._store(host, path, data)

    def upload(self, host, local_path, path, mode):
        self._mutate(host, 'upload', path)
        if os.path.dirname(path) not in self.host(host).dirs:
            raise FileNotFoundError(os.path.dirname(path))
        with open(local_path, 'rb') as handle:
            self._store(host, path, handle.read())

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
            h.inodes[destination] = h.inodes.pop(source)
            return
        if source not in h.dirs:
            raise FileNotFoundError(source)
        for old in [p for p in h.files if p.startswith(source + '/')]:
            h.files[destination + old[len(source):]] = h.files.pop(old)
            h.inodes[destination + old[len(source):]] = h.inodes.pop(old)
        h.dirs = {destination + d[len(source):] if d == source or d.startswith(source + '/') else d for d in h.dirs}

    def remove(self, host, path):
        self._mutate(host, 'remove', path)
        self.host(host).files.pop(path, None)
        self.host(host).inodes.pop(path, None)

    def remove_tree(self, host, path, owner):
        self._mutate(host, 'remove_tree', path)
        if os.path.dirname(path) != owner.rstrip('/'):
            raise ValueError('%s is not directly below %s' % (path, owner))
        h = self.host(host)
        h.files = {p: data for p, data in h.files.items() if not p.startswith(path + '/')}
        h.inodes = {p: inode for p, inode in h.inodes.items() if p in h.files}
        h.dirs = {d for d in h.dirs if d != path and not d.startswith(path + '/')}

    def free_bytes(self, host, path):
        return self.host(host).free

    def systemctl(self, host, *args):
        self._mutate(host, 'systemctl', args)
        if args[0] == 'daemon-reload':
            self._reload(host)
        elif args[0] in ('restart', 'start'):
            self._start(host, args[1])
        elif args[0] in ('enable', 'disable'):
            self.host(host).units[args[1]]['enabled'] = args[0] == 'enable'
        elif args[0] == 'stop':
            self.host(host).units[args[1]].update(active='inactive', pid=0, exe=None, exec_start=None)
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
        if argv[:2] == ['python3', '-c']:
            from wallet_pir_ops.deploy.deployment_alerts import CONTROL
            if argv[2] == CONTROL:
                return 0, json.dumps({'outcome': None})
            from tools.enhance.lib.deploy_prerequisites import PROGRAM
            if argv[2] == PROGRAM:
                value = json.loads(argv[3])
                if value['user'] not in self.service_users:
                    return 1, 'service account does not exist'
                for name in value['credentials']:
                    if not name.startswith('-') and name not in self.host(host).files:
                        return 1, 'credential environment file is unavailable'
                for path in value['directories']:
                    if self.cache_owners.get((host, path), value['user']) != value['user']:
                        return 1, 'existing cache has a foreign owner'
                if value['prepare']:
                    for path in value['directories']:
                        if path not in self.host(host).dirs:
                            self.mkdir(host, path, 0o750)
                            self.cache_owners[host, path] = value['user']
                return 0, '{}'
        data = self.host(host).files.get(argv[0])
        # A staged binary's self-check; any other command is an exact check.
        if data is not None and argv[1:] == ['--help']:
            return (1, 'illegal instruction') if sha256(data) in self.self_check_fails else (0, 'Usage: ...')
        if data is not None and argv[1:] == ['--contract-info']:
            # Releases built before the flag existed refuse it, as clap does.
            report = self.contract_reports.get(sha256(data))
            return (2, "error: unexpected argument '--contract-info' found") if report is None else (0, report)
        if data is not None and self.product_check is not None:
            return self.product_check(host, list(argv))
        if isinstance(self.exact_result, BaseException):
            raise self.exact_result
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

"""A single local "host" behind the deploy `Executor`, for unprivileged operator rehearsals.

Files are real files and processes are real processes: a unit's effective
ExecStart is started with its Environment and EnvironmentFile, its MainPID's
executable digest is read from /proc, and health is real HTTP. Systemd's own
paths (/etc/systemd/system, /run, and service roots under /opt) are mapped
under a private root, since the rehearsal runs without privileges; every
other path, including every inventory path, is used as given. What this does
not do is enforce the sandbox (ProtectSystem, ReadOnlyPaths, users): only a
real systemd host qualifies that.
"""
import contextlib
import hashlib
import os
import shlex
import shutil
import signal
import subprocess
import time
import urllib.error
import urllib.request

from wallet_pir_ops.deploy import units
from wallet_pir_ops.deploy.remote import Executor, LockHeld

MAPPED = ('/etc/systemd/', '/run/', '/opt/')


class Lock:
    def verify(self):
        pass


class LocalSystemd(Executor):
    def __init__(self, root, logs):
        self.root = root
        self.logs = logs
        os.makedirs(logs, exist_ok=True)
        self.units = {}
        self.held = False
        self.log = []

    def path(self, path):
        return self.root + path if path.startswith(MAPPED) else path

    def argv(self, argv):
        return [self.path(a) if a.startswith(MAPPED) else a for a in argv]

    # --------------------------------------------------------------- files

    def check_identity(self, host):
        pass

    def read(self, host, path):
        try:
            with open(self.path(path)) as handle:
                return handle.read()
        except FileNotFoundError:
            return None

    def sha256(self, host, path):
        try:
            with open(self.path(path), 'rb') as handle:
                return hashlib.sha256(handle.read()).hexdigest()
        except (FileNotFoundError, IsADirectoryError):
            return None

    def write(self, host, path, data, mode):
        self.log.append(('write', path))
        target = self.path(path)
        temporary = target + '.tmp-rehearsal'
        with open(temporary, 'wb') as handle:
            handle.write(bytes(data))
        os.chmod(temporary, mode)
        os.replace(temporary, target)

    def upload(self, host, local_path, path, mode):
        self.log.append(('upload', path))
        shutil.copyfile(local_path, self.path(path))
        os.chmod(self.path(path), mode)

    def mkdir(self, host, path, mode):
        self.log.append(('mkdir', path))
        os.makedirs(self.path(path), mode=mode, exist_ok=True)

    def rename(self, host, source, destination):
        self.log.append(('rename', destination))
        if os.path.exists(self.path(destination)):
            raise FileExistsError(destination)
        os.rename(self.path(source), self.path(destination))

    def remove(self, host, path):
        self.log.append(('remove', path))
        with contextlib.suppress(FileNotFoundError):
            os.remove(self.path(path))

    def free_bytes(self, host, path):
        return shutil.disk_usage(self.root).free

    def run(self, host, argv, timeout):
        result = subprocess.run(self.argv(argv), capture_output=True, text=True, timeout=timeout)
        return result.returncode, (result.stdout + result.stderr)[-20000:]

    def http_get(self, host, url, timeout=5):
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(url, timeout=timeout) as response:
                return response.status, response.read(1 << 16).decode(errors='replace')
        except urllib.error.HTTPError as error:
            return error.code, error.read(1 << 16).decode(errors='replace')
        except (urllib.error.URLError, OSError) as error:
            return 0, str(error)

    @contextlib.contextmanager
    def hold_lock(self, host, path):
        if self.held:
            raise LockHeld('%s is held' % path)
        self.held = True
        try:
            yield Lock()
        finally:
            self.held = False

    # ------------------------------------------------------------- systemd

    def files(self, unit):
        fragment = '/etc/systemd/system/' + unit
        directory = self.path('/etc/systemd/system/%s.d' % unit)
        drop_ins = sorted(os.listdir(directory)) if os.path.isdir(directory) else []
        paths = ([fragment] if os.path.exists(self.path(fragment)) else []) + \
            ['/etc/systemd/system/%s.d/%s' % (unit, name) for name in drop_ins if name.endswith('.conf')]
        return [(path, self.read('local', path)) for path in paths]

    def alive(self, state):
        process = state.get('process')
        return process is not None and process.poll() is None

    def probe_unit(self, host, unit):
        state = self.units.get(unit)
        if state is None or not state['loaded']:
            return {'load_state': 'not-found', 'active_state': 'inactive', 'sub_state': 'dead', 'main_pid': 0,
                    'need_daemon_reload': False, 'fragment_path': None, 'fragment_text': None, 'drop_ins': [],
                    'exe_sha256': None}
        alive = self.alive(state)
        pid = state['process'].pid if alive else 0
        exe = None
        if alive:
            with open('/proc/%d/exe' % pid, 'rb') as handle:
                exe = hashlib.sha256(handle.read()).hexdigest()
        loaded = state['loaded']
        return {'load_state': 'loaded', 'active_state': 'active' if alive else state.get('failed', 'inactive'),
                'sub_state': 'running' if alive else 'dead', 'main_pid': pid,
                'need_daemon_reload': self.files(unit) != loaded,
                'fragment_path': loaded[0][0], 'fragment_text': self.read(host, loaded[0][0]),
                'drop_ins': [{'path': p, 'text': self.read(host, p)} for p, _ in loaded[1:]],
                'exe_sha256': exe}

    def systemctl(self, host, *args):
        self.log.append(('systemctl', args))
        verb = args[0]
        if verb == 'daemon-reload':
            directory = self.path('/etc/systemd/system')
            for name in os.listdir(directory):
                if name.endswith('.service'):
                    self.units.setdefault(name, {'loaded': [], 'enabled': False})
            for unit, state in list(self.units.items()):
                state['loaded'] = self.files(unit)
                if not state['loaded'] and not self.alive(state):
                    del self.units[unit]
        elif verb in ('enable', 'disable'):
            self.units[args[1]]['enabled'] = verb == 'enable'
        elif verb == 'stop':
            self.stop(args[1])
        elif verb == 'restart':
            self.stop(args[1])
            self.start(args[1])
        else:
            raise AssertionError('unexpected systemctl %s' % (args,))

    def stop(self, unit):
        state = self.units.get(unit)
        if state is None or not self.alive(state):
            return
        state['process'].send_signal(signal.SIGTERM)
        try:
            state['process'].wait(60)
        except subprocess.TimeoutExpired:
            state['process'].kill()
            state['process'].wait()
        state['failed'] = 'inactive'

    def start(self, unit):
        state = self.units[unit]
        config = units.effective(text for _, text in state['loaded'])['Service']
        argv = self.argv(shlex.split(units.exec_start(units.effective(t for _, t in state['loaded']))))
        environment = {'PATH': '/usr/bin:/bin'}
        for path in config.get('EnvironmentFile', []):
            optional = path.startswith('-')
            path = path.lstrip('-')
            if not os.path.exists(path):
                if optional:
                    continue
                state['failed'] = 'failed'
                return
            for line in open(path).read().splitlines():
                if '=' in line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    environment[key.strip()] = value.strip()
        for entry in config.get('Environment', []):
            for item in shlex.split(entry):
                key, value = item.split('=', 1)
                environment[key] = value
        for name in config.get('RuntimeDirectory', []):
            for item in name.split():
                os.makedirs(self.path('/run/' + item), mode=0o700, exist_ok=True)
        log = open(os.path.join(self.logs, unit + '.log'), 'ab')
        state['process'] = subprocess.Popen(argv, env=environment, stdout=log, stderr=log,
                                            stdin=subprocess.DEVNULL, start_new_session=True)
        time.sleep(0.5)
        if not self.alive(state):
            state['failed'] = 'failed'

    def shutdown(self):
        for unit in list(self.units):
            self.stop(unit)

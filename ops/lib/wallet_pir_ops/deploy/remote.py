"""The one interface through which the deploy tool touches a host.

`Executor` lists every remote action. `SSHExecutor` implements each as one
call of `host_helper.py` over SSH; tests substitute a fake that models files
and systemd. Nothing else in the package opens a connection.
"""
import contextlib
import json
from pathlib import Path
import shlex
import subprocess

from .. import hostlock, pinned_ssh
from . import host_helper
from .descriptors import LOCK_PATH

HELPER = Path(host_helper.__file__).read_text()


class RemoteError(RuntimeError):
    pass


class LockHeld(RuntimeError):
    pass


class Executor:
    """Remote actions, each on one named inventory host. Mutating ones are marked."""

    def check_identity(self, host):
        """Raise unless the deploy identity can run a command on `host`."""
        raise NotImplementedError

    def probe_unit(self, host, unit):
        """`host_helper.probe_unit`: loaded state, unit files and running executable digest."""
        raise NotImplementedError

    def sha256(self, host, path):
        raise NotImplementedError

    def read(self, host, path):
        """Text of `path`, or None if absent."""
        raise NotImplementedError

    def write(self, host, path, data, mode):
        """Mutating: atomically replace `path` with `data` (bytes)."""
        raise NotImplementedError

    def upload(self, host, local_path, path, mode):
        """Mutating: atomically replace `path` with a local file's bytes."""
        raise NotImplementedError

    def mkdir(self, host, path, mode):
        """Mutating: create `path` and its parents if missing."""
        raise NotImplementedError

    def rename(self, host, source, destination):
        """Mutating: move a file or directory; refuses an existing destination."""
        raise NotImplementedError

    def remove(self, host, path):
        """Mutating: remove a file if present."""
        raise NotImplementedError

    def free_bytes(self, host, path):
        raise NotImplementedError

    def systemctl(self, host, *args):
        """Mutating: run systemctl; raise on failure."""
        raise NotImplementedError

    def http_get(self, host, url, timeout=5):
        """`(status, body)` of a GET made from `host`; status 0 when nothing answered."""
        raise NotImplementedError

    def run(self, host, argv, timeout):
        """`(returncode, output)` of a command on `host`."""
        raise NotImplementedError

    def hold_lock(self, host, path):
        """Context manager holding `flock -n path` on `host`; raises LockHeld if taken."""
        raise NotImplementedError


class SSHExecutor(Executor):
    """Every action is `python3 -c <host_helper> <request>` over SSH.

    `ssh.mode` "pinned" uses `pinned_ssh`: inventory IPv4 addresses, one key and
    a known-hosts file re-verified against its digest before every connection,
    with no user SSH configuration. "config" uses the runner's own SSH
    configuration and host aliases, for a workstation that already has them;
    it still refuses unknown host keys and interactive prompts.
    """

    def __init__(self, inventory):
        self.inventory = inventory
        self.pinned = {}

    def transport(self, host):
        ssh = self.inventory.ssh
        entry = self.inventory.hosts[host]
        if ssh['mode'] == 'config':
            # An optional operator SSH config supplies aliases, e.g. ProxyJump
            # through the coordinator to private addresses.
            config = ['-F', str(Path(ssh['config_file']).expanduser())] if ssh.get('config_file') else []
            return ['ssh', *config, '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
                    '-o', 'ConnectTimeout=10', '-o', 'ForwardAgent=no', entry.get('ssh_alias', host)]
        if host not in self.pinned:
            self.pinned[host] = pinned_ssh.PinnedSSH(entry['address'], Path(ssh['key']).expanduser(),
                                                     Path(ssh['known_hosts']).expanduser(),
                                                     ssh['known_hosts_sha256'], ssh.get('user', 'root'))
        client = self.pinned[host]
        client.check_host_keys()
        return ['ssh', *client.options, client.user + '@' + client.host]

    def call(self, host, op, stdin=b'', deadline=120, **arguments):
        """Run one helper operation; `deadline` bounds the whole SSH call."""
        command = shlex.join(['python3', '-c', HELPER, json.dumps({'op': op, **arguments})])
        argv = self.transport(host) + [command]
        if isinstance(stdin, Path):
            with open(stdin, 'rb') as handle:
                result = subprocess.run(argv, stdin=handle, capture_output=True, timeout=deadline)
        else:
            result = subprocess.run(argv, input=stdin, capture_output=True, timeout=deadline)
        if result.returncode:
            raise RemoteError('%s: %s failed (exit %d): %s' % (host, op, result.returncode,
                                                               result.stderr.decode(errors='replace').strip()[-2000:]))
        try:
            reply = json.loads(result.stdout)
        except ValueError:
            raise RemoteError('%s: %s returned no reply' % (host, op)) from None
        if not reply.get('ok'):
            raise RemoteError('%s: %s: %s' % (host, op, reply.get('error')))
        return reply['result']

    def check_identity(self, host):
        self.call(host, 'ping', deadline=30)

    def probe_unit(self, host, unit):
        return self.call(host, 'probe_unit', unit=unit)

    def sha256(self, host, path):
        return self.call(host, 'sha256', deadline=300, path=path)

    def read(self, host, path):
        return self.call(host, 'read', path=path)

    def write(self, host, path, data, mode):
        self.call(host, 'write', stdin=data, path=path, mode=mode)

    def upload(self, host, local_path, path, mode):
        self.call(host, 'write', stdin=Path(local_path), deadline=1800, path=path, mode=mode)

    def mkdir(self, host, path, mode):
        self.call(host, 'mkdir', path=path, mode=mode)

    def rename(self, host, source, destination):
        self.call(host, 'rename', source=source, destination=destination)

    def remove(self, host, path):
        self.call(host, 'remove', path=path)

    def free_bytes(self, host, path):
        return self.call(host, 'free_bytes', path=path)

    def systemctl(self, host, *args):
        self.call(host, 'systemctl', deadline=600, args=list(args))

    def http_get(self, host, url, timeout=5):
        reply = self.call(host, 'http_get', deadline=timeout + 30, url=url, timeout=timeout)
        return reply['status'], reply['body']

    def run(self, host, argv, timeout):
        reply = self.call(host, 'run', deadline=timeout + 30, argv=list(argv), timeout=timeout)
        return reply['returncode'], reply['output']

    @contextlib.contextmanager
    def hold_lock(self, host, path):
        # The lock lives as long as this SSH session: flock holds it while `cat`
        # waits on our stdin, and closing stdin (or losing the connection)
        # releases it. `verify` notices a lost session before each side effect.
        command = shlex.join(['flock', '-n', path, 'sh', '-c', 'echo locked; exec cat >/dev/null'])
        process = subprocess.Popen(self.transport(host) + [command], stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        try:
            if process.stdout.readline() != b'locked\n':
                raise LockHeld('%s is held on %s (or the host refused the session)' % (path, host))
            yield RemoteLock(process, host, path)
        finally:
            process.stdin.close()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()


class RemoteLock:
    def __init__(self, process, host, path):
        self.process, self.host, self.path = process, host, path

    def verify(self):
        if self.process.poll() is not None:
            raise LockHeld('lost %s on %s: its SSH session ended' % (self.path, self.host))


class ProductionLock(hostlock.PinnedHostLock):
    """The production lock taken directly, when the runner is the pinned coordinator."""
    PATH = Path(LOCK_PATH)


def production_lock(inventory, executor):
    """The production lock the inventory names, as a context manager yielding an object with `verify()`."""
    if inventory.lock['type'] == 'pinned_host':
        return ProductionLock({'type': 'pinned_host', 'machine_id': inventory.lock['machine_id']})
    return executor.hold_lock(inventory.lock['host'], LOCK_PATH)

"""One coordinator behind the deploy `Executor`, run by the real host helper on local paths.

Package tests point the layout at a temporary directory, so every remote action
is `host_helper` code acting on real files, links and modes. `fail` injects a
transport failure on the Nth mutating call: `before` the effect (it never
happened) or `after` it (it happened and the reply was lost). `partial` makes an
unpack stop after its first file. Every mutating call is logged; the executor
has no systemctl at all, so a package transaction that tried one would fail.
"""
import contextlib
import io
import os

from wallet_pir_ops.deploy import host_helper
from wallet_pir_ops.deploy.remote import Executor, LockHeld, RemoteError


class Lock:
    def __init__(self):
        self.lost = False

    def verify(self):
        if self.lost:
            raise LockHeld('lost')


class LocalHost(Executor):
    def __init__(self):
        self.log = []
        self.fail = None          # (index of mutating call, 'before' | 'after' | 'partial')
        self.after = None         # callable(op, arguments) run after each successful mutation
        self.held = set()
        self.free = 1 << 40
        self.refuse_identity = set()

    def mutate(self, op, effect, **arguments):
        index = len(self.log)
        self.log.append((op, arguments.get('path') or arguments.get('destination') or arguments.get('source')))
        mode = self.fail[1] if self.fail and self.fail[0] == index else None
        if mode == 'before':
            raise RemoteError('connection reset before %s' % op)
        result = effect()
        if self.after:
            self.after(op, arguments)
        if mode == 'after':
            raise RemoteError('connection reset after %s' % op)
        return result

    def mutations(self):
        return [op for op, _ in self.log]

    def check_identity(self, host):
        if host in self.refuse_identity:
            raise RuntimeError('Permission denied (publickey)')

    def read(self, host, path):
        return host_helper.read(path)

    def sha256(self, host, path):
        return host_helper.sha256(path)

    def free_bytes(self, host, path):
        return self.free

    def run(self, host, argv, timeout):
        result = host_helper.run(argv, timeout)
        return result['returncode'], result['output']

    def write(self, host, path, data, mode):
        self.mutate('write', lambda: host_helper.write(path, mode, io.BytesIO(bytes(data))), path=path)

    def mkdir(self, host, path, mode):
        self.mutate('mkdir', lambda: host_helper.mkdir(path, mode), path=path)

    def rename(self, host, source, destination):
        self.mutate('rename', lambda: host_helper.rename(source, destination), destination=destination)

    def remove(self, host, path):
        self.mutate('remove', lambda: host_helper.remove(path), path=path)

    def package_observe(self, host, root, files, revision, machine_id_path):
        observed = host_helper.package_observe(root, files, revision, machine_id_path)
        observed['free_bytes'] = self.free
        return observed

    def package_tree(self, host, path):
        return host_helper.package_tree(path)

    def package_unpack(self, host, archive, path, archive_sha256, manifest_sha256):
        def effect():
            with open(archive, 'rb') as handle:
                data = handle.read()
            if self.fail != (len(self.log) - 1, 'partial'):
                return host_helper.package_unpack(path, archive_sha256, manifest_sha256, io.BytesIO(data))
            # Stop before the second file, as a dropped connection would.
            written, real_open = [], os.open
            def interrupted(target, *arguments):
                if str(target).startswith(path + '/') and not os.path.isdir(target):
                    if written:
                        raise RemoteError('connection reset during unpack')
                    written.append(target)
                return real_open(target, *arguments)
            os.open = interrupted
            try:
                host_helper.package_unpack(path, archive_sha256, manifest_sha256, io.BytesIO(data))
            finally:
                os.open = real_open
        self.mutate('package_unpack', effect, path=path)

    def package_copy(self, host, source, destination, sha256, mode):
        self.mutate('package_copy', lambda: host_helper.package_copy(source, destination, sha256, mode),
                    destination=destination)

    def replace_link(self, host, path, target):
        self.mutate('replace_link', lambda: host_helper.replace_link(path, target), path=path)

    def remove_tree(self, host, path, owner):
        self.mutate('remove_tree', lambda: host_helper.remove_tree(path, owner), path=path)

    @contextlib.contextmanager
    def hold_lock(self, host, path):
        if (host, path) in self.held:
            raise LockHeld('%s is held on %s' % (path, host))
        self.held.add((host, path))
        self.lock = Lock()
        try:
            yield self.lock
        finally:
            self.held.discard((host, path))

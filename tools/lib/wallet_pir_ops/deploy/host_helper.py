"""Host side of wallet-pir-deploy: one operation per invocation.

The runner sends this file's source as `python3 -c <source> <request json>`
over SSH, so a host needs nothing installed beyond the Python 3 that its
cloud-init already requires. The request names one operation and its
arguments; file contents arrive on stdin. The reply is one JSON object on
stdout, `{"ok": true, "result": ...}` or `{"ok": false, "error": "..."}`.

Standard library only, and no state between invocations: every operation is
safe to repeat, which is what lets a rollback be re-run after an interruption.
"""
import hashlib
import fcntl
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import subprocess
import sys
import tarfile
import urllib.error
import urllib.request

try:
    _ANCILLARY
except NameError:
    from wallet_pir_ops import ancillary_baseline as _ANCILLARY

CHUNK = 1 << 20
SHOW = 'LoadState,ActiveState,SubState,MainPID,InvocationID,NeedDaemonReload,FragmentPath,DropInPaths,UnitFileState'


def sha256(path):
    """Hex digest of a file's bytes (following links), or None if it is absent."""
    digest = hashlib.sha256()
    try:
        with open(path, 'rb') as handle:
            for chunk in iter(lambda: handle.read(CHUNK), b''):
                digest.update(chunk)
    except FileNotFoundError:
        return None
    return digest.hexdigest()


def read(path):
    try:
        with open(path, 'rb') as handle:
            return handle.read().decode()
    except FileNotFoundError:
        return None


def fsync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write(path, mode, stream=None):
    """Replace `path` with stdin's bytes, durable and atomic once this returns."""
    stream = stream or sys.stdin.buffer
    directory = os.path.dirname(path)
    temporary = os.path.join(directory, '.wallet-pir-deploy-%d-%s' % (os.getpid(), os.path.basename(path)))
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, 'wb') as handle:
            for chunk in iter(lambda: stream.read(CHUNK), b''):
                handle.write(chunk)
            handle.flush()
            os.fchmod(handle.fileno(), mode)
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        fsync_directory(directory)
    finally:
        if os.path.lexists(temporary):
            os.unlink(temporary)


def mkdir(path, mode):
    os.makedirs(path, mode=mode, exist_ok=True)


def rename(source, destination):
    """Move a file or directory; never replaces an existing destination."""
    if os.path.lexists(destination):
        raise FileExistsError(destination)
    os.rename(source, destination)
    fsync_directory(os.path.dirname(destination))


def remove(path):
    if os.path.lexists(path):
        os.unlink(path)
        fsync_directory(os.path.dirname(path))


def free_bytes(path):
    """Bytes available to root on the filesystem that holds `path` or its nearest ancestor."""
    while not os.path.exists(path):
        path = os.path.dirname(path)
    info = os.statvfs(path)
    return info.f_bavail * info.f_frsize


def provider_collection(root, retained, expected=None):
    try:
        module = _PROVIDER_COLLECTION
    except NameError:
        import importlib.util
        source = Path(__file__).resolve().parents[4] / 'tools/transparent/lib/provider_collection.py'
        spec = importlib.util.spec_from_file_location('provider_collection', source)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module.run(root, retained, expected)


def probe_unit(unit):
    """What systemd has loaded for `unit`, the files it loaded, and the running executable's digest.

    Drop-ins are listed as systemd applies them: sorted by file name across
    directories. The file texts are read now; `need_daemon_reload` says whether
    they still match what systemd loaded.
    """
    shown = subprocess.run(['systemctl', 'show', unit, '--no-pager', '-p', SHOW],
                           capture_output=True, text=True, check=True).stdout
    properties = dict(line.split('=', 1) for line in shown.splitlines() if '=' in line)
    fragment = properties.get('FragmentPath') or None
    drop_ins = sorted(properties.get('DropInPaths', '').split(), key=os.path.basename)
    pid = int(properties.get('MainPID') or 0)
    return {
        'load_state': properties.get('LoadState', ''),
        'active_state': properties.get('ActiveState', ''),
        'sub_state': properties.get('SubState', ''),
        'main_pid': pid,
        'invocation_id': properties.get('InvocationID') or None,
        'unit_file_state': properties.get('UnitFileState', ''),
        'need_daemon_reload': properties.get('NeedDaemonReload') == 'yes',
        'fragment_path': fragment,
        'fragment_text': read(fragment) if fragment else None,
        'drop_ins': [{'path': path, 'text': read(path)} for path in drop_ins],
        'exe_sha256': sha256('/proc/%d/exe' % pid) if pid else None,
    }


def publisher_resources(paths):
    """Read coordinator identity, headroom and managed-file metadata without opening state stores."""
    memory = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    disks, files = {}, {}
    for name in paths:
        path = Path(name)
        if path.is_symlink():
            raise ValueError('publisher managed path is a symlink: ' + name)
        files[name] = None
        if path.exists():
            info = path.stat()
            if not stat.S_ISREG(info.st_mode) or info.st_uid != 0:
                raise ValueError('publisher managed file must be a root-owned regular file: ' + name)
            files[name] = {'mode': stat.S_IMODE(info.st_mode)}
        parent = path.parent
        while not parent.exists():
            parent = parent.parent
        info = os.statvfs(parent)
        disks[name] = {'available': info.f_bavail * info.f_frsize, 'total': info.f_blocks * info.f_frsize,
                       'device': parent.stat().st_dev}
    return {'machine_id': Path('/etc/machine-id').read_text().strip(), 'files': files, 'disks': disks,
            'memory_available': int(memory['MemAvailable'].split()[0]) * 1024,
            'memory_total': int(memory['MemTotal'].split()[0]) * 1024}


def publisher_unlocked(paths):
    """Check existing owner locks together after stopping services; never create or remove a lock."""
    handles = []
    try:
        for name in paths:
            if not os.path.exists(name):
                continue
            fd = os.open(name, os.O_RDWR | os.O_NOFOLLOW)
            handles.append(fd)
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise ValueError('owner lock is not a regular file: ' + name)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        for fd in handles:
            os.close(fd)


def systemctl(args):
    result = subprocess.run(['systemctl', *args], capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError('systemctl %s: %s' % (' '.join(args), (result.stderr or result.stdout).strip()[-2000:]))


def http_get(url, timeout):
    """Status and body of a GET made from this host; status 0 when nothing answered."""
    # Loopback and private addresses only: never through an environment proxy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(url, timeout=timeout) as response:
            return {'status': response.status, 'body': response.read(1 << 16).decode(errors='replace')}
    except urllib.error.HTTPError as error:
        return {'status': error.code, 'body': error.read(1 << 16).decode(errors='replace')}
    except (urllib.error.URLError, OSError) as error:
        return {'status': 0, 'body': str(error)}


def run(argv, timeout):
    try:
        result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, stdin=subprocess.DEVNULL)
    except subprocess.TimeoutExpired:
        return {'returncode': 124, 'output': 'timed out after %ss' % timeout}
    return {'returncode': result.returncode, 'output': (result.stdout + result.stderr)[-4000:]}


WRAPPER_MARKER = b'wallet-pir-deploy.py'
INHERITED_MARKER = b'WALLET_PIR_PRODUCTION_LOCK_FDS='


def bounded(path, limit):
    """Bytes of one regular file, never through a link; refuses past `limit`."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, 'rb') as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise ValueError('not a regular file: ' + path)
        data = handle.read(limit + 1)
    if len(data) > limit:
        raise ValueError('file exceeds read bound: ' + path)
    return data


RECEIPTS = '.snapshot-fleet'


def owner_records(roots, entry_limit, byte_limit, depth_limit=8, record_limit=1 << 20):
    """Every entry of the owner namespaces, to any depth within finite bounds.

    Each non-request `*.json` file is returned as an owner record. Snapshot
    receipt trees (`*.snapshot-fleet`) hold copies, not owners: their files are
    listed and type-checked but not parsed. Every other file is listed with its
    size and kind. Exceeding a bound, a link or a special file refuses: nothing
    below the bounds is skipped silently.
    """
    records, files, total, entries = {}, [], 0, 0
    for root in roots:
        stack = [(root, 0, False)]
        while stack:
            path, depth, receipt = stack.pop()
            try:
                info = os.lstat(path)
            except FileNotFoundError:
                continue
            if not stat.S_ISDIR(info.st_mode):
                raise ValueError('owner namespace is not a directory: ' + path)
            for name in sorted(os.listdir(path)):
                entries += 1
                if entries > entry_limit:
                    raise ValueError('owner namespaces exceed the entry bound')
                child = os.path.join(path, name)
                child_info = os.lstat(child)
                mode = child_info.st_mode
                if stat.S_ISLNK(mode):
                    raise ValueError('owner namespace contains a link: ' + child)
                if stat.S_ISDIR(mode):
                    if depth+1 > depth_limit:
                        raise ValueError('owner namespace exceeds the depth bound: ' + child)
                    stack.append((child, depth+1, receipt or name.endswith(RECEIPTS)))
                    continue
                if not stat.S_ISREG(mode):
                    raise ValueError('owner namespace contains a special file: ' + child)
                if receipt:
                    kind = 'receipt'
                elif name.endswith('.request.json'):
                    kind = 'request'
                elif name.endswith('.json'):
                    kind = 'record'
                    data = bounded(child, record_limit)
                    total += len(data)
                    if total > byte_limit:
                        raise ValueError('owner namespaces exceed the byte bound')
                    records[child] = data.decode()
                else:
                    kind = 'other'
                files.append([child, child_info.st_size, kind])
    return records, files


def matches(value, roots, tools):
    """Whether a path or argument belongs to an operation executable class."""
    value = value[:-len(' (deleted)')] if value.endswith(' (deleted)') else value
    return any(value.startswith(root.rstrip('/') + '/') for root in roots) or os.path.basename(value) in tools


def process_chain(proc, pid):
    """This probe and its ancestors, which are never owner evidence."""
    chain = set()
    while pid > 1 and pid not in chain:
        chain.add(pid)
        try:
            with open(os.path.join(proc, str(pid), 'stat')) as handle:
                pid = int(handle.read().rsplit(')', 1)[1].split()[1])
        except (FileNotFoundError, ProcessLookupError):
            break
    return chain


def ownership_probe(roots, lock_path, machine_id_path, classes, proc='/proc', entry_limit=65536, byte_limit=64 << 20,
                    process_limit=65536, fd_limit=1 << 20, ancillary=False):
    """Read-only owner and process evidence of one host, in one bounded pass.

    Returns the owner namespace inventory and records, the production lock's
    kernel holders, the boot identity and every live process: its stat line,
    executable path and cgroup, and whether it holds the lock file open, carries
    the inherited-lock variable, runs the deploy wrapper, or runs an executable
    or argument of an operation class (`classes` names their roots and tool
    names). Environment and command-line contents are never returned.
    Unreadable evidence is reported, and every bound refuses.
    """
    machine = bounded(machine_id_path, 4096).decode()
    proof=_ANCILLARY.observe(machine.strip(),proc=Path(proc)) if ancillary else None
    records, files = owner_records(roots, entry_limit, byte_limit)
    try:
        with open(os.path.join(proc, 'sys/kernel/random/boot_id')) as handle:
            boot_id = handle.read().strip()
    except FileNotFoundError:
        boot_id = None
    roots_class, tools = classes['roots'], set(classes['tools'])
    try:
        lock = os.stat(lock_path)
    except FileNotFoundError:
        lock = None
    wanted = (lock and '%02x:%02x:%d' % (os.major(lock.st_dev), os.minor(lock.st_dev), lock.st_ino))
    holders = []
    with open(os.path.join(proc, 'locks')) as handle:
        for line in handle:
            fields = line.split()
            if wanted and len(fields) > 5 and wanted in fields[4:6]:
                holders.append(line.strip())
    with open(os.path.join(proc, 'stat')) as handle:
        boot = next(int(line.split()[1]) for line in handle if line.startswith('btime '))
    own = process_chain(proc, os.getpid())
    pids = sorted(int(name) for name in os.listdir(proc) if name.isdigit())
    if len(pids) > process_limit:
        raise ValueError('process table exceeds bound')
    processes, descriptors = [], 0
    for pid in pids:
        base = os.path.join(proc, str(pid))
        try:
            with open(os.path.join(base, 'stat')) as handle:
                entry = {'pid': pid, 'stat': handle.read()}
        except (FileNotFoundError, ProcessLookupError):
            continue  # Exited while the table was read.
        if pid in own:
            entry['self'] = True
            processes.append(entry)
            continue
        unknown = []
        try:
            names = os.listdir(os.path.join(base, 'fd'))
            descriptors += len(names)
            if descriptors > fd_limit:
                raise ValueError('descriptor tables exceed bound')
            entry['lock_fd'] = False
            for name in names:
                try:
                    target = os.stat(os.path.join(base, 'fd', name))
                except (FileNotFoundError, ProcessLookupError):
                    continue
                if lock is not None and (target.st_dev, target.st_ino) == (lock.st_dev, lock.st_ino):
                    entry['lock_fd'] = True
        except (FileNotFoundError, ProcessLookupError):
            continue
        except PermissionError:
            unknown.append('fd')
        for key, name, marker, limit in (('inherited', 'environ', INHERITED_MARKER, 1 << 22),
                                         ('wrapper', 'cmdline', WRAPPER_MARKER, 1 << 22)):
            try:
                with open(os.path.join(base, name), 'rb') as handle:
                    data = handle.read(limit + 1)
                if len(data) > limit:
                    unknown.append(name)
                entry[key] = marker in data
                if name == 'cmdline':
                    entry['command_sha256']=hashlib.sha256(data).hexdigest()
                    entry['command_bytes']=len(data)
                    argv = [a.decode(errors='replace') for a in data.split(b'\0') if a]
                    entry['class_argv'] = any(matches(a, roots_class, tools) for a in argv[:1]) or \
                        any(a.startswith('/') and matches(a, roots_class, ()) for a in argv)
            except (FileNotFoundError, ProcessLookupError):
                entry[key] = False
                entry.setdefault('class_argv', False)
            except PermissionError:
                unknown.append(name)
        try:
            entry['exe'] = os.readlink(os.path.join(base, 'exe'))
            entry['class_exe'] = matches(entry['exe'], roots_class, tools)
        except (FileNotFoundError, ProcessLookupError):
            entry['exe'], entry['class_exe'] = None, False  # Kernel threads have none.
        except PermissionError:
            unknown.append('exe')
        try:
            with open(os.path.join(base, 'cgroup')) as handle:
                entry['cgroup'] = next((line.strip().split(':', 2)[2] for line in handle if line.startswith('0::')), None)
        except (FileNotFoundError, ProcessLookupError):
            entry['cgroup'] = None
        except PermissionError:
            unknown.append('cgroup')
        if unknown:
            entry['unknown'] = unknown
        processes.append(entry)
    return {'machine_id': machine, 'records': records, 'files': files, 'lock_holders': holders, 'boot_unix': boot,
            'boot_id': boot_id,
            'ticks': os.sysconf('SC_CLK_TCK'), 'processes': processes,'ancillary':proof}


# ------------------------------------------------------- operations package
# The package lives in one root-owned tree on the coordinator. These operations
# read it without following links, and change it only by creating a new
# transaction-owned directory, renaming a whole directory, or replacing the
# `current` link; none of them touches a unit or a serving process.

PACKAGE_LIMIT = 64 << 20
PACKAGE_ENTRIES = 8192


def summary(path):
    """Kind, owner and permission bits of `path` itself, or None if absent."""
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return None
    mode = info.st_mode
    kind = ('link' if stat.S_ISLNK(mode) else 'dir' if stat.S_ISDIR(mode)
            else 'file' if stat.S_ISREG(mode) else 'other')
    result = {'kind': kind, 'uid': info.st_uid, 'gid': info.st_gid, 'mode': stat.S_IMODE(mode)}
    if kind == 'link':
        result['target'] = os.readlink(path)
    return result


def file_digest(path, limit=PACKAGE_LIMIT):
    return hashlib.sha256(bounded(path, limit)).hexdigest()


def package_observe(root, files, revision, machine_id_path='/etc/machine-id'):
    """Everything a package plan binds on this host, read in one pass without changes.

    `files` are the managed files outside the releases (the operator inventory
    and its host-key pins); each is summarized with its digest.
    """
    parents = [os.path.dirname(root), root] + ['%s/%s' % (root, n) for n in ('releases', 'staging', 'transactions')]
    parents += sorted({os.path.dirname(path) for path in files})
    staging = '%s/staging' % root
    staged = sorted(os.listdir(staging)) if (summary(staging) or {}).get('kind') == 'dir' else []
    managed = {}
    for path in files:
        state = summary(path)
        if state and state['kind'] == 'file':
            state['sha256'] = file_digest(path, 1 << 20)
        managed[path] = state
    pointer = '%s/transactions/latest.json' % root
    pointer_text = bounded(pointer, 4096).decode() if (summary(pointer) or {}).get('kind') == 'file' else None
    return {'machine_id': bounded(machine_id_path, 4096).decode().strip(),
            'paths': {path: summary(path) for path in parents},
            'current': summary('%s/current' % root),
            'release': summary('%s/releases/%s' % (root, revision)),
            'staging': staged, 'files': managed, 'pointer': pointer_text,
            'free_bytes': free_bytes(root)}


def package_tree(path, entry_limit=PACKAGE_ENTRIES):
    """Every entry below `path` with owner, mode and file digest; links are listed, never followed."""
    root = summary(path)
    if root is None or root['kind'] != 'dir':
        return {'root': root, 'entries': {}}
    entries, stack = {}, [path]
    while stack:
        directory = stack.pop()
        for name in sorted(os.listdir(directory)):
            if len(entries) >= entry_limit:
                raise ValueError('package tree exceeds the entry bound')
            child = os.path.join(directory, name)
            record = summary(child)
            relative = os.path.relpath(child, path)
            if record['kind'] == 'dir':
                stack.append(child)
            elif record['kind'] == 'file':
                record['sha256'] = file_digest(child)
            entries[relative] = record
    return {'root': root, 'entries': entries}


def package_mode(name):
    """`bundle.mode`: the runner's tree check refuses any difference."""
    return 0o755 if name.endswith(('.py', '.sh')) else 0o644


def package_unpack(path, archive_sha256, manifest_sha256, stream=None):
    """Create `path` holding exactly the files of the gzip tar on stdin.

    `path` must not exist: a staging directory belongs to one transaction. The
    archive must have the planned digest and its `operations.json` the planned
    manifest digest; that manifest closes the file set. Any other member, or a
    member whose digest differs from the manifest, refuses.
    """
    stream = stream or sys.stdin.buffer
    data = stream.read(PACKAGE_LIMIT + 1)
    if len(data) > PACKAGE_LIMIT or hashlib.sha256(data).hexdigest() != archive_sha256:
        raise ValueError('package archive differs from the plan')
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as archive:
        manifest = archive.extractfile('operations.json').read()
    if hashlib.sha256(manifest).hexdigest() != manifest_sha256:
        raise ValueError('package manifest differs from the plan')
    files = {name: [digest, package_mode(name)] for name, digest in json.loads(manifest)['files'].items()}
    files['operations.json'] = [manifest_sha256, 0o644]
    for name in files:
        parts = PurePosixPath(name)
        if parts.is_absolute() or '..' in parts.parts or str(parts) != name:
            raise ValueError('unsafe package path: ' + name)
    os.mkdir(path, 0o755)
    os.chmod(path, 0o755)
    written, directories = set(), {path}
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as archive:
        for member in archive:
            name = member.name
            if not member.isfile() or name not in files or name in written:
                raise ValueError('package archive member is not planned: ' + name)
            content = archive.extractfile(member).read()
            digest, mode = files[name]
            if hashlib.sha256(content).hexdigest() != digest or mode not in (0o644, 0o755):
                raise ValueError('package member differs from the plan: ' + name)
            target = os.path.join(path, name)
            parent = os.path.dirname(target)
            missing = []
            while parent not in directories and not os.path.isdir(parent):
                missing.append(parent)
                parent = os.path.dirname(parent)
            for directory in reversed(missing):
                os.mkdir(directory, 0o755)
                os.chmod(directory, 0o755)
                directories.add(directory)
            descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(descriptor, 'wb') as handle:
                handle.write(content)
                handle.flush()
                os.fchmod(handle.fileno(), mode)
                os.fsync(handle.fileno())
            written.add(name)
    if written != set(files):
        raise ValueError('package archive lacks planned files: %s' % sorted(set(files) - written)[:5])
    for directory, _, _ in os.walk(path):
        fsync_directory(directory)
    fsync_directory(os.path.dirname(path))


def package_copy(source, destination, sha256, mode):
    """Copy one regular file, never through a link, to a new file; the copy must have `sha256`."""
    data = bounded(source, 1 << 20)
    if hashlib.sha256(data).hexdigest() != sha256:
        raise ValueError('%s changed since it was planned' % source)
    if os.path.lexists(destination):
        if file_digest(destination) != sha256:
            raise FileExistsError(destination)
        return
    write(destination, mode, io.BytesIO(data))


def replace_link(path, target):
    """Atomically point the symlink `path` at `target`; refuses to replace anything but a link."""
    if os.path.lexists(path) and not os.path.islink(path):
        raise ValueError('%s is not a link' % path)
    temporary = os.path.join(os.path.dirname(path), '.wallet-pir-deploy-%d-link' % os.getpid())
    if os.path.lexists(temporary):
        os.unlink(temporary)
    os.symlink(target, temporary)
    os.replace(temporary, path)
    fsync_directory(os.path.dirname(path))


def remove_tree(path, owner):
    """Remove a transaction-owned directory directly below `owner`, never following links."""
    if os.path.dirname(path) != owner.rstrip('/') or os.path.basename(path) in ('', '.', '..'):
        raise ValueError('%s is not directly below %s' % (path, owner))
    if os.path.lexists(path):
        if os.path.islink(path):
            raise ValueError('%s is a link' % path)
        shutil.rmtree(path)
        fsync_directory(owner)


OPERATIONS = {
    'publisher_unlocked': publisher_unlocked,
    'publisher_resources': publisher_resources,
    'ownership_probe': ownership_probe,
    'package_observe': package_observe,
    'package_tree': package_tree,
    'package_unpack': package_unpack,
    'package_copy': package_copy,
    'replace_link': replace_link,
    'remove_tree': remove_tree,
    'ping': lambda: None,
    'sha256': sha256,
    'read': read,
    'write': write,
    'mkdir': mkdir,
    'rename': rename,
    'remove': remove,
    'free_bytes': free_bytes,
    'probe_unit': probe_unit,
    'provider_collection': provider_collection,
    'systemctl': systemctl,
    'http_get': http_get,
    'run': run,
}


def main():
    request = json.loads(sys.argv[1])
    operation = OPERATIONS[request.pop('op')]
    try:
        reply = {'ok': True, 'result': operation(**request)}
    except Exception as error:  # reported to the runner, which refuses to continue
        reply = {'ok': False, 'error': '%s: %s' % (type(error).__name__, error)}
    sys.stdout.write(json.dumps(reply))


if __name__ == '__main__':
    main()

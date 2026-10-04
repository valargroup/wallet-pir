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
import json
import os
import stat
import subprocess
import sys
import urllib.error
import urllib.request

CHUNK = 1 << 20
SHOW = 'LoadState,ActiveState,SubState,MainPID,NeedDaemonReload,FragmentPath,DropInPaths'


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
        'need_daemon_reload': properties.get('NeedDaemonReload') == 'yes',
        'fragment_path': fragment,
        'fragment_text': read(fragment) if fragment else None,
        'drop_ins': [{'path': path, 'text': read(path)} for path in drop_ins],
        'exe_sha256': sha256('/proc/%d/exe' % pid) if pid else None,
    }


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
                    process_limit=65536, fd_limit=1 << 20):
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
            'ticks': os.sysconf('SC_CLK_TCK'), 'processes': processes}


OPERATIONS = {
    'ownership_probe': ownership_probe,
    'ping': lambda: None,
    'sha256': sha256,
    'read': read,
    'write': write,
    'mkdir': mkdir,
    'rename': rename,
    'remove': remove,
    'free_bytes': free_bytes,
    'probe_unit': probe_unit,
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

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


def read_many(paths, limit=1 << 20):
    """Bounded texts of up to 512 files: None when absent, never through a link."""
    if not isinstance(paths, list) or len(paths) > 512:
        raise ValueError('read_many takes at most 512 paths')
    result = {}
    for path in paths:
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError:
            result[path] = None
            continue
        with os.fdopen(descriptor, 'rb') as handle:
            if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                raise ValueError('not a regular file: ' + path)
            data = handle.read(limit + 1)
        if len(data) > limit:
            raise ValueError('file exceeds read bound: ' + path)
        result[path] = data.decode()
    return result


def list_json(path, limit=512):
    """Sorted `*.json` names of a real directory, or None when it is absent."""
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return None
    if not stat.S_ISDIR(info.st_mode):
        raise ValueError('not a directory: ' + path)
    names = sorted(name for name in os.listdir(path) if name.endswith('.json'))
    if len(names) > limit:
        raise ValueError('too many records in ' + path)
    return names


def proc_stats(pids):
    """`/proc/PID/stat` of up to 512 PIDs, boot time and clock ticks per second."""
    if not isinstance(pids, list) or len(pids) > 512 or not all(isinstance(p, int) and p > 0 for p in pids):
        raise ValueError('proc_stats takes at most 512 positive PIDs')
    boot = next(int(line.split()[1]) for line in open('/proc/stat') if line.startswith('btime '))
    stats = {}
    for pid in pids:
        try:
            with open('/proc/%d/stat' % pid) as handle:
                stats[str(pid)] = handle.read()
        except (FileNotFoundError, ProcessLookupError):
            stats[str(pid)] = None
    return {'boot_unix': boot, 'ticks': os.sysconf('SC_CLK_TCK'), 'stats': stats}


OPERATIONS = {
    'read_many': read_many,
    'list_json': list_json,
    'proc_stats': proc_stats,
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

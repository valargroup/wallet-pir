"""Host-side checks and journalled retirement of an unused query cache.

The operator holds the fleet lock. Plans bind the installed process/configuration
and every cache entry's metadata; apply checks the same plan before deletion.
Publication roots and the active cache cannot be selected.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def checked_cache(path, active):
    path, active = Path(path), Path(active)
    require(path.is_absolute() and path.parent == active.parent and path != active,
            'cleanup must name an inactive sibling of the installed cache')
    require(path.name.startswith('https-') and path.is_dir(), 'cleanup must name an existing HTTPS cache')
    require(path.resolve() == path and active.resolve() == active, 'cache paths must not traverse symbolic links')
    require(not any(part in ('.', '..') for part in path.parts), 'invalid cache path')
    return path


def tree_identity(root):
    """Metadata identity and conservative reclaim estimate, without reading large files."""
    entries, unique = [], {}
    for directory, folders, files in os.walk(root, followlinks=False):
        for path in [Path(directory), *[Path(directory) / name for name in folders + files]]:
            info = path.lstat()
            link = stat.S_ISLNK(info.st_mode)
            require(stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode) or link,
                    'retired cache contains a special file')
            entries.append([str(path.relative_to(root)), info.st_dev, info.st_ino, info.st_mode,
                            info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_nlink,
                            os.readlink(path) if link else None])
            if stat.S_ISREG(info.st_mode):
                unique[info.st_dev, info.st_ino] = info
    return {'tree_sha256': digest(sorted(entries)), 'entries': len(entries),
            'reclaimable_bytes': sum(info.st_blocks * 512 for info in unique.values() if info.st_nlink == 1)}


def under(target, root):
    return target == str(root) or target.startswith(str(root) + '/')


def require_unused(root, active, proc=Path('/proc'), configuration=Path('/etc')):
    """Reject process handles, mappings, configuration references and active-tree links."""
    # SSH's helper ancestors carry the reviewed path in their command arguments.
    # They still must pass the open-handle and memory-mapping checks below.
    ancestors, parent = set(), os.getpid()
    while parent > 1 and str(parent) not in ancestors:
        ancestors.add(str(parent))
        try:
            status = (proc / str(parent) / 'status').read_text()
        except FileNotFoundError:
            break
        parent = int(next(line.split()[1] for line in status.splitlines() if line.startswith('PPid:')))
    for process in proc.iterdir():
        if not process.name.isdecimal() or process.name == str(os.getpid()):
            continue
        try:
            if process.name not in ancestors:
                require(str(root).encode() not in (process / 'cmdline').read_bytes(),
                        'cache is named by a running process: ' + process.name)
            require(str(root) not in (process / 'maps').read_text(), 'cache is mapped by a running process')
            for descriptor in [process / 'cwd', *list((process / 'fd').iterdir())]:
                try:
                    target = os.readlink(descriptor)
                except FileNotFoundError:
                    continue
                require(not under(target, root), 'cache has an open process handle')
        except (FileNotFoundError, ProcessLookupError):
            continue
    for directory, folders, files in os.walk(configuration, followlinks=False):
        for name in files:
            path = Path(directory) / name
            try:
                if path.is_symlink() or path.stat().st_size > 1024 * 1024:
                    continue
                require(str(root).encode() not in path.read_bytes(), 'cache is referenced by installed configuration: ' + str(path))
            except FileNotFoundError:
                continue
    for directory, folders, files in os.walk(active, followlinks=False):
        for name in folders + files:
            path = Path(directory) / name
            if path.is_symlink():
                require(not under(str(path.resolve()), root), 'active cache links into the retired cache')
    mountinfo = proc / 'self/mountinfo'
    for line in mountinfo.read_text().splitlines():
        require(not under(line.split()[4], root), 'cache contains a mount point')


def atomic_json(path, value):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    with temporary.open('w') as stream:
        json.dump(value, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.chmod(0o600)
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def current_cache(request):
    unit = request['unit']
    fields = dict(line.split('=', 1) for line in subprocess.check_output(
        ['systemctl', 'show', unit, '-p', 'MainPID', '-p', 'InvocationID', '-p', 'ActiveState'], text=True).splitlines())
    require(fields['ActiveState'] == 'active' and int(fields['MainPID']) > 0, 'query worker must be running')
    arguments = Path('/proc', fields['MainPID'], 'cmdline').read_bytes().decode().split('\0')
    require('--config' in arguments and arguments[arguments.index('--config') + 1] == request['configuration'],
            'worker does not run the reviewed configuration')
    raw = Path(request['configuration']).read_bytes()
    settings = json.loads(raw)['transaction_details']['settings']
    return Path(settings['state_dir']), {'unit': fields, 'configuration_sha256': hashlib.sha256(raw).hexdigest()}


def plan(request):
    active, installed = current_cache(request)
    root = checked_cache(request['path'], active)
    require_unused(root, active)
    return {'machine_id': Path('/etc/machine-id').read_text().strip(), 'request': request,
            'active_cache': str(active), 'installed': installed, 'cache': tree_identity(root)}


def prune(request, expected, transaction, receipt_root=Path('/opt/transparent-txid-display/maintenance'),
          intent_recorded=False):
    require(shutil.rmtree.avoids_symlink_attacks, 'cache cleanup requires directory-descriptor deletion')
    reviewed = plan(request)
    require(digest(reviewed) == expected, 'cache cleanup plan is stale')
    root = Path(request['path'])
    receipt = receipt_root / transaction / 'state.json'
    require(not receipt.exists(), 'cleanup receipt already exists')
    usage = os.statvfs(root)
    state = {'phase': 'deleting', 'plan': reviewed, 'available_before': usage.f_bavail * usage.f_frsize}
    # A completely full filesystem cannot record intent locally. The operator
    # first records it durably on the coordinator and its own disk in that case.
    if not intent_recorded:
        atomic_json(receipt, state)
    shutil.rmtree(root)
    usage = os.statvfs(root.parent)
    state.update(phase='complete', available_after=usage.f_bavail * usage.f_frsize)
    atomic_json(receipt, state)
    return state


def main():
    operation, raw, expected, transaction = sys.argv[1:]
    request = json.loads(raw)
    require(operation in ('plan', 'prune'), 'unknown cache operation')
    result = plan(request) if operation == 'plan' else prune(request, expected, transaction, intent_recorded=True)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()

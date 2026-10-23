"""Copy a query cache to an attached volume, then switch its existing path.

Preparation copies while the worker runs. Activation stops only that worker,
finishes the copy, retains the original directory, binds the volume at the same
path, and installs boot dependencies before starting the unchanged executable.
Every step records intent and completion; failures hold the recorded state.
"""
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys

from . import cache_storage_host as cache


def run(arguments, timeout=60):
    return subprocess.check_output(arguments, text=True, stderr=subprocess.STDOUT, timeout=timeout).strip()


def mount_source(path):
    result = subprocess.run(['findmnt', '-n', '-o', 'SOURCE', '--mountpoint', str(path)],
                            capture_output=True, text=True, timeout=10)
    cache.require(result.returncode in (0, 1), 'cannot inspect mount point')
    return result.stdout.strip() if result.returncode == 0 else None


def device_info(name, size_gib):
    cache.require(re.fullmatch('[a-z][a-z0-9-]{1,62}', name), 'invalid volume name')
    device = Path('/dev/disk/by-id/scsi-0DO_Volume_' + name)
    cache.require(device.exists() and stat.S_ISBLK(device.stat().st_mode), 'approved volume device is absent')
    probe = subprocess.run(['blkid', '-o', 'export', str(device)], capture_output=True, text=True, timeout=15)
    cache.require(probe.returncode in (0, 2), 'cannot inspect volume filesystem')
    metadata = dict(line.split('=', 1) for line in probe.stdout.splitlines())
    cache.require(metadata.get('TYPE') in (None, 'ext4'), 'approved volume contains an unsupported filesystem')
    if not metadata.get('TYPE'):
        signatures = json.loads(run(['wipefs', '--no-act', '--json', str(device)]))
        cache.require(not signatures.get('signatures'), 'unformatted volume has existing filesystem or partition signatures')
        cache.require(not run(['lsblk', '-nr', '-o', 'MOUNTPOINTS', str(device)]), 'unformatted volume is mounted')
    size = int(run(['blockdev', '--getsize64', str(device)]))
    cache.require(size >= size_gib * 1024 ** 3, 'attached device is smaller than the approved volume')
    return {'path': str(device), 'resolved': str(device.resolve()), 'uuid': metadata.get('UUID'), 'bytes': size}


def plan(request):
    cache.require(shutil.which('rsync'), 'rsync is required before preparing the volume')
    active, installed = cache.current_cache(request)
    cache.require(active.is_dir() and active.resolve() == active, 'active cache is not an ordinary directory')
    device = device_info(request['volume_name'], request['size_gib'])
    mount = Path('/mnt') / request['volume_name']
    existing = mount_source(mount)
    if existing:
        cache.require(Path(existing).resolve() == Path(device['resolved']), 'volume mount belongs to another device')
    else:
        cache.require(not mount.exists() or not any(mount.iterdir()), 'volume mount directory is not empty')
    cache.require(not mount_source(active), 'active cache already has a dedicated mount; inspect its transaction')
    info = active.stat()
    return {'request': request, 'machine_id': Path('/etc/machine-id').read_text().strip(),
            'active_cache': str(active), 'active_identity': [info.st_dev, info.st_ino],
            'installed': installed, 'device': device, 'mount': str(mount), 'destination': str(mount / 'cache')}


def receipt_path(transaction):
    cache.require(re.fullmatch('[A-Za-z0-9-]+', transaction), 'invalid storage transaction')
    return Path('/opt/transparent-txid-display/maintenance') / transaction / 'state.json'


def save(state):
    cache.atomic_json(receipt_path(state['transaction']), state)


def effect(state, name, operation):
    if name in state['completed']:
        return
    state.update(phase=name, intent=name)
    save(state)
    operation()
    state['completed'].append(name)
    state['intent'] = None
    save(state)


def require_volume(state):
    planned = state['plan']
    observed = device_info(planned['request']['volume_name'], planned['request']['size_gib'])
    cache.require(observed == planned['device'], 'attached device identity changed')
    source = mount_source(planned['mount'])
    cache.require(source and Path(source).resolve() == Path(observed['resolved']), 'approved data volume is not mounted')
    return Path(planned['destination'])


def copy_cache(source, destination, log, live):
    cache.require(source.is_dir() and destination.parent.is_dir(), 'copy directories disappeared')
    cache.require(source.stat().st_dev != destination.parent.stat().st_dev, 'cache copy must cross to the new volume')
    with log.open('a') as stream:
        result = subprocess.run(['rsync', '-aHAX', '--numeric-ids', '--delete', '--info=progress2',
                                 str(source) + '/', str(destination) + '/'],
                                stdout=stream, stderr=subprocess.STDOUT, timeout=2400)
    cache.require(result.returncode == 0 or live and result.returncode == 24,
                  'cache copy failed; inspect ' + str(log))


def prepare(request, expected, transaction):
    reviewed = plan(request)
    cache.require(cache.digest(reviewed) == expected, 'volume storage plan is stale')
    cache.require(not receipt_path(transaction).exists(), 'preparation receipt already exists')
    state = {'transaction': transaction, 'plan': reviewed, 'phase': 'preparing', 'completed': [], 'intent': None}
    save(state)
    if not reviewed['device']['uuid']:
        def format_volume():
            observed = device_info(request['volume_name'], request['size_gib'])
            cache.require(observed == reviewed['device'], 'blank volume identity changed before formatting')
            run(['mkfs.ext4', '-m', '0', '-L', 'wallet-pir-txid', observed['path']], 120)
            state['plan']['device'] = device_info(request['volume_name'], request['size_gib'])
            cache.require(state['plan']['device']['uuid'], 'new filesystem UUID was not observed')
        effect(state, 'format approved blank volume', format_volume)
    mount = Path(reviewed['mount'])
    def attach():
        mount.mkdir(mode=0o755, parents=True, exist_ok=True)
        if not mount_source(mount):
            run(['mount', '-t', 'ext4', reviewed['device']['path'], str(mount)])
        require_volume(state)
    effect(state, 'mount volume', attach)
    destination = require_volume(state)
    effect(state, 'copy live cache', lambda: copy_cache(Path(reviewed['active_cache']), destination,
                                                       receipt_path(transaction).parent / 'copy.log', True))
    state['phase'] = 'prepared'
    save(state)
    return state


def move_original(active, backup, identity):
    if backup.exists():
        info = backup.stat()
        cache.require([info.st_dev, info.st_ino] == identity, 'retained cache directory changed')
    else:
        info = active.stat()
        cache.require([info.st_dev, info.st_ino] == identity, 'active cache directory changed')
        active.rename(backup)
    if not active.exists():
        active.mkdir(mode=stat.S_IMODE(backup.stat().st_mode))
        os.chown(active, backup.stat().st_uid, backup.stat().st_gid)
    cache.require(not any(active.iterdir()), 'cache mount target is not empty')


def fstab_after(before, planned):
    mount, active = planned['mount'], planned['active_cache']
    lines = [line for line in before.splitlines() if line.strip() and not line.lstrip().startswith('#')]
    cache.require(not any(len(line.split()) > 1 and line.split()[1] in (mount, active) for line in lines),
                  'fstab already contains one of the selected mount paths')
    return before.rstrip() + '\n# Wallet PIR transaction-detail query cache\n' + (
        'UUID=%s %s ext4 defaults,nofail,discard 0 2\n'
        '%s %s none bind,x-systemd.requires-mounts-for=%s 0 0\n'
        % (planned['device']['uuid'], mount, planned['destination'], active, mount))


def write_text(path, text, mode):
    temporary = path.with_name(path.name + '.wallet-pir-tmp')
    with temporary.open('w') as stream:
        stream.write(text)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.chmod(mode)
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def activate(request, transaction):
    state = json.loads(receipt_path(transaction).read_text())
    planned = state['plan']
    cache.require(planned['request'] == request, 'storage request differs from preparation')
    destination = require_volume(state)
    active = Path(planned['active_cache'])
    backup = active.with_name(active.name + '.before-volume-' + transaction)
    unit = request['unit']
    if 'backup' not in state:
        observed_active, installed = cache.current_cache(request)
        cache.require(str(observed_active) == str(active) and installed == planned['installed'],
                      'query worker changed since preparation')
        fstab = Path('/etc/fstab').read_text()
        dropin = Path('/etc/systemd/system') / (unit + '.d') / '99-wallet-pir-cache-storage.conf'
        cache.require(not dropin.exists(), 'cache storage drop-in already exists')
        state.update(backup=str(backup), fstab_before=fstab, fstab_after=fstab_after(fstab, planned),
                     dropin=str(dropin), dropin_text='[Unit]\nRequiresMountsFor=' + str(destination.parent)
                     + ' ' + str(active) + '\nConditionPathIsMountPoint=' + str(active) + '\n')
        save(state)
    if 'stop worker' not in state['completed']:
        observed = dict(line.split('=', 1) for line in run(
            ['systemctl', 'show', unit, '-p', 'MainPID', '-p', 'ActiveState']).splitlines())
        if observed['ActiveState'] == 'active':
            observed_active, installed = cache.current_cache(request)
            cache.require(str(observed_active) == str(active) and installed == planned['installed'],
                          'query worker changed before shutdown')
        else:
            cache.require(state['intent'] == 'stop worker' and observed['MainPID'] == '0',
                          'only the recorded worker stop may be resumed')
    effect(state, 'stop worker', lambda: run(['systemctl', 'stop', unit], 180))
    effect(state, 'copy stopped cache', lambda: copy_cache(active, destination,
                                                         receipt_path(transaction).parent / 'copy.log', False))
    effect(state, 'retain original cache', lambda: move_original(active, backup, planned['active_identity']))
    def bind():
        if not mount_source(active):
            run(['mount', '--bind', str(destination), str(active)])
        cache.require(active.stat().st_dev == destination.stat().st_dev
                      and active.stat().st_ino == destination.stat().st_ino, 'cache bind mount does not use the approved copy')
    effect(state, 'bind cache path', bind)
    def persist():
        observed = Path('/etc/fstab').read_text()
        cache.require(observed in (state['fstab_before'], state['fstab_after']), 'fstab changed during storage activation')
        write_text(Path('/etc/fstab'), state['fstab_after'], 0o644)
        dropin = Path(state['dropin'])
        dropin.parent.mkdir(mode=0o755, exist_ok=True)
        cache.require(not dropin.exists() or dropin.read_text() == state['dropin_text'], 'storage drop-in changed')
        write_text(dropin, state['dropin_text'], 0o644)
    effect(state, 'persist cache mounts', persist)
    effect(state, 'reload worker unit', lambda: run(['systemctl', 'daemon-reload']))
    effect(state, 'start worker', lambda: run(['systemctl', 'start', unit], 180))
    state['phase'] = 'serving-started'
    save(state)
    return state


def status(transaction):
    state = json.loads(receipt_path(transaction).read_text())
    state['filesystems'] = run(['df', '-B1', '/', state['plan']['active_cache'], state['plan']['mount']])
    state['worker'] = run(['systemctl', 'show', state['plan']['request']['unit'], '-p', 'ActiveState', '-p', 'MainPID'])
    return state


def summary(state):
    planned = state['plan']
    result = {key: state.get(key) for key in ('transaction', 'phase', 'intent', 'completed', 'backup')}
    result.update(active_cache=planned['active_cache'], mount=planned['mount'],
                  destination=planned['destination'], device=planned['device'])
    for key in ('filesystems', 'worker'):
        if key in state:
            result[key] = state[key]
    return result


def main():
    operation, raw, expected, transaction = sys.argv[1:]
    request = json.loads(raw)
    if operation == 'plan':
        result = plan(request)
    elif operation == 'prepare':
        result = prepare(request, expected, transaction)
    elif operation == 'activate':
        result = activate(request, transaction)
    elif operation == 'status':
        result = status(transaction)
    else:
        raise ValueError('unknown cache storage operation')
    print(json.dumps(result if operation == 'plan' else summary(result), sort_keys=True))


if __name__ == '__main__':
    main()

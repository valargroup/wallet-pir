"""Read-only Linux filesystem observations, sent to hosts by the resource sampler.

Only mounted, writable local disks are included. UUID identifies a filesystem
across reboot; the resolved device name is the fallback. Bind mounts and Btrfs
subvolumes share one observation. No directories or file contents are scanned.
"""
import json
import os
import subprocess


def disk_filesystems():
    mounts = json.loads(subprocess.check_output(
        ['findmnt', '--json', '--list', '--output', 'TARGET,SOURCE,FSTYPE,OPTIONS,UUID'],
        text=True, timeout=3))['filesystems']
    disks = {}
    for mount in sorted(mounts, key=lambda m: (len(m['target']), m['target'])):
        source = mount['source'] or ''
        device = os.path.realpath(source.split('[', 1)[0])
        if (not source.startswith('/dev/') or device.startswith('/dev/loop')
                or 'rw' not in (mount['options'] or '').split(',')):
            continue
        filesystem_id = mount.get('uuid') or mount['fstype'] + ':' + device
        if filesystem_id in disks:
            continue
        entry = {'id': filesystem_id, 'mount_point': mount['target'], 'device': device}
        try:
            stat = os.statvfs(mount['target'])
            entry.update(available_bytes=stat.f_bavail * stat.f_frsize,
                         total_bytes=stat.f_blocks * stat.f_frsize)
        except OSError:
            entry['error'] = 'filesystem unavailable'
        disks[filesystem_id] = entry
    if not disks:
        raise ValueError('no writable local filesystems observed')
    return list(disks.values())

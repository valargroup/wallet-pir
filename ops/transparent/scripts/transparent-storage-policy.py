#!/usr/bin/env python3
"""Keep online discard out of the worker's shared ext4 publication filesystem.

The privileged worker prestart reapplies this policy at every boot. No fstab,
fsync, journal, write barrier, or scheduled fstrim setting is changed.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

INSTALLED = '/usr/local/lib/transparent-pir/storage-policy.py'
PRESTART = 'ExecStartPre=+/usr/bin/python3 '+INSTALLED+' --apply'
PATHS = ('/opt/transparent-publisher', '/srv/transparent-pir/runtime-cache')


def validate_mounts(mounts):
    if len(mounts) != len(PATHS):
        raise RuntimeError('missing worker filesystem evidence')
    first = mounts[0]
    for mount in mounts:
        options = set(mount['options'].split(','))
        if (mount['target'] != '/' or mount['fstype'] != 'ext4'
                or not mount['source'].startswith('/dev/')
                or mount['source'] != first['source'] or 'rw' not in options
                or options & {'ro', 'nobarrier', 'barrier=0', 'noload'}):
            raise RuntimeError('storage policy requires a writable shared ext4 root with durability barriers')
        if options != set(first['options'].split(',')):
            raise RuntimeError('worker paths disagree on filesystem options')
    return first


def inspect():
    mounts = []
    for path in PATHS:
        # Older workers may not have created the optional runtime cache yet.
        # Resolve its existing ancestor without creating a directory; subsequent
        # checks inspect the actual cache once it exists.
        probe = Path(path)
        while not probe.exists() and probe != probe.parent:
            probe = probe.parent
        value = json.loads(subprocess.check_output(['findmnt', '-J', '-T', str(probe)], text=True))
        if len(value['filesystems']) != 1:
            raise RuntimeError('ambiguous worker filesystem')
        mounts.append(value['filesystems'][0])
    return validate_mounts(mounts)


def configure(apply=False, preflight=False, restore_options=None):
    before = inspect()
    if apply or restore_options is not None:
        # Restore only the option this helper owns; never roll unrelated mount
        # settings back from an old snapshot.
        desired = 'discard' if restore_options is not None and 'discard' in restore_options.split(',') else 'nodiscard'
        subprocess.run(['mount', '-o', 'remount,'+desired, '/'], check=True)
        after = inspect()
        old = set(before['options'].split(',')) - {'discard', 'nodiscard'}
        new = set(after['options'].split(',')) - {'discard', 'nodiscard'}
        if old != new or before['source'] != after['source']:
            raise RuntimeError('remount changed an unrelated filesystem property')
        if ('discard' in after['options'].split(',')) != (desired == 'discard'):
            raise RuntimeError('requested discard policy was not applied')
    else:
        after = before
    if not preflight and restore_options is None and 'discard' in after['options'].split(','):
        raise RuntimeError('online discard remains enabled')
    return {'mount':after, 'online_discard': 'discard' in after['options'].split(',')}


def check_persistence(unit):
    if 'UnitFileState=enabled' not in unit.splitlines() or not any(
            line.startswith('ExecStartPre=') and INSTALLED+' --apply' in line
            for line in unit.splitlines()):
        raise RuntimeError('worker lacks the enabled persistent storage prestart')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument('--apply', action='store_true')
    modes.add_argument('--preflight', action='store_true')
    modes.add_argument('--check', action='store_true')
    modes.add_argument('--restore-options-file', type=Path)
    args = parser.parse_args()
    restore = args.restore_options_file.read_text().strip() if args.restore_options_file else None
    state = configure(args.apply, args.preflight, restore)
    if args.check:
        unit = subprocess.check_output(['systemctl','show','transparent-shard-server',
                                       '-p','ExecStartPre','-p','UnitFileState'], text=True)
        check_persistence(unit)
        state['persistent'] = True
    state['helper_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    print(json.dumps(state))


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Disable a virtio framebuffer console while retaining an enabled serial console.

Used by the worker's privileged ExecStartPre and read-only acceptance checks.
No driver is unloaded and no boot arguments or serial-console settings change.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

INSTALLED = '/usr/local/lib/transparent-pir/headless-console.py'
PRESTART = 'ExecStartPre=+/usr/bin/python3 '+INSTALLED+' --apply'


def inspect(root=Path('/')):
    consoles = (root/'proc/consoles').read_text()
    serial = any(line.split()[0].startswith(('ttyS', 'hvc', 'ttyAMA')) and '(E' in line
                 for line in consoles.splitlines() if line.split())
    if not serial:
        raise RuntimeError('headless mitigation requires an enabled serial console')
    framebuffers = (root/'proc/fb').read_text().splitlines()
    if any(line.split()[-1] != 'virtio_gpudrmfb' for line in framebuffers if line.split()):
        raise RuntimeError('refusing to change a non-virtio framebuffer console')
    bindings = []
    for path in sorted((root/'sys/class/vtconsole').glob('vtcon*')):
        if 'frame buffer device' in (path/'name').read_text():
            value = (path/'bind').read_text().strip()
            if value not in ('0', '1'):
                raise RuntimeError('invalid framebuffer console binding')
            bindings.append({'path':str(path/'bind'), 'bound':value == '1'})
    if bindings and not framebuffers:
        raise RuntimeError('console binding lacks an identifiable virtio framebuffer')
    if framebuffers and not bindings:
        raise RuntimeError('framebuffer exists without an inspectable console binding')
    return {'serial_console_enabled':serial, 'framebuffers':framebuffers, 'bindings':bindings}


def configure(root=Path('/'), apply=False, preflight=False):
    state = inspect(root)
    if apply:
        for entry in state['bindings']:
            if entry['bound']:
                Path(entry['path']).write_text('0\n')
        state = inspect(root)
    if not preflight and any(entry['bound'] for entry in state['bindings']):
        raise RuntimeError('framebuffer console is still bound')
    return state


def check_persistence(unit):
    if 'UnitFileState=enabled' not in unit.splitlines():
        raise RuntimeError('worker is not enabled across boot')
    if not any(line.startswith('ExecStartPre=') and INSTALLED+' --apply' in line
               for line in unit.splitlines()):
        raise RuntimeError('worker lacks the loaded headless ExecStartPre')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument('--apply', action='store_true')
    modes.add_argument('--preflight', action='store_true')
    modes.add_argument('--check', action='store_true')
    args = parser.parse_args()
    state = configure(apply=args.apply, preflight=args.preflight)
    if args.check:
        unit = subprocess.check_output(['systemctl','show','transparent-shard-server',
                                       '-p','ExecStartPre','-p','UnitFileState'], text=True)
        check_persistence(unit)
        state['persistent'] = True
    state['helper_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    print(json.dumps(state))


if __name__ == '__main__':
    main()

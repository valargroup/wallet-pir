#!/usr/bin/env python3
"""Render and install the public transparent router.

The router owns no API version. It is a shell (site, internal listener,
catch-all 404, proxy errors) around one route file per wallet API version,
`tools/transparent/deploy/api/v<N>.caddy`. Each version's fleet keeps its own
router; the public router only sends `/v<N>/*` there. Adding a version is one
more file and one `apply`; retiring one replaces its file with a stub.

The route files are inlined, so an install is one validated file swap with
rollback on a failed reload (`caddy_swap_command` in transparent-live-fleet.py).
`--withdraw vN` answers that prefix 503 without touching the others.

Config (JSON): public_host, router_host, ssh_key, known_hosts, and optional
internal_listen and router_file.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys

HERE = Path(__file__).resolve().parent
ROUTES = HERE.parent / 'deploy/api'
ADDRESS = re.compile(r'[A-Za-z0-9_.:-]+')
VERSION_FILE = re.compile(r'(v[1-9][0-9]*)\.caddy')


def live_fleet():
    spec = importlib.util.spec_from_file_location('transparent_live_fleet', HERE / 'transparent-live-fleet.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def version_routes(directory, withdrawn=()):
    """`(version, text)` for each route file, in version order.

    A file may only handle paths under its own prefix, so one version's file
    can never shadow another's.
    """
    found = {}
    for path in Path(directory).iterdir():
        match = VERSION_FILE.fullmatch(path.name)
        if not match:
            raise ValueError(f'unexpected route file {path.name}')
        version = match[1]
        text = path.read_text()
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith('handle') and not stripped.startswith(f'handle /{version}/'):
                raise ValueError(f'{path.name} handles a path outside /{version}/: {stripped}')
        found[version] = text
    unknown = set(withdrawn) - set(found)
    if unknown:
        raise ValueError(f'no route file to withdraw for {sorted(unknown)}')
    for version in withdrawn:
        found[version] = (f'# {version} withdrawn by an operator.\nhandle /{version}/* {{\n'
                          f'\theader Retry-After 1\n\trespond "transparent {version} maintenance" 503\n}}\n')
    return sorted(found.items(), key=lambda item: int(item[0][1:]))


def render(config, directory=ROUTES, withdrawn=()):
    host = config['public_host']
    internal = config.get('internal_listen')
    if not ADDRESS.fullmatch(host) or internal and not ADDRESS.fullmatch(internal):
        raise ValueError('invalid router addresses')
    body = ['\trequest_body {\n\t\tmax_size 1MB\n\t}']
    for version, text in version_routes(directory, withdrawn):
        body += ['\t' + line if line else line for line in text.rstrip('\n').split('\n')]
    body += ['\thandle {\n\t\trespond 404\n\t}', live_fleet().ROUTER_PROXY_ERRORS.rstrip('\n')]
    sites = [host] + (['http://' + internal] if internal else [])
    return '{\n\tservers {\n\t\tmetrics\n\t}\n}\n' + '\n'.join(site + ' {\n' + '\n'.join(body) + '\n}\n' for site in sites)


def apply(config, text):
    command = live_fleet().caddy_swap_command(config.get('router_file', '/etc/caddy/Caddyfile'))
    ssh = ['ssh', '-i', config['ssh_key'], '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes',
           '-o', 'StrictHostKeyChecking=yes', '-o', 'UserKnownHostsFile=' + config['known_hosts'],
           '-o', 'ConnectTimeout=10', 'root@' + config['router_host'], command]
    subprocess.run(ssh, input=text.encode(), check=True, timeout=60)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0], allow_abbrev=False)
    parser.add_argument('action', choices=('render', 'apply'))
    parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--routes', type=Path, default=ROUTES)
    parser.add_argument('--withdraw', action='append', default=[], metavar='vN')
    args = parser.parse_args(argv)
    config = json.loads(args.config.read_text())
    text = render(config, args.routes, args.withdraw)
    if args.action == 'render':
        sys.stdout.write(text)
    else:
        apply(config, text)


if __name__ == '__main__':
    main()

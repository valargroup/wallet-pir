"""Compose existing product role renderers into one checked query-host unit.

Products keep their own HTTPS acquisition settings and state. The final binary's
offline check validates the combined listener/path layout and compiled features.
Receiver is deliberately refused until the root binary can host it.
"""
import ipaddress
import json
from pathlib import PurePosixPath

from .paths import ROOT
from .role_config import directory, disjoint, settings_of, worker
from tools.enhance.lib.roles import combined as enhance
from tools.status.lib.roles import combined as status
from tools.transparent.lib.roles import history_worker
from tools.txid.lib.roles import display_worker

CONFIG = '/etc/wallet-pir-query/query-server.json'
TEMPLATE = ROOT / 'tools/deploy/query-server/pir-query-server.service'
PRODUCTS = {'enhance': enhance, 'status': status,
            'transparent_history': history_worker, 'transaction_details': display_worker}


def listener(value):
    """Only fixed loopback sockets belong to the single-node deployment."""
    try:
        host, port = value.rsplit(':', 1)
        valid = ipaddress.ip_address(host.strip('[]')).is_loopback and 1 <= int(port) <= 65535
    except (AttributeError, ValueError):
        valid = False
    if not valid:
        raise ValueError('query node listeners must use fixed loopback sockets')
    return value


def ca_files(value):
    """Collect trust-store paths without reading or mounting product state."""
    if isinstance(value, dict):
        for key, child in value.items():
            if key in ('publisher_ca', 'archive_ca', 'trusted_ca') and child is not None:
                yield directory(child)
            else:
                yield from ca_files(child)


def combined(settings, *, release, template=None):
    """Render all currently supported products and their shared wallet listener.

    Refuse partial archives, remote archive forwarding, intersecting state roots,
    non-HTTPS publishers and inconsistent public listeners. Configurations for
    products absent from the binary fail its returned `--check` before install.
    """
    s = settings_of(settings, ('listen', 'management_listen', *PRODUCTS), ('config_path',))
    public, management = listener(s['listen']), listener(s['management_listen'])
    sockets = {public, management}
    if public == management:
        raise ValueError('management and wallet listeners must differ')
    history = s['transparent_history']
    if not isinstance(history, dict) or history.get('full_holdings') is not True or history.get('archive_upstream'):
        raise ValueError('query node requires local full_holdings with no archive_upstream')
    config = directory(s.get('config_path', CONFIG))
    document = {'management_listen': management, 'public_listen': public}
    writable = []
    for name, renderer in PRODUCTS.items():
        product = s[name]
        if not isinstance(product, dict):
            raise ValueError(name + ' settings must be an object')
        if set(product) & {'limits', 'credentials'}:
            raise ValueError('set limits and credentials on the host, not individual products')
        rendered = renderer(product, release=release)
        child = json.loads(next(iter(rendered.files.values())))
        if child.get('public_listen', public) != public:
            raise ValueError(name + ' wallet listener differs from the shared listener')
        body = child[name]['settings']
        for field in ('listen', 'control_listen'):
            if field in body:
                address = listener(body[field])
                if address in sockets:
                    raise ValueError('query node listeners collide')
                sockets.add(address)
        for root in rendered.directories:
            if not PurePosixPath(root).is_relative_to('/srv/wallet-pir-query'):
                raise ValueError('query node state must be on its mounted data volume')
            for previous in writable:
                disjoint([root], [previous])
            writable.append(root)
        document[name] = child[name]
    readonly = [config, *ca_files(s)]
    return worker(TEMPLATE.read_text() if template is None else template,
                  directory(release) + '/pir-query-server', ['--config', config],
                  configurations={config: document}, writable=writable,
                  readonly=readonly, settings=s, check=('--config', config, '--check'))

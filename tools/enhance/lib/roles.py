"""Enhance serving roles: one pir-query-server process and its sandbox.

Settings come from the operator inventory, which owns hosts, paths, budgets
and credential file references; the template owns the sandbox. Each renderer
writes `/etc/enhance-pir/<role>.json` and a unit whose command is
`{release}/pir-query-server --config` that file. A second external worker on
the same host is another unit: it gets a file named for that unit and its own
loopback management port, so the two processes do not share a configuration
or a listener. The publication application runs on `pir-publisher` and is not rendered
here.

Each role independently follows its configured HTTPS publisher. Static domain
responsibilities and private downstream peers belong to the operator inventory.
Preflight validates configuration without opening caches or loading GPU drivers.
"""
import re
import zlib
from urllib.parse import urlsplit

from wallet_pir_ops.paths import ROOT
from wallet_pir_ops.role_config import address, directory, number, settings_of, worker as render_worker

CONFIG_ROOT = '/etc/enhance-pir'
QUERY_BINARY = 'pir-query-server'
WORKER_TEMPLATE = ROOT / 'tools/enhance/deploy/enhance-pir-worker.role.service'
INGRESS_TEMPLATE = ROOT / 'tools/enhance/deploy/enhance-pir-query-ingress.role.service'
ROUTER_TEMPLATE = ROOT / 'tools/enhance/deploy/enhance-pir-packing-router.role.service'
# Loopback only. Deploy health stays on the role's /internal/health; this port
# is the process's own /health and /ready.
MANAGEMENT = {
    'worker': '127.0.0.1:18091',
    'external-worker': '127.0.0.1:18091',
    'query-ingress': '127.0.0.1:18083',
    'packing-router': '127.0.0.1:18093',
    'combined': '127.0.0.1:18087',
}


def _matvec(settings):
    backend = settings.get('matvec_backend', 'cpu')
    if backend not in ('cpu', 'cuda'):
        raise ValueError('matvec_backend is cpu or cuda')
    device = settings.get('cuda_device')
    if device is not None and backend != 'cuda':
        raise ValueError('cuda_device requires matvec_backend cuda')
    return {'matvec_backend': backend,
            'cuda_device': None if device is None else int(number(device, 'cuda_device'))}


def _origin(value, scheme):
    if not isinstance(value, str):
        raise ValueError('peer origin must be a URL')
    parsed = urlsplit(value)
    if (parsed.scheme != scheme or not parsed.hostname or parsed.username or parsed.password
            or parsed.path not in ('', '/') or parsed.query or parsed.fragment):
        raise ValueError('expected an ' + scheme + ' origin without credentials or a path')
    return value.rstrip('/')


def _synchronization(value, role):
    required = {'publisher_origin', 'download_budget_bytes', 'cache_limit_bytes',
                {'worker': 'shards', 'packing-router': 'workers', 'query-ingress': 'routers'}[role]}
    optional = {'trusted_ca'} | ({'preferred'} if role == 'packing-router' else set())
    if not isinstance(value, dict) or required - value.keys() or value.keys() - required - optional:
        raise ValueError('incomplete or unknown Enhance synchronization settings')
    config = dict(value)
    config['publisher_origin'] = _origin(config['publisher_origin'], 'https')
    for name, minimum in [('download_budget_bytes', 6 * 1024 * 1024), ('cache_limit_bytes', 1)]:
        if type(config[name]) is not int or config[name] < minimum:
            raise ValueError(name + ' is too small')
    if config.get('trusted_ca') is not None:
        config['trusted_ca'] = directory(config['trusted_ca'])
    if role == 'worker':
        shards = config['shards']
        if (not isinstance(shards, list) or not shards or len(shards) > 7
                or any(type(s) is not int or not 0 <= s < 24 for s in shards)
                or len(set(shards)) != len(shards)):
            raise ValueError('worker needs distinct domain responsibilities below 24')
    else:
        name = 'workers' if role == 'packing-router' else 'routers'
        peers = config[name]
        if not isinstance(peers, dict) or not peers or len(peers) > (6 if name == 'workers' else 24):
            raise ValueError('invalid domain peer map')
        for shard, origins in peers.items():
            if not isinstance(shard, str) or not shard.isdecimal() or str(int(shard)) != shard or not 0 <= int(shard) < 24:
                raise ValueError('domain keys must be canonical integers below 24')
            if (not isinstance(origins, list) or not 1 <= len(origins) <= 2
                    or (name == 'workers' and len(set(origins)) != len(origins))):
                raise ValueError('each domain needs one or two distinct peers')
            for origin in origins:
                if name == 'workers':
                    _origin(origin, 'http')
                else:
                    if not isinstance(origin, dict) or set(origin) != {'control_origin', 'query_origin'}:
                        raise ValueError('router needs explicit control and query origins')
                    _origin(origin['control_origin'], 'http')
                    _origin(origin['query_origin'], 'http')
            if name == 'routers' and len({(v['control_origin'], v['query_origin']) for v in origins}) != len(origins):
                raise ValueError('router endpoints must be distinct')
        preferred = config.get('preferred', {})
        if not isinstance(preferred, dict) or any(
                shard not in peers or not isinstance(origins, list) or not set(origins) <= set(peers[shard])
                for shard, origins in preferred.items()):
            raise ValueError('preferred workers must be configured replicas')
    return config


def _binding(role, unit, listen):
    """Config path and loopback management port for one process.

    The descriptor's unit keeps `/etc/enhance-pir/<role>.json` and the role's
    port. Another external-worker unit on that host is a second process: its
    file is named for the unit, and its port is in 20000-29999 so it does not
    bind the first worker's `127.0.0.1:18091` or its own service listener.
    """
    if role != 'external-worker' or unit is None:
        return '%s/%s.json' % (CONFIG_ROOT, role), MANAGEMENT[role]
    if not unit.endswith('.service'):
        raise ValueError('external worker unit must be a systemd service name')
    stem = unit[:-len('.service')]
    if not re.fullmatch(r'[A-Za-z0-9._-]+', stem):
        raise ValueError('external worker unit name must be a plain file name')
    port = 20000 + (zlib.crc32(unit.encode()) % 10000)
    management = '127.0.0.1:%d' % port
    if management == listen:
        management = '127.0.0.1:%d' % (20000 + ((port - 20000 + 1) % 10000))
    return '%s/%s.json' % (CONFIG_ROOT, stem), management


def _render(role, template, release, settings, body, writable, unit=None, public=None):
    """One query-server process: config file, command, and the template's sandbox.
    A wallet-facing role is reached on the host's `public_listen`."""
    path, management = _binding(role, unit, body.get('listen', public))
    document = {'management_listen': management}
    if public is not None:
        document['public_listen'] = public
    document['enhance'] = {'enabled': True, 'settings': body}
    ca = body['synchronization'].get('trusted_ca')
    return render_worker(template, directory(release) + '/' + QUERY_BINARY, ['--config', path],
                         configurations={path: document}, writable=writable, readonly=[path, *([ca] if ca else [])], settings=settings,
                         check=['--config', path, '--check'])


def _worker(role, settings, release, template, unit=None):
    """`pir-query-server` worker: CPU by default; CUDA only when the inventory says so."""
    parsed = settings_of(settings, ('listen', 'data_dir', 'synchronization'), ('sealed_shards', 'matvec_backend', 'cuda_device'))
    data = directory(parsed['data_dir'])
    body = {'role': 'worker', 'listen': address(parsed['listen'], 'listen'), 'data_dir': data,
            'sealed_shards': int(number(parsed.get('sealed_shards', 6), 'sealed_shards', 1)),
            'matvec': _matvec(parsed), 'synchronization': _synchronization(parsed['synchronization'], 'worker')}
    text = WORKER_TEMPLATE.read_text() if template is None else template
    return _render(role, text, release, parsed, body, [data], unit)


def worker(settings, *, release, template=None):
    """Fleet worker and the external worker share this command and sandbox."""
    return _worker('worker', settings, release, template)


def external_worker(settings, *, release, template=None, unit=None):
    """The worker renderer. A non-default unit on this host gets its own config file."""
    return _worker('external-worker', settings, release, template, unit)


def query_ingress(settings, *, release, template=None):
    """Public query ingress. `requests` and `uploads` keep the process defaults when omitted."""
    parsed = settings_of(settings, ('listen', 'control_listen', 'data_dir', 'synchronization'), ('requests', 'uploads'))
    data = directory(parsed['data_dir'])
    public = address(parsed['listen'], 'listen')
    body = {'role': 'query-ingress', 'data_dir': data,
            'control_listen': address(parsed['control_listen'], 'control_listen'),
            'synchronization': _synchronization(parsed['synchronization'], 'query-ingress')}
    if 'requests' in parsed:
        body['requests'] = int(number(parsed['requests'], 'requests', 1))
    if 'uploads' in parsed:
        body['uploads'] = int(number(parsed['uploads'], 'uploads', 1))
    text = INGRESS_TEMPLATE.read_text() if template is None else template
    return _render('query-ingress', text, release, parsed, body, [data], public=public)


def packing_router(settings, *, release, template=None):
    """Packing router. `max_objects` and `requests` keep the process defaults when omitted."""
    parsed = settings_of(settings, ('data_dir', 'synchronization', 'listen', 'control_listen'),
                         ('max_objects', 'requests'))
    data = directory(parsed['data_dir'])
    body = {'role': 'packing-router', 'data_dir': data,
            'synchronization': _synchronization(parsed['synchronization'], 'packing-router'),
            'listen': address(parsed['listen'], 'listen'),
            'control_listen': address(parsed['control_listen'], 'control_listen')}
    if 'max_objects' in parsed:
        body['max_objects'] = int(number(parsed['max_objects'], 'max_objects', 1))
    if 'requests' in parsed:
        body['requests'] = int(number(parsed['requests'], 'requests', 1))
    text = ROUTER_TEMPLATE.read_text() if template is None else template
    return _render('packing-router', text, release, parsed, body, [data])


def combined(settings, *, release, template=None):
    """Ingress, packing router and worker in one process for the worker's domains.

    Wallets reach it on the host's `public_listen`, rendered from `listen`;
    `control_listen` is private. Its three components call each other directly,
    so it names no peers and needs no internal token between them. The process
    keeps each component's state in its own subdirectory of `data_dir`.
    """
    parsed = settings_of(settings, ('listen', 'control_listen', 'data_dir', 'synchronization'),
                         ('sealed_shards', 'matvec_backend', 'cuda_device', 'requests', 'uploads',
                          'max_objects', 'packing_requests'))
    data = directory(parsed['data_dir'])
    public = address(parsed['listen'], 'listen')
    control = address(parsed['control_listen'], 'control_listen')
    if public == control:
        raise ValueError('listen and control_listen must differ')
    body = {'role': 'combined', 'data_dir': data, 'control_listen': control,
            'sealed_shards': int(number(parsed.get('sealed_shards', 6), 'sealed_shards', 1)),
            'matvec': _matvec(parsed), 'synchronization': _synchronization(parsed['synchronization'], 'worker')}
    for name in ('requests', 'uploads', 'max_objects', 'packing_requests'):
        if name in parsed:
            body[name] = int(number(parsed[name], name, 1))
    text = WORKER_TEMPLATE.read_text() if template is None else template
    return _render('combined', text, release, parsed, body, [data], public=public)


def report(service, role, health, ready_status):
    """Enhance worker health, separated into the report's axes."""
    if health is None:
        return {'executable': {'binary_sha256': 'unobserved'}, 'publication': 'unobserved',
                'preparation': 'unobserved', 'readiness': 'unobserved'}
    executable = {'binary_sha256': health.get('binary_sha256'), 'incarnation': health.get('incarnation')}
    active = health.get('active')
    ready = health.get('ready') is True and ready_status == 200
    return {
        'executable': executable,
        'publication': {'loaded': active},
        'preparation': health.get('preparing', 'none since start'),
        'readiness': {'verified': bool(active), 'warm': bool(active),
                      'authorized': ready, 'ready': ready},
    }

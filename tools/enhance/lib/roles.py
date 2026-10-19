"""Enhance serving roles: one pir-query-server process and its sandbox.

Settings come from the operator inventory, which owns hosts, paths, budgets
and credential file references; the template owns the sandbox. Each renderer
writes `/etc/enhance-pir/<role>.json` and a unit whose command is
`{release}/pir-query-server --config` that file. The coordinator keeps
`enhance-pir-server` and is not rendered here.

An Enhance worker is reserved, filled and activated by its coordinator and
only serves once a packing router lists it. It has no staged inputs of its
own, so it has no preflight input check: the product says so rather than
manufacturing one.
"""
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


def _origin(value):
    if (not isinstance(value, str) or any(character.isspace() for character in value)
            or not value.startswith(('http://', 'https://'))):
        raise ValueError('artifact_origin must be an http:// or https:// origin')
    return value


def _render(role, template, release, settings, body, writable):
    """One query-server process: config file, command, and the template's sandbox."""
    path = '%s/%s.json' % (CONFIG_ROOT, role)
    document = {'management_listen': MANAGEMENT[role],
                'enhance': {'enabled': True, 'settings': body}}
    return render_worker(template, directory(release) + '/' + QUERY_BINARY, ['--config', path],
                         configurations={path: document}, writable=writable, readonly=[path], settings=settings)


def _worker(role, settings, release, template):
    """`pir-query-server` worker: CPU by default; CUDA only when the inventory says so."""
    parsed = settings_of(settings, ('listen', 'data_dir'), ('sealed_shards', 'matvec_backend', 'cuda_device'))
    data = directory(parsed['data_dir'])
    body = {'role': 'worker', 'listen': address(parsed['listen'], 'listen'), 'data_dir': data,
            'sealed_shards': int(number(parsed.get('sealed_shards', 6), 'sealed_shards', 1)),
            'matvec': _matvec(parsed)}
    text = WORKER_TEMPLATE.read_text() if template is None else template
    return _render(role, text, release, parsed, body, [data])


def worker(settings, *, release, template=None):
    """Fleet worker and the external worker share this command and sandbox."""
    return _worker('worker', settings, release, template)


def external_worker(settings, *, release, template=None):
    """The worker renderer, with the external role's own config file."""
    return _worker('external-worker', settings, release, template)


def query_ingress(settings, *, release, template=None):
    """Public query ingress. `requests` and `uploads` keep the process defaults when omitted."""
    parsed = settings_of(settings, ('listen', 'control_listen', 'data_dir'), ('requests', 'uploads'))
    data = directory(parsed['data_dir'])
    body = {'role': 'query-ingress', 'data_dir': data, 'listen': address(parsed['listen'], 'listen'),
            'control_listen': address(parsed['control_listen'], 'control_listen')}
    if 'requests' in parsed:
        body['requests'] = int(number(parsed['requests'], 'requests', 1))
    if 'uploads' in parsed:
        body['uploads'] = int(number(parsed['uploads'], 'uploads', 1))
    text = INGRESS_TEMPLATE.read_text() if template is None else template
    return _render('query-ingress', text, release, parsed, body, [data])


def packing_router(settings, *, release, template=None):
    """Packing router. `max_objects` and `requests` keep the process defaults when omitted."""
    parsed = settings_of(settings, ('data_dir', 'artifact_origin', 'listen', 'control_listen'),
                         ('max_objects', 'requests'))
    data = directory(parsed['data_dir'])
    body = {'role': 'packing-router', 'data_dir': data, 'artifact_origin': _origin(parsed['artifact_origin']),
            'listen': address(parsed['listen'], 'listen'),
            'control_listen': address(parsed['control_listen'], 'control_listen')}
    if 'max_objects' in parsed:
        body['max_objects'] = int(number(parsed['max_objects'], 'max_objects', 1))
    if 'requests' in parsed:
        body['requests'] = int(number(parsed['requests'], 'requests', 1))
    text = ROUTER_TEMPLATE.read_text() if template is None else template
    return _render('packing-router', text, release, parsed, body, [data])


def report(service, role, health, ready_status):
    """Enhance worker health, separated into the report's axes."""
    if health is None:
        return {'executable': {'binary_sha256': 'unobserved'}, 'publication': 'unobserved',
                'preparation': 'unobserved', 'readiness': 'unobserved'}
    executable = {'binary_sha256': health.get('binary_sha256'), 'incarnation': health.get('incarnation')}
    published = health.get('published') or []
    preparation = health.get('preparation')
    return {
        'executable': executable,
        'publication': {'assigned': health.get('candidate'), 'loaded': health.get('published_manifest_digests'),
                        'generations': published},
        'preparation': preparation if preparation is not None else 'none since start',
        'readiness': {'verified': bool(published), 'warm': bool(published),
                      # Activation and commit come only from the coordinator.
                      'authorized': bool(published),
                      'ready': 'unsupported; a packing router must also list it'},
    }

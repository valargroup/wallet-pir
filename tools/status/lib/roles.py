"""External Status worker role: one pir-query-server process and its sandbox.

Settings come from the operator inventory; the template owns the sandbox. The
renderer writes `/etc/status-pir/external-worker.json` and a unit whose
command is `{release}/pir-query-server --config` that file. A Status worker is
an autonomous HTTPS consumer: it fetches publisher-prepared material, verifies
it and activates it locally. It has no staged inputs of its own, so it has no
preflight input check. Private listeners remain available for monitoring and
encrypted evaluation.
"""
import ipaddress

from wallet_pir_ops.paths import ROOT
from wallet_pir_ops.role_config import address, directory, settings_of, worker as render_worker

TEMPLATE = ROOT / 'tools/status/deploy/status-worker.role.service'
CONFIG = '/etc/status-pir/external-worker.json'
# Loopback only: the process's own /health and /ready. Deploy health stays on
# the role's control routes.
MANAGEMENT = '127.0.0.1:18485'


def status_worker(settings, *, release, template=None):
    """Render an HTTPS-fed Status worker without publisher control sessions."""
    return _local_role(settings, release=release, template=TEMPLATE.read_text() if template is None else template,
                       role='worker', external=True)


def report(service, role, health, ready_status):
    """Status worker identity, separated into the report's axes."""
    if health is None:
        return {'executable': {'binary_sha256': 'unobserved'}, 'publication': 'unobserved',
                'preparation': 'unobserved', 'readiness': 'unobserved'}
    executable = {'binary_sha256': health.get('binary_sha256'), 'incarnation': health.get('incarnation')}
    # The identity route carries no publication; serving readiness is checked
    # separately after autonomous HTTPS synchronization.
    return {'executable': executable, 'publication': 'unsupported', 'preparation': 'unsupported',
            'readiness': {'verified': 'unsupported', 'warm': 'unsupported', 'authorized': 'unsupported',
                          'ready': 'unsupported'}}


def _local_role(settings, *, release, template, role, external=False):
    """Render a Status HTTPS consumer and its private evaluation listeners."""
    from urllib.parse import urlsplit
    required = ('network_hex', 'state_dir', 'listen', 'publisher')
    optional = ('worker_origin', 'publisher_ca') + (('query_listen',) if role == 'router' else ())
    config = settings_of(settings, required, optional)
    network = config['network_hex']
    if not isinstance(network, str) or len(network) != 64 or any(c not in '0123456789abcdef' for c in network):
        raise ValueError('network_hex must be 32 bytes of lowercase hex')
    origin = urlsplit(config['publisher'])
    if (origin.scheme != 'https' or not origin.hostname or origin.username or origin.password
            or origin.path not in ('', '/') or origin.query or origin.fragment):
        raise ValueError('publisher must be an explicit HTTPS origin')
    body = dict(config, role=role)
    body['state_dir'] = directory(body['state_dir'])
    body.setdefault('worker_origin', 'http://' + body['listen'])
    readonly = []
    if 'publisher_ca' in body:
        body['publisher_ca'] = directory(body['publisher_ca'])
        readonly.append(body['publisher_ca'])
    for name in ('listen', 'query_listen', 'worker_origin'):
        if name not in body:
            continue
        value = body[name]
        if name == 'worker_origin':
            peer = urlsplit(value)
            if peer.scheme != 'http' or peer.username or peer.password or peer.path not in ('', '/') or peer.query or peer.fragment:
                raise ValueError('worker_origin must be a loopback HTTP origin')
            value = peer.netloc
        address(value, name)
        host, port = value.rsplit(':', 1)
        if not ipaddress.ip_address(host.strip('[]')).is_loopback or not 1 <= int(port) <= 65535:
            raise ValueError(name + ' must use a loopback address and a valid port')
    path = CONFIG if external else '/etc/status-pir/' + role + '.json'
    management = MANAGEMENT if external else '127.0.0.1:' + ('18481' if role == 'worker' else '18482')
    document = {'management_listen': management, 'status': {'enabled': True, 'settings': body}}
    return render_worker(template, directory(release) + '/pir-query-server', ['--config', path],
                         configurations={path: document}, writable=[body['state_dir']],
                         readonly=[path, *readonly], settings=config, check=('--config', path, '--check'))


def worker(settings, *, release, template=None):
    """Render the existing CPU evaluation worker and its checked configuration."""
    if template is None:
        template = (ROOT / 'tools/status/deploy/status-worker.service.in').read_text()
    return _local_role(settings, release=release, template=template, role='worker')


def router(settings, *, release, template=None):
    """Render the existing packing router and its wallet listener configuration."""
    if template is None:
        template = (ROOT / 'tools/status/deploy/status-router.service.in').read_text()
    return _local_role(settings, release=release, template=template, role='router')

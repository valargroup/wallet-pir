"""Render Receiver root hosts without mutating infrastructure or reading secrets."""
import re
from urllib.parse import urlsplit
from wallet_pir_ops.paths import ROOT
from wallet_pir_ops.role_config import address, directory, number, settings_of, worker

QUERY_TEMPLATE = ROOT / 'tools/receiver/deploy/receiver-query.service'
PUBLISHER_TEMPLATE = ROOT / 'tools/receiver/digitalocean/receiver-pir.service.in'

def query(settings, *, release, template=None):
    """Render an HTTPS-only Receiver query host and its offline check."""
    s = settings_of(settings, ('config_path', 'state_dir', 'listen', 'management_listen',
                               'publisher', 'genesis', 'activation_height'),
                    ('publisher_ca', 'poll_seconds', 'bootstrap_max_bytes',
                     'table_memory_mib', 'retention_seconds'))
    config_path, state = directory(s['config_path']), directory(s['state_dir'])
    origin = urlsplit(s['publisher'])
    if (origin.scheme != 'https' or not origin.netloc or origin.username is not None
            or origin.password is not None or origin.path not in ('', '/') or origin.query or origin.fragment):
        raise ValueError('Receiver publisher must be an HTTPS origin')
    genesis = s['genesis']
    if (not isinstance(genesis, list) or len(genesis) != 32
            or any(type(byte) is not int or not 0 <= byte <= 255 for byte in genesis)):
        raise ValueError('Receiver genesis must contain 32 bytes')
    body = {'publisher': s['publisher'], 'genesis': genesis, 'state_dir': state,
            'activation_height': int(number(s['activation_height'], 'activation_height', 1))}
    readonly = [config_path]
    if s.get('publisher_ca') is not None:
        body['publisher_ca'] = directory(s['publisher_ca'])
        readonly.append(body['publisher_ca'])
    for name in ('poll_seconds', 'bootstrap_max_bytes', 'table_memory_mib', 'retention_seconds'):
        if name in s:
            body[name] = int(number(s[name], name, 1))
    document = {'management_listen': address(s['management_listen'], 'management_listen'),
                'public_listen': address(s['listen'], 'listen'),
                'receiver': {'enabled': True, 'settings': body}}
    return worker(QUERY_TEMPLATE.read_text() if template is None else template,
                  directory(release) + '/pir-query-server', ['--config', config_path],
                  configurations={config_path: document}, writable=[state], readonly=readonly,
                  settings=s, check=('--config', config_path, '--check'))

def publisher(settings, *, release, template=None):
    """Preserve the existing Receiver store under the root publisher's chain feed."""
    s = settings_of(settings, ('config_path', 'management_listen', 'chain', 'receiver'), ('near_key_id',))
    config_path = directory(s['config_path'])
    body = dict(s['receiver'])
    state = directory(body['data_dir'])
    document = {'management_listen': address(s['management_listen'], 'management_listen'),
                'chain': s['chain'], 'receiver': {'enabled': True, 'settings': body}}
    readonly = [config_path]
    if s['chain'].get('rpc_cookie') is not None:
        readonly.append(directory(s['chain']['rpc_cookie']))
    key_id = s.get('near_key_id', 'disabled')
    if not isinstance(key_id, str) or not re.fullmatch(r'[A-Za-z0-9_.-]+', key_id):
        raise ValueError('near_key_id must name an installed audit file, without path syntax')
    text = (PUBLISHER_TEMPLATE.read_text() if template is None else template).replace('@NEAR_KEY@', key_id)
    return worker(text,
                  directory(release) + '/pir-publisher', ['--config', config_path],
                  configurations={config_path: document}, writable=[state], readonly=readonly,
                  settings=s, check=('--config', config_path, '--check'))

def report(service, role, health, ready_status):
    """Root management reports readiness; object presence grants no authority."""
    identity = (health or {}).get('identity') or {}
    return {'executable': {'binary_sha256': identity.get('binary_sha256', 'unobserved'),
                           'incarnation': identity.get('incarnation')},
            'publication': health if health is not None else 'unobserved',
            'preparation': 'unobserved',
            'readiness': {'ready': bool(health and health.get('ready') and ready_status == 200)}}

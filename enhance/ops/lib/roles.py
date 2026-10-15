"""External Enhance and Status worker roles: real arguments and their sandbox.

Settings come from the operator inventory, which owns hosts, paths, budgets
and credential file references; the templates own the sandbox. Workers keep
their dependencies: an Enhance worker is reserved, filled and activated by
its coordinator and only serves once a packing router lists it; a Status
worker is fenced, prepared and activated by the Status controller and pulls
its artifact from the router's artifact origin. Neither has staged inputs of
its own, so neither has a preflight input check: the product says so rather
than manufacturing one.
"""
from wallet_pir_ops.paths import ROOT
from wallet_pir_ops.role_config import address, directory, number, settings_of, worker as render_worker

ENHANCE_TEMPLATE = ROOT / 'enhance/ops/deploy/enhance-pir-worker.role.service'
STATUS_TEMPLATE = ROOT / 'enhance/ops/deploy/status-worker.role.service'


def worker(settings, *, release, template=None):
    """`enhance-pir-server worker`: CPU by default; CUDA only when the inventory says so."""
    s = settings_of(settings, ('listen', 'data_dir'), ('sealed_shards', 'matvec_backend', 'cuda_device'))
    data = directory(s['data_dir'])
    backend = s.get('matvec_backend', 'cpu')
    if backend not in ('cpu', 'cuda'):
        raise ValueError('matvec_backend is cpu or cuda')
    arguments = ['--sealed-shards', number(s.get('sealed_shards', 6), 'sealed_shards', 1), 'worker',
                 '--listen', address(s['listen'], 'listen'), '--data-dir', data, '--matvec-backend', backend]
    if s.get('cuda_device') is not None:
        if backend != 'cuda':
            raise ValueError('cuda_device requires matvec_backend cuda')
        arguments += ['--cuda-device', number(s['cuda_device'], 'cuda_device')]
    template = ENHANCE_TEMPLATE.read_text() if template is None else template
    return render_worker(template, directory(release) + '/enhance-pir-server', arguments,
                         writable=[data], settings=s)


def status_worker(settings, *, release, template=None):
    """`status-pir role --role worker`, fenced and fed by the Status controller."""
    s = settings_of(settings, ('network_hex', 'listen', 'artifact_origin', 'state_dir'), ('cuda',))
    network = s['network_hex']
    if not isinstance(network, str) or not network or any(c not in '0123456789abcdef' for c in network):
        raise ValueError('network_hex must be lowercase hex')
    origin = s['artifact_origin']
    if not isinstance(origin, str) or not origin.startswith('http://'):
        raise ValueError('artifact_origin must be the router\'s private http:// origin')
    address(origin[len('http://'):].rstrip('/'), 'artifact_origin')
    state = directory(s['state_dir'])
    arguments = ['role', '--role', 'worker', '--network-hex', network, '--state-dir', state,
                 '--listen', address(s['listen'], 'listen'), '--artifact-origin', origin]
    if s.get('cuda', False) is not False:
        if s['cuda'] is not True:
            raise ValueError('cuda must be a boolean')
        arguments.append('--cuda')
    template = STATUS_TEMPLATE.read_text() if template is None else template
    return render_worker(template, directory(release) + '/status-pir', arguments, writable=[state], settings=s)


def report(service, role, health, ready_status):
    """Enhance worker or Status identity, separated into the report's axes."""
    if health is None:
        return {'executable': {'binary_sha256': 'unobserved'}, 'publication': 'unobserved',
                'preparation': 'unobserved', 'readiness': 'unobserved'}
    executable = {'binary_sha256': health.get('binary_sha256'), 'incarnation': health.get('incarnation')}
    if service == 'status':
        # The identity route carries no publication; control health is the
        # controller's to interpret and its schema is closed.
        return {'executable': executable, 'publication': 'unsupported', 'preparation': 'unsupported',
                'readiness': {'verified': 'unsupported', 'warm': 'unsupported', 'authorized': 'unsupported',
                              'ready': 'unsupported'}}
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

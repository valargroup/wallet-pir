"""External Status worker role: real arguments and their sandbox.

Settings come from the operator inventory; the template owns the sandbox. A
Status worker is fenced, prepared and activated by the Status controller and
pulls its artifact from the router's artifact origin. It has no staged inputs
of its own, so it has no preflight input check: the product says so rather
than manufacturing one.
"""
from wallet_pir_ops.paths import ROOT
from wallet_pir_ops.role_config import address, directory, settings_of, worker as render_worker

TEMPLATE = ROOT / 'tools/status/deploy/status-worker.role.service'


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
    template = TEMPLATE.read_text() if template is None else template
    return render_worker(template, directory(release) + '/status-pir', arguments, writable=[state], settings=s)


def report(service, role, health, ready_status):
    """Status worker identity, separated into the report's axes."""
    if health is None:
        return {'executable': {'binary_sha256': 'unobserved'}, 'publication': 'unobserved',
                'preparation': 'unobserved', 'readiness': 'unobserved'}
    executable = {'binary_sha256': health.get('binary_sha256'), 'incarnation': health.get('incarnation')}
    # The identity route carries no publication; control health is the
    # controller's to interpret and its schema is closed.
    return {'executable': executable, 'publication': 'unsupported', 'preparation': 'unsupported',
            'readiness': {'verified': 'unsupported', 'warm': 'unsupported', 'authorized': 'unsupported',
                          'ready': 'unsupported'}}

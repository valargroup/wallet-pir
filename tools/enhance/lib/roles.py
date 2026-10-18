"""External Enhance worker role: real arguments and their sandbox.

Settings come from the operator inventory, which owns hosts, paths, budgets
and credential file references; the template owns the sandbox. An Enhance
worker is reserved, filled and activated by its coordinator and only serves
once a packing router lists it. It has no staged inputs of its own, so it has
no preflight input check: the product says so rather than manufacturing one.
"""
from wallet_pir_ops.paths import ROOT
from wallet_pir_ops.role_config import address, directory, number, settings_of, worker as render_worker

ENHANCE_TEMPLATE = ROOT / 'tools/enhance/deploy/enhance-pir-worker.role.service'


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

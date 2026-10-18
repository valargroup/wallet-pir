"""External Receiver worker role: `receiver-directory serve-publication`.

The rendered configuration names an operator-owned publication record and the
directory publications are staged under, both read-only; the state directory
is the only writable path. The preflight check is the service's own loader in
`--check` mode: it verifies the recorded publication exactly as startup does
and exits without serving. No partner key is ever part of this role.
"""
from pathlib import PurePosixPath

from wallet_pir_ops.paths import ROOT
from wallet_pir_ops.role_config import address, directory, number, settings_of, worker as render_worker

TEMPLATE = ROOT / 'tools/receiver/deploy/receiver-serve-publication.service'


def serve_publication(settings, *, release, template=None):
    s = settings_of(settings, ('config_path', 'publication_record', 'publication_root', 'state_dir', 'bind',
                               'rpc_urls'),
                    ('cookie', 'no_auth', 'poll_seconds', 'witnesses', 'min_rows'))
    config_path, record, root, state = (directory(s[name]) for name in
                                        ('config_path', 'publication_record', 'publication_root', 'state_dir'))
    urls = s['rpc_urls']
    if not isinstance(urls, list) or not urls or any(not isinstance(u, str) or not u.startswith('http://')
                                                     for u in urls):
        raise ValueError('rpc_urls must list http:// node endpoints')
    cookie, no_auth = s.get('cookie'), s.get('no_auth', False)
    if (cookie is None) == (no_auth is not True):
        raise ValueError('set exactly one of cookie and no_auth')
    config = {'publication_record': record, 'bind': address(s['bind'], 'bind'), 'state_dir': state,
              'rpc_urls': urls}
    reads = [config_path, directory(str(PurePosixPath(record).parent)), root]
    if cookie is not None:
        config['cookie'] = directory(cookie)
        reads.append(config['cookie'])
    else:
        config['no_auth'] = True
    for name in ('poll_seconds', 'min_rows'):
        if name in s:
            config[name] = int(number(s[name], name, 1))
    if 'witnesses' in s:
        if type(s['witnesses']) is not bool:
            raise ValueError('witnesses must be a boolean')
        config['witnesses'] = s['witnesses']
    template = TEMPLATE.read_text() if template is None else template
    return render_worker(template, directory(release) + '/receiver-directory',
                         ['serve-publication', '--config', config_path],
                         configurations={config_path: config}, writable=[state],
                         readonly=list(dict.fromkeys(reads)), settings=s,
                         check=('serve-publication', '--check', '--config', config_path))


def report(service, role, health, ready_status):
    """Receiver health, separated into the report's axes (see wallet_pir_ops.worker_report)."""
    if health is None:
        return {'executable': {'binary_sha256': 'unobserved'}, 'publication': 'unobserved',
                'preparation': 'unobserved', 'readiness': 'unobserved'}
    identity = health.get('identity') or {}
    serving = health.get('serving')
    if role == 'serve-publication':
        worker = health.get('publication') or {}
        publication = {'assigned': worker.get('assigned'), 'loaded': worker.get('loaded'), 'serving': serving}
        preparation = {'preparing': worker.get('preparing'), 'refusal': worker.get('refusal')}
        verified = worker.get('loaded') is not None
    else:
        publication = {'assigned': 'unsupported', 'loaded': serving, 'serving': serving}
        preparation = 'unsupported'
        verified = serving is not None
    return {
        'executable': {'binary_sha256': identity.get('binary_sha256'), 'incarnation': identity.get('incarnation')},
        'publication': publication,
        'preparation': preparation,
        # The operator's record is the authority here; the worker activates it
        # itself once it verifies and its anchor is canonical.
        'readiness': {'verified': verified, 'warm': serving is not None, 'authorized': serving is not None,
                      'ready': serving is not None, 'epoch': health.get('epoch')},
    }

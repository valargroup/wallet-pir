"""External txid display worker role: `pir-query-server` under the display controller.

The release's `pir-query-server` serves the role from a generated
`transaction_details` section in `/etc/transparent-pir/display-worker.json`;
this module owns only that configuration and the role's sandbox. Publication,
activation and rollback stay in the `txid-display` driver (`txid_display_poc.py`).
"""
from wallet_pir_ops.paths import ROOT
from wallet_pir_ops.role_config import address, directory, number, settings_of, worker as render_worker

TEMPLATE = ROOT / 'tools/txid/deploy/transparent-display-worker.role.service'
CONTROL_SOCKET = '/run/transparent-display-worker/control.sock'
CONFIG = '/etc/transparent-pir/display-worker.json'
# The preflight verifies the initial publication without the controller's
# socket, active record or collect roots, so it reads its own subset.
CHECK = '/etc/transparent-pir/display-worker.check.json'
# Loopback only: the process's own /health and /ready. The check binds nothing.
MANAGEMENT = '127.0.0.1:18196'


def display_worker(settings, *, release, template=None):
    """`pir-query-server` for one display role, under the display controller.

    Shipped publications are read-only unless the inventory names collect
    roots, which the worker may then prune. The preflight check verifies the
    initial publication, when one is named, with the server's own loader.
    """
    s = settings_of(settings, ('listen', 'role', 'state_dir', 'publication_root'),
                    ('cache_bytes', 'build_slots', 'query_slots', 'retain_revisions', 'publication_dir',
                     'collect_roots', 'runtime_cache_dir', 'runtime_cache_max_bytes', 'loaded_only'))
    if s['role'] not in ('archive-owner', 'recent-replica', 'combined'):
        raise ValueError('role is archive-owner, recent-replica or combined')
    root, state = directory(s['publication_root']), directory(s['state_dir'])
    listen = address(s['listen'], 'listen')
    if listen == MANAGEMENT:
        raise ValueError('listen must differ from the management listener %s' % MANAGEMENT)
    body = {'listen': listen, 'role': s['role']}
    body.update({name: int(number(s[name], name, 1))
                 for name in ('cache_bytes', 'build_slots', 'query_slots', 'retain_revisions') if name in s})
    writes, reads = [state], []
    collect = [directory(p) for p in s.get('collect_roots', [])]
    if collect:
        writes += collect
    else:
        reads.append(root)
    if 'runtime_cache_dir' in s:
        cache = directory(s['runtime_cache_dir'])
        body['runtime_cache_dir'] = cache
        if 'runtime_cache_max_bytes' in s:
            body['runtime_cache_max_bytes'] = int(number(s['runtime_cache_max_bytes'], 'runtime_cache_max_bytes', 1))
        writes.append(cache)
    if s.get('loaded_only', False) is True:
        body['loaded_only'] = True
    configurations, check = {}, ()
    if 'publication_dir' in s:
        body['publication_dir'] = directory(s['publication_dir'])
        configurations[CHECK] = _document(dict(body))
        check = ('--config', CHECK, '--verify-only')
    body.update(control_socket=CONTROL_SOCKET, active_record=state + '/active.json')
    if collect:
        body['collect_root'] = collect
    configurations = {CONFIG: _document(body), **configurations}
    template = TEMPLATE.read_text() if template is None else template
    return render_worker(template, directory(release) + '/pir-query-server', ['--config', CONFIG],
                         configurations=configurations, writable=writes, readonly=[*configurations, *reads],
                         settings=s, check=check)


def _document(body):
    """The host configuration: one enabled `transaction_details` section, never `verify_only`."""
    return {'management_listen': MANAGEMENT, 'transaction_details': {'enabled': True, 'settings': body}}

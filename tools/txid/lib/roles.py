"""External txid display worker role: `transparent-txid-server` under the display controller.

The server is built from the Transparent shard-server package; this module owns
only the display role's arguments and sandbox. Publication, activation and
rollback stay in the `txid-display` driver (`txid_display_poc.py`).
"""
from wallet_pir_ops.paths import ROOT
from wallet_pir_ops.role_config import address, directory, settings_of, sizes, worker as render_worker

TEMPLATE = ROOT / 'tools/txid/deploy/transparent-display-worker.role.service'
CONTROL_SOCKET = '/run/transparent-display-worker/control.sock'


def display_worker(settings, *, release, template=None):
    """`transparent-txid-server` for one display role, under the display controller.

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
    arguments = ['--listen', address(s['listen'], 'listen'), '--role', s['role']]
    arguments += sizes(s, ('cache_bytes', 'build_slots', 'query_slots', 'retain_revisions'))
    writes, reads = [state], []
    collect = [directory(p) for p in s.get('collect_roots', [])]
    if collect:
        writes += collect
    else:
        reads.append(root)
    if 'runtime_cache_dir' in s:
        cache = directory(s['runtime_cache_dir'])
        arguments += ['--runtime-cache-dir', cache]
        arguments += sizes(s, ('runtime_cache_max_bytes',))
        writes.append(cache)
    if s.get('loaded_only', False) is True:
        arguments.append('--loaded-only')
    check = ()
    if 'publication_dir' in s:
        initial = directory(s['publication_dir'])
        arguments += ['--publication-dir', initial]
        check = (*arguments, '--verify-only')
    arguments += ['--control-socket', CONTROL_SOCKET, '--active-record', state + '/active.json']
    for path in collect:
        arguments += ['--collect-root', path]
    template = TEMPLATE.read_text() if template is None else template
    return render_worker(template, directory(release) + '/transparent-txid-server', arguments,
                         writable=writes, readonly=reads, settings=s, check=check)

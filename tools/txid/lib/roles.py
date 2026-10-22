"""External txid display worker role: `pir-query-server` synchronizing from its HTTPS publisher.

The release's `pir-query-server` serves the role from a generated
`transaction_details` section in `/etc/transparent-pir/display-worker.json`;
this module owns only that configuration and the role's sandbox. The worker
holds combined (sealed archive and recent) holdings, downloaded into its state
directory by its own HTTPS synchronization.
"""
from wallet_pir_ops.paths import ROOT
from wallet_pir_ops.role_config import address, directory, number, settings_of, worker as render_worker
from tools.transparent.lib.roles import publisher_source

TEMPLATE = ROOT / 'tools/txid/deploy/transparent-display-worker.role.service'
CONFIG = '/etc/transparent-pir/display-worker.json'
# Loopback only: the process's own /health and /ready.
MANAGEMENT = '127.0.0.1:18196'


def display_worker(settings, *, release, template=None):
    """`pir-query-server` for the combined display role, under its HTTPS publisher.

    The state directory is writable; the configuration and any CA file are
    read-only. There is no preflight loader check: `--verify-only` with a
    publisher synchronizes into the live state, so the engine installs and
    restarts the unit and readiness proves it.
    """
    s = settings_of(settings, ('listen', 'state_dir', 'publisher', 'publisher_network', 'publisher_genesis'),
                    ('publisher_ca', 'cache_bytes', 'build_slots', 'query_slots', 'retain_revisions',
                     'bootstrap_max_bytes'))
    state = directory(s['state_dir'])
    listen = address(s['listen'], 'listen')
    if listen == MANAGEMENT:
        raise ValueError('listen must differ from the management listener %s' % MANAGEMENT)
    body = {'listen': listen, 'role': 'combined', 'state_dir': state, **publisher_source(s)}
    body.update({name: int(number(s[name], name, 1))
                 for name in ('cache_bytes', 'build_slots', 'query_slots', 'retain_revisions', 'bootstrap_max_bytes')
                 if name in s})
    reads = [CONFIG]
    if 'publisher_ca' in s:
        body['publisher_ca'] = directory(s['publisher_ca'])
        reads.append(body['publisher_ca'])
    template = TEMPLATE.read_text() if template is None else template
    return render_worker(template, directory(release) + '/pir-query-server', ['--config', CONFIG],
                         configurations={CONFIG: _document(body)}, writable=[state], readonly=reads, settings=s)


def _document(body):
    """The host configuration: one enabled `transaction_details` section, never `verify_only`."""
    return {'management_listen': MANAGEMENT, 'transaction_details': {'enabled': True, 'settings': body}}

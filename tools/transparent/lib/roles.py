"""Transparent publisher configuration and its matching systemd sandbox."""
from pathlib import Path
import shlex

from wallet_pir_ops.paths import ROOT
from wallet_pir_ops.role_config import directory, render, sizes
from wallet_pir_ops.deploy import units


def publisher(config, *, config_path, state_dir, executable='/usr/local/bin/transparent-publish-controller',
              template=None):
    """Render a journal writer or follower with permissions derived from its config."""
    if not isinstance(config, dict):
        raise ValueError('publisher configuration must be an object')
    follow = config.get('follow_journal', False)
    if type(follow) is not bool:
        raise ValueError('follow_journal must be a boolean')
    data, publications, initial = (directory(config[name]) for name in
                                  ('data_dir', 'publication_root', 'initial_publication'))
    state, config_path = directory(str(state_dir)), directory(str(config_path))
    writes = [publications, initial, state]
    if config.get('snapshot_dir') is not None:
        writes.append(directory(config['snapshot_dir']))
    if follow:
        # A writable ancestor would undo the intended read-only journal boundary.
        if any(Path(data).is_relative_to(p) or Path(p).is_relative_to(data) for p in writes):
            raise ValueError('follower journal must be separate from writable role paths')
        reads = [data, config_path]
    else:
        writes.append(data)
        reads = [config_path]
    if any(Path(config_path).is_relative_to(p) for p in writes):
        raise ValueError('publisher configuration must be outside writable role directories')
    source = ROOT / 'tools/transparent/deploy/transparent-publish-controller.service'
    template = source.read_text() if template is None else template
    template_reads = []
    for section, key, value in units.entries(template):
        if section == 'Service' and key == 'ReadOnlyPaths':
            if value:
                template_reads.extend(shlex.split(value))
            else:
                template_reads.clear()
    reads.extend(p for p in template_reads if p != data and p not in writes and p != config_path)
    return render(template, executable,
                  ['--config', config_path], {config_path: config}, writes, reads)


HISTORY_TEMPLATE = ROOT / 'tools/transparent/deploy/transparent-history-worker.role.service'
HISTORY_SOCKET = '/run/transparent-history-worker/control.sock'
# The release payload's host binary (`pir-query-server`, built with its
# `transparent-history` and `transparent-detail` features, and the descriptor's
# `binary`); unit names keep their own history.
SERVER = 'pir-query-server'
# One generated, root-owned configuration per process, read-only to the
# service, and its loopback management listener (the host's own /health and
# /ready), distinct from every other process on the host.
CONFIGS = {
    'history-worker': ('/etc/transparent-pir/history-worker.json', '127.0.0.1:18191'),
    'https-recent': ('/etc/transparent-pir/history-https-recent.json', '127.0.0.1:18192'),
    'provider': ('/etc/transparent-pir/history-provider.json', '127.0.0.1:18193'),
}


def integers(s, names, low=1):
    """Each present setting as a JSON integer, refused unless an integer of at least `low`."""
    from wallet_pir_ops.role_config import number
    return {name: int(number(s[name], name, low)) for name in names if name in s}


def document(management, listen, section, body):
    """The host configuration: one enabled Transparent section, never `verify_only`."""
    if management == listen:
        raise ValueError('listen must differ from the management listener %s' % management)
    return {'management_listen': management, section: {'enabled': True, 'settings': body}}


def history_worker(settings, *, release, template=None):
    """The shard server under publication control.

    The staged shard set and assignment are read-only inputs; the active
    record and any runtime cache are state. The command is
    `pir-query-server --config` a generated `transparent_history` section, and
    the preflight check is the server's own loader, `--verify-only`, over that
    same file: it verifies the set (or the recorded active publication) and
    exits without listening.
    """
    from wallet_pir_ops.role_config import address, settings_of, worker as render_worker
    s = settings_of(settings, ('listen', 'shard_dir', 'state_dir'),
                    ('cache_bytes', 'build_slots', 'query_slots', 'retain_revisions', 'assignment', 'worker_id',
                     'prune_excess', 'runtime_cache_dir', 'runtime_cache_max_bytes'))
    shards, state = directory(s['shard_dir']), directory(s['state_dir'])
    listen = address(s['listen'], 'listen')
    body = {'listen': listen, 'shard_dir': shards}
    body.update(integers(s, ('cache_bytes', 'build_slots', 'query_slots', 'retain_revisions')))
    reads, writes = [shards], [state]
    if ('assignment' in s) != ('worker_id' in s):
        raise ValueError('assignment and worker_id go together')
    if 'assignment' in s:
        assignment = directory(s['assignment'])
        if not isinstance(s['worker_id'], str) or not s['worker_id'].replace('-', '').isalnum():
            raise ValueError('worker_id must be a plain name')
        body.update(assignment=assignment, worker_id=s['worker_id'])
        reads.append(assignment)
    if s.get('prune_excess', False) is True:
        body['prune_excess'] = True
    if 'runtime_cache_dir' in s:
        cache = directory(s['runtime_cache_dir'])
        body['runtime_cache_dir'] = cache
        body.update(integers(s, ('runtime_cache_max_bytes',)))
        writes.append(cache)
    body.update(control_socket=HISTORY_SOCKET, active_record=state + '/active.json')
    path, management = CONFIGS['history-worker']
    template = HISTORY_TEMPLATE.read_text() if template is None else template
    return render_worker(template, directory(release) + '/' + SERVER, ['--config', path],
                         configurations={path: document(management, listen, 'transparent_history', body)},
                         writable=writes, readonly=[path, *reads], settings=s,
                         check=('--config', path, '--verify-only'))


def history_provider(settings, *, release, template=None):
    """Autonomous HTTPS consumer; no fleet assignment, controller or local build."""
    return _provider(settings, release, template, 'provider')


def _provider(settings, release, template, config):
    import ipaddress
    import re
    from urllib.parse import urlsplit
    from wallet_pir_ops.role_config import address, settings_of, worker as render_worker, set_service
    s = settings_of(settings, ('listen', 'state_dir', 'publisher', 'publisher_network',
                              'publisher_genesis', 'archive_upstream'),
                    ('publisher_ca', 'archive_ca', 'cache_bytes', 'bootstrap_max_bytes',
                     'query_slots', 'query_waiters', 'body_bytes', 'query_deadline_secs',
                     'upload_deadline_secs'))
    listen = address(s['listen'], 'listen')
    host, port = listen.rsplit(':', 1)
    if not ipaddress.ip_address(host.strip('[]')).is_loopback or not 0 < int(port) < 65536:
        raise ValueError('provider listen must be loopback; expose wallet routes through operator TLS')
    state = directory(s['state_dir'])
    body = {'listen': listen, 'shard_dir': state}
    for name in ('publisher', 'archive_upstream'):
        value = s[name]
        if not isinstance(value, str):
            raise ValueError(name + ' must be an explicit trusted HTTPS origin')
        origin = urlsplit(value)
        if (origin.scheme != 'https' or not origin.hostname or origin.username is not None
                or origin.password is not None or origin.path not in ('', '/')
                or origin.query or origin.fragment or any(c.isspace() for c in value)):
            raise ValueError(name + ' must be an explicit trusted HTTPS origin')
        body[name] = value
    if s['publisher_network'] != 'main' or not isinstance(s['publisher_genesis'], str) or not re.fullmatch('[0-9a-f]{64}', s['publisher_genesis']):
        raise ValueError('publisher_network must be main and publisher_genesis a lowercase digest')
    body.update(publisher_network=s['publisher_network'], publisher_genesis=s['publisher_genesis'])
    path, management = CONFIGS[config]
    # ProtectSystem=strict already keeps the release read-only; naming its
    # directory would make every upgrade look like unit drift.
    reads = [path]
    if Path(state).is_relative_to(directory(release)) or Path(directory(release)).is_relative_to(state):
        raise ValueError('provider state must be separate from the release')
    for name in ('publisher_ca', 'archive_ca'):
        if name in s:
            ca = directory(s[name])
            reads.append(ca)
            body[name] = ca
    body.update(integers(s, ('cache_bytes', 'bootstrap_max_bytes', 'query_slots',
                             'body_bytes', 'query_deadline_secs', 'upload_deadline_secs')))
    body.update(integers(s, ('query_waiters',), 0))
    executable = directory(release) + '/' + SERVER
    source = ROOT / 'tools/transparent/deploy/transparent-history-provider.role.service'
    rendered = render_worker(source.read_text() if template is None else template,
                             executable, ['--config', path],
                             configurations={path: document(management, listen, 'transparent_history', body)},
                             writable=[state], readonly=reads, settings=s,
                             check=('--config', path, '--verify-only'))
    # Every supervised start/restart uses the same strict loader as install.
    from dataclasses import replace
    return replace(rendered, unit=set_service(rendered.unit, {
        'ExecStartPre': shlex.join([executable, *rendered.check])}))


HTTPS_RECENT_FLEET = ('member_id', 'fleet_host', 'fleet_state')


def https_recent(settings, *, release, template=None):
    """The provider process as a managed recent replica.

    Rendered as the standalone provider, with its own configuration file and
    management listener so both may share a host; the extra settings name this
    member in the fleet roster and where the fleet's active publication and
    membership records are read. Its own HTTPS synchronization is its only
    data update path; the fleet never stages, prepares or activates it.
    """
    if not isinstance(settings, dict):
        raise ValueError('settings must be an object')
    missing = [name for name in HTTPS_RECENT_FLEET if name not in settings]
    if missing:
        raise ValueError('https-recent settings missing %s' % missing)
    if not isinstance(settings['member_id'], str) or not settings['member_id'].replace('-', '').isalnum():
        raise ValueError('member_id must be a plain roster id')
    if not isinstance(settings['fleet_host'], str) or not settings['fleet_host']:
        raise ValueError('fleet_host must name an inventory host')
    directory(settings['fleet_state'])
    return _provider({k: v for k, v in settings.items() if k not in HTTPS_RECENT_FLEET},
                     release, template, 'https-recent')


def report(service, role, health, ready_status):
    """Transparent shard-server health, separated into the report's axes.

    History and txid display workers run the same server and report the same health.
    """
    if role in ('provider', 'https-recent'):
        axes = ('synchronization', 'verified_publication', 'runtime_readiness',
                'active_coverage', 'archive_reachability')
        result = {axis: (health or {}).get(axis, 'unobserved') for axis in axes}
        result['executable'] = {key: (health or {}).get(key, 'unobserved')
                                for key in ('binary_sha256', 'incarnation')}
        result['readiness'] = {'ready': None if ready_status is None else 200 <= ready_status < 300}
        return result
    if health is None:
        return {'executable': {'binary_sha256': 'unobserved'}, 'publication': 'unobserved',
                'preparation': 'unobserved', 'readiness': 'unobserved'}
    loaded = health.get('map_sha256')
    warm = health.get('warm_runtimes') is not None and health.get('warm_runtimes') == health.get('target_runtimes')
    return {
        'executable': {'binary_sha256': health.get('binary_sha256'), 'incarnation': health.get('incarnation')},
        # Assignment and the last preparation live on the root-only control
        # socket, which this report does not reach.
        'publication': {'assigned': 'unsupported', 'loaded': loaded},
        'preparation': 'unsupported',
        'readiness': {'verified': loaded is not None, 'warm': warm, 'authorized': 'unsupported',
                      'ready': None if ready_status is None else 200 <= ready_status < 300},
    }

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
    source = ROOT / 'ops/transparent/deploy/transparent-publish-controller.service'
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


HISTORY_TEMPLATE = ROOT / 'ops/transparent/deploy/transparent-history-worker.role.service'
HISTORY_SOCKET = '/run/transparent-history-worker/control.sock'


def history_worker(settings, *, release, template=None):
    """`transparent-shard-server` under publication control.

    The staged shard set and assignment are read-only inputs; the active
    record and any runtime cache are state. The preflight check is the
    server's own loader, `--verify-only`, over the same arguments: it verifies
    the set (or the recorded active publication) and exits without listening.
    """
    from wallet_pir_ops.role_config import address, settings_of, worker as render_worker
    s = settings_of(settings, ('listen', 'shard_dir', 'state_dir'),
                    ('cache_bytes', 'build_slots', 'query_slots', 'retain_revisions', 'assignment', 'worker_id',
                     'prune_excess', 'runtime_cache_dir', 'runtime_cache_max_bytes'))
    shards, state = directory(s['shard_dir']), directory(s['state_dir'])
    arguments = ['--listen', address(s['listen'], 'listen'), '--shard-dir', shards]
    arguments += sizes(s, ('cache_bytes', 'build_slots', 'query_slots', 'retain_revisions'))
    reads, writes = [shards], [state]
    if ('assignment' in s) != ('worker_id' in s):
        raise ValueError('assignment and worker_id go together')
    if 'assignment' in s:
        assignment = directory(s['assignment'])
        if not isinstance(s['worker_id'], str) or not s['worker_id'].replace('-', '').isalnum():
            raise ValueError('worker_id must be a plain name')
        arguments += ['--assignment', assignment, '--worker-id', s['worker_id']]
        reads.append(assignment)
    if s.get('prune_excess', False) is True:
        arguments.append('--prune-excess')
    if 'runtime_cache_dir' in s:
        cache = directory(s['runtime_cache_dir'])
        arguments += ['--runtime-cache-dir', cache]
        arguments += sizes(s, ('runtime_cache_max_bytes',))
        writes.append(cache)
    arguments += ['--control-socket', HISTORY_SOCKET, '--active-record', state + '/active.json']
    template = HISTORY_TEMPLATE.read_text() if template is None else template
    return render_worker(template, directory(release) + '/transparent-shard-server', arguments,
                         writable=writes, readonly=reads, settings=s, check=(*arguments, '--verify-only'))


def history_provider(settings, *, release, template=None):
    """Autonomous HTTPS consumer; no fleet assignment, controller or local build."""
    import ipaddress
    import re
    from urllib.parse import urlsplit
    from wallet_pir_ops.role_config import address, settings_of, worker as render_worker, set_service, number
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
    arguments = ['--listen', listen, '--shard-dir', state]
    for name in ('publisher', 'archive_upstream'):
        value = s[name]
        if not isinstance(value, str):
            raise ValueError(name + ' must be an explicit trusted HTTPS origin')
        origin = urlsplit(value)
        if (origin.scheme != 'https' or not origin.hostname or origin.username is not None
                or origin.password is not None or origin.path not in ('', '/')
                or origin.query or origin.fragment or any(c.isspace() for c in value)):
            raise ValueError(name + ' must be an explicit trusted HTTPS origin')
        arguments += ['--' + name.replace('_', '-'), value]
    if s['publisher_network'] != 'main' or not isinstance(s['publisher_genesis'], str) or not re.fullmatch('[0-9a-f]{64}', s['publisher_genesis']):
        raise ValueError('publisher_network must be main and publisher_genesis a lowercase digest')
    arguments += ['--publisher-network', s['publisher_network'], '--publisher-genesis', s['publisher_genesis']]
    reads = [directory(release)]
    for name in ('publisher_ca', 'archive_ca'):
        if name in s:
            path = directory(s[name])
            reads.append(path)
            arguments += ['--' + name.replace('_', '-'), path]
    arguments += sizes(s, ('cache_bytes', 'bootstrap_max_bytes', 'query_slots',
                           'body_bytes', 'query_deadline_secs', 'upload_deadline_secs'))
    if 'query_waiters' in s:
        arguments += ['--query-waiters', number(s['query_waiters'], 'query_waiters')]
    executable = directory(release) + '/transparent-shard-server'
    source = ROOT / 'ops/transparent/deploy/transparent-history-provider.role.service'
    rendered = render_worker(source.read_text() if template is None else template,
                             executable, arguments, writable=[state], readonly=reads,
                             settings=s, check=(*arguments, '--verify-only'))
    # Every supervised start/restart uses the same strict loader as install.
    from dataclasses import replace
    return replace(rendered, unit=set_service(rendered.unit, {
        'ExecStartPre': shlex.join([executable, *rendered.check])}))


def report(service, role, health, ready_status):
    """Transparent shard-server health, separated into the report's axes.

    History and txid display workers run the same server and report the same health.
    """
    if role == 'provider':
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

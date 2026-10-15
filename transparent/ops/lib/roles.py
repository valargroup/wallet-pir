"""Transparent publisher configuration and its matching systemd sandbox."""
from pathlib import Path
import shlex

from wallet_pir_ops.paths import ROOT
from wallet_pir_ops.role_config import directory, render
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
    source = ROOT / 'transparent/ops/deploy/transparent-publish-controller.service'
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


HISTORY_TEMPLATE = ROOT / 'transparent/ops/deploy/transparent-history-worker.role.service'
DISPLAY_TEMPLATE = ROOT / 'transparent/ops/deploy/transparent-display-worker.role.service'
HISTORY_SOCKET = '/run/transparent-history-worker/control.sock'
DISPLAY_SOCKET = '/run/transparent-display-worker/control.sock'


def _sizes(s, names):
    from wallet_pir_ops.role_config import number
    arguments = []
    for name in names:
        if name in s:
            arguments += ['--' + name.replace('_', '-'), number(s[name], name, 1)]
    return arguments


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
    arguments += _sizes(s, ('cache_bytes', 'build_slots', 'query_slots', 'retain_revisions'))
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
        arguments += _sizes(s, ('runtime_cache_max_bytes',))
        writes.append(cache)
    arguments += ['--control-socket', HISTORY_SOCKET, '--active-record', state + '/active.json']
    template = HISTORY_TEMPLATE.read_text() if template is None else template
    return render_worker(template, directory(release) + '/transparent-shard-server', arguments,
                         writable=writes, readonly=reads, settings=s, check=(*arguments, '--verify-only'))


def display_worker(settings, *, release, template=None):
    """`transparent-txid-server` for one display role, under the display controller.

    Shipped publications are read-only unless the inventory names collect
    roots, which the worker may then prune. The preflight check verifies the
    initial publication, when one is named, with the server's own loader.
    """
    from wallet_pir_ops.role_config import address, settings_of, worker as render_worker
    s = settings_of(settings, ('listen', 'role', 'state_dir', 'publication_root'),
                    ('cache_bytes', 'build_slots', 'query_slots', 'retain_revisions', 'publication_dir',
                     'collect_roots', 'runtime_cache_dir', 'runtime_cache_max_bytes', 'loaded_only'))
    if s['role'] not in ('archive-owner', 'recent-replica', 'combined'):
        raise ValueError('role is archive-owner, recent-replica or combined')
    root, state = directory(s['publication_root']), directory(s['state_dir'])
    arguments = ['--listen', address(s['listen'], 'listen'), '--role', s['role']]
    arguments += _sizes(s, ('cache_bytes', 'build_slots', 'query_slots', 'retain_revisions'))
    writes, reads = [state], []
    collect = [directory(p) for p in s.get('collect_roots', [])]
    if collect:
        writes += collect
    else:
        reads.append(root)
    if 'runtime_cache_dir' in s:
        cache = directory(s['runtime_cache_dir'])
        arguments += ['--runtime-cache-dir', cache]
        arguments += _sizes(s, ('runtime_cache_max_bytes',))
        writes.append(cache)
    if s.get('loaded_only', False) is True:
        arguments.append('--loaded-only')
    check = ()
    if 'publication_dir' in s:
        initial = directory(s['publication_dir'])
        arguments += ['--publication-dir', initial]
        check = (*arguments, '--verify-only')
    arguments += ['--control-socket', DISPLAY_SOCKET, '--active-record', state + '/active.json']
    for path in collect:
        arguments += ['--collect-root', path]
    template = DISPLAY_TEMPLATE.read_text() if template is None else template
    return render_worker(template, directory(release) + '/transparent-txid-server', arguments,
                         writable=writes, readonly=reads, settings=s, check=check)


def report(service, role, health, ready_status):
    """Transparent history or display health, separated into the report's axes."""
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

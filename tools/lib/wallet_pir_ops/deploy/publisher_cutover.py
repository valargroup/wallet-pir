"""Targeted cutover of standalone publication owners into one `pir-publisher` unit.

The generic engine deploys a binary into a unit that already exists. A first
cutover must instead stop and disable the reviewed old owners, install the
unified unit, configuration and operator inventory, and start the unified
process as the only writer of the same journals and publications. This module
does that as one journaled transaction on one pinned host:

    plan       read-only; binds every input and the observed before-state
    preflight  plan plus refusals (identity, drift, owners, space, inventory)
    deploy     requires the reviewed plan digest; re-plans under the lock
    status     journal and observed unit state
    reconcile  resolves a step whose outcome is unknown, then finishes or rolls back
    rollback   stops the unified unit first, then restores every prior byte and state

The request (see `docs/publisher.md`) contains no credentials and no arbitrary
commands: exact checks come from the reviewed inventory's
`services.publisher.exact_check`, and the only extra files it may change are the
whitelisted ancillary files below. Journals, publications and source data are
reused in place; nothing is rewound.
"""
import json
import os
from pathlib import Path
import re
import tempfile
import time

from .. import durable
from ..fleet import Fleet
from . import descriptors, units
from .engine import DeployError, file_sha256
from .remote import production_lock

SCHEMA = 'wallet-pir-publisher-cutover/1'
SHA256 = re.compile('^[0-9a-f]{64}$')
REVISION = re.compile('^[0-9a-f]{40}$')
MACHINE_ID = re.compile('^[0-9a-f]{32}$')
UNIT = 'pir-publisher.service'
UNIT_PATH = '/etc/systemd/system/' + UNIT
CONFIG_PATH = '/etc/wallet-pir/publisher.json'
INVENTORY_PATH = '/etc/wallet-pir/fleet.json'
ROOT = '/opt/pir-publisher'
TEMPLATE = Path(__file__).resolve().parents[3] / 'deploy/publisher/pir-publisher.service'
# Ancillary files a cutover may change, with the paths each kind may name.
# Anything else (scripts, other units, probe definitions) is out of scope.
ANCILLARY = {
    'quality-hosts': re.compile(r'^/etc/pir-quality/hosts\.json$'),
    'unit-drop-in': re.compile(r'^/etc/systemd/system/(?P<unit>[A-Za-z0-9@._-]+\.service)\.d/[A-Za-z0-9._-]+\.conf$'),
}
# The Transparent product sections and every field each typed configuration accepts.
PRODUCTS = {
    'transparent_history': (
        {'data_dir', 'publication_root', 'initial_publication', 'recent_from', 'recent_geometry',
         'archive_geometry', 'rpc_url', 'rpc_cookie', 'listen', 'source_sha'},
        {'fleet_command', 'fleet_config', 'shadow', 'directory_choice', 'range_profile', 'follow_journal',
         'snapshot_dir', 'ship_runtimes', 'distribute_prepared', 'http_only'}),
    'transaction_details': (
        {'root', 'journal', 'rpc_url', 'rpc_cookie', 'poll_ms', 'status_listen'},
        {'workers', 'trust_sealed', 'ship_runtimes', 'http_only', 'max_recent_records'}),
}
WRITABLE = {'transparent_history': ('publication_root', 'data_dir', 'snapshot_dir'),
            'transaction_details': ('root', 'journal')}
SERVICE_KEYS = {'user': 'User', 'group': 'Group', 'working_directory': 'WorkingDirectory',
                'memory_max': 'MemoryMax', 'cpu_quota': 'CPUQuota', 'protect_system': 'ProtectSystem'}
LIST_KEYS = {'read_write_paths': 'ReadWritePaths', 'read_only_paths': 'ReadOnlyPaths'}
UNIT_KEYS = {'after': 'After', 'wants': 'Wants'}
PLAIN = re.compile(r'^[A-Za-z0-9@%._:/+-]+$')
MIN_FREE_RATIO = 0.20
STEPS = ('stage', 'pause', 'stop-old', 'install', 'start', 'ready', 'exact', 'resume')
# A step from which on the host differs from its before-state.
MUTATING = ('pause', 'stop-old', 'install', 'start', 'ready', 'exact', 'resume')


class CutoverError(DeployError):
    pass


def require(condition, message):
    if not condition:
        raise CutoverError(message)


def sha256(data):
    import hashlib
    return hashlib.sha256(data).hexdigest()


def render_config(request):
    """The typed unified configuration: Transparent history and transaction details only.

    Every field must be one the Rust configuration accepts; `http_only` is
    explicitly false so the publisher keeps controlling the existing workers.
    Writable roots of the two products may not overlap.
    """
    settings = request.get('settings')
    require(isinstance(settings, dict) and set(settings) == set(PRODUCTS),
            'settings names exactly %s' % sorted(PRODUCTS))
    config = {'management_listen': request.get('management_listen', '127.0.0.1:18080')}
    roots = []
    for product, (required, optional) in PRODUCTS.items():
        values = settings[product]
        require(isinstance(values, dict), '%s settings must be an object' % product)
        missing, unknown = required - set(values), set(values) - required - optional
        require(not missing, '%s settings missing %s' % (product, sorted(missing)))
        require(not unknown, '%s settings has unknown fields %s' % (product, sorted(unknown)))
        require(values.get('http_only', False) is False, '%s must keep http_only false' % product)
        for flag in ('ship_runtimes', 'distribute_prepared', 'shadow', 'follow_journal', 'trust_sealed'):
            require(type(values.get(flag, False)) is bool, '%s.%s must be boolean' % (product, flag))
        values = {**values, 'http_only': False}
        for key in WRITABLE[product]:
            if product == 'transparent_history' and key == 'data_dir' and values.get('follow_journal'):
                continue
            path = values.get(key)
            if path is not None:
                require(isinstance(path, str) and descriptors.PLAIN_PATH.match(path),
                        '%s.%s must be a plain absolute path' % (product, key))
                roots.append((product, os.path.normpath(path)))
        config[product] = {'enabled': True, 'settings': values}
    for index, (owner, path) in enumerate(roots):
        for other, existing in roots[index + 1:]:
            overlap = path == existing or path.startswith(existing + '/') or existing.startswith(path + '/')
            require(owner == other or not overlap, 'writable state of %s and %s overlaps: %s, %s'
                    % (owner, other, path, existing))
    return config


def render_unit(request, binary):
    """The unit template with the reviewed service identity, sandbox and resource budget.

    The template's cooperative shutdown (`TimeoutStopSec=infinity`, `KillMode=mixed`)
    is kept: publication owners finish their boundary instead of being killed.
    """
    service = request.get('service', {})
    require(isinstance(service, dict), 'service must be an object')
    allowed = set(SERVICE_KEYS) | set(LIST_KEYS) | set(UNIT_KEYS)
    require(not set(service) - allowed, 'service has unknown fields %s' % sorted(set(service) - allowed))
    text = units.render(TEMPLATE.read_text(), {'BIN': binary})
    unit_lines, service_lines = [], []
    for key, name in UNIT_KEYS.items():
        for value in service.get(key, []):
            require(isinstance(value, str) and descriptors.UNIT.match(value), 'service.%s names units' % key)
            unit_lines.append('%s=%s' % (name, value))
    for key, name in SERVICE_KEYS.items():
        if key in service:
            require(isinstance(service[key], str) and PLAIN.match(service[key]), 'service.%s is a plain value' % key)
            service_lines.append('%s=%s' % (name, service[key]))
    for key, name in LIST_KEYS.items():
        values = service.get(key, [])
        require(isinstance(values, list) and all(isinstance(v, str) and descriptors.PLAIN_PATH.match(v)
                                                 for v in values), 'service.%s lists plain absolute paths' % key)
        if values:
            service_lines.append('%s=%s' % (name, ' '.join(values)))
    if unit_lines:
        text = text.replace('Wants=network-online.target\n', 'Wants=network-online.target\n' + ''.join(
            line + '\n' for line in unit_lines), 1)
    if service_lines:
        text = text.replace('[Install]\n', ''.join(line + '\n' for line in service_lines) + '\n[Install]\n', 1)
    config = units.effective([text])['Service']
    require(config.get('TimeoutStopSec') == ['infinity'] and config.get('KillMode') == ['mixed'],
            'the unified unit must keep cooperative shutdown')
    return text


def writable_roots(config):
    roots = []
    for product in PRODUCTS:
        values = config[product]['settings']
        for key in WRITABLE[product]:
            if values.get(key) and not (key == 'data_dir' and values.get('follow_journal')):
                roots.append(values[key])
    return roots


def load_request(path, expected):
    data = Path(path).read_bytes()
    require(SHA256.match(expected or '') and sha256(data) == expected, 'request digest differs from --request-sha256')
    request = json.loads(data)
    require(request.get('schema') == SCHEMA, 'request schema must be %s' % SCHEMA)
    known = {'schema', 'host', 'machine_id', 'source_sha', 'archive', 'binary_sha256', 'features',
             'management_listen', 'settings', 'service', 'predecessors', 'paused_units', 'inventory',
             'ancillary', 'ready_timeout', 'free_space_paths', 'memory_budget_bytes'}
    require(not set(request) - known, 'request has unknown fields %s' % sorted(set(request) - known))
    require(isinstance(request.get('host'), str) and request['host'], 'request names the inventory host')
    require(MACHINE_ID.match(request.get('machine_id') or ''), 'machine_id is the host /etc/machine-id')
    require(REVISION.match(request.get('source_sha') or ''), 'source_sha is a full source revision')
    require(SHA256.match(request.get('binary_sha256') or ''), 'binary_sha256 is required')
    require(request.get('features') == ['transparent'], 'the cutover binary is built with features ["transparent"]')
    archive = request.get('archive')
    require(isinstance(archive, dict) and set(archive) == {'path', 'sha256'} and SHA256.match(archive['sha256']),
            'archive is {"path", "sha256"} of the pir-publisher release bundle')
    predecessors = request.get('predecessors', [])
    require(isinstance(predecessors, list), 'predecessors is a list')
    seen = set()
    for entry in predecessors:
        require(isinstance(entry, dict) and set(entry) <= {'unit', 'enabled', 'active', 'owner_locks'}
                and descriptors.UNIT.match(entry.get('unit', '')) and entry['unit'] != UNIT,
                'each predecessor is {"unit", "enabled", "active", "owner_locks"}')
        require(entry['unit'] not in seen, 'predecessor %s is listed twice' % entry['unit'])
        require(type(entry.get('enabled')) is bool and type(entry.get('active')) is bool,
                '%s: enabled and active are the reviewed booleans' % entry['unit'])
        locks = entry.get('owner_locks', [])
        require(isinstance(locks, list) and all(isinstance(p, str) and descriptors.PLAIN_PATH.match(p) for p in locks),
                '%s: owner_locks are plain absolute paths' % entry['unit'])
        seen.add(entry['unit'])
    paused = request.get('paused_units', [])
    require(isinstance(paused, list) and all(isinstance(u, str) and descriptors.UNIT.match(u) for u in paused)
            and len(set(paused)) == len(paused) and not set(paused) & (seen | {UNIT}),
            'paused_units lists distinct units other than the owners')
    inventory = request.get('inventory')
    require(isinstance(inventory, dict) and set(inventory) == {'after', 'after_sha256'}
            and SHA256.match(inventory['after_sha256']), 'inventory is {"after", "after_sha256"}')
    for entry in request.get('ancillary', []):
        require(isinstance(entry, dict) and set(entry) == {'kind', 'path', 'after', 'after_sha256'}
                and entry['kind'] in ANCILLARY and SHA256.match(entry['after_sha256']),
                'ancillary entries are {"kind", "path", "after", "after_sha256"} of a whitelisted kind')
        match = ANCILLARY[entry['kind']].match(entry['path'])
        require(match, '%s may not change %s' % (entry['kind'], entry['path']))
        if entry['kind'] == 'unit-drop-in':
            require(match['unit'] in paused, 'a drop-in may change only a paused unit')
    paths = [p for e in request.get('ancillary', []) for p in [e['path']]]
    require(len(paths) == len(set(paths)), 'an ancillary path is listed twice')
    return request, data


class Cutover:
    """One host's cutover. `executor` is the deploy `Executor`; tests substitute a fake."""

    def __init__(self, inventory, executor, state_dir, out=print, sleep=time.sleep, clock=time.monotonic):
        self.inventory, self.ex, self.out = inventory, executor, out
        self.state = Path(state_dir) / 'publisher-cutover'
        self.sleep, self.clock = sleep, clock

    # ------------------------------------------------------------- inputs

    def inputs(self, request_path, request_sha256):
        """Verified local inputs: request, release binary, rendered files and their digests."""
        request, request_bytes = load_request(request_path, request_sha256)
        base = Path(request_path).resolve().parent
        resolve = lambda p: (base / p).resolve()  # noqa: E731
        archive = resolve(request['archive']['path'])
        require(file_sha256(archive) == request['archive']['sha256'], 'archive digest differs from the request')
        work = Path(tempfile.mkdtemp(prefix='publisher-cutover-', dir=self.scratch()))
        from .. import release
        release.extract(archive, work / 'bundle', request['source_sha'], 'pir-publisher')
        binary = work / 'bundle' / 'pir-publisher'
        require(file_sha256(binary) == request['binary_sha256'], 'release binary digest differs from the request')
        release_dir = '%s/releases/%s' % (ROOT, request['source_sha'])
        config = render_config(request)
        config_bytes = (json.dumps(config, sort_keys=True, indent=2) + '\n').encode()
        unit_bytes = render_unit(request, release_dir + '/pir-publisher').encode()
        inventory_bytes = resolve(request['inventory']['after']).read_bytes()
        require(sha256(inventory_bytes) == request['inventory']['after_sha256'], 'inventory digest differs')
        self.check_inventory(inventory_bytes, request, config)
        ancillary = []
        for entry in request.get('ancillary', []):
            data = resolve(entry['after']).read_bytes()
            require(sha256(data) == entry['after_sha256'], '%s digest differs' % entry['path'])
            if entry['kind'] == 'quality-hosts':
                json.loads(data)
            else:
                lines = units.entries(data.decode())
                require(lines and all(section == 'Service' and key == 'ExecStart' for section, key, _ in lines),
                        '%s may only replace the paused unit\'s ExecStart' % entry['path'])
            ancillary.append({'kind': entry['kind'], 'path': entry['path'], 'data': data})
        return {'request': request, 'request_sha256': sha256(request_bytes), 'binary': binary,
                'release_dir': release_dir, 'config': config_bytes, 'unit': unit_bytes,
                'inventory': inventory_bytes, 'ancillary': ancillary, 'work': work}

    def scratch(self):
        self.state.mkdir(mode=0o700, parents=True, exist_ok=True)
        return self.state

    def check_inventory(self, data, request, config):
        """The installed inventory is a complete fleet whose publisher matches this cutover."""
        with tempfile.NamedTemporaryFile('wb', suffix='.json', dir=self.scratch()) as handle:
            handle.write(data)
            handle.flush()
            fleet = Fleet.load(handle.name)
        problems = fleet.problems(complete=True)
        require(not problems, 'the after inventory is not a complete fleet: %s' % '; '.join(problems))
        publisher = fleet.inventory.services.get('publisher')
        require(publisher, 'the after inventory must name the unified publisher')
        require(sorted(publisher['managed_products']) == sorted(PRODUCTS),
                'publisher.managed_products must be exactly %s' % sorted(PRODUCTS))
        server = publisher['roles']['server'][0]
        require(server['host'] == request['host'], 'publisher.server runs on the cutover host')
        require(server.get('unit', UNIT) == UNIT, 'publisher.server unit must be %s' % UNIT)
        listen = {**publisher.get('vars', {}), **server.get('vars', {})}.get('management_listen')
        require(listen == config['management_listen'], 'inventory management_listen differs from the configuration')
        check = descriptors.exact_check(descriptors.load_descriptors(TEMPLATE.parent / 'services.toml')['publisher'],
                                        fleet.inventory)
        require(check and check['host'] == request['host'], 'publisher.exact_check runs on the cutover host')

    # --------------------------------------------------------- observation

    def enabled(self, host, unit):
        code, output = self.ex.run(host, ['systemctl', 'is-enabled', unit], 30)
        return output.strip() in ('enabled', 'enabled-runtime')

    def unit_state(self, host, unit):
        probe = self.ex.probe_unit(host, unit)
        files = [] if probe['fragment_path'] is None else [{'path': probe['fragment_path'],
                                                             'text': probe['fragment_text']}]
        files += probe['drop_ins']
        return {'unit': unit, 'loaded': probe['load_state'] == 'loaded', 'active': probe['active_state'] == 'active',
                'enabled': self.enabled(host, unit) if probe['load_state'] == 'loaded' else False,
                'files': files, 'files_sha256': durable.digest(files)}

    def file_state(self, host, path):
        text = self.ex.read(host, path)
        return {'path': path, 'text': text, 'sha256': None if text is None else sha256(text.encode())}

    def lock_free(self, host, path):
        code, _ = self.ex.run(host, ['flock', '--nonblock', '--conflict-exit-code', '75', path, 'true'], 30)
        return code == 0

    def free_space(self, host, path):
        code, output = self.ex.run(host, ['df', '--block-size=1', '--output=size,avail', path], 30)
        require(code == 0, 'cannot read free space of %s' % path)
        size, avail = (int(v) for v in output.strip().splitlines()[-1].split())
        return size, avail

    def observe(self, inputs):
        request, host = inputs['request'], inputs['request']['host']
        machine_id = (self.ex.read(host, '/etc/machine-id') or '').strip()
        owners = [self.unit_state(host, p['unit']) for p in request.get('predecessors', [])]
        paused = [self.unit_state(host, u) for u in request.get('paused_units', [])]
        files = {'unit': self.file_state(host, UNIT_PATH), 'config': self.file_state(host, CONFIG_PATH),
                 'inventory': self.file_state(host, INVENTORY_PATH)}
        for entry in inputs['ancillary']:
            files[entry['path']] = self.file_state(host, entry['path'])
        return {'machine_id': machine_id, 'publisher': self.unit_state(host, UNIT), 'owners': owners,
                'paused': paused, 'files': files}

    # ---------------------------------------------------------------- plan

    def plan(self, inputs, before):
        request = inputs['request']
        files = {'unit': {'path': UNIT_PATH, 'sha256': sha256(inputs['unit'])},
                 'config': {'path': CONFIG_PATH, 'sha256': sha256(inputs['config'])},
                 'inventory': {'path': INVENTORY_PATH, 'sha256': sha256(inputs['inventory'])}}
        for entry in inputs['ancillary']:
            files[entry['path']] = {'path': entry['path'], 'sha256': sha256(entry['data'])}
        return {
            'schema': SCHEMA, 'host': request['host'], 'machine_id': request['machine_id'],
            'source_sha': request['source_sha'], 'request_sha256': inputs['request_sha256'],
            'archive_sha256': request['archive']['sha256'], 'binary_sha256': request['binary_sha256'],
            'features': request['features'], 'release_dir': inputs['release_dir'],
            'products': sorted(PRODUCTS), 'files': files,
            'predecessors': request.get('predecessors', []), 'paused_units': request.get('paused_units', []),
            'before': {'machine_id': before['machine_id'],
                       'publisher': {k: before['publisher'][k] for k in ('loaded', 'active', 'enabled', 'files_sha256')},
                       'owners': [{k: o[k] for k in ('unit', 'loaded', 'active', 'enabled', 'files_sha256')}
                                  for o in before['owners']],
                       'paused': [{k: o[k] for k in ('unit', 'active', 'enabled', 'files_sha256')}
                                  for o in before['paused']],
                       'files': {k: v['sha256'] for k, v in before['files'].items()}},
            'probes': {'ready_url': 'http://%s/ready' % json.loads(inputs['config'])['management_listen'],
                       'exact_check': 'inventory services.publisher.exact_check'},
        }

    def problems(self, inputs, before):
        """Every reason not to start; none of these checks mutates the host."""
        request, host = inputs['request'], inputs['request']['host']
        problems = []
        if before['machine_id'] != request['machine_id']:
            problems.append('host machine id %r differs from the request' % before['machine_id'])
        for spec, state in zip(request.get('predecessors', []), before['owners']):
            if not state['loaded']:
                problems.append('%s is not installed' % spec['unit'])
            if state['enabled'] != spec['enabled'] or state['active'] != spec['active']:
                problems.append('%s is enabled=%s active=%s; the request reviewed enabled=%s active=%s'
                                % (spec['unit'], state['enabled'], state['active'], spec['enabled'], spec['active']))
        publisher = before['publisher']
        if publisher['active'] and any(o['active'] for o in before['owners']):
            problems.append('%s and a standalone owner are both active: two writers' % UNIT)
        if publisher['loaded'] and not publisher['files']:
            problems.append('%s is loaded without a unit file' % UNIT)
        if publisher['enabled'] and not publisher['active']:
            problems.append('%s is enabled but not running; resolve it before cutover' % UNIT)
        budget = request.get('memory_budget_bytes')
        for path in [*request.get('free_space_paths', ['/']), *writable_roots(json.loads(inputs['config']))]:
            try:
                size, avail = self.free_space(host, path)
            except Exception as error:  # noqa: BLE001 - reported, never mutating
                problems.append('free space of %s: %s' % (path, error))
                continue
            staged = inputs['binary'].stat().st_size if path == '/' else 0
            if size and (avail - staged) / size < MIN_FREE_RATIO:
                problems.append('%s would keep less than %d%% free' % (path, MIN_FREE_RATIO * 100))
        if budget is not None:
            memory = json.dumps(request.get('service', {}).get('memory_max'))
            if not isinstance(budget, int) or budget <= 0 or 'memory_max' not in request.get('service', {}):
                problems.append('memory_budget_bytes needs a positive integer and service.memory_max (%s)' % memory)
        return problems

    # ------------------------------------------------------------- journal

    def journal_path(self, identifier):
        return self.state / (identifier + '.json')

    def save(self, journal):
        durable.atomic_json(self.journal_path(journal['id']), journal, mode=0o600)

    def latest(self, identifier=None):
        pointer = self.state / 'latest.json'
        if identifier is None:
            if not pointer.exists():
                return None
            identifier = json.loads(pointer.read_text())['id']
        path = self.journal_path(identifier)
        require(path.exists(), 'no cutover transaction %s' % identifier)
        return json.loads(path.read_text())

    def mark(self, journal, step, status, **fields):
        journal['steps'][step] = status
        journal['events'].append({'unix': round(time.time(), 3), 'step': step, 'status': status, **fields})
        self.save(journal)

    # ----------------------------------------------------------- commands

    def inspect(self, request_path, request_sha256, preflight=False):
        inputs = self.inputs(request_path, request_sha256)
        self.ex.check_identity(inputs['request']['host'])
        before = self.observe(inputs)
        plan = self.plan(inputs, before)
        problems = self.problems(inputs, before) if preflight else []
        return plan, durable.digest(plan), problems

    def deploy(self, request_path, request_sha256, expect_plan_sha256):
        latest = self.latest()
        require(latest is None or latest['status'] in ('committed', 'rolled-back', 'refused'),
                'transaction %s is %s; reconcile it before another cutover' % (
                    latest and latest['id'], latest and latest['status']))
        inputs = self.inputs(request_path, request_sha256)
        host = inputs['request']['host']
        self.ex.check_identity(host)
        with production_lock(self.inventory, self.ex) as lock:
            before = self.observe(inputs)
            plan = self.plan(inputs, before)
            digest = durable.digest(plan)
            require(digest == expect_plan_sha256,
                    'the host or inputs changed since review: plan %s, reviewed %s' % (digest, expect_plan_sha256))
            problems = self.problems(inputs, before)
            require(not problems, 'preflight refuses: %s' % '; '.join(problems))
            identifier = 'publisher-cutover-%s-%s' % (time.strftime('%Y%m%dT%H%M%SZ', time.gmtime()),
                                                     inputs['request']['source_sha'][:12])
            journal = {'id': identifier, 'status': 'running', 'plan': plan, 'plan_sha256': digest,
                       'before': before, 'steps': {s: 'pending' for s in STEPS}, 'events': [],
                       'transaction_dir': '%s/transactions/%s' % (ROOT, identifier)}
            self.save(journal)
            durable.atomic_json(self.state / 'latest.json', {'id': identifier}, mode=0o600)
            return self.run(journal, inputs, lock)

    def run(self, journal, inputs, lock):
        """Run pending steps; a failed check rolls back, a transport failure leaves it for reconcile."""
        try:
            for step in STEPS:
                if journal['steps'][step] == 'done':
                    continue
                lock.verify()
                self.mark(journal, step, 'intent')
                getattr(self, 'step_' + step.replace('-', '_'))(journal, inputs)
                self.mark(journal, step, 'done')
        except CutoverError as error:
            journal['events'].append({'unix': round(time.time(), 3), 'error': str(error)})
            self.out('error: %s; rolling back' % error)
            self.restore(journal, lock)
            raise CutoverError('cutover rolled back: %s' % error)
        except Exception as error:
            journal['status'] = 'uncertain'
            journal['events'].append({'unix': round(time.time(), 3), 'error': repr(error)})
            self.save(journal)
            raise CutoverError('cutover outcome unknown (%s); run publisher-cutover-reconcile' % error)
        journal['status'] = 'committed'
        self.save(journal)
        return journal

    # ------------------------------------------------------------- steps

    def step_stage(self, journal, inputs):
        """Only new paths: the release directory and this transaction's private directory."""
        host, release = inputs['request']['host'], inputs['release_dir']
        staged = journal['transaction_dir'] + '/staged'
        self.ex.mkdir(host, release, 0o755)
        binary = release + '/pir-publisher'
        if self.ex.sha256(host, binary) != inputs['request']['binary_sha256']:
            self.ex.upload(host, str(inputs['binary']), binary, 0o755)
        require(self.ex.sha256(host, binary) == inputs['request']['binary_sha256'], 'staged binary digest differs')
        self.ex.mkdir(host, staged, 0o700)
        for name, data in self.payload(inputs):
            path = '%s/%s' % (staged, name)
            self.ex.write(host, path, data, 0o600)
            require(self.ex.sha256(host, path) == sha256(data), 'staged %s digest differs' % name)
        # Static check only: it opens no state, listener or credential, so no second writer starts.
        code, output = self.ex.run(host, [binary, '--config', staged + '/publisher.json', '--check'], 120)
        require(code == 0, 'staged configuration failed --check: %s' % output.strip()[-500:])

    def payload(self, inputs):
        yield 'publisher.json', inputs['config']
        yield UNIT, inputs['unit']
        yield 'fleet.json', inputs['inventory']
        for index, entry in enumerate(inputs['ancillary']):
            yield 'ancillary-%d' % index, entry['data']

    def step_pause(self, journal, inputs):
        host = inputs['request']['host']
        for unit in inputs['request'].get('paused_units', []):
            self.ex.systemctl(host, 'stop', unit)

    def step_stop_old(self, journal, inputs):
        """Stop and disable every reviewed owner, and wait until each one released its locks."""
        host = inputs['request']['host']
        for spec in inputs['request'].get('predecessors', []):
            self.ex.systemctl(host, 'stop', spec['unit'])
            if spec['enabled']:
                self.ex.systemctl(host, 'disable', spec['unit'])
        for spec in inputs['request'].get('predecessors', []):
            state = self.ex.probe_unit(host, spec['unit'])
            require(state['active_state'] != 'active' and not state['main_pid'], '%s did not stop' % spec['unit'])
            for path in spec.get('owner_locks', []):
                require(self.lock_free(host, path), '%s still holds %s' % (spec['unit'], path))

    def step_install(self, journal, inputs):
        host, files = inputs['request']['host'], journal['plan']['files']
        targets = {'publisher.json': CONFIG_PATH, UNIT: UNIT_PATH, 'fleet.json': INVENTORY_PATH}
        for index, entry in enumerate(inputs['ancillary']):
            targets['ancillary-%d' % index] = entry['path']
        for name, data in self.payload(inputs):
            path = targets[name]
            self.ex.mkdir(host, os.path.dirname(path), 0o755)
            self.ex.write(host, path, data, 0o644 if path.endswith(('.service', '.conf')) else 0o600)
        for record in files.values():
            require(self.ex.sha256(host, record['path']) == record['sha256'], '%s digest differs' % record['path'])

    def step_start(self, journal, inputs):
        host = inputs['request']['host']
        self.ex.systemctl(host, 'daemon-reload')
        self.ex.systemctl(host, 'enable', UNIT)
        self.ex.systemctl(host, 'restart', UNIT)

    def step_ready(self, journal, inputs):
        host = inputs['request']['host']
        url = journal['plan']['probes']['ready_url']
        deadline = self.clock() + int(inputs['request'].get('ready_timeout', 900))
        while True:
            status, body = self.ex.http_get(host, url)
            try:
                ready = status == 200 and json.loads(body).get('ready') is True
            except ValueError:
                ready = False
            if ready:
                return
            state = self.ex.probe_unit(host, UNIT)
            require(state['active_state'] == 'active', '%s is %s' % (UNIT, state['active_state']))
            require(self.clock() < deadline, '%s did not become ready' % UNIT)
            self.sleep(5)

    def step_exact(self, journal, inputs):
        check = json.loads(inputs['inventory'])['services']['publisher']['exact_check']
        code, output = self.ex.run(check['host'], check['argv'], int(check.get('timeout', 600)))
        require(code == 0, 'independent exact check failed: %s' % output.strip()[-500:])

    def step_resume(self, journal, inputs):
        host = inputs['request']['host']
        if inputs['ancillary']:
            self.ex.systemctl(host, 'daemon-reload')
        for state in journal['before']['paused']:
            if state['active']:
                self.ex.systemctl(host, 'restart', state['unit'])

    # ------------------------------------------------------------ reverse

    def restore(self, journal, lock):
        """Stop and join the unified unit, then restore every prior file and unit state."""
        host, before = journal['plan']['host'], journal['before']
        if all(journal['steps'][step] == 'pending' for step in MUTATING):
            # Only new release and transaction paths were written; every live unit and file is as before.
            journal['status'] = 'refused'
            self.save(journal)
            return journal
        journal['status'] = 'rolling-back'
        self.save(journal)
        lock.verify()
        start = lambda unit: self.ex.probe_unit(host, unit)['active_state'] == 'active' or self.ex.systemctl(  # noqa: E731
            host, 'restart', unit)
        for state in before['paused']:
            self.ex.systemctl(host, 'stop', state['unit'])
        if self.ex.probe_unit(host, UNIT)['load_state'] == 'loaded':
            self.ex.systemctl(host, 'stop', UNIT)
            if not before['publisher']['enabled']:
                self.ex.systemctl(host, 'disable', UNIT)
        state = self.ex.probe_unit(host, UNIT)
        require(state['active_state'] != 'active' and not state['main_pid'], '%s did not stop' % UNIT)
        for record in before['files'].values():
            if record['sha256'] is None:
                self.ex.remove(host, record['path'])
            elif self.ex.sha256(host, record['path']) != record['sha256']:
                self.ex.write(host, record['path'], record['text'].encode(), 0o644 if record['path'].endswith(
                    ('.service', '.conf')) else 0o600)
        self.ex.systemctl(host, 'daemon-reload')
        if before['publisher']['enabled']:
            self.ex.systemctl(host, 'enable', UNIT)
        if before['publisher']['active']:
            self.ex.systemctl(host, 'restart', UNIT)
        for owner in before['owners']:
            if owner['enabled']:
                self.ex.systemctl(host, 'enable', owner['unit'])
            if owner['active']:
                start(owner['unit'])
        for owner in before['owners']:
            state = self.ex.probe_unit(host, owner['unit'])
            require((state['active_state'] == 'active') == owner['active'],
                    '%s did not return to its prior state' % owner['unit'])
        for state in before['paused']:
            if state['active']:
                self.ex.systemctl(host, 'restart', state['unit'])
        journal['status'] = 'rolled-back'
        journal['events'].append({'unix': round(time.time(), 3), 'status': 'rolled-back'})
        self.save(journal)
        return journal

    def rollback(self, identifier=None):
        journal = self.latest(identifier)
        require(journal is not None, 'no cutover transaction')
        require(journal['status'] != 'rolled-back', 'transaction %s is already rolled back' % journal['id'])
        with production_lock(self.inventory, self.ex) as lock:
            return self.restore(journal, lock)

    def reconcile(self, request_path, request_sha256, identifier=None):
        """Resolve each step whose outcome is unknown by observation, then finish or roll back."""
        journal = self.latest(identifier)
        require(journal is not None, 'no cutover transaction')
        if journal['status'] in ('committed', 'rolled-back'):
            return journal
        inputs = self.inputs(request_path, request_sha256)
        require(inputs['request_sha256'] == journal['plan']['request_sha256'],
                'reconcile needs the transaction\'s own request')
        host = journal['plan']['host']
        with production_lock(self.inventory, self.ex) as lock:
            if journal['status'] == 'rolling-back':
                return self.restore(journal, lock)
            for step in STEPS:
                if journal['steps'][step] != 'intent':
                    continue
                # Steps are idempotent: re-running one whose effect landed is a no-op,
                # so an unknown step is retried rather than guessed complete.
                self.mark(journal, step, 'pending', reconciled=True)
            publisher = self.ex.probe_unit(host, UNIT)
            owners_active = [s['unit'] for s in journal['plan']['predecessors']
                             if self.ex.probe_unit(host, s['unit'])['active_state'] == 'active']
            require(not (publisher['active_state'] == 'active' and owners_active),
                    'two writers are active (%s and %s); stop one manually' % (UNIT, owners_active))
            journal['status'] = 'running'
            self.save(journal)
            return self.run(journal, inputs, lock)

    def status(self, identifier=None):
        journal = self.latest(identifier)
        if journal is None:
            return {'transaction': None}
        host = journal['plan']['host']
        units_now = {u: self.ex.probe_unit(host, u)['active_state']
                     for u in [UNIT, *[p['unit'] for p in journal['plan']['predecessors']]]}
        return {'transaction': journal['id'], 'status': journal['status'], 'steps': journal['steps'],
                'plan_sha256': journal['plan_sha256'], 'units': units_now}


def register(commands):
    for name in ('plan', 'preflight', 'deploy', 'reconcile', 'status', 'rollback'):
        command = commands.add_parser('publisher-cutover-' + name)
        if name not in ('status', 'rollback'):
            command.add_argument('--request', required=True)
            command.add_argument('--request-sha256', required=True)
        if name == 'deploy':
            command.add_argument('--expect-plan-sha256', required=True)
        if name in ('reconcile', 'status', 'rollback'):
            command.add_argument('--transaction')


def dispatch(args, inventory, executor, out, **options):
    cutover = Cutover(inventory, executor, args.state_dir, out=out, **options)
    command = args.command[len('publisher-cutover-'):]
    if command in ('plan', 'preflight'):
        plan, digest, problems = cutover.inspect(args.request, args.request_sha256, command == 'preflight')
        out(json.dumps({'operation': args.command, 'plan': plan, 'plan_sha256': digest, 'problems': problems,
                        'deploy': 'publisher-cutover-deploy --request %s --request-sha256 %s --expect-plan-sha256 %s'
                                  % (args.request, args.request_sha256, digest)}, sort_keys=True))
        return 1 if problems else 0
    if command == 'status':
        out(json.dumps(cutover.status(args.transaction), sort_keys=True))
        return 0
    if command == 'deploy':
        journal = cutover.deploy(args.request, args.request_sha256, args.expect_plan_sha256)
    elif command == 'reconcile':
        journal = cutover.reconcile(args.request, args.request_sha256, args.transaction)
    else:
        journal = cutover.rollback(args.transaction)
    out(json.dumps({'operation': args.command, 'transaction': journal['id'], 'status': journal['status'],
                    'rollback': 'publisher-cutover-rollback --transaction %s' % journal['id']}, sort_keys=True))
    return 0

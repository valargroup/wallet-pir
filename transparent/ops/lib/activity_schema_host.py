"""Concrete host transitions for the reviewed v10 -> v11 schema recipe.

The coordinator recipe invokes these programs on each pinned host. They never
open public routing or start the quality supervisor. Capture stops product
writers before copying; recovery restores the same binaries, units, controller
state and source namespaces and leaves routing/load/scaler deferred. The recipe
must verify canonical private recovery before its separate reopen phase.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import time
import urllib.error
import urllib.request

from wallet_pir_ops import inherited_lock, transparent_map

SPEC = importlib.util.spec_from_file_location('schema_host_baseline', Path(__file__).with_name('activity_schema_baseline.py'))
B = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(B)

ROOT = Path('/opt/transparent-publisher')
LOCK = '/run/lock/wallet-pir-production.lock'
PUBLIC_METADATA = ('https://enhance-pir.valargroup.dev/v1/shards',
                   'https://transparent-pir.valargroup.dev/v1/shards')
QUALITY = 'transparent-quality-rollout.service'
AUTHORITY = ('transparent-publish-controller.service', 'transparent-replica-reconciler.service',
             'transparent-control-sessions.service')
LOAD = 'transparent-5qps-continuous.service'
SCALER = 'transparent-fleet-scaler.service'
FILTER = 'transparent-filter-server.service'
WORKER = 'transparent-shard-server.service'
CACHE = Path('/srv/transparent-pir/v11/runtime-cache')
UNITS = {'coordinator': (*AUTHORITY, FILTER, LOAD, SCALER), 'worker': (WORKER,), 'router': ('caddy.service',)}
WRITERS = {'coordinator': (SCALER, LOAD, *AUTHORITY, FILTER), 'worker': (WORKER,), 'router': ()}
START = {'coordinator': (FILTER, *AUTHORITY), 'worker': (WORKER,), 'router': ()}
BINARIES = {'coordinator': ('transparent-publish-controller', 'transparent-filter-server', 'shard-assign', 'shard-control'),
            'worker': ('transparent-shard-server', 'shard-control'), 'router': ()}
HEX = re.compile(r'^[0-9a-f]{64}$')
TXN = re.compile(r'^transparent-schema-[A-Za-z0-9-]+$')


def require(ok, message):
    if not ok:
        raise ValueError(message)


def checksum(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate host-plan field')
        result[key] = value
    return result


def load(path):
    with Path(path).open('rb') as stream:
        data = stream.read(256 * 1024 + 1)
    require(len(data) <= 256 * 1024, 'host plan exceeds bound')
    return json.loads(data, object_pairs_hook=unique)


def unit_paths(units):
    return {'/etc/systemd/system/'+u for u in units}


def required_files(role):
    result = unit_paths(UNITS[role]) | {'/usr/local/bin/'+b for b in BINARIES[role]}
    if role == 'coordinator':
        result |= {str(ROOT/p) for p in ('controller.json', 'fleet.json', 'roster.json', 'state',
                                        'credentials', 'scaler')}
        result |= {'/etc/caddy/Caddyfile', '/etc/pir-quality/qualified-workers.json'}
    elif role == 'worker':
        result.add(str(ROOT/'active.json'))
    else:
        result = {'/etc/caddy/Caddyfile'}
    return result


def deferred(role, path):
    """Recovery may not reopen routing, resume load or re-enable scaling."""
    return (path == '/etc/caddy/Caddyfile' or path.startswith('/etc/caddy/Caddyfile.') or
            (role == 'coordinator' and (path.startswith('/opt/transparent-5qps-') or
             path == '/etc/pir-quality/qualified-workers.json' or path == str(ROOT/'scaler') or path.startswith(str(ROOT/'scaler')+'/'))))


def install_targets(role):
    result = unit_paths(UNITS[role]) | {'/usr/local/bin/'+b for b in BINARIES[role]}
    # Caddy is installed only by the recipe's withdrawal/reopen program; host
    # preparation cannot accidentally expose candidate metadata.
    if role == 'coordinator':
        result |= {str(ROOT/'v11'/p) for p in ('controller.json', 'fleet.json', 'roster.json')}
    return result


def validate(plan):
    require(isinstance(plan, dict) and set(plan) == {'version', 'role', 'machine_id', 'source_sha',
            'transaction', 'baseline_root', 'baseline', 'installs', 'worker'}, 'invalid host-plan fields')
    require(type(plan['version']) is int and plan['version'] == 1, 'unsupported host plan')
    role = plan['role']
    require(role in UNITS, 'unsupported host role')
    require(isinstance(plan['machine_id'], str) and re.fullmatch('[0-9a-f]{32}', plan['machine_id']), 'invalid machine identity')
    require(isinstance(plan['source_sha'], str) and re.fullmatch('[0-9a-f]{40}', plan['source_sha']), 'invalid source identity')
    require(isinstance(plan['transaction'], str) and TXN.fullmatch(plan['transaction']), 'invalid transaction')
    root = '/opt/transparent-publisher/schema-rollback/'+plan['transaction']
    require(plan['baseline_root'] == root, 'baseline root is not transaction-bound')
    B.validate_plan(plan['baseline'], root)
    files = plan['baseline']['files']
    declared = {item['path'] for item in files if item['required']}
    require(required_files(role) <= declared, 'baseline omits product rollback state')
    # Installed unit drop-ins affect ExecStart/sandbox/resource limits and must
    # be represented even when the directory is absent on the predecessor.
    names = {item['path'] for item in files}
    require({p+'.d' for p in unit_paths(UNITS[role])} <= names, 'baseline omits unit drop-ins')
    if role == 'router':
        allowed = {'/etc/systemd/system/caddy.service', '/usr/lib/systemd/system/caddy.service',
                   '/lib/systemd/system/caddy.service'}
        require('/etc/systemd/system/caddy.service' in names and len(allowed & declared) == 1,
                'router baseline must bind its actual Caddy fragment and absent /etc override')
        require({p+'.d' for p in allowed & declared} <= names, 'baseline omits vendor unit drop-ins')
    if role == 'worker':
        require(str(ROOT/'active.invalid.json') in names, 'baseline omits invalidation state')
        require({'/usr/local/lib/transparent-pir/storage-policy.py', '/usr/local/lib/transparent-pir/headless-console.py'} <= names,
                'baseline omits installed worker prestart helpers')
    if role == 'coordinator':
        # Load executable, supervisor, permit and fixture must be captured as a
        # complete tree, rather than guessing one historical supervisor name.
        require(any(p.startswith('/opt/transparent-5qps-') and p.count('/') == 2 for p in declared),
                'baseline omits continuous-load tree')
    require(isinstance(plan['installs'], list) and len(plan['installs']) <= 32, 'invalid installs')
    targets = set()
    for item in plan['installs']:
        require(isinstance(item, dict) and set(item) == {'source', 'target', 'sha256', 'mode'}, 'invalid install')
        require(item['target'] in install_targets(role) and item['target'] not in targets, 'unsupported/duplicate install target')
        targets.add(item['target'])
        source = B.safe_path(item['source'])
        require(source != Path(item['target']) and isinstance(item['sha256'], str) and HEX.fullmatch(item['sha256']), 'invalid install source')
        require(type(item['mode']) is int and item['mode'] in (0o600, 0o644, 0o755), 'invalid install mode')
        expected_mode = 0o755 if item['target'].startswith('/usr/local/bin/') else 0o644 if item['target'].endswith('.service') else 0o600
        require(item['mode'] == expected_mode, 'incorrect product file mode')
    require(role == 'router' or unit_paths(START[role]) <= targets, 'candidate omits authority/worker units')
    require({'/usr/local/bin/'+b for b in BINARIES[role]} <= targets, 'candidate omits product binaries')
    if role == 'coordinator':
        require({str(ROOT/'v11'/p) for p in ('controller.json', 'fleet.json', 'roster.json')} <= targets,
                'candidate omits separate controller configuration')
    if role == 'worker':
        w = plan['worker']
        require(isinstance(w, dict) and set(w) == {'id', 'directory', 'assignment', 'assignment_sha256', 'map_sha256', 'map_file_sha256', 'binary_sha256'}, 'invalid worker target')
        require(isinstance(w['id'], str) and re.fullmatch('[a-zA-Z0-9-]{1,64}', w['id']), 'invalid worker id')
        for key in ('map_sha256', 'map_file_sha256', 'binary_sha256', 'assignment_sha256'):
            require(isinstance(w[key], str) and HEX.fullmatch(w[key]), 'invalid worker identity')
        directory = B.safe_path(w['directory'])
        require(directory.parent == Path('/srv/transparent-pir/v11/publications') and directory.name == w['map_file_sha256'],
                'worker publication is outside v11 namespace')
        require(w['assignment'] == str(directory/'assignment.json'), 'worker assignment is outside candidate')
        binary = next(i for i in plan['installs'] if i['target'] == '/usr/local/bin/transparent-shard-server')
        require(binary['sha256'] == w['binary_sha256'], 'worker binary identity disagrees')
    else:
        require(plan['worker'] is None, 'non-worker has worker target')
    return plan


class Commands:
    def run(self, argv, *, data=None, timeout=30):
        return subprocess.run(argv, input=data, capture_output=True, check=True, timeout=timeout,
                              **inherited_lock.options()).stdout

    def state(self, unit):
        data = self.run(['systemctl', 'show', unit, '--property=ActiveState,SubState,MainPID,Result,NRestarts,FragmentPath,DropInPaths,ControlGroup']).decode()
        return dict(line.split('=', 1) for line in data.splitlines() if '=' in line)

    def empty_cgroup(self, state):
        group = state.get('ControlGroup', '')
        if not group:
            return True
        require(group.startswith('/') and '..' not in Path(group).parts, 'unsafe product cgroup')
        root = Path('/sys/fs/cgroup')/group.lstrip('/')
        if not root.exists():
            return True
        paths = list(root.rglob('cgroup.procs'))
        require(len(paths) <= 1024, 'product cgroup exceeds quiescence bound')
        return all(not p.read_text().strip() for p in paths)

    def unit(self, action, *units):
        if units:
            self.run(['systemctl', action, *units], timeout=60)

    def metadata_status(self, url):
        request = urllib.request.Request(url, headers={'Cache-Control': 'no-cache'})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status
        except urllib.error.HTTPError as error:
            return error.code

    def control(self):
        reply = json.loads(self.run(['/usr/local/bin/shard-control', '/run/transparent-pir/control.sock'],
                                   data=b'{"operation":"status"}', timeout=5))
        require(isinstance(reply, dict) and reply.get('ok') is True and isinstance(reply.get('result'), dict),
                'worker control did not return a successful status')
        return reply['result']


class Host:
    def __init__(self, plan, commands=None):
        self.plan = validate(plan)
        self.role = plan['role']
        self.root = Path(plan['baseline_root'])
        self.commands = commands or Commands()

    def identity(self, mutation=False):
        require(os.geteuid() == 0 and Path('/etc/machine-id').read_text().strip() == self.plan['machine_id'],
                'host identity does not match the reviewed root plan')
        if mutation:
            inherited_lock.descriptors(required=True, path=LOCK)

    def withdrawn(self):
        require(all(self.commands.metadata_status(url) == 503 for url in PUBLIC_METADATA),
                'both public metadata origins must be withdrawn')

    def quiet(self, units):
        for unit in units:
            s = self.commands.state(unit)
            require(s.get('MainPID') == '0' and s.get('ActiveState') in ('inactive', 'failed'),
                    'product writer did not quiesce: '+unit)
            require(self.commands.empty_cgroup(s), 'product writer has surviving descendants: '+unit)

    def preflight(self):
        # Every source is checked before stopping anything or copying state.
        for item in self.plan['installs']:
            source = Path(item['source'])
            require(source.is_file() and not source.is_symlink() and source.stat().st_size <= 256*1024*1024 and checksum(source) == item['sha256'],
                    'candidate source checksum changed')
        for item in self.plan['baseline']['files']:
            path = Path(item['path'])
            require(not item['required'] or path.exists() or path.is_symlink(), 'required predecessor file is absent')
        for item in self.plan['baseline']['retained']:
            B.retained_identity(item)
        self.effective_units()
        if self.role == 'coordinator':
            self.quiet((QUALITY,))
            self.predecessor_coordinator()
        self.validate_units()
        if self.role == 'worker':
            self.predecessor_worker()
            w = self.plan['worker']
            require(checksum(Path(w['directory'])/'shards.json') == w['map_file_sha256'] and
                    transparent_map.served_sha256(load(Path(w['directory'])/'shards.json')) == w['map_sha256'] and
                    load(w['assignment'])['set']['map_sha256'] == w['map_sha256'] and
                    checksum(w['assignment']) == w['assignment_sha256'], 'staged worker map/assignment disagrees')
            candidate = next(i for i in self.plan['installs'] if i['target'] == '/etc/systemd/system/'+WORKER)
            start = next(shlex.split(line[10:]) for line in Path(candidate['source']).read_text().splitlines() if line.startswith('ExecStart='))
            binary = next(i['source'] for i in self.plan['installs'] if i['target'] == '/usr/local/bin/transparent-shard-server')
            self.commands.run([binary, *start[1:], '--verify-only'], timeout=300)

    def effective_units(self):
        required = {i['path'] for i in self.plan['baseline']['files'] if i['required']}
        names = {i['path'] for i in self.plan['baseline']['files']}
        for unit in UNITS[self.role]:
            state = self.commands.state(unit)
            fragment = state.get('FragmentPath')
            allowed = {'/etc/systemd/system/'+unit}
            if self.role == 'router':
                allowed |= {'/usr/lib/systemd/system/'+unit, '/lib/systemd/system/'+unit}
            require(fragment in allowed and fragment in required, 'unexpected or uncaptured product unit fragment')
            dirs = {'/etc/systemd/system/'+unit+'.d', fragment+'.d'}
            require(dirs <= names, 'baseline omits effective unit drop-in directories')
            drops = shlex.split(state.get('DropInPaths', ''))
            require(all(str(Path(p).parent) in dirs for p in drops), 'uncaptured external unit drop-in')

    def predecessor_coordinator(self):
        old = load(ROOT/'controller.json')
        names = {i['path'] for i in self.plan['baseline']['files']}
        required = {i['path'] for i in self.plan['baseline']['files'] if i['required']}
        publication = B.safe_path(old['publication_root'])
        require(publication != Path('/srv/transparent-activity/full-v11/publications'), 'predecessor already uses candidate namespace')
        require(str(publication/'active.json') in required and
                {str(publication/n) for n in ('withdrawn.json', 'activation.json')} <= names,
                'baseline omits controller activation/withdrawal records')
        retained = [Path(i['path']).resolve() for i in self.plan['baseline']['retained']]
        for path in (B.safe_path(old['data_dir']), B.safe_path(old['initial_publication'])):
            physical = path.resolve()
            require(any(physical == r or r in physical.parents for r in retained),
                    'baseline omits controller journal/initial publication')

    def predecessor_worker(self):
        unit = Path('/etc/systemd/system/'+WORKER).read_text()
        starts = [shlex.split(line[10:]) for line in unit.splitlines() if line.startswith('ExecStart=')]
        require(len(starts) == 1, 'predecessor worker has ambiguous ExecStart')
        args = starts[0]
        def one(flag):
            values = [args[n+1] for n, a in enumerate(args[:-1]) if a == flag]
            values += [a[len(flag)+1:] for a in args if a.startswith(flag+'=')]
            require(len(values) == 1, 'predecessor worker option is missing/ambiguous')
            return B.safe_path(values[0])
        require(one('--active-record') == ROOT/'active.json', 'predecessor is not the v10 control namespace')
        retained = [Path(i['path']).resolve() for i in self.plan['baseline']['retained']]
        copied = {Path(i['path']).resolve() for i in self.plan['baseline']['files'] if i['required']}
        def retained_tree(path):
            physical = path.resolve()
            return any(physical == r or r in physical.parents for r in retained)
        for path in (one('--shard-dir'), one('--runtime-cache-dir')):
            require(retained_tree(path), 'baseline omits predecessor publication/cache namespace')
        assignment = one('--assignment')
        require(retained_tree(assignment) or assignment.resolve() in copied, 'baseline omits predecessor assignment')
        active = load(ROOT/'active.json')
        require(isinstance(active, dict) and set(active) == {'directory', 'assignment', 'map_sha256'} and
                isinstance(active['map_sha256'], str) and HEX.fullmatch(active['map_sha256']), 'malformed predecessor active record')
        require(retained_tree(B.safe_path(active['directory'])), 'baseline omits active publication namespace')
        active_assignment = B.safe_path(active['assignment'])
        require(retained_tree(active_assignment) or active_assignment.resolve() in copied, 'baseline omits active assignment')

    def validate_units(self):
        candidates = {i['target']: Path(i['source']) for i in self.plan['installs']}
        if self.role == 'worker':
            unit = candidates['/etc/systemd/system/'+WORKER].read_text()
            starts = [shlex.split(line[len('ExecStart='):]) for line in unit.splitlines() if line.startswith('ExecStart=')]
            require(len(starts) == 1 and starts[0][0] == '/usr/local/bin/transparent-shard-server', 'worker requires one fixed executable')
            args = starts[0][1:]
            def value(flag):
                values = []
                for index, arg in enumerate(args):
                    if arg == flag:
                        require(index+1 < len(args) and not args[index+1].startswith('--'), 'incomplete worker option')
                        values.append(args[index+1])
                    elif arg.startswith(flag+'='):
                        values.append(arg[len(flag)+1:])
                require(len(values) == 1, 'missing/duplicate worker option: '+flag)
                return values[0]
            w = self.plan['worker']
            expected = {'--shard-dir': w['directory'], '--assignment': w['assignment'], '--worker-id': w['id'],
                        '--active-record': '/opt/transparent-publisher/v11/active.json',
                        '--runtime-cache-dir': '/srv/transparent-pir/v11/runtime-cache',
                        '--control-socket': '/run/transparent-pir/control.sock'}
            require(all(value(k) == v for k, v in expected.items()), 'worker unit disagrees with v11 namespace/assignment')
            require('RuntimeDirectory=transparent-pir' in unit.splitlines(), 'worker control runtime directory omitted')
        elif self.role == 'coordinator':
            unit = candidates['/etc/systemd/system/'+AUTHORITY[0]].read_text()
            require('ExecStart=/usr/local/bin/transparent-publish-controller --config '+json.dumps(str(ROOT/'v11/controller.json')) in unit.splitlines(),
                    'publisher unit has wrong controller namespace')
            controller = load(candidates[str(ROOT/'v11/controller.json')])
            require(controller.get('data_dir') == '/srv/transparent-activity/full-v3/journal' and
                    controller.get('publication_root') == '/srv/transparent-activity/full-v11/publications' and
                    controller.get('initial_publication') == '/srv/transparent-activity/full-v11/publications/initial' and
                    controller.get('fleet_config') == str(ROOT/'v11/fleet.json'), 'controller namespace is not v11')
            fleet = load(candidates[str(ROOT/'v11/fleet.json')])
            expected = {'state_dir': str(ROOT/'v11/state'), 'worker_schema': 'transparent-shard-v11',
                        'worker_active_record': str(ROOT/'v11/active.json'),
                        'worker_runtime_cache_dir': '/srv/transparent-pir/v11/runtime-cache',
                        'worker_root': '/srv/transparent-pir/v11/publications'}
            require(all(fleet.get(k) == v for k, v in expected.items()), 'fleet control/cache namespace is not v11')
            for name in AUTHORITY[1:]:
                text = candidates['/etc/systemd/system/'+name].read_text()
                starts = [line for line in text.splitlines() if line.startswith('ExecStart=')]
                require(len(starts) == 1 and str(ROOT/'v11/fleet.json') in shlex.split(starts[0][10:]),
                        'fleet service still refers to old controller configuration')

    def capture(self):
        self.preflight()
        require(not self.root.exists(), 'baseline already exists; verify/reconcile instead of recapturing')
        states = {unit: self.commands.state(unit) for unit in UNITS[self.role]}
        state_path = self.root.with_suffix('.units.json')
        require(not state_path.exists(), 'capture intent already exists; reconcile instead of recapturing')
        proof = None
        if self.role == 'worker':
            status = self.commands.control()
            active = load(ROOT/'active.json')
            require(status.get('warm') is True and status.get('invalidated') is False and
                    status.get('candidate') is None and status.get('preparing') is None and
                    status.get('active') == active, 'predecessor worker is not quiescent and warm')
            proof = {'active': active, 'assignment_sha256': checksum(active['assignment'])}
        # Preserve original service states BEFORE the first stop. An interrupted
        # copy must not lose whether publication/load/scaling were active.
        state = {'plan_sha256': hashlib.sha256(encode(self.plan)).hexdigest(), 'units': states, 'worker': proof}
        state_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        B.atomic(state_path, encode(state))
        # A failure here intentionally leaves stopped writers stopped. The outer
        # schema journal owns recovery; this helper never guesses a reopen.
        if self.role == 'coordinator':
            self.commands.unit('stop', *WRITERS[self.role])
            self.quiet(WRITERS[self.role])
        receipt = B.capture(self.root, self.plan['baseline'])
        # State is outside the baseline payload inventory; never add unrecorded
        # files to a baseline whose complete.json is the final copy receipt.
        return receipt['plan_sha256']

    def saved(self):
        record = B.verify(self.root)
        state_path = self.root.with_suffix('.units.json')
        require(state_path.is_file() and not state_path.is_symlink() and state_path.stat().st_uid == os.geteuid() and
                state_path.stat().st_mode & 0o077 == 0, 'baseline unit state must be a private owned file')
        state = load(state_path)
        require(set(state) == {'plan_sha256', 'units', 'worker'} and state['plan_sha256'] == hashlib.sha256(encode(self.plan)).hexdigest() and
                set(state['units']) == set(UNITS[self.role]), 'baseline unit state is incomplete or belongs to another plan')
        return record, state

    def stage(self):
        self.saved()
        self.withdrawn()
        self.preflight()
        stop = UNITS[self.role] if self.role != 'router' else ()
        self.commands.unit('stop', *stop)
        self.quiet(stop)
        for item in self.plan['installs']:
            path = Path(item['target'])
            require(not path.is_symlink() and path.parent.resolve() == path.parent, 'candidate target has a symlink ancestor')
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            # Copy through a separate inode so running predecessors and rollback
            # copies cannot be modified by overwriting an executable in place.
            data = Path(item['source']).read_bytes()
            require(hashlib.sha256(data).hexdigest() == item['sha256'], 'candidate changed during copy')
            B.atomic(path, data, item['mode'])
            path.chmod(item['mode'])
            if item['target'].endswith('.service'):
                drops = Path(item['target']+'.d')
                displaced = Path(str(drops)+'.activity-v10-'+self.plan['transaction'])
                require(not displaced.exists(), 'unit drop-in displacement needs explicit reconciliation')
                if drops.exists():
                    os.rename(drops, displaced)
                    B.sync_dir(drops.parent)
        if self.role == 'coordinator':
            (ROOT/'v11/state').mkdir(parents=True, exist_ok=True, mode=0o700)
        elif self.role == 'worker':
            (ROOT/'v11').mkdir(parents=True, exist_ok=True, mode=0o700)
            CACHE.mkdir(parents=True, exist_ok=True, mode=0o700)
            active = ROOT/'v11/active.json'
            require(not active.exists() and not active.is_symlink() and not (ROOT/'v11/active.invalid.json').exists(),
                    'candidate worker activation state already exists; reconcile before staging')
            B.atomic(active, encode({k:self.plan['worker'][k] for k in ('directory', 'assignment', 'map_sha256')}))
        self.commands.run(['systemctl', 'daemon-reload'])

    def activate(self):
        self.saved()
        self.withdrawn()
        self.quiet(UNITS[self.role] if self.role != 'router' else ())
        for item in self.plan['installs']:
            path = Path(item['target'])
            require(path.is_file() and not path.is_symlink() and checksum(path) == item['sha256'], 'installed candidate changed')
        self.commands.unit('start', *START[self.role])
        if self.role == 'coordinator':
            self.quiet((SCALER, LOAD))

    def restore(self):
        self.withdrawn()
        _, state = self.saved()  # Validate all rollback bytes before stopping.
        stop = UNITS[self.role] if self.role != 'router' else ()
        self.commands.unit('stop', *stop)
        self.quiet(stop)
        include = [i['path'] for i in self.plan['baseline']['files'] if not deferred(self.role, i['path'])]
        B.restore(self.root, include)
        if self.role == 'coordinator':
            # Restoring the old fleet state may restore maintenance=false. Fence
            # controller/reconciler route retries before restarting either one.
            (ROOT/'state').mkdir(parents=True, exist_ok=True, mode=0o700)
            B.atomic(ROOT/'state/maintenance.json', encode({'enabled': True}))
        self.commands.run(['systemctl', 'daemon-reload'])
        # Restore only previously active product units. Load/scaler/Caddy remain
        # governed by the outer verify/reopen phases and the total 900s budget.
        start = [u for u in START[self.role] if state['units'][u].get('ActiveState') == 'active']
        self.commands.unit('start', *start)
        if self.role == 'coordinator':
            self.quiet((SCALER, LOAD))

    def verify_worker(self, *, rollback=False):
        require(self.role == 'worker', 'warm worker verification requires a worker plan')
        record, saved_state = self.saved()
        status = self.commands.control()
        require(isinstance(status, dict) and status.get('warm') is True and status.get('invalidated') is False and
                status.get('candidate') is None and status.get('preparing') is None, 'worker is not quiescent and warm')
        active = status.get('active', {})
        if rollback:
            index = next(n for n, i in enumerate(record['plan']['files']) if i['path'] == str(ROOT/'active.json'))
            expected = load(self.root/'files'/str(index))
            binary_hash = record['files']['/usr/local/bin/transparent-shard-server']['.']['sha256']
            proof = saved_state['worker']
            require(isinstance(proof, dict) and set(proof) == {'active', 'assignment_sha256'} and
                    proof['active'] == expected and isinstance(proof['assignment_sha256'], str) and HEX.fullmatch(proof['assignment_sha256']),
                    'rollback assignment proof is incomplete')
            assignment_hash = proof['assignment_sha256']
        else:
            expected = {k: self.plan['worker'][k] for k in ('directory', 'assignment', 'map_sha256')}
            binary_hash = self.plan['worker']['binary_sha256']
            assignment_hash = self.plan['worker']['assignment_sha256']
        require(checksum(expected['assignment']) == assignment_hash, 'worker assignment bytes changed')
        require(active == expected, 'worker active identity differs from verified target')
        unit = self.commands.state(WORKER)
        require(unit.get('ActiveState') == 'active' and unit.get('MainPID', '0') != '0', 'worker process is not running')
        require(checksum('/proc/'+unit['MainPID']+'/exe') == binary_hash, 'running worker executable is not the verified binary')
        revisions = status.get('revisions')
        require(isinstance(revisions, list) and revisions, 'worker omitted advertised revisions')
        # Return every advertised anchor for the coordinator's independently
        # accepted node comparison. This is warm identity evidence, not a query
        # oracle or permission to reopen metadata.
        for r in revisions:
            require(isinstance(r, dict) and isinstance(r.get('digest'), str) and HEX.fullmatch(r['digest']) and
                    type(r.get('end_height')) is int and r['end_height'] >= 0 and
                    isinstance(r.get('terminal_block_hash'), str) and HEX.fullmatch(r['terminal_block_hash']),
                    'malformed advertised revision anchor')
        return {'worker_id': self.plan['worker']['id'], 'active': active, 'binary_sha256': binary_hash,
                'revisions': revisions, 'checked_unix': time.time()}

    def restore_routing(self):
        require(self.role == 'router', 'original routing restore requires the router plan')
        self.withdrawn()  # Coordinator guard must still cover both origins.
        self.saved()
        B.restore(self.root, ['/etc/caddy/Caddyfile'])
        self.commands.run(['caddy', 'validate', '--config', '/etc/caddy/Caddyfile'])
        self.commands.unit('reload', 'caddy.service')

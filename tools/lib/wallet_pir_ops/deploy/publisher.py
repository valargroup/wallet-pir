"""Transfer the existing Transparent publishers to one owner, without moving data.

The reviewed request contains configuration and independent probe commands, never
credentials. A plan captures stable service/configuration state, not moving
publication tips. Deployment stages the binary before stopping owners and saves
its recovery journal on the coordinator before every effect. Reconciliation rolls
an interrupted cutover back; it never guesses whether to continue activation.
"""
import hashlib
import json
from pathlib import Path
import re
import shlex
import tempfile
import time

from .. import durable, release
from ..fleet import Fleet
from ..paths import ROOT
from . import descriptors, units
from .engine import DeployError, file_sha256
from .remote import production_lock
from .transaction import new_id

SCHEMA = 'transparent-publisher-cutover/1'
EXPANDED_SCHEMA = 'publisher-cutover/2'
EXPANDED_PRODUCTS = ['enhance', 'status', 'transparent_history', 'transaction_details']
ADOPTED = ('enhance-pir-coordinator.service', 'status-controller-qualification.service')
PRODUCTS = ['transparent_history', 'transaction_details']
OLD = ('transparent-v12-publish-controller.service', 'transparent-txid-display-k8-controller.service')
UNIT = 'pir-publisher.service'
LOAD = 'transparent-v2-5qps-continuous.service'
ROOT_DIR = '/opt/pir-publisher'
CONFIG = '/etc/wallet-pir/publisher.json'
INVENTORY = '/etc/wallet-pir/transparent-fleet.json'
SAMPLER = '/etc/pir-quality/hosts.json'
HISTORY = '/opt/transparent-publisher/v12/controller.json'
LOAD_DROP = '/etc/systemd/system/' + LOAD + '.d/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-publisher.conf'
UNIT_PATH = '/etc/systemd/system/' + UNIT
REGISTRY = ROOT / 'tools/deploy/publisher/transparent-services.toml'
TEMPLATE = ROOT / 'tools/deploy/publisher/pir-publisher-transparent.service'
FINAL = ('committed', 'rolled-back')


def require(condition, message):
    if not condition:
        raise DeployError(message)


def sha(text):
    return hashlib.sha256(text.encode()).hexdigest() if text is not None else None


def load_request(path, expected):
    data = Path(path).read_bytes()
    require(hashlib.sha256(data).hexdigest() == expected, 'publisher request checksum differs')
    request = json.loads(data)
    require(isinstance(request, dict) and set(request) == {'schema', 'host', 'machine_id', 'release', 'configuration', 'inventory', 'checks'},
            'publisher request has unknown or missing fields')
    require(request['schema'] in (SCHEMA, EXPANDED_SCHEMA), 'unsupported publisher cutover schema')
    require(isinstance(request['host'], str) and re.fullmatch('[A-Za-z0-9_.-]+', request['host']), 'invalid host')
    require(isinstance(request['machine_id'], str) and re.fullmatch('[a-f0-9]{32}', request['machine_id']), 'invalid machine id')
    version = request['release']
    require(isinstance(version, dict) and set(version) == {'git_sha', 'binary_sha256', 'binary_bytes'}, 'release fields must be explicit')
    require(isinstance(version['git_sha'], str) and isinstance(version['binary_sha256'], str)
            and re.fullmatch('[a-f0-9]{40}', version['git_sha']) and re.fullmatch('[a-f0-9]{64}', version['binary_sha256']),
            'release requires full source and executable digests')
    require(type(version['binary_bytes']) is int and version['binary_bytes'] > 0, 'invalid executable size')
    products = EXPANDED_PRODUCTS if request['schema'] == EXPANDED_SCHEMA else PRODUCTS
    config = request['configuration']
    require(isinstance(config, dict) and set(config) == {'management_listen', *products} and config['management_listen'] == '127.0.0.1:18080',
            'cutover products must match the schema, with loopback management on 18080')
    for name in products:
        product = config[name]
        require(isinstance(product, dict) and set(product) == {'enabled', 'settings'} and product['enabled'] is True
                and isinstance(product['settings'], dict),
                'every cutover product must be enabled')
        if name not in PRODUCTS:
            continue
        require(product['settings'].get('http_only') is False, 'autonomous HTTP publishing is out of scope')
    checks = request['checks']
    require(isinstance(checks, dict) and set(checks) == set(products), 'each product needs an independent exact check')
    for check in checks.values():
        require(isinstance(check, dict) and set(check) == {'host', 'argv', 'timeout'} and isinstance(check['argv'], list) and check['argv']
                and all(isinstance(a, str) and a for a in check['argv'])
                and type(check['timeout']) is int and 1 <= check['timeout'] <= 900, 'invalid exact check')
    with tempfile.NamedTemporaryFile('w') as handle:
        json.dump(request['inventory'], handle)
        handle.flush()
        inventory = descriptors.load_inventory(handle.name)
    require(inventory.lock == {'type': 'remote', 'host': request['host']}, 'cutover requires the coordinator production lock')
    require(all(c['host'] in inventory.hosts for c in checks.values()), 'probe host missing from inventory')
    publisher = inventory.services.get('publisher', {})
    require(publisher.get('managed_products') == products, 'managed products differ from configuration')
    if request['schema'] == EXPANDED_SCHEMA:
        require(publisher.get('exact_check') == {'products': checks}, 'inventory and cutover exact checks differ')
        descriptors.exact_check(descriptors.load_descriptors(ROOT / 'tools/deploy/publisher/services.toml')['publisher'], inventory)
    require(publisher.get('roles', {}).get('server') == [{'host': request['host'], 'vars': {'management_listen': '127.0.0.1:18080'}}],
            'publisher must run once on the pinned coordinator')
    registry = ROOT / 'tools/deploy/publisher/all-services.toml' if request['schema'] == EXPANDED_SCHEMA else REGISTRY
    problems = Fleet(inventory, descriptors.load_descriptors(registry)).problems(complete=True)
    require(not problems, 'candidate inventory is incomplete or has conflicting owners: ' + '; '.join(problems))
    return request, inventory


def stable_unit(state):
    return {key: state.get(key) for key in ('load_state', 'active_state', 'main_pid', 'invocation_id',
            'need_daemon_reload', 'fragment_path', 'fragment_text', 'drop_ins', 'exe_sha256', 'unit_file_state')}


def display_settings(state):
    """Translate the live standalone command, rejecting any unhandled behavior."""
    command = shlex.split(units.exec_start(units.effective(
        [state['fragment_text'], *(entry['text'] for entry in state['drop_ins'])])))
    require(len(command) > 2 and command[1] == 'run', 'display controller is not in run mode')
    values, flags = {}, set()
    args = iter(command[2:])
    boolean = {'--trust-sealed', '--ship-runtimes'}
    allowed = {'--root', '--journal', '--workers', '--rpc-url', '--rpc-cookie', '--poll-ms', '--status-listen',
               '--mode', '--blocks-per-step', '--step-interval-ms', '--max-recent-records'}
    for arg in args:
        require(arg not in values and arg not in flags, 'duplicate display argument')
        if arg in boolean:
            flags.add(arg)
        else:
            require(arg in allowed, 'unhandled display argument: ' + arg)
            values[arg] = next(args, None)
            require(values[arg] is not None, 'missing display argument value')
    require(values.get('--mode', 'live') in ('live', 'replay-then-live'), 'display replay-only mode cannot be adopted')
    return {'root': values['--root'], 'journal': values['--journal'], 'workers': values['--workers'],
            'rpc_url': values.get('--rpc-url', 'http://127.0.0.1:8232'), 'rpc_cookie': values['--rpc-cookie'],
            'poll_ms': int(values.get('--poll-ms', 500)), 'status_listen': values.get('--status-listen', '127.0.0.1:8098'),
            'trust_sealed': '--trust-sealed' in flags, 'ship_runtimes': '--ship-runtimes' in flags, 'http_only': False,
            'max_recent_records': int(values['--max-recent-records']) if '--max-recent-records' in values else None}


class Cutover:
    def __init__(self, request, inventory, executor, state_dir, *, clock=time.monotonic, sleep=time.sleep):
        self.request, self.inventory, self.ex = request, inventory, executor
        self.host = request['host']
        self.expanded = request['schema'] == EXPANDED_SCHEMA
        self.old = (*OLD, *ADOPTED) if self.expanded else OLD
        self.state_dir = Path(state_dir) / 'publisher-cutover'
        self.clock, self.sleep = clock, sleep
        self.lock = None

    def read(self, path):
        return self.ex.read(self.host, path)

    def resources(self, extra_bytes=0):
        config = self.request['configuration']
        history = config['transparent_history']['settings']
        display = config['transaction_details']['settings']
        paths = [CONFIG, INVENTORY, SAMPLER, UNIT_PATH, LOAD_DROP, ROOT_DIR + '/latest.json',
                 history['data_dir'] + '/writer.lock', history['publication_root'] + '/controller.lock',
                 display['journal'] + '/writer.lock', display['root'] + '/controller.lock']
        if self.expanded:
            from .publisher_products import inputs, locks
            paths += [*inputs(config), *locks(config)]
        result = self.ex.publisher_resources(self.host, paths)
        require(result['machine_id'] == self.request['machine_id'], 'coordinator machine identity differs')
        require(result['memory_available'] >= result['memory_total'] * .2, 'less than 20 percent available memory')
        minimum = 50 if self.expanded else 24
        require(result['memory_total'] >= minimum * (1 << 30), 'coordinator is too small for the combined publisher budget')
        for disk in result['disks'].values():
            require(disk['available'] - extra_bytes >= disk['total'] * .2, 'staging would leave less than 20 percent disk headroom')
        return result

    def latest(self):
        pointer = self.read(ROOT_DIR + '/latest.json')
        if pointer is None:
            return None
        identifier = json.loads(pointer)['id']
        require(re.fullmatch('publisher-[A-Za-z0-9-]+', identifier), 'invalid publisher transaction pointer')
        value = json.loads(self.read(ROOT_DIR + '/transactions/' + identifier + '/journal.json'))
        require(value['id'] == identifier and value['schema'] in (SCHEMA, EXPANDED_SCHEMA), 'invalid publisher transaction journal')
        return value

    def capture(self):
        previous = self.latest()
        require(previous is None or previous['phase'] in FINAL, 'unfinished publisher cutover; reconcile or rollback first')
        resource = self.resources(self.request['release']['binary_bytes'])
        states = {name: stable_unit(self.ex.probe_unit(self.host, name)) for name in (*self.old, UNIT, LOAD)}
        for state in states.values():
            require(not state['need_daemon_reload'], 'unit changed without daemon-reload')
        unified = states[UNIT]
        if unified['active_state'] == 'active':
            require(all(states[name]['active_state'] == 'inactive' and not states[name]['main_pid'] for name in OLD),
                    'standalone and unified publishers both own state')
            require(not unified['drop_ins'], 'unexpected publisher drop-ins; reconcile before changing ownership')
            installed = json.loads(self.read(CONFIG))
            if self.expanded:
                from .publisher_products import verify_adoption
                verify_adoption(self, states, installed)
            else:
                require(self.request['configuration'] == installed,
                        'publisher updates must preserve the installed product settings')
        else:
            require(not self.expanded, 'expanded cutover requires the existing Transparent publisher')
            require(unified['load_state'] == 'not-found', 'publisher exists but is not running; reconcile it first')
            require(all(states[name]['active_state'] == 'active' for name in OLD), 'both predecessor controllers must be running')
        for name in (*self.old, LOAD):
            state = states[name]
            require(state['load_state'] == 'loaded' and state['fragment_text'], 'missing captured unit: ' + name)
        executables = {}
        for name, state in states.items():
            if state['active_state'] == 'active':
                command = units.exec_start(units.effective([state['fragment_text'], *(d['text'] for d in state['drop_ins'])]))
                path = units.split_exec(command)[1]
                require(self.ex.sha256(self.host, path) == state['exe_sha256'], 'running executable differs from its rollback path: ' + name)
                executables[path] = state['exe_sha256']
        require(states[LOAD]['active_state'] == 'active', 'continuous exact load must be running at capture')
        config = self.request['configuration']
        if unified['active_state'] != 'active':
            old_history = json.loads(self.read(HISTORY))
            for key in ('ship_runtimes', 'distribute_prepared', 'http_only'):
                old_history.setdefault(key, False)
            require(config['transparent_history']['settings'] == old_history, 'history configuration changes more than process ownership')
            require(config['transaction_details']['settings'] == display_settings(states[OLD[1]]),
                    'display configuration changes more than process ownership')
            status, body = self.ex.http_get(self.host, 'http://' + config['transaction_details']['settings']['status_listen'] + '/v1/status')
            require(status == 200 and json.loads(body).get('mode') == 'live', 'display replay must finish before cutover')
        sampler = json.loads(self.read(SAMPLER))
        targets = sampler.get('targets', [])
        sources = {source for target in targets for source in target.get('sources', [])}
        require({'transparent-publisher', 'txid-controller'} <= sources, 'both publisher monitoring sources must exist')
        for target in targets:
            if set(target.get('sources', [])) & {'transparent-publisher', 'txid-controller'}:
                target['unit'] = UNIT
            if 'units' in target:
                target['units'] = [u for u in target['units'] if u not in OLD]
        load_command = units.exec_start(units.effective([states[LOAD]['fragment_text'],
            *(entry['text'] for entry in states[LOAD]['drop_ins'])]))
        load_arguments = shlex.split(load_command)
        require(load_arguments.count('--controller-unit') == 1, 'unknown load controller binding')
        position = load_arguments.index('--controller-unit') + 1
        require(position < len(load_arguments) and load_arguments[position] in
                ('transparent-v12-publish-controller', 'transparent-v12-publish-controller.service', 'pir-publisher', UNIT),
                'unknown load controller binding')
        load_arguments[position] = 'pir-publisher'
        load_command = shlex.join(load_arguments)
        binary = ROOT_DIR + '/releases/' + self.request['release']['binary_sha256'] + '/pir-publisher'
        desired = {
            CONFIG: json.dumps(config, indent=2) + '\n',
            INVENTORY: json.dumps(self.request['inventory'], indent=2) + '\n',
            UNIT_PATH: TEMPLATE.read_text().replace('@BIN@', binary),
            SAMPLER: json.dumps(sampler, indent=2) + '\n',
            LOAD_DROP: '[Service]\nExecStart=\nExecStart=' + load_command + '\n',
        }
        if self.expanded:
            from .publisher_products import render, inputs, monitoring
            desired[UNIT_PATH] = render(config, binary, states)
            monitoring(sampler, self.old)
            desired[SAMPLER] = json.dumps(sampler, indent=2) + '\n'
            # Pin inputs without reinstalling them or changing their permissions.
            for path in inputs(config):
                require(self.read(path) is not None, 'missing adopted configuration: ' + path)
        before = {path: {'text': self.read(path), 'mode': (resource['files'][path] or {}).get('mode', 0o600)} for path in desired}
        return {'request_sha256': durable.digest(self.request), 'before': before, 'units': states,
                'desired': desired, 'binary': binary, 'executables': executables,
                'previous_transaction': previous and previous['id'],
                'inputs': {path: sha(self.read(path)) for path in inputs(config)} if self.expanded else {}}

    def plan(self):
        plan = self.capture()
        return {'plan': plan, 'plan_sha256': durable.digest(plan)}

    def save(self, journal, phase=None):
        if phase:
            journal['phase'] = phase
        self.lock.verify()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        durable.atomic_json(self.state_dir / (journal['id'] + '.json'), journal, mode=0o600)
        path = ROOT_DIR + '/transactions/' + journal['id']
        self.ex.mkdir(self.host, path, 0o700)
        self.ex.write(self.host, path + '/journal.json', durable.canonical(journal), 0o600)

    def effect(self, journal, name, action):
        journal['intent'] = name
        self.save(journal)
        self.lock.verify()
        action()
        journal['completed'].append(name)
        journal['intent'] = None
        self.save(journal)

    def stop(self, name):
        self.ex.systemctl(self.host, 'stop', name)
        observed = self.ex.probe_unit(self.host, name)
        require(not observed['main_pid'] and observed['active_state'] in ('inactive', 'failed'), 'owner did not stop: ' + name)

    def unlocked(self):
        config = self.request['configuration']
        history, display = (config[name]['settings'] for name in PRODUCTS)
        paths = [history['data_dir'] + '/writer.lock', history['publication_root'] + '/controller.lock',
                 display['journal'] + '/writer.lock', display['root'] + '/controller.lock']
        if self.expanded:
            from .publisher_products import locks
            paths += locks(config)
        self.ex.publisher_unlocked(self.host, paths)

    def write(self, path, text, mode=0o600):
        self.ex.mkdir(self.host, str(Path(path).parent), 0o755)
        self.ex.write(self.host, path, text.encode(), mode)

    def ready(self, unified):
        deadline = self.clock() + 900
        config = self.request['configuration']
        if unified:
            probes = [('http://127.0.0.1:18080/ready', lambda value: value.get('ready') is True)]
        else:
            probes = [('http://' + config['transparent_history']['settings']['listen'] + '/v1/status',
                       lambda value: value.get('phase') == 'serving'),
                      ('http://' + config['transaction_details']['settings']['status_listen'] + '/v1/status',
                       lambda value: value.get('phase') == 'running' and not value.get('halted') and not value.get('stalled'))]
        while True:
            self.lock.verify()
            passed = True
            for url, predicate in probes:
                status, body = self.ex.http_get(self.host, url)
                try:
                    passed &= status == 200 and predicate(json.loads(body))
                except (ValueError, TypeError):
                    passed = False
            if passed:
                return
            require(self.clock() < deadline, 'publisher readiness deadline exceeded')
            self.sleep(3)

    def exact(self, journal):
        for product, check in self.request['checks'].items():
            code, output = self.ex.run(check['host'], check['argv'], check['timeout'])
            require(code == 0, product + ' independent encrypted-answer probe failed: ' + output[-1000:])
            journal.setdefault('checks', {})[product] = {'passed': True, 'output': output[-2000:]}
            self.save(journal)

    def deploy(self, binary, expected_plan):
        require(file_sha256(binary) == self.request['release']['binary_sha256'] and
                Path(binary).stat().st_size == self.request['release']['binary_bytes'], 'publisher executable does not match request')
        with production_lock(self.inventory, self.ex) as self.lock:
            plan = self.capture()
            require(durable.digest(plan) == expected_plan, 'publisher plan is stale; review a fresh plan')
            identifier = new_id('publisher', self.request['release']['binary_sha256'])
            journal = {'schema': self.request['schema'], 'id': identifier, 'phase': 'staging', 'request': self.request,
                       'plan': plan, 'completed': [], 'intent': None}
            self.save(journal)
            self.ex.write(self.host, ROOT_DIR + '/latest.json', durable.canonical({'id': identifier}), 0o600)
            self.effect(journal, 'stage executable', lambda: self.stage(binary, plan['binary']))
            staged_config = ROOT_DIR + '/transactions/' + identifier + '/publisher.json'
            self.effect(journal, 'stage configuration', lambda: self.write(staged_config, plan['desired'][CONFIG]))
            code, output = self.ex.run(self.host, [plan['binary'], '--config', staged_config, '--check'], 60)
            require(code == 0, 'staged publisher refuses its configuration: ' + output[-1000:])
            self.resources()
            self.save(journal, 'installing')
            self.effect(journal, 'pause continuous load', lambda: self.stop(LOAD))
            for name in (*self.old, UNIT):
                if plan['units'][name]['load_state'] != 'not-found':
                    self.effect(journal, 'stop ' + name, lambda name=name: self.stop(name))
            self.unlocked()
            for name in self.old:
                self.effect(journal, 'disable ' + name, lambda name=name: self.ex.systemctl(self.host, 'disable', name))
            for path, text in plan['desired'].items():
                mode = 0o644 if path.startswith('/etc/systemd/') else 0o600
                self.effect(journal, 'install ' + path, lambda path=path, text=text, mode=mode: self.write(path, text, mode))
            self.effect(journal, 'reload services', lambda: self.ex.systemctl(self.host, 'daemon-reload'))
            self.effect(journal, 'enable publisher', lambda: self.ex.systemctl(self.host, 'enable', UNIT))
            self.effect(journal, 'start publisher', lambda: self.ex.systemctl(self.host, 'start', UNIT))
            self.save(journal, 'verifying')
            self.ready(True)
            self.exact(journal)
            self.effect(journal, 'resume continuous load', lambda: self.ex.systemctl(self.host, 'start', LOAD))
            self.save(journal, 'committed')
            return journal

    def restart(self):
        """Exercise cooperative shutdown without changing the retained rollback baseline."""
        with production_lock(self.inventory, self.ex) as self.lock:
            journal = self.latest()
            require(journal is not None and journal['request'] == self.request and journal['phase'] == 'committed',
                    'restart requires this committed publisher transaction')
            for path, text in journal['plan']['desired'].items():
                require(self.read(path) == text, 'managed file changed before restart: ' + path)
            for path, digest in journal['plan'].get('inputs', {}).items():
                require(sha(self.read(path)) == digest, 'adopted input changed before restart: ' + path)
            require(self.ex.probe_unit(self.host, UNIT)['exe_sha256'] == self.request['release']['binary_sha256'],
                    'running publisher executable changed before restart')
            self.save(journal, 'restarting')
            self.effect(journal, 'restart pause load', lambda: self.stop(LOAD))
            self.effect(journal, 'restart stop publisher', lambda: self.stop(UNIT))
            self.unlocked()
            self.effect(journal, 'restart start publisher', lambda: self.ex.systemctl(self.host, 'start', UNIT))
            self.ready(True)
            self.exact(journal)
            self.effect(journal, 'restart resume load', lambda: self.ex.systemctl(self.host, 'start', LOAD))
            self.save(journal, 'committed')
            return journal

    def stage(self, binary, target):
        self.ex.mkdir(self.host, str(Path(target).parent), 0o755)
        current = self.ex.sha256(self.host, target)
        require(current in (None, self.request['release']['binary_sha256']), 'immutable publisher release differs')
        if current is None:
            self.ex.upload(self.host, binary, target, 0o755)
        require(self.ex.sha256(self.host, target) == self.request['release']['binary_sha256'], 'uploaded publisher digest differs')

    def rollback(self):
        with production_lock(self.inventory, self.ex) as self.lock:
            require(self.ex.publisher_resources(self.host, [CONFIG])['machine_id'] == self.request['machine_id'],
                    'coordinator machine identity differs')
            journal = self.latest()
            require(journal is not None, 'no publisher cutover to restore')
            require(journal['request'] == self.request, 'request differs from latest publisher cutover')
            if journal['phase'] == 'rolled-back':
                return journal
            plan = journal['plan']
            for path, digest in plan['executables'].items():
                require(self.ex.sha256(self.host, path) == digest, 'rollback executable changed: ' + path)
            for path, desired in plan['desired'].items():
                require(self.read(path) in (plan['before'][path]['text'], desired), 'managed file changed outside transaction: ' + path)
            for name in self.old:
                observed = stable_unit(self.ex.probe_unit(self.host, name))
                require(all(observed[key] == plan['units'][name][key] for key in ('fragment_text', 'drop_ins')),
                        'predecessor service configuration changed: ' + name)
            self.save(journal, 'restoring')
            for name in (LOAD, UNIT, *self.old):
                if self.ex.probe_unit(self.host, name)['load_state'] != 'not-found':
                    self.effect(journal, 'restore stop ' + name, lambda name=name: self.stop(name))
            self.unlocked()
            if self.ex.probe_unit(self.host, UNIT)['load_state'] != 'not-found':
                self.effect(journal, 'disable publisher', lambda: self.ex.systemctl(self.host, 'disable', UNIT))
            for path, previous in plan['before'].items():
                def restore(path=path, previous=previous):
                    if previous['text'] is None:
                        self.ex.remove(self.host, path)
                    else:
                        self.write(path, previous['text'], previous['mode'])
                self.effect(journal, 'restore ' + path, restore)
            self.effect(journal, 'restore reload', lambda: self.ex.systemctl(self.host, 'daemon-reload'))
            for name in (*self.old, UNIT):
                before = plan['units'][name]
                if before['load_state'] == 'not-found':
                    continue
                enabled = before['unit_file_state'] in ('enabled', 'enabled-runtime')
                self.effect(journal, 'restore enable ' + name,
                            lambda name=name, enabled=enabled: self.ex.systemctl(self.host, 'enable' if enabled else 'disable', name))
                if before['active_state'] == 'active':
                    self.effect(journal, 'restore start ' + name, lambda name=name: self.ex.systemctl(self.host, 'start', name))
            self.ready(plan['units'][UNIT]['active_state'] == 'active')
            self.exact(journal)
            self.effect(journal, 'restore continuous load', lambda: self.ex.systemctl(self.host, 'start', LOAD))
            self.save(journal, 'rolled-back')
            return journal


def register(commands):
    for operation in ('plan', 'preflight', 'deploy', 'status', 'reconcile', 'rollback', 'restart'):
        command = commands.add_parser('publisher-cutover-' + operation, allow_abbrev=False)
        command.add_argument('--request', required=True)
        command.add_argument('--request-sha256', required=True)
        if operation in ('preflight', 'deploy'):
            command.add_argument('--archive', required=True)
        if operation == 'deploy':
            command.add_argument('--expect-plan-sha256', required=True)


def dispatch(args, executor=None, **_options):
    from .remote import SSHExecutor
    request, inventory = load_request(args.request, args.request_sha256)
    executor = executor or SSHExecutor(inventory)
    target = Cutover(request, inventory, executor, args.state_dir)
    operation = args.command.removeprefix('publisher-cutover-')
    if operation in ('rollback', 'reconcile'):
        return target.rollback()
    if operation == 'restart':
        return target.restart()
    if operation == 'status':
        return {'transaction': target.latest(), 'units': {name: stable_unit(executor.probe_unit(request['host'], name))
                for name in (*target.old, UNIT, LOAD)}}
    if operation == 'plan':
        return target.plan()
    with tempfile.TemporaryDirectory(prefix='publisher-artifact-') as directory:
        bundle = Path(directory) / 'bundle'
        release.extract(Path(args.archive), bundle, request['release']['git_sha'], 'pir-publisher')
        require(json.loads((bundle / 'candidate.json').read_text()).get('source_dirty') is False,
                'publisher artifact must come from a clean source revision')
        binary = bundle / 'pir-publisher'
        require(file_sha256(binary) == request['release']['binary_sha256'], 'artifact executable differs from request')
        if operation == 'preflight':
            return target.plan()
        return target.deploy(binary, args.expect_plan_sha256)

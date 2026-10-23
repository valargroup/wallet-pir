"""Update the unified publisher and its matching serving roles without moving data.

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
from . import descriptors, publisher_products, units
from .engine import DeployError, file_sha256
from .remote import RemoteError, production_lock
from .transaction import new_id

SCHEMA = 'transparent-publisher-cutover/1'
EXPANDED_SCHEMA = 'publisher-cutover/2'
SERVING_SCHEMA = 'publisher-cutover/3'
HTTPS_SCHEMA = 'publisher-https-cutover/1'
ENHANCE_HTTPS_SCHEMA = 'enhance-https-cutover/1'
SERVING_SCHEMAS = (SERVING_SCHEMA, HTTPS_SCHEMA, ENHANCE_HTTPS_SCHEMA)
EXPANDED_PRODUCTS = ['enhance', 'status', 'transparent_history', 'transaction_details']
ADOPTED = ('enhance-pir-coordinator.service', 'status-controller-qualification.service')
PRODUCTS = ['transparent_history', 'transaction_details']
OLD = ('transparent-v12-publish-controller.service', 'transparent-txid-display-k8-controller.service')
UNIT = 'pir-publisher.service'
LOAD = 'transparent-v2-5qps-continuous.service'
LOAD_LATCH = '/srv/transparent-activity/canonical-load/v12/latched.json'
ROOT_DIR = '/opt/pir-publisher'
CONFIG = '/etc/wallet-pir/publisher.json'
INVENTORY = '/etc/wallet-pir/transparent-fleet.json'
SAMPLER = '/etc/pir-quality/hosts.json'
LOAD_DROP = '/etc/systemd/system/' + LOAD + '.d/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-publisher.conf'
UNIT_PATH = '/etc/systemd/system/' + UNIT
# The completed Status HTTPS cutover (tools/status/lib/https_cutover.py, retired
# after 6b6b923b) left this ExecStart-only override on the publisher. An ordinary
# update owns it only when its bytes are exactly that renderer's output for the
# running executable.
STATUS_HTTPS_DROP = '/etc/systemd/system/' + UNIT + '.d/' + 'z' * 36 + '-status-https.conf'
FINAL = ('committed', 'rolled-back')


def require(condition, message):
    if not condition:
        raise DeployError(message)


def sha(text):
    return hashlib.sha256(text.encode()).hexdigest() if text is not None else None


def load_request(path, expected, *, recovery=False):
    data = Path(path).read_bytes()
    require(hashlib.sha256(data).hexdigest() == expected, 'publisher request checksum differs')
    request = json.loads(data)
    require(isinstance(request, dict) and set(request) == ({'schema', 'host', 'machine_id', 'release', 'configuration', 'inventory', 'checks'}
                | ({'serving'} if request.get('schema') in SERVING_SCHEMAS else set())
                | ({'https'} if request.get('schema') == HTTPS_SCHEMA else set())
                | ({'resume_load'} if 'resume_load' in request else set())
                | ({'expected_disk_latch_sha256'} if 'expected_disk_latch_sha256' in request else set())
                | ({'preserve_serving'} if 'preserve_serving' in request else set())
                | ({'prepared_storage'} if 'prepared_storage' in request else set())
                | ({'enhance_https'} if request.get('schema') == ENHANCE_HTTPS_SCHEMA else set())),
            'publisher request has unknown or missing fields')
    require(request['schema'] in (SCHEMA, EXPANDED_SCHEMA, *SERVING_SCHEMAS), 'unsupported publisher cutover schema')
    if 'prepared_storage' in request:
        from .publisher_storage import validate_reference
        require(request['schema'] == SERVING_SCHEMA, 'prepared storage requires the current publisher cutover')
        validate_reference(request['prepared_storage'])
    if 'expected_disk_latch_sha256' in request:
        require(request['schema'] in (SERVING_SCHEMA, ENHANCE_HTTPS_SCHEMA) and request.get('resume_load') is False
                and isinstance(request['expected_disk_latch_sha256'], str)
                and re.fullmatch('[a-f0-9]{64}', request['expected_disk_latch_sha256']),
                'expected_disk_latch_sha256 pins an existing disk-only incident with resume_load:false')
    if 'preserve_serving' in request:
        require(request['schema'] == SERVING_SCHEMA and type(request['preserve_serving']) is bool,
                'preserve_serving must be a boolean for publisher-cutover/3 only')
    if 'resume_load' in request:
        require(request['schema'] in (SERVING_SCHEMA, ENHANCE_HTTPS_SCHEMA) and type(request['resume_load']) is bool,
                'resume_load must be a boolean for a publisher update or Enhance HTTPS cutover')
    require(recovery or request['schema'] == SERVING_SCHEMA,
            'completed ownership and HTTPS migrations are recovery-only; deploy the unified four-product fleet')
    require(isinstance(request['host'], str) and re.fullmatch('[A-Za-z0-9_.-]+', request['host']), 'invalid host')
    require(isinstance(request['machine_id'], str) and re.fullmatch('[a-f0-9]{32}', request['machine_id']), 'invalid machine id')
    version = request['release']
    require(isinstance(version, dict) and set(version) == {'git_sha', 'binary_sha256', 'binary_bytes'}, 'release fields must be explicit')
    require(isinstance(version['git_sha'], str) and isinstance(version['binary_sha256'], str)
            and re.fullmatch('[a-f0-9]{40}', version['git_sha']) and re.fullmatch('[a-f0-9]{64}', version['binary_sha256']),
            'release requires full source and executable digests')
    require(type(version['binary_bytes']) is int and version['binary_bytes'] > 0, 'invalid executable size')
    products = EXPANDED_PRODUCTS if request['schema'] in (EXPANDED_SCHEMA, *SERVING_SCHEMAS) else PRODUCTS
    config = request['configuration']
    require(isinstance(config, dict) and set(config) == {'management_listen', *products} and config['management_listen'] == '127.0.0.1:18080',
            'cutover products must match the schema, with loopback management on 18080')
    for name in products:
        product = config[name]
        require(isinstance(product, dict) and set(product) == {'enabled', 'settings'} and product['enabled'] is True
                and isinstance(product['settings'], dict),
                'every cutover product must be enabled')
    if not recovery:
        publisher_products.require_current_configuration(config)
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
    if request['schema'] in (EXPANDED_SCHEMA, *SERVING_SCHEMAS):
        require(publisher.get('exact_check') == {'products': checks}, 'inventory and cutover exact checks differ')
        descriptors.exact_check(descriptors.load_descriptors(ROOT / 'tools/deploy/publisher/services.toml')['publisher'], inventory)
    require(publisher.get('roles', {}).get('server') == [{'host': request['host'], 'vars': {'management_listen': '127.0.0.1:18080'}}],
            'publisher must run once on the pinned coordinator')
    registry = ROOT / 'tools/deploy/publisher/all-services.toml' if request['schema'] in (EXPANDED_SCHEMA, *SERVING_SCHEMAS) else ROOT / 'tools/deploy/publisher/transparent-services.toml'
    problems = Fleet(inventory, descriptors.load_descriptors(registry)).problems(complete=True)
    require(not problems, 'candidate inventory is incomplete or has conflicting owners: ' + '; '.join(problems))
    if request['schema'] in SERVING_SCHEMAS:
        from .publisher_serving import validate_request
        validate_request(request, inventory)
    if request['schema'] == HTTPS_SCHEMA:
        from .publisher_https import validate
        validate(request, inventory)
    if request['schema'] == ENHANCE_HTTPS_SCHEMA:
        from .enhance_https import validate
        validate(request, inventory)
    return request, inventory


def exec_override(command):
    return '[Service]\nExecStart=\nExecStart=' + command + '\n'


def status_https_override(state, executable_sha256):
    """Return the installed Status HTTPS override text, or None without drop-ins.

    Refuses any other drop-in, and an override that differs from the Status
    renderer's output: the fragment command with only its executable replaced by
    a digest-named publisher release whose digest is the running executable's.
    The override changes only ExecStart, so the fragment keeps every sandbox,
    environment and resource setting.
    """
    if not state['drop_ins']:
        return None
    require([d['path'] for d in state['drop_ins']] == [STATUS_HTTPS_DROP],
            'unexpected publisher drop-ins; reconcile before changing ownership')
    text = state['drop_ins'][0]['text']
    try:
        fragment = units.exec_start(units.effective([state['fragment_text']]))
        _, previous, _ = units.split_exec(fragment)
        _, override, _ = units.split_exec(units.exec_start(units.effective([text])))
    except units.UnitError as error:
        raise DeployError('unknown Status HTTPS publisher override: ' + str(error)) from error
    release = re.fullmatch(re.escape(ROOT_DIR) + '/releases/([a-f0-9]{64})/pir-publisher', override)
    require(release is not None and release[1] == executable_sha256,
            'Status HTTPS override does not run the pinned running publisher release')
    require(fragment.count(previous) == 1 and text == exec_override(fragment.replace(previous, override, 1)),
            'Status HTTPS publisher override changed; reconcile before changing ownership')
    return text


def stable_unit(state):
    return {key: state.get(key) for key in ('load_state', 'active_state', 'main_pid', 'invocation_id',
            'need_daemon_reload', 'fragment_path', 'fragment_text', 'drop_ins', 'exe_sha256', 'unit_file_state')}


class Cutover:
    def __init__(self, request, inventory, executor, state_dir, *, clock=time.monotonic, sleep=time.sleep):
        self.request, self.inventory, self.ex = request, inventory, executor
        self.host = request['host']
        self.resume_load = request.get('resume_load', True)
        self.expanded = request['schema'] in (EXPANDED_SCHEMA, *SERVING_SCHEMAS)
        self.old = (*OLD, *ADOPTED) if self.expanded else OLD
        self.state_dir = Path(state_dir) / 'publisher-cutover'
        self.clock, self.sleep = clock, sleep
        self.lock = None
        self.serving = None
        self.supervision = None
        self.identifier = None
        if request['schema'] in SERVING_SCHEMAS:
            from .publisher_serving import Serving
            if request['schema'] == ENHANCE_HTTPS_SCHEMA:
                from .enhance_https import EnhanceServing
                self.serving = EnhanceServing(self)
            elif request['schema'] == HTTPS_SCHEMA:
                from .publisher_https import HttpsServing
                self.serving = HttpsServing(self)
            else:
                self.serving = Serving(self)

    def read(self, path):
        return self.ex.read(self.host, path)

    def resources(self, extra_bytes=0, allocations=None):
        config = self.request['configuration']
        history = config['transparent_history']['settings']
        display = config['transaction_details']['settings']
        paths = [CONFIG, INVENTORY, SAMPLER, UNIT_PATH, STATUS_HTTPS_DROP, LOAD_DROP, ROOT_DIR + '/latest.json',
                 history['data_dir'] + '/writer.lock', history['publication_root'] + '/controller.lock',
                 display['journal'] + '/writer.lock', display['root'] + '/controller.lock']
        if self.expanded:
            from .publisher_products import locks
            paths += locks(config)
        paths += list(allocations or {})
        result = self.ex.publisher_resources(self.host, paths)
        require(result['machine_id'] == self.request['machine_id'], 'coordinator machine identity differs')
        require(result['memory_available'] >= result['memory_total'] * .2, 'less than 20 percent available memory')
        minimum = 50 if self.expanded else 24
        require(result['memory_total'] >= minimum * (1 << 30), 'coordinator is too small for the combined publisher budget')
        budgets = {}
        for path, amount in (allocations or {}).items():
            device = result['disks'][path]['device']
            budgets[device] = budgets.get(device, 0) + amount
        for disk in result['disks'].values():
            reserved = budgets.get(disk['device'], 0) if allocations is not None else extra_bytes
            require(disk['available'] - reserved >= disk['total'] * .2, 'staging would leave less than 20 percent disk headroom')
        return result

    def latest(self):
        pointer = self.read(ROOT_DIR + '/latest.json')
        if pointer is None:
            return None
        identifier = json.loads(pointer)['id']
        require(re.fullmatch('publisher-[A-Za-z0-9-]+', identifier), 'invalid publisher transaction pointer')
        value = json.loads(self.read(ROOT_DIR + '/transactions/' + identifier + '/journal.json'))
        require(value['id'] == identifier and value['schema'] in (SCHEMA, EXPANDED_SCHEMA, *SERVING_SCHEMAS), 'invalid publisher transaction journal')
        return value

    def capture(self):
        require(self.request['schema'] == SERVING_SCHEMA,
                'completed ownership and HTTPS migrations are recovery-only')
        previous = self.latest()
        require(previous is None or previous['phase'] in FINAL, 'unfinished publisher cutover; reconcile or rollback first')
        resource = self.resources(self.request['release']['binary_bytes'], None)
        states = {name: stable_unit(self.ex.probe_unit(self.host, name)) for name in (*self.old, UNIT, LOAD)}
        for state in states.values():
            require(not state['need_daemon_reload'], 'unit changed without daemon-reload')
        unified = states[UNIT]
        require(unified['active_state'] == 'active' and unified['main_pid'], 'unified publisher must already be running')
        require(all(states[name]['active_state'] != 'active' and not states[name]['main_pid'] for name in self.old),
                'standalone and unified publishers both own state')
        override = status_https_override(unified, unified['exe_sha256'])
        config = self.request['configuration']
        installed = json.loads(self.read(CONFIG))
        if 'prepared_storage' in self.request:
            from .publisher_storage import Handoff
            Handoff(self).capture(installed)
        else:
            require(config == publisher_products.configuration_for_update(installed),
                    'publisher updates must preserve every product setting')
        executables = {}
        for name, state in states.items():
            if state['active_state'] == 'active':
                command = units.exec_start(units.effective([state['fragment_text'], *(d['text'] for d in state['drop_ins'])]))
                path = units.split_exec(command)[1]
                require(self.ex.sha256(self.host, path) == state['exe_sha256'], 'running executable differs from its rollback path: ' + name)
                executables[path] = state['exe_sha256']
        if self.resume_load:
            require(states[LOAD]['active_state'] == 'active', 'continuous exact load must be running at capture')
        else:
            self.require_stopped_load(states[LOAD])
        sampler = json.loads(self.read(SAMPLER))
        targets = sampler.get('targets', [])
        sources = {source for target in targets for source in target.get('sources', [])}
        require({'transparent-publisher', 'txid-controller'} <= sources, 'both publisher monitoring sources must exist')
        require(all(target.get('unit') == UNIT for target in targets
                    if set(target.get('sources', [])) & {'transparent-publisher', 'txid-controller'}),
                'publisher monitoring must already follow the unified owner')
        load_command = units.exec_start(units.effective([states[LOAD]['fragment_text'],
            *(entry['text'] for entry in states[LOAD]['drop_ins'])]))
        load_arguments = shlex.split(load_command)
        require(load_arguments.count('--controller-unit') == 1, 'unknown load controller binding')
        position = load_arguments.index('--controller-unit') + 1
        require(position < len(load_arguments) and load_arguments[position] in
                ('pir-publisher', UNIT),
                'unknown load controller binding')
        from .publisher_products import render
        binary = ROOT_DIR + '/releases/' + self.request['release']['binary_sha256'] + '/pir-publisher'
        desired = {
            CONFIG: json.dumps(config, indent=2) + '\n',
            INVENTORY: json.dumps(self.request['inventory'], indent=2) + '\n',
            UNIT_PATH: render(config, binary, states),
            SAMPLER: json.dumps(sampler, indent=2) + '\n',
            LOAD_DROP: '[Service]\nExecStart=\nExecStart=' + load_command + '\n',
        }
        if override is not None:
            # Keep the override and point it at the candidate, so rollback
            # restores the pinned bytes and running executable together.
            desired[STATUS_HTTPS_DROP] = exec_override(units.exec_start(units.effective([desired[UNIT_PATH]])))
        before = {path: {'text': self.read(path), 'mode': (resource['files'][path] or {}).get('mode', 0o600)} for path in desired}
        serving = self.serving.capture() if self.serving else None
        return {'serving': serving, 'request_sha256': durable.digest(self.request), 'before': before, 'units': states,
                'desired': desired, 'binary': binary, 'executables': executables,
                'previous_transaction': previous and previous['id']}

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
        if self.supervision and journal['phase'] not in ('restoring', 'rolled-back'):
            self.supervision.guard()
        journal['intent'] = name
        self.save(journal)
        self.lock.verify()
        action()
        journal['completed'].append(name)
        journal['intent'] = None
        self.save(journal)

    def require_stopped_load(self, state=None):
        """Preserve a reviewed disk-only incident while its stopped owner is repaired."""
        state = state if state is not None else self.ex.probe_unit(self.host, LOAD)
        require(state['active_state'] in ('inactive', 'failed') and state['main_pid'] == 0,
                'resume_load:false requires the continuous exact load stopped with MainPID 0')
        observed = self.ex.sha256(self.host, LOAD_LATCH)
        expected = self.request.get('expected_disk_latch_sha256')
        if expected is None:
            require(observed is None, 'continuous load has an unresolved critical latch: ' + LOAD_LATCH)
            return
        require(observed == expected, 'reviewed disk latch changed or disappeared')
        try:
            incident = json.loads(self.read(LOAD_LATCH))
        except (TypeError, ValueError) as error:
            raise DeployError('reviewed disk latch is unreadable') from error
        reasons = incident.get('reason') if isinstance(incident, dict) else None
        allowed = {name + ': disk headroom below 20%' for name in
                   ('transparent-pir-v12-archive', 'transparent-pir-v12-recent', 'transparent-router')}
        require(isinstance(incident, dict) and set(incident) == {'utc', 'reason'}
                and isinstance(reasons, list) and reasons and all(isinstance(r, str) and r in allowed for r in reasons),
                'only the reviewed disk-headroom incident may remain during a stopped-checker repair')

    def stop(self, name):
        self.ex.systemctl(self.host, 'stop', name)
        observed = self.ex.probe_unit(self.host, name)
        require(not observed['main_pid'] and observed['active_state'] in ('inactive', 'failed'), 'owner did not stop: ' + name)

    def stop_candidate_for_restore(self, journal):
        """Bound rollback of our candidate without terminating another owner.

        Runtime preparation can outlive SIGTERM. Stop the service first, then
        terminate only the same candidate invocation if its graceful stop stalls.
        The ordinary predecessor stop path keeps its existing behavior.
        """
        before = self.ex.probe_unit(self.host, UNIT)
        candidate = self.request['release']['binary_sha256']
        if not before['main_pid'] or before['exe_sha256'] != candidate:
            require(not before['main_pid'] or before['exe_sha256'] == journal['plan']['units'][UNIT]['exe_sha256'],
                    'publisher owner changed before recovery stop')
            return self.stop(UNIT)
        self.ex.systemctl(self.host, 'stop', UNIT, '--no-block')
        deadline = self.clock() + 60
        forced = False
        while True:
            self.lock.verify()
            state = self.ex.probe_unit(self.host, UNIT)
            if not state['main_pid'] and state['active_state'] in ('inactive', 'failed'):
                return
            require(state['invocation_id'] == before['invocation_id'] and
                    all(state[key] == before[key] for key in ('fragment_text', 'drop_ins')) and
                    (not state['main_pid'] or (state['main_pid'] == before['main_pid'] and
                                               state['exe_sha256'] == candidate)),
                    'publisher invocation changed during recovery stop')
            if self.clock() >= deadline:
                require(not forced, 'candidate publisher did not stop after termination')
                def terminate():
                    try:
                        self.ex.systemctl(self.host, 'kill', UNIT, '--signal=SIGKILL')
                    except RemoteError as error:
                        # systemd can report an auxiliary-process error after
                        # successfully stopping the main process. Accept only
                        # that known case, with independently observed teardown.
                        if 'Failed to send signal SIGKILL to auxiliary processes' not in str(error):
                            raise
                        stopped = self.ex.probe_unit(self.host, UNIT)
                        require(not stopped['main_pid'] and stopped['active_state'] in ('inactive', 'failed'),
                                'candidate publisher remains active after auxiliary-process signal error')
                self.effect(journal, 'terminate stalled candidate publisher', terminate)
                forced = True
                deadline = self.clock() + 30
            self.sleep(1)

    def unlocked(self):
        config = self.request['configuration']
        history, display = (config[name]['settings'] for name in PRODUCTS)
        paths = [history['data_dir'] + '/writer.lock', history['publication_root'] + '/controller.lock',
                 display['journal'] + '/writer.lock', display['root'] + '/controller.lock']
        if self.expanded:
            from .publisher_products import locks
            paths += locks(config)
        if 'prepared_storage' in self.request:
            from .publisher_storage import Handoff
            paths += [pair['source'] + '/controller.lock'
                      for pair in Handoff(self).manifest()['plan']['roots'].values()]
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

    def unchanged_during_staging(self, plan):
        require(all(self.read(path) == previous['text'] for path, previous in plan['before'].items()),
                'managed files changed during staging')
        require(all(stable_unit(self.ex.probe_unit(self.host, name)) == before
                    for name, before in plan['units'].items()), 'publisher fleet changed during staging')
        if self.serving:
            self.serving.unchanged_during_staging(plan['serving'])

    def deploy(self, binary, expected_plan, serving_binary=None):
        require(file_sha256(binary) == self.request['release']['binary_sha256'] and
                Path(binary).stat().st_size == self.request['release']['binary_bytes'], 'publisher executable does not match request')
        require(self.request['schema'] != ENHANCE_HTTPS_SCHEMA or self.supervision is not None,
                'Enhance HTTPS deployment requires the independent supervised runner')
        with production_lock(self.inventory, self.ex) as self.lock:
            plan = self.capture()
            require(durable.digest(plan) == expected_plan, 'publisher plan is stale; review a fresh plan')
            identifier = self.identifier or new_id('publisher', self.request['release']['binary_sha256'])
            journal = {'schema': self.request['schema'], 'id': identifier, 'phase': 'staging', 'request': self.request,
                       'plan': plan, 'completed': [], 'intent': None}
            self.save(journal)
            self.ex.write(self.host, ROOT_DIR + '/latest.json', durable.canonical({'id': identifier}), 0o600)
            self.effect(journal, 'stage executable', lambda: self.stage(binary, plan['binary']))
            staged_config = ROOT_DIR + '/transactions/' + identifier + '/publisher.json'
            self.effect(journal, 'stage configuration', lambda: self.write(staged_config, plan['desired'][CONFIG]))
            code, output = self.ex.run(self.host, [plan['binary'], '--config', staged_config, '--check'], 60)
            require(code == 0, 'staged publisher refuses its configuration: ' + output[-1000:])
            if self.serving:
                self.serving.stage(journal, serving_binary)
            if self.request['schema'] == SERVING_SCHEMA and 'prepared_storage' not in self.request:
                # Fully verifies a pin of the active History publication while
                # the predecessor serves; a refusal never stops it.
                from .publisher_restore import HistoryRestore
                HistoryRestore(self).prepare(journal, plan['binary'], staged_config)
            self.unchanged_during_staging(plan)
            self.resources()
            self.save(journal, 'installing')
            if self.supervision:
                self.supervision.arm()
            if self.resume_load:
                self.effect(journal, 'pause continuous load', lambda: self.stop(LOAD))
            else:
                self.require_stopped_load()
            self.effect(journal, 'stop ' + UNIT, lambda: self.stop(UNIT))
            self.unlocked()
            from .publisher_restore import HistoryRestore
            if 'prepared_storage' in self.request:
                from .publisher_storage import Handoff
                Handoff(self).install(journal)
            else:
                HistoryRestore(self).install(journal)
            enhance_https = self.request['schema'] == ENHANCE_HTTPS_SCHEMA
            if enhance_https:
                self.serving.prepare_handoff(journal)
            elif self.serving:
                self.serving.activate(journal)
            for path, text in plan['desired'].items():
                mode = 0o644 if path.startswith('/etc/systemd/') else 0o600
                self.effect(journal, 'install ' + path, lambda path=path, text=text, mode=mode: self.write(path, text, mode))
            self.effect(journal, 'reload services', lambda: self.ex.systemctl(self.host, 'daemon-reload'))
            self.effect(journal, 'enable publisher', lambda: self.ex.systemctl(self.host, 'enable', UNIT))
            self.effect(journal, 'start publisher', lambda: self.ex.systemctl(self.host, 'start', UNIT))
            self.save(journal, 'verifying')
            self.ready(True)
            if enhance_https:
                self.serving.route(journal)
                self.serving.publication_ready()
                self.serving.activate(journal)
            if self.serving:
                self.serving.verify(journal)
                self.serving.route(journal)
            self.exact(journal)
            if self.supervision:
                self.serving.verify_advancement(journal)
                self.exact(journal)
            if self.resume_load:
                self.effect(journal, 'resume continuous load', lambda: self.ex.systemctl(self.host, 'start', LOAD))
            else:
                self.require_stopped_load()
            if self.supervision:
                self.supervision.accept({'products': {name: {'ready': True, 'advanced': True, 'exact': True}
                    for name in self.request['checks']}, 'progress': journal['product_progress'],
                    'request_sha256': durable.digest(self.request)})
            self.save(journal, 'committed')
            return journal

    def continue_installation(self):
        """Resume only a lost connection after the recorded predecessor stop."""
        require(self.request['schema'] == SERVING_SCHEMA and not self.resume_load,
                'installation recovery requires publisher-cutover/3 with its checker stopped')
        with production_lock(self.inventory, self.ex) as self.lock:
            journal = self.latest()
            from .publisher_restore import staged_effects
            require(journal is not None and journal['request'] == self.request
                    and journal['phase'] == 'installing'
                    and journal['intent'] == 'stop ' + UNIT
                    and journal['completed'] == ['stage executable', 'stage configuration',
                                                 *staged_effects(journal)],
                    'installation recovery requires the interrupted predecessor stop')
            plan = journal['plan']
            self.resources()
            require(all(self.read(path) == previous['text'] for path, previous in plan['before'].items()),
                    'managed file changed before installation recovery')
            require(self.ex.sha256(self.host, plan['binary']) == self.request['release']['binary_sha256'],
                    'staged publisher changed before installation recovery')
            for name, before in plan['units'].items():
                observed = stable_unit(self.ex.probe_unit(self.host, name))
                if name == UNIT:
                    require(not observed['main_pid'] and observed['active_state'] in ('inactive', 'failed')
                            and all(observed[k] == before[k] for k in
                                    ('fragment_text', 'drop_ins', 'fragment_path', 'unit_file_state')),
                            'predecessor is not stopped on its recorded configuration')
                else:
                    require(observed == before, 'publisher fleet changed before installation recovery')
            if not self.resume_load:
                self.require_stopped_load()
            if self.serving:
                self.serving.unchanged_during_staging(plan['serving'])
            self.unlocked()
            from .publisher_restore import HistoryRestore
            if 'prepared_storage' in self.request:
                from .publisher_storage import Handoff
                Handoff(self).install(journal)
            else:
                HistoryRestore(self).install(journal)
            if self.serving:
                self.serving.activate(journal)
            for path, text in plan['desired'].items():
                mode = 0o644 if path.startswith('/etc/systemd/') else 0o600
                self.effect(journal, 'install ' + path, lambda path=path, text=text, mode=mode: self.write(path, text, mode))
            self.effect(journal, 'reload services', lambda: self.ex.systemctl(self.host, 'daemon-reload'))
            self.effect(journal, 'enable publisher', lambda: self.ex.systemctl(self.host, 'enable', UNIT))
            self.effect(journal, 'start publisher', lambda: self.ex.systemctl(self.host, 'start', UNIT))
            self.save(journal, 'verifying')
        return self.verify()

    def verify(self):
        """Finish checks after a lost connection, without restarting a running candidate."""
        require(self.request['schema'] == SERVING_SCHEMA, 'verification recovery requires publisher-cutover/3')
        with production_lock(self.inventory, self.ex) as self.lock:
            journal = self.latest()
            require(journal is not None and journal['request'] == self.request
                    and journal['phase'] == 'verifying' and journal['intent'] is None,
                    'verification recovery requires this interrupted verification')
            self.resources()
            plan = journal['plan']
            def unchanged():
                require(all(self.read(path) == text for path, text in plan['desired'].items()),
                        'managed file changed before verification recovery')
                state = self.ex.probe_unit(self.host, UNIT)
                require(state['active_state'] == 'active' and not state['need_daemon_reload']
                        and state['exe_sha256'] == self.request['release']['binary_sha256'],
                        'verification recovery requires the running candidate')
                return state['invocation_id']
            invocation = unchanged()
            if not self.resume_load:
                self.require_stopped_load()
            if self.serving:
                self.serving.restore_guard(journal)
            self.ready(True)
            if self.serving:
                self.serving.verify(journal)
                edge = plan['serving']
                if self.read('/etc/caddy/Caddyfile') == edge['edge_before']:
                    self.serving.route(journal)
                else:
                    self.effect(journal, 'verify reload wallet edge',
                                lambda: self.ex.systemctl(self.host, 'reload', 'caddy.service'))
            self.exact(journal)
            require(unchanged() == invocation, 'publisher invocation changed during verification recovery')
            if self.resume_load:
                self.effect(journal, 'resume continuous load', lambda: self.ex.systemctl(self.host, 'start', LOAD))
            else:
                self.require_stopped_load()
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
            require(self.ex.probe_unit(self.host, UNIT)['exe_sha256'] == self.request['release']['binary_sha256'],
                    'running publisher executable changed before restart')
            if not self.resume_load:
                self.require_stopped_load()
            self.save(journal, 'restarting')
            if self.resume_load:
                self.effect(journal, 'restart pause load', lambda: self.stop(LOAD))
            self.effect(journal, 'restart stop publisher', lambda: self.stop(UNIT))
            self.unlocked()
            self.effect(journal, 'restart start publisher', lambda: self.ex.systemctl(self.host, 'start', UNIT))
            self.ready(True)
            self.exact(journal)
            if self.resume_load:
                self.effect(journal, 'restart resume load', lambda: self.ex.systemctl(self.host, 'start', LOAD))
            else:
                self.require_stopped_load()
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
            return self.restore_transaction()

    def restore_transaction(self):
        """Roll the latest transaction back; the caller holds the production lock."""
        require(self.ex.publisher_resources(self.host, [CONFIG])['machine_id'] == self.request['machine_id'],
                'coordinator machine identity differs')
        journal = self.latest()
        require(journal is not None, 'no publisher cutover to restore')
        require(journal['request'] == self.request, 'request differs from latest publisher cutover')
        if journal['phase'] == 'rolled-back':
            return journal
        require('storage_cleanup' not in journal, 'storage cleanup retired rollback for this cutover')
        plan = journal['plan']
        for path, digest in plan['executables'].items():
            require(self.ex.sha256(self.host, path) == digest, 'rollback executable changed: ' + path)
        for path, desired in plan['desired'].items():
            allowed = [plan['before'][path]['text'], desired]
            if path == UNIT_PATH and 'sandbox_repair' in journal:
                allowed.append(journal['sandbox_repair']['before'])
            if path in journal.get('release_repair', {}).get('before', {}):
                allowed.append(journal['release_repair']['before'][path])
            require(self.read(path) in allowed, 'managed file changed outside transaction: ' + path)
        for name in self.old:
            observed = stable_unit(self.ex.probe_unit(self.host, name))
            require(all(observed[key] == plan['units'][name][key] for key in ('fragment_text', 'drop_ins')),
                    'predecessor service configuration changed: ' + name)
        if self.serving:
            self.serving.restore_guard(journal)
        from .publisher_restore import HistoryRestore
        history_restore = HistoryRestore(self)
        history_restore.guard(journal)
        if self.request['schema'] == HTTPS_SCHEMA:
            self.serving.cleanup_staging(journal)
        if journal['phase'] == 'staging':
            self.unchanged_during_staging(plan)
            history_restore.rollback(journal)
            journal['staging_only'] = True
            self.save(journal, 'rolled-back')
            return journal
        self.save(journal, 'restoring')
        for name in (LOAD, UNIT, *self.old):
            if self.ex.probe_unit(self.host, name)['load_state'] != 'not-found':
                self.effect(journal, 'restore stop ' + name,
                            lambda name=name: self.stop_candidate_for_restore(journal) if name == UNIT else self.stop(name))
        self.unlocked()
        if self.serving:
            self.serving.restore(journal)
        history_restore.rollback(journal)
        if 'prepared_storage' in self.request:
            from .publisher_storage import Handoff
            Handoff(self).rollback(journal)
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
        if self.serving:
            self.serving.verify(journal, restored=True)
        self.exact(journal)
        if plan['units'][LOAD]['active_state'] == 'active':
            self.effect(journal, 'restore continuous load', lambda: self.ex.systemctl(self.host, 'start', LOAD))
        self.save(journal, 'rolled-back')
        return journal


def register(commands):
    cleanup = commands.add_parser('publisher-cutover-prune-qualification', allow_abbrev=False)
    cleanup.add_argument('--host', action='append', required=True)
    cleanup.add_argument('--confirm-path', required=True, choices=('/srv/enhance-pir-v7/qualification-worker',))
    for operation in ('plan', 'preflight', 'deploy', 'status', 'reconcile', 'rollback', 'restart', 'verify', 'continue', 'repair-enhance-plan', 'repair-enhance', 'repair-sandbox-plan', 'repair-sandbox', 'repair-release-plan', 'repair-release'):
        command = commands.add_parser('publisher-cutover-' + operation, allow_abbrev=False)
        command.add_argument('--request', required=True)
        command.add_argument('--request-sha256', required=True)
        command.add_argument('--serving-binary')
        command.add_argument('--cuda-binary')
        if operation in ('repair-enhance', 'repair-sandbox', 'repair-release'):
            command.add_argument('--expect-plan-sha256', required=True)
        if operation in ('preflight', 'deploy', 'repair-release-plan', 'repair-release'):
            command.add_argument('--archive', required=True)
        if operation == 'deploy':
            command.add_argument('--expect-plan-sha256', required=True)
            command.add_argument('--supervision', choices=('coordinator', 'operator'), default='coordinator')

    for operation in ('run', 'watchdog'):
        command = commands.add_parser('publisher-cutover-' + operation, allow_abbrev=False)
        command.add_argument('--transaction', required=True)
        command.add_argument('--supervision', choices=('coordinator', 'operator'), default='coordinator')


def dispatch(args, executor=None, **_options):
    from .remote import SSHExecutor
    operation = args.command.removeprefix('publisher-cutover-')
    if operation == 'prune-qualification':
        from .enhance_https import prune_qualification
        return prune_qualification(args, executor)
    if operation in ('run', 'watchdog'):
        if args.supervision == 'operator':
            from .cutover_operator import dispatch as supervised
            return supervised(args)
        from .cutover_runner import dispatch as supervised
        return supervised(args)
    request, inventory = load_request(args.request, args.request_sha256,
                                      recovery=operation in ('status', 'rollback', 'reconcile', 'restart', 'verify', 'continue',
                                                         'repair-enhance-plan', 'repair-enhance',
                                                         'repair-sandbox-plan', 'repair-sandbox',
                                                         'repair-release-plan', 'repair-release'))
    executor = executor or SSHExecutor(inventory)
    target = Cutover(request, inventory, executor, args.state_dir)
    if target.serving:
        target.serving.binary = getattr(args, 'serving_binary', None)
        if request['schema'] == ENHANCE_HTTPS_SCHEMA:
            target.serving.cuda_binary = getattr(args, 'cuda_binary', None)
    if operation in ('repair-enhance-plan', 'repair-enhance'):
        from .enhance_repair import dispatch as repair
        return repair(target, args)
    if operation in ('repair-sandbox-plan', 'repair-sandbox'):
        from .publisher_sandbox import dispatch as repair_sandbox
        return repair_sandbox(target, args)
    if operation in ('repair-release-plan', 'repair-release'):
        from .publisher_release_repair import dispatch as repair_release
        return repair_release(target, args)
    if operation in ('rollback', 'reconcile'):
        return target.rollback()
    if operation == 'continue':
        return target.continue_installation()
    if operation == 'restart':
        return target.restart()
    if operation == 'verify':
        return target.verify()
    if operation == 'status':
        result = {'transaction': target.latest(), 'units': {name: stable_unit(executor.probe_unit(request['host'], name))
                  for name in (*target.old, UNIT, LOAD)}}
        if target.serving:
            result['serving_units'] = {t.key: stable_unit(executor.probe_unit(t.host, t.unit))
                                       for t in target.serving.deployer.targets}
            result['wallet_edge_sha256'] = sha(target.read('/etc/caddy/Caddyfile'))
            if request['schema'] == HTTPS_SCHEMA:
                result['https_consumers'] = {c['host'] + '/' + c['unit']:
                    stable_unit(executor.probe_unit(c['host'], c['unit'])) for c in request['https']['consumers']}
                result['history_edge_sha256'] = sha(executor.read(request['https']['history_edge']['host'],
                                                                 '/etc/caddy/Caddyfile'))
        if request['schema'] == ENHANCE_HTTPS_SCHEMA:
            from .cutover_operator import status as operator_status
            operator = operator_status(args.state_dir, args.request_sha256)
            if operator is not None:
                result['operator_supervision'] = operator
            pointer = target.read(ROOT_DIR + '/supervised-latest.json')
            if pointer is not None:
                from .cutover_guard import unit_name
                pointer = json.loads(pointer)
                unit_name(pointer['id'])
                if pointer['request_sha256'] == args.request_sha256:
                    result['supervision'] = json.loads(target.read(ROOT_DIR + '/transactions/' + pointer['id'] + '/decision.json'))
        return result
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
        if target.serving:
            target.serving.binary = args.serving_binary
        try:
            return target.deploy(binary, args.expect_plan_sha256, args.serving_binary)
        except Exception:
            latest = target.latest()
            if target.serving and latest and latest['request'] == request and latest['phase'] not in FINAL:
                target.rollback()
            raise

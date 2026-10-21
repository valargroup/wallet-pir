"""Worker or controller binary upgrade for the current v12 history and 4k8 display fleets.

One transaction changes one component's executable: the workers, or the
controller (staged under `<service root>/controller`, so a worker binary can
never be selected as the controller). An explicit History staging_adapter request
may also change only its controller's fleet_command. The release identity is the request's Git
revision and binary digest; the publication identity is everything the
controller owns (configuration, fleet bytes, source revision, journal, roots)
plus each worker's ready identity, and it must be identical before, after each
restart and after rollback. Staging, journals, rollback and the production lock
are the shared engine's; this module adds only the product refusals. Anything
outside the supported topology refuses rather than being adapted.

When the inventory's unified publisher manages the product, `pir-publisher.service`
is the controller authority: its product settings in the installed publisher
configuration replace the standalone controller's, and the product's own status
listener reports authority. Only worker transactions run here then; the
publisher's executable changes through `publisher-cutover-*`.
"""
import dataclasses
import hashlib
import json
import os
import re
from pathlib import Path

from wallet_pir_ops import durable
from wallet_pir_ops.deploy import descriptors, units
from wallet_pir_ops.deploy.engine import Deployer, DeployError

SCHEMA = 'transparent-binary-upgrade/1'
GIT = re.compile('^[0-9a-f]{40}$')
SHA256 = re.compile('^[0-9a-f]{64}$')
# The supported topologies; another unit set is unknown state, not a variant.
PRODUCTS = {
    'transparent-history': {
        'workers': ('transparent-v12-archive.service', 'transparent-v12-recent.service'),
        # Existing product upgrade budget, not the binary engine's generic 180 seconds.
        'worker_warm_seconds': 1800,
        'controller': 'transparent-v12-publish-controller.service',
        'config': '/opt/transparent-publisher/v12/controller.json',
        # The product's section of the unified publisher configuration and the
        # setting naming its status listener.
        'publisher_product': 'transparent_history',
        'status_listen': 'listen',
        # Values the controller configuration must hold; a single journal
        # writer (follow_journal false) and no shipped runtimes.
        'config_equals': {'follow_journal': False, 'data_dir': '/srv/transparent-activity/full-v3/journal',
                          'publication_root': '/srv/transparent-activity/full-v12/publications'},
        'config_absent_or': {'ship_runtimes': False},
        'publication_keys': ('source_sha',),
        # Ownership a restart must not change. The map, assignment digests,
        # replica group and shard count follow the controller's current
        # `<map>.assignment.json`, which changes with every publication.
        'ready_keys': ('role', 'worker_id'),
        'roles': ('archive-owner', 'recent-replica'),
        # The continuous exact-answer load latches on any worker hash other
        # than its pins; a worker upgrade pauses it and re-pins it.
        'load': {'unit': 'transparent-v2-5qps-continuous.service', 'root': '/srv/transparent-activity/canonical-load/v12'},
    },
    'transparent-display': {
        'workers': ('transparent-txid-display-worker.service',),
        'worker_warm_seconds': 7200,
        'controller': 'transparent-txid-display-k8-controller.service',
        # Run mode takes no --geometry: root/display-root.json pins txid-4k8,
        # so the request pins that file's digest under controller.files.
        'controller_root_file': 'display-root.json',
        'publisher_product': 'transaction_details',
        'status_listen': 'status_listen',
        'publication_keys': ('root',),
        'ready_keys': ('role',),
        'roles': ('combined',),
        'load': None,
    },
}
# pir-apm on the controller host. Its configuration names each service's alert_mode
# but is read only at APM start; the incident database records the mode each family
# last evaluated in, and monitor-status reports delivery. Read for reporting only.
# The workers' release executable (the descriptors' `binary`).
SERVER = 'pir-query-server'
APM_CONFIG = '/etc/pir-quality/transparent-production.json'
APM_STATE = '/var/lib/pir-apm/incidents.sqlite'
APM_LISTEN = '127.0.0.1:3002'
APM_SERVICE = {'transparent-history': 'transparent', 'transparent-display': 'txid'}
REQUEST_KEYS = {'schema', 'service', 'component', 'release', 'publication', 'controller', 'workers', 'load'}
COMPONENTS = ('workers', 'controller')
# The unified owner, as installed by `publisher-cutover-*`.
PUBLISHER_UNIT = 'pir-publisher.service'
PUBLISHER_CONFIG = '/etc/wallet-pir/publisher.json'
# Autonomous HTTPS History providers (publisher `http_only`): each worker pins its
# configuration and unit, and its holdings decide the only ready identity it may
# report. A full-holdings provider has no assignment; a recent provider owns the
# `assignment.json` its own bootstrap staged (routing/manager.rs).
PROVIDER_PIN_KEYS = {'ready_identity', 'limits', 'args', 'config_sha256', 'unit_sha256'}
PROVIDER_IDENTITY = {True: {'role': None, 'worker_id': None},
                     False: {'role': 'recent-replica', 'worker_id': 'provider'}}


def problem_if(condition, message, problems):
    if condition:
        problems.append(message)


def load_request(path, expected_sha256):
    """The pinned request; its bytes must hash to the reviewed digest."""
    with open(path, 'rb') as handle:
        data = handle.read()
    if hashlib.sha256(data).hexdigest() != expected_sha256:
        raise DeployError('request checksum differs from --request-sha256')
    request = json.loads(data)
    validate(request)
    return request


def validate(request):
    def require(condition, message):
        if not condition:
            raise DeployError('invalid upgrade request: ' + message)
    require(isinstance(request, dict) and set(request) in (REQUEST_KEYS, REQUEST_KEYS | {'staging_adapter'}), 'keys must be %s' % sorted(REQUEST_KEYS))
    require(request['schema'] == SCHEMA, 'schema must be ' + SCHEMA)
    product = PRODUCTS.get(request['service'])
    require(product is not None, 'service must be one of %s' % sorted(PRODUCTS))
    require(request['component'] in COMPONENTS, 'component must be one of %s' % list(COMPONENTS))
    load = request['load']
    workers = request['workers']
    require(isinstance(workers, dict) and workers, 'workers maps target keys to pinned state')
    providers = request['service'] == 'transparent-history' and all(
        isinstance(p, dict) and set(p) == PROVIDER_PIN_KEYS for p in workers.values())
    if providers:
        require(request['component'] == 'workers' and load is None,
                'HTTPS providers have only worker transactions and no fleet load pins; load must be null')
    elif product['load'] is None:
        require(load is None, '%s has no continuous load to coordinate; load must be null' % request['service'])
    else:
        require(isinstance(load, dict) and set(load) == {'host', 'pins_sha256', 'resume_after_upgrade'} and isinstance(load['host'], str)
                and bool(SHA256.match(load['pins_sha256'] or '')) and type(load['resume_after_upgrade']) is bool,
                'load is {host, pins_sha256, resume_after_upgrade}')
    release = request['release']
    require(isinstance(release, dict) and set(release) == {'git_sha', 'binary_sha256'}, 'release is {git_sha, binary_sha256}')
    require(bool(GIT.match(release['git_sha'])), 'release.git_sha is a full 40-digit revision')
    require(bool(SHA256.match(release['binary_sha256'])), 'release.binary_sha256 is 64 lowercase hex digits')
    publication = request['publication']
    require(isinstance(publication, dict) and set(publication) == set(product['publication_keys'])
            and all(isinstance(v, str) and v for v in publication.values()),
            'publication names exactly %s' % list(product['publication_keys']))
    controller = request['controller']
    require(isinstance(controller, dict) and set(controller) == {'host', 'unit_sha256', 'files'},
            'controller is {host, unit_sha256, files}')
    require(bool(SHA256.match(controller['unit_sha256'])), 'controller.unit_sha256 pins its effective unit')
    files = controller['files']
    require(isinstance(files, dict) and files and all(p.startswith('/') and SHA256.match(d or '') for p, d in files.items()),
            'controller.files maps absolute paths to sha256')
    if 'config' in product:
        # Which file holds the settings depends on the owner; controller_problems checks it.
        require(product['config'] in files or PUBLISHER_CONFIG in files,
                'controller.files must pin %s or %s' % (product['config'], PUBLISHER_CONFIG))
    for key, pinned in workers.items():
        if providers:
            require(all(SHA256.match(pinned[k] or '') for k in ('config_sha256', 'unit_sha256')),
                    '%s pins config_sha256 and unit_sha256' % key)
            require(pinned['ready_identity'] in PROVIDER_IDENTITY.values(),
                    '%s.ready_identity must be a full or recent provider identity' % key)
        else:
            require(isinstance(pinned, dict) and set(pinned) == {'ready_identity', 'limits', 'args'},
                    '%s is {ready_identity, limits, args}, or every worker pins %s' % (key, sorted(PROVIDER_PIN_KEYS)))
            ready = pinned['ready_identity']
            require(isinstance(ready, dict) and set(ready) == set(product['ready_keys']),
                    '%s.ready_identity pins exactly %s' % (key, list(product['ready_keys'])))
            require(ready['role'] in product['roles'], '%s.ready_identity.role must be one of %s' % (key, list(product['roles'])))
        require(isinstance(pinned['limits'], dict) and isinstance(pinned['args'], list), '%s limits/args' % key)
    adapter = request.get('staging_adapter')
    if 'staging_adapter' in request:
        require(request['service'] == 'transparent-history' and request['component'] == 'controller',
                'a staging adapter belongs only to the History controller')
        require(isinstance(adapter, dict) and set(adapter) == {'sha256', 'configuration', 'previous_configuration'},
                'staging_adapter is {sha256, configuration, previous_configuration}')
        require(isinstance(adapter['sha256'], str) and bool(SHA256.fullmatch(adapter['sha256'])), 'invalid staging adapter digest')
        require(isinstance(adapter['configuration'], str), 'adapter configuration must be JSON text')
        try:
            configuration = json.loads(adapter['configuration'])
        except ValueError as error:
            raise DeployError('invalid adapter configuration JSON') from error
        require(isinstance(configuration, dict) and configuration.get('fleet_command') == adapter_path(adapter),
                'adapter configuration must select its immutable digest path')
        previous = adapter['previous_configuration']
        require(isinstance(previous, str) and hashlib.sha256(previous.encode()).hexdigest() == files[product['config']],
                'previous adapter configuration must match the pinned controller file')
        try:
            old = json.loads(previous)
        except ValueError as error:
            raise DeployError('invalid previous adapter configuration JSON') from error
        require(isinstance(old, dict) and {**old, 'fleet_command': adapter_path(adapter)} == configuration,
                'staging adapter update may change only controller fleet_command')
    return request


def adapter_path(adapter):
    return '/opt/transparent-publisher/v12/adapters/%s/transparent-live-fleet.py' % adapter['sha256']


def notification_state(ex, host, product_service):
    """Report lines on alert delivery for this service, from the running APM, never inferred.

    A running observer or continuous load proves exact-answer coverage, not alert
    delivery. The configuration on disk is labelled as configured only: APM reads
    it at start, so it can disagree with the running process. The running mode is
    the one each family last recorded in APM's incident database, and delivery
    health comes from APM's monitor-status; either is unknown when unreadable.
    """
    from tools.transparent.lib import alert_activation
    service = APM_SERVICE[product_service]
    lines = []
    try:
        config = json.loads(ex.read(host, APM_CONFIG) or 'null')
        configured = {s['service']: s.get('alert_mode', 'shadow') for s in config['services']}.get(service)
    except (TypeError, KeyError, ValueError):
        configured = None
    lines.append('notifications: %s alert_mode configured on disk (read only at APM start): %s'
                 % (service, configured or 'unknown'))
    modes = alert_activation.running_modes(ex, host, APM_STATE)
    running = None if modes is None else modes.get('mode:quality-' + service)
    lines.append('notifications: %s mode of the running APM: %s' % (
        service, running or ('unknown (incident database unreadable)' if modes is None else 'unknown (not recorded)')))
    if configured and running and configured != running:
        lines.append('notifications: configured %s differs from running %s; APM has not restarted since the change'
                     % (configured, running))
    lines.append(alert_activation.delivery_line('notifications: APM', alert_activation.monitor_status(ex, host, APM_LISTEN)))
    if running != 'active':
        lines.append('notifications: %s incidents during this upgrade are recorded only; Slack is not notified' % service)
    return lines


def unit_digest(state):
    """The normalized effective unit: every setting and argument, with the ExecStart binary abstracted."""
    config = units.effective([state['fragment_text'] or ''] + [d['text'] for d in state['drop_ins']])
    return hashlib.sha256(json.dumps(units.normalized(config), sort_keys=True).encode()).hexdigest()


class Upgrade:
    """The product refusals for one pinned request; every method only reads."""

    def __init__(self, request, service, inventory, publisher=None):
        self.request = request
        self.product = PRODUCTS[request['service']]
        self.service = service
        self.inventory = inventory
        # The unified publisher's descriptor, when the registry defines one.
        self.publisher = publisher
        self.unified = False
        self.digest = durable.digest(request)
        self.providers = all('config_sha256' in pinned for pinned in request['workers'].values())

    def unified_owner(self, controllers):
        """The single pir-publisher server managing this product, None when none does, or refusal."""
        spec = self.inventory.services.get('publisher')
        if spec is None or self.product['publisher_product'] not in (spec.get('managed_products') or []):
            return None
        if controllers:
            raise DeployError('unsupported topology: %s and a standalone controller both own %s'
                              % (PUBLISHER_UNIT, self.request['service']))
        if self.publisher is None:
            raise DeployError('the inventory names a unified publisher; pass --descriptors '
                              'tools/deploy/publisher/transparent-services.toml')
        servers = descriptors.targets(self.publisher, self.inventory)
        if [(t.host, t.unit) for t in servers] != [(self.request['controller']['host'], PUBLISHER_UNIT)]:
            raise DeployError('unsupported topology: the unified owner must be %s on %s'
                              % (PUBLISHER_UNIT, self.request['controller']['host']))
        return servers[0]

    def worker_targets(self):
        """Exactly the supported worker set, or refusal."""
        everything = descriptors.targets(self.service, self.inventory)
        workers = [t for t in everything if t.role.name == 'worker']
        others = [t.key for t in everything if t.role.name == 'external-worker']
        if others:
            raise DeployError('unsupported topology: external workers %s are present' % others)
        units_ = sorted(t.unit for t in workers)
        if units_ != sorted(self.product['workers']):
            raise DeployError('unsupported topology: workers %s, expected exactly %s' % (units_, list(self.product['workers'])))
        if sorted(t.key for t in workers) != sorted(self.request['workers']):
            raise DeployError('request pins workers %s but the inventory has %s'
                              % (sorted(self.request['workers']), sorted(t.key for t in workers)))
        controllers = [t for t in everything if t.role.name == 'controller']
        owner = self.unified_owner(controllers)
        if owner is not None:
            # The product's status listener is read from the installed configuration.
            self.unified, self.controller, self.controller_url = True, owner, None
        elif [(t.host, t.unit) for t in controllers] != [(self.request['controller']['host'], self.product['controller'])]:
            raise DeployError('unsupported topology: controller must be %s on %s'
                              % (self.product['controller'], self.request['controller']['host']))
        else:
            self.controller = controllers[0]
            self.controller_url = self.controller.url(self.controller.role.health)
        workers = [dataclasses.replace(t, role=dataclasses.replace(t.role, ready_timeout=self.product['worker_warm_seconds']))
                   for t in workers]
        self.workers = workers
        return workers

    def component(self, ex):
        """The targets and service this transaction changes; the controller gets its own release root and name."""
        workers = self.worker_targets()
        if self.request['component'] == 'workers':
            return workers, self.service
        if self.unified:
            raise DeployError('%s owns the %s controller; change its executable with publisher-cutover-* '
                              '(docs/publisher.md), not transparent-binary-*' % (PUBLISHER_UNIT, self.request['service']))
        state = ex.probe_unit(self.controller.host, self.controller.unit)
        try:
            binary = units.split_exec(units.exec_start(units.effective(
                [state['fragment_text'] or ''] + [d['text'] for d in state['drop_ins']])))[1]
        except units.UnitError as error:
            raise DeployError('controller ExecStart: %s' % error) from error
        name = os.path.basename(binary)
        if name == self.service.binary:
            raise DeployError('controller runs %s, the worker executable; refusing to select it as the controller' % binary)
        return [self.controller], dataclasses.replace(self.service, root=self.service.root + '/controller', binary=name)

    def settings_path(self):
        return PUBLISHER_CONFIG if self.unified else self.product.get('config')

    def controller_settings(self, ex):
        """The controller's product settings: the standalone file, or the enabled product's publisher section."""
        text = ex.read(self.request['controller']['host'], self.settings_path())
        document = json.loads(text) if text else {}
        if not self.unified:
            return document
        section = document.get(self.product['publisher_product']) if isinstance(document, dict) else None
        if not isinstance(section, dict) or section.get('enabled') is not True or not isinstance(section.get('settings'), dict):
            raise ValueError('%s does not enable %s' % (PUBLISHER_CONFIG, self.product['publisher_product']))
        return section['settings']

    def status_url(self, ex):
        if self.controller_url is not None:
            return self.controller_url
        return 'http://%s/v1/status' % self.controller_settings(ex)[self.product['status_listen']]

    def controller_problems(self, ex, *, check_authority=True):
        problems = []
        controller = self.request['controller']
        host = controller['host']
        state = ex.probe_unit(host, self.controller.unit)
        problem_if(check_authority and state['active_state'] != 'active', 'controller is %s' % state['active_state'], problems)
        problem_if(unit_digest(state) != controller['unit_sha256'], 'controller unit changed from the pinned bytes', problems)
        if self.unified:
            problem_if(PUBLISHER_CONFIG not in controller['files'], 'controller.files must pin ' + PUBLISHER_CONFIG, problems)
            standalone = ex.probe_unit(host, self.product['controller'])
            problem_if(standalone['active_state'] not in ('inactive', 'failed') or standalone['main_pid'],
                       'standalone %s is %s beside %s' % (self.product['controller'], standalone['active_state'],
                                                          PUBLISHER_UNIT), problems)
        elif 'config' in self.product:
            problem_if(self.product['config'] not in controller['files'],
                       'controller.files must pin ' + self.product['config'], problems)
        files = dict(controller['files'])
        adapter = self.request.get('staging_adapter')
        if adapter and ex.sha256(host, self.product['config']) == hashlib.sha256(adapter['configuration'].encode()).hexdigest():
            files[self.product['config']] = hashlib.sha256(adapter['configuration'].encode()).hexdigest()
            files[adapter_path(adapter)] = adapter['sha256']
        for path, digest in sorted(files.items()):
            problem_if(ex.sha256(host, path) != digest, 'controller file %s differs from its pinned digest' % path, problems)
        try:
            config = self.controller_settings(ex) if self.settings_path() else {}
        except ValueError as error:
            return problems + ['controller settings: %s' % error]
        if 'config' in self.product:
            for key, value in self.product['config_equals'].items():
                problem_if(config.get(key) != value, 'controller %s is %r, expected %r' % (key, config.get(key), value), problems)
            for key, value in self.request['publication'].items():
                problem_if(config.get(key) != value, 'publication %s is %r, request pins %r' % (key, config.get(key), value), problems)
            http_only = config.get('http_only') is True
            if self.providers:
                # Providers synchronize from the publisher's HTTPS publication with its runtimes.
                problem_if(not self.unified or not http_only or config.get('ship_runtimes') is not True,
                           'HTTPS provider workers need %s with http_only and ship_runtimes' % PUBLISHER_UNIT, problems)
            else:
                problem_if(http_only, 'controller is http_only; pin each provider worker configuration instead', problems)
                for key, value in self.product['config_absent_or'].items():
                    problem_if(config.get(key, value) != value, 'controller %s must stay %r' % (key, value), problems)
                for key in ('fleet_config', 'fleet_command'):
                    problem_if(config.get(key) not in files, 'controller %s is not a pinned file' % key, problems)
        else:
            root = self.request['publication']['root']
            if self.unified:
                problem_if(config.get('root') != root, 'publisher display root is %r, request pins %r'
                           % (config.get('root'), root), problems)
            else:
                words = units.split_exec(units.exec_start(units.effective(
                    [state['fragment_text'] or ''] + [d['text'] for d in state['drop_ins']])))[2].split()
                problem_if('--geometry' in words, 'controller passes --geometry, which run mode does not take', problems)
                problem_if(not any(words[i:i + 2] == ['--root', root] for i in range(len(words))),
                           'controller does not run --root %s' % root, problems)
            layout_path = '%s/%s' % (root, self.product['controller_root_file'])
            problem_if(layout_path not in controller['files'],
                       'controller.files must pin %s' % layout_path, problems)
            try:
                layout = json.loads(ex.read(host, layout_path) or 'null')
                geometry = layout.get('geometry') if isinstance(layout, dict) else None
            except ValueError:
                geometry = None
            problem_if(geometry != 'txid-4k8', 'display layout must use txid-4k8, found %r' % geometry, problems)
        if check_authority:
            problems.extend(self.authority_problems(ex)[1])
        return problems

    def worker_problems(self, ex, target):
        """Limits, arguments and ready identity of one worker against its pin."""
        pinned = self.request['workers'][target.key]
        state = ex.probe_unit(target.host, target.unit)
        problems = []
        service = units.effective([state['fragment_text'] or ''] + [d['text'] for d in state['drop_ins']]).get('Service', {})
        for key, value in sorted(pinned['limits'].items()):
            actual = service.get(key)
            actual = actual[-1] if isinstance(actual, list) else actual
            problem_if(actual != value, '%s %s is %r, pinned %r' % (target.key, key, actual, value), problems)
        tail = units.split_exec((service.get('ExecStart') or [''])[-1])[2] if service.get('ExecStart') else ''
        words = tail.split()
        # The release's pir-query-server takes its settings only as `--config
        # FILE`; a worker still running a retired executable's flags cannot be
        # carried by swapping the binary under the same arguments.
        problem_if(self.request['component'] == 'workers' and self.service.binary == SERVER
                   and words[:1] != ['--config'],
                   '%s runs a retired executable with command-line settings; %s takes only --config FILE, '
                   'so install its configured unit before a binary-only upgrade' % (target.key, SERVER), problems)
        if self.providers:
            problem_if(unit_digest(state) != pinned['unit_sha256'], '%s unit changed from the pinned bytes' % target.key, problems)
            try:
                path, config = self.provider_config(ex, target)
                problem_if(ex.sha256(target.host, path) != pinned['config_sha256'],
                           '%s configuration %s differs from its pinned digest' % (target.key, path), problems)
                expected = PROVIDER_IDENTITY[config.get('full_holdings') is True]
                problem_if(pinned['ready_identity'] != expected, '%s configuration makes it %r, request pins %r'
                           % (target.key, expected, pinned['ready_identity']), problems)
                problem_if(not config.get('publisher') or config.get('assignment') or config.get('worker_id'),
                           '%s is not an autonomous HTTPS provider configuration' % target.key, problems)
            except (TypeError, KeyError, ValueError) as error:
                problems.append('%s provider configuration: %s' % (target.key, error))
        for index in range(0, len(pinned['args']), 2):
            pair = pinned['args'][index:index + 2]
            present = any(words[i:i + len(pair)] == pair for i in range(len(words)))
            problem_if(not present, '%s does not run %s' % (target.key, ' '.join(pair)), problems)
        problems.extend(self.ready_problems(ex, target.key, target.host, target.url(target.role.ready_url or target.role.health)))
        return problems

    def authority_problems(self, ex):
        """The controller's current map and its health; the map advances during a rollout."""
        controller = self.request['controller']
        try:
            url = self.status_url(ex)
        except (KeyError, TypeError, ValueError) as error:
            return None, ['controller status listener is unknown: %s' % error]
        status, body = ex.http_get(controller['host'], url)
        try:
            document = json.loads(body) if status == 200 else None
        except ValueError:
            document = None
        if not isinstance(document, dict):
            return None, ['controller status answered %s' % status]
        problems = []
        if self.request['service'] == 'transparent-display':
            published = document.get('published')
            current = published.get('map_sha256') if isinstance(published, dict) else None
            problem_if(document.get('phase') != 'running', 'controller phase is %r' % document.get('phase'), problems)
            problem_if(document.get('halted') is not None, 'controller is halted: %r' % document.get('halted'), problems)
            problem_if(document.get('stalled') is not False, 'controller stalled is %r' % document.get('stalled'), problems)
        else:
            current = document.get('map_sha256')
            problem_if(document.get('phase') != 'serving', 'controller phase is %r' % document.get('phase'), problems)
        if not isinstance(current, str) or not SHA256.match(current):
            return None, problems + ['controller reports no current map_sha256']
        return current, problems

    def ready_problems(self, ex, key, host, url):
        """Pinned ownership fields, readiness, and the controller's current map before or after the read."""
        pinned = self.request['workers'][key]['ready_identity']
        before, problems = self.authority_problems(ex)
        status, body = ex.http_get(host, url)
        after, after_problems = self.authority_problems(ex)
        problems += after_problems
        try:
            document = json.loads(body) if status == 200 else None
        except ValueError:
            document = None
        if not isinstance(document, dict):
            return problems + ['%s ready answered %s' % (key, status)]
        problems += ['%s ready %s is %r, pinned %r' % (key, field, document.get(field), value)
                     for field, value in sorted(pinned.items()) if document.get(field) != value]
        problem_if(document.get('map_sha256') not in {before, after} - {None},
                   '%s serves map %r, controller current is %r' % (key, document.get('map_sha256'), after or before),
                   problems)
        if self.request['service'] == 'transparent-display':
            problem_if(document.get('ready') is not True or document.get('warm') is not True,
                       '%s is not ready and warm' % key, problems)
            problem_if(type(document.get('covered_through')) is not int, '%s has no covered_through' % key, problems)
        else:
            problems += self.provider_problems(ex, key, document) if self.providers else self.assignment_problems(ex, key, document)
            problem_if(document.get('ready') is not True, '%s is not ready' % key, problems)
            problem_if(type(document.get('target_runtimes')) is not int or document.get('target_runtimes', 0) <= 0
                       or type(document.get('warm_runtimes')) is not int,
                       '%s runtime counts are missing or invalid' % key, problems)
            problem_if(document.get('warm_runtimes') != document.get('target_runtimes'),
                       '%s warm_runtimes %r of target %r' % (key, document.get('warm_runtimes'),
                                                             document.get('target_runtimes')), problems)
            problem_if(type(document.get('prewarm_failed')) is not int or document.get('prewarm_failed') != 0, '%s prewarm_failed %r' % (key, document.get('prewarm_failed')),
                       problems)
        return problems

    def assignment_problems(self, ex, key, document):
        """History ownership against the controller's assignment for the map the worker serves."""
        from tools.transparent.lib import activity_schema_routing as routing
        host, map_sha256 = self.request['controller']['host'], document.get('map_sha256')
        try:
            config = self.controller_settings(ex)
            fleet = json.loads(ex.read(host, config['fleet_config']) or 'null')
            path = '%s/%s.assignment.json' % (fleet['state_dir'], map_sha256)
            assignment = json.loads(ex.read(host, path) or 'null')
            require_ = assignment['set']['map_sha256'] == map_sha256
            entry = [w for w in assignment['workers'] if w['id'] == document.get('worker_id')]
            expected = {'assignment_sha256': routing.assignment_digest(assignment),
                        'worker_assignment_sha256': routing.worker_assignment_digest(assignment, document.get('worker_id')),
                        'role': entry[0]['role'], 'replica_group': entry[0]['replica_group'],
                        'assigned_shards': len(entry[0]['shards'])}
        except (TypeError, KeyError, IndexError, ValueError) as error:
            return ['%s: no valid controller assignment for map %r (%s)' % (key, map_sha256, error)]
        problems = [] if require_ else ['%s: assignment for %s names another map' % (key, map_sha256)]
        return problems + ['%s ready %s is %r, the controller assignment says %r' % (key, field, document.get(field), value)
                           for field, value in sorted(expected.items()) if document.get(field) != value]

    def provider_config(self, ex, target):
        """The `--config FILE` path of a provider unit and its parsed settings."""
        state = ex.probe_unit(target.host, target.unit)
        service = units.effective([state['fragment_text'] or ''] + [d['text'] for d in state['drop_ins']]).get('Service', {})
        words = units.split_exec(service['ExecStart'][-1])[2].split()
        if words[:1] != ['--config'] or len(words) < 2:
            raise ValueError('unit does not run --config FILE')
        config = json.loads(ex.read(target.host, words[1]) or 'null')
        if not isinstance(config, dict):
            raise ValueError('%s is not a JSON object' % words[1])
        return words[1], config

    def provider_problems(self, ex, key, document):
        """Provider ownership from its own `<shard_dir>/active.json` record (history/live.rs Publication).

        Full holdings own every shard and record no assignment. A recent provider
        records the `assignment.json` it staged inside its publication directory;
        the ready identity must match that assignment's `provider` entry.
        """
        from tools.transparent.lib import activity_schema_routing as routing
        target = next(t for t in self.workers if t.key == key)
        map_sha256 = document.get('map_sha256')
        try:
            _, config = self.provider_config(ex, target)
            root = config['shard_dir'].rstrip('/')
            active = json.loads(ex.read(target.host, root + '/active.json') or 'null')
            directory = active['directory'].rstrip('/')
            problems = [] if active['map_sha256'] == map_sha256 else [
                '%s active record names map %r, ready serves %r' % (key, active['map_sha256'], map_sha256)]
            problem_if(not directory.startswith(root + '/'), '%s active publication escapes %s' % (key, root), problems)
            if config.get('full_holdings') is True:
                problem_if(active.get('assignment') is not None, '%s full provider records an assignment' % key, problems)
                return problems
            problem_if(active.get('assignment') != directory + '/assignment.json',
                       '%s active assignment is %r, not its staged publication\'s' % (key, active.get('assignment')), problems)
            assignment = json.loads(ex.read(target.host, active['assignment']) or 'null')
            problem_if(assignment['set']['map_sha256'] != map_sha256, '%s assignment names another map' % key, problems)
            entry = [w for w in assignment['workers'] if w['id'] == 'provider']
            expected = {'assignment_sha256': routing.assignment_digest(assignment),
                        'worker_assignment_sha256': routing.worker_assignment_digest(assignment, 'provider'),
                        'role': entry[0]['role'], 'replica_group': entry[0]['replica_group'],
                        'assigned_shards': len(entry[0]['shards'])}
        except (TypeError, KeyError, IndexError, ValueError, AttributeError) as error:
            return ['%s: no valid provider active record for map %r (%s)' % (key, map_sha256, error)]
        return problems + ['%s ready %s is %r, its active assignment says %r' % (key, field, document.get(field), value)
                           for field, value in sorted(expected.items()) if document.get(field) != value]

    def problems(self, ex, targets=None):
        problems = self.controller_problems(ex)
        for target in self.workers if targets is None else targets:
            problems.extend(self.worker_problems(ex, target))
        return problems

    def worker_ids(self):
        return {pinned['ready_identity']['worker_id']: key for key, pinned in self.request['workers'].items()}

    def load_problems(self, ex, check_pin):
        """No latch, and pins naming exactly the binaries the workers run now."""
        load, spec = self.request['load'], self.product['load']
        if load is None:
            return []
        host, root = load['host'], spec['root']
        problems = []
        problem_if(ex.sha256(host, root + '/latched.json') is not None,
                   'continuous load has an unresolved critical latch (%s/latched.json)' % root, problems)
        if check_pin:
            problem_if(ex.sha256(host, root + '/pins.json') != load['pins_sha256'],
                       'load pins.json differs from load.pins_sha256', problems)
        try:
            pins = json.loads(ex.read(host, root + '/pins.json') or 'null')
            ids = self.worker_ids()
            if set(pins) != set(ids):
                return problems + ['load pins %s, workers are %s' % (sorted(pins), sorted(ids))]
        except (TypeError, ValueError) as error:
            return problems + ['load pins.json is unreadable: %s' % error]
        targets = {t.key: t for t in self.workers}
        for worker_id, key in sorted(ids.items()):
            running = ex.probe_unit(targets[key].host, targets[key].unit)['exe_sha256']
            problem_if(pins[worker_id] != running, 'load pins %s to %s but it runs %s' % (worker_id, pins[worker_id], running),
                       problems)
        return problems


class UpgradeDeployer(Deployer):
    """The shared engine with the product refusals under its lock and after each restart.

    A worker transaction with a continuous load stops the load before the first
    restart, and after commit writes pins naming the new binary and starts it
    again. A failed transaction or a rollback leaves the load stopped, its latch
    and incidents untouched, with pins naming the binaries that run.
    """

    def __init__(self, upgrade, operation, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.upgrade = upgrade
        self.operation = operation
        self.load_was_active = None

    def assess(self, *args, **kwargs):
        plans, problems = super().assess(*args, **kwargs)
        adapter = self.upgrade.request.get('staging_adapter')
        if not adapter:
            return plans, problems
        host = self.upgrade.controller.host
        path = self.upgrade.product['config']
        previous = self.ex.read(host, path)
        desired = adapter['configuration']
        if previous != desired:
            if self.ex.sha256(host, path) != self.upgrade.request['controller']['files'][path]:
                problems.append('controller configuration differs from the reviewed prior digest')
            plans[0].changes.append({'op': 'write', 'path': path, 'previous': previous, 'new': desired})
            plans[0].action = 'restart'
        source = getattr(self, 'staging_adapter', None)
        staged = self.ex.sha256(host, adapter_path(adapter))
        if staged not in (None, adapter['sha256']):
            problems.append('immutable staging adapter path holds different bytes')
        if staged is None and (not source or hashlib.sha256(Path(source).read_bytes()).hexdigest() != adapter['sha256']):
            problems.append('provide the checksum-bound --staging-adapter before deployment')
        return plans, problems

    def stage(self, *args, **kwargs):
        super().stage(*args, **kwargs)
        adapter = self.upgrade.request.get('staging_adapter')
        if adapter:
            self.lock.verify()
            host, path = self.upgrade.controller.host, adapter_path(adapter)
            if self.ex.sha256(host, path) is None:
                self.ex.mkdir(host, os.path.dirname(path), 0o755)
                partial = path + '.next'
                self.ex.upload(host, self.staging_adapter, partial, 0o755)
                if self.ex.sha256(host, partial) != adapter['sha256']:
                    raise DeployError('uploaded staging adapter checksum differs from the reviewed request')
                self.ex.rename(host, partial, path)
            if self.ex.sha256(host, path) != adapter['sha256']:
                raise DeployError('staged adapter checksum differs from the reviewed request')

    def check(self, record, expected):
        # Worker /health is diagnostic; /ready owns the readiness fields.
        # Use the endpoint already captured in the transaction for both
        # activation and rollback, without changing its recorded unit or URLs.
        verify = record['verify']
        if record['role'] == 'worker' and verify.get('ready_url'):
            record = {**record, 'verify': {**verify, 'health': verify['ready_url'], 'ready_url': None}}
        ok, reason = super().check(record, expected)
        if ok and record['role'] == 'controller':
            # A listening status endpoint can precede publication restoration.
            # Keep polling within the existing role deadline until authority is ready.
            _, problems = self.upgrade.authority_problems(self.ex)
            if problems:
                return False, '; '.join(problems)
        return ok, reason

    def wait_verified(self, record, expected):
        # Use the product's existing cold-start budget for both new transactions
        # and recovery of a transaction whose generic worker deadline was shorter.
        if record['role'] == 'worker':
            record = {**record, 'verify': {**record['verify'], 'timeout': self.upgrade.product['worker_warm_seconds']}}
        return super().wait_verified(record, expected)

    def schema_fence(self):
        super().schema_fence()
        if self.operation == 'rollback':
            # Recovery must remain possible when the new process is unhealthy.
            # Keep the pinned configuration and unit contract checks; the shared
            # journal independently checks ownership of every restored change.
            problems = self.upgrade.controller_problems(self.ex, check_authority=False)
        else:
            problems = self.upgrade.problems(self.ex)
            problems += self.upgrade.load_problems(self.ex, check_pin=True)
        if problems:
            raise DeployError('product state refused under the lock; nothing was changed:\n  ' + '\n  '.join(problems))

    def pause_load(self, journal, *, refuse_latch=True):
        load = self.upgrade.request['load']
        if load is None or self.load_was_active is not None:
            return
        unit = self.upgrade.product['load']['unit']
        self.load_was_active = self.ex.probe_unit(load['host'], unit)['active_state'] == 'active'
        journal.event('load paused', host=load['host'], unit=unit, was_active=self.load_was_active)
        self.ex.systemctl(load['host'], 'stop', unit)
        if refuse_latch and self.ex.sha256(load['host'], self.upgrade.product['load']['root'] + '/latched.json') is not None:
            raise DeployError('a critical load latch appeared while pausing load')
        self.out('exact-answer load: %s stopped for the restarts' % unit)

    def repin_load(self, journal):
        """Pins naming what the workers run now, written atomically; returns the pins."""
        load, root = self.upgrade.request['load'], self.upgrade.product['load']['root']
        targets = {t.key: t for t in self.upgrade.workers}
        pins = {worker_id: self.ex.probe_unit(targets[key].host, targets[key].unit)['exe_sha256']
                for worker_id, key in sorted(self.upgrade.worker_ids().items())}
        data = (json.dumps(pins, indent=2) + '\n').encode()
        journal.event('load pins', host=load['host'], previous_sha256=self.ex.sha256(load['host'], root + '/pins.json'),
                      new_sha256=hashlib.sha256(data).hexdigest())
        self.ex.write(load['host'], root + '/pins.json', data, 0o644)
        return pins

    def wait_for_controller(self):
        """Let the controller recover from a worker restart within its own readiness budget.

        A warm worker can answer HTTP before the controller retries a failed
        control call. Publication and ownership checks still run after recovery.
        """
        timeout = self.upgrade.controller.role.ready_timeout
        deadline = self.clock() + timeout
        while True:
            _, problems = self.upgrade.authority_problems(self.ex)
            if not problems:
                return
            if self.clock() >= deadline:
                raise DeployError('controller did not resume publication within %ss: %s'
                                  % (timeout, '; '.join(problems)))
            self.sleep(self.poll_seconds)

    def activate(self, journal, index, sha):
        if self.upgrade.request['component'] == 'workers':
            self.pause_load(journal)
        super().activate(journal, index, sha)
        record = journal.hosts[index]
        adapter = self.upgrade.request.get('staging_adapter')
        if adapter and self.ex.read(record['host'], self.upgrade.product['config']) != adapter['configuration']:
            raise DeployError('controller did not select the reviewed staging adapter configuration')
        if self.upgrade.request['component'] == 'workers':
            self.wait_for_controller()
            # A restarted worker must come back owning the controller's current assignment.
            problems = self.upgrade.ready_problems(self.ex, record['key'], record['host'],
                                                   record['verify']['ready_url'] or record['verify']['health'])
            problems += self.upgrade.controller_problems(self.ex)
        else:
            # A restarted controller must keep its contract and every worker its current map.
            problems = self.upgrade.problems(self.ex)
        if problems:
            raise DeployError('publication identity changed after restarting %s: %s' % (record['key'], '; '.join(problems)))

    def _deploy(self, *args, **kwargs):
        try:
            journal = super()._deploy(*args, **kwargs)
        except BaseException:
            if self.load_was_active is not None:
                self.out('exact-answer load: %s left stopped for investigation; pins still name the restored binaries'
                         % self.upgrade.product['load']['unit'])
            raise
        if journal is not None and self.load_was_active is not None:
            journal.set_status('verifying')
            unit = self.upgrade.product['load']['unit']
            host = self.upgrade.request['load']['host']
            try:
                pins = self.repin_load(journal)
                if self.upgrade.request['load']['resume_after_upgrade']:
                    # restart starts the stopped unit; it re-reads the new pins.
                    self.ex.systemctl(host, 'restart', unit)
                    self.out('exact-answer load: %s started with pins %s' % (unit, json.dumps(pins, sort_keys=True)))
                else:
                    self.out('exact-answer load: %s stays stopped as requested; pins %s' % (unit, json.dumps(pins, sort_keys=True)))
            except BaseException as error:
                # The worker checks passed, but monitoring is part of this rollout.
                # Keep the lock while restoring workers and their pins. Preserve
                # the stopped observer and its incident history for investigation.
                journal.event('monitoring failed', error=str(error))
                self.ex.systemctl(host, 'stop', unit)
                self.rollback_journal(journal)
                self.repin_load(journal)
                self.refresh_baseline()
                raise DeployError('monitoring recovery failed; workers rolled back and load left stopped: %s' % error) from error
            journal.set_status('committed')
        return journal

    def rollback_journal(self, journal, force=False):
        component = {(t.host, t.unit) for t in self.targets}
        if any((r['host'], r['unit']) not in component for r in journal.hosts):
            raise DeployError('transaction %s changed units outside this request\'s %s component'
                              % (journal.id, self.upgrade.request['component']))
        explicit = self.operation == 'rollback' and self.upgrade.request['component'] == 'workers'
        if explicit:
            self.pause_load(journal, refuse_latch=False)
        super().rollback_journal(journal, force)
        if explicit and self.load_was_active is not None:
            pins = self.repin_load(journal)
            self.out('exact-answer load: %s stays stopped after rollback; pins %s; start it after the exact gate'
                     % (self.upgrade.product['load']['unit'], json.dumps(pins, sort_keys=True)))

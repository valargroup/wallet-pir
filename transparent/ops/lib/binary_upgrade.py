"""Worker binary upgrade for the current v12 history and 4k8 display fleets.

Only the worker executable changes. The release identity is the request's Git
revision and binary digest; the publication identity is everything the
controller owns (configuration, fleet bytes, source revision, journal, roots)
plus each worker's ready identity, and it must be identical before, after each
restart and after rollback. Staging, journals, rollback and the production lock
are the shared engine's; this module adds only the product refusals. Anything
outside the supported topology refuses rather than being adapted.
"""
import hashlib
import json
import re

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
        'controller': 'transparent-v12-publish-controller.service',
        'config': '/opt/transparent-publisher/v12/controller.json',
        # Values the controller configuration must hold; a single journal
        # writer (follow_journal false) and no shipped runtimes.
        'config_equals': {'follow_journal': False, 'data_dir': '/srv/transparent-activity/full-v3/journal',
                          'publication_root': '/srv/transparent-activity/full-v12/publications'},
        'config_absent_or': {'ship_runtimes': False},
        'publication_keys': ('source_sha',),
        # Ownership a restart must not change; map_sha256 follows the controller.
        'ready_keys': ('assigned_shards', 'assignment_sha256', 'replica_group', 'role', 'worker_assignment_sha256',
                       'worker_id'),
        'roles': ('archive-owner', 'recent-replica'),
    },
    'transparent-display': {
        'workers': ('transparent-txid-display-worker.service',),
        'controller': 'transparent-txid-display-k8-controller.service',
        # Run mode takes no --geometry: root/display-root.json pins txid-4k8,
        # so the request pins that file's digest under controller.files.
        'controller_root_file': 'display-root.json',
        'publication_keys': ('root',),
        'ready_keys': ('role',),
        'roles': ('combined',),
    },
}
REQUEST_KEYS = {'schema', 'service', 'release', 'publication', 'controller', 'workers'}


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
    require(isinstance(request, dict) and set(request) == REQUEST_KEYS, 'keys must be %s' % sorted(REQUEST_KEYS))
    require(request['schema'] == SCHEMA, 'schema must be ' + SCHEMA)
    product = PRODUCTS.get(request['service'])
    require(product is not None, 'service must be one of %s' % sorted(PRODUCTS))
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
        require(product['config'] in files, 'controller.files must pin ' + product['config'])
    workers = request['workers']
    require(isinstance(workers, dict) and workers, 'workers maps target keys to pinned state')
    for key, pinned in workers.items():
        require(isinstance(pinned, dict) and set(pinned) == {'ready_identity', 'limits', 'args'},
                '%s is {ready_identity, limits, args}' % key)
        ready = pinned['ready_identity']
        require(isinstance(ready, dict) and set(ready) == set(product['ready_keys']),
                '%s.ready_identity pins exactly %s' % (key, list(product['ready_keys'])))
        require(ready['role'] in product['roles'], '%s.ready_identity.role must be one of %s' % (key, list(product['roles'])))
        require(isinstance(pinned['limits'], dict) and isinstance(pinned['args'], list), '%s limits/args' % key)
    return request


def unit_digest(state):
    """The normalized effective unit: every setting and argument, with the ExecStart binary abstracted."""
    config = units.effective([state['fragment_text'] or ''] + [d['text'] for d in state['drop_ins']])
    return hashlib.sha256(json.dumps(units.normalized(config), sort_keys=True).encode()).hexdigest()


class Upgrade:
    """The product refusals for one pinned request; every method only reads."""

    def __init__(self, request, service, inventory):
        self.request = request
        self.product = PRODUCTS[request['service']]
        self.service = service
        self.inventory = inventory
        self.digest = durable.digest(request)

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
        if [(t.host, t.unit) for t in controllers] != [(self.request['controller']['host'], self.product['controller'])]:
            raise DeployError('unsupported topology: controller must be %s on %s'
                              % (self.product['controller'], self.request['controller']['host']))
        self.controller_url = controllers[0].url(controllers[0].role.health)
        return workers

    def controller_problems(self, ex):
        problems = []
        controller = self.request['controller']
        host = controller['host']
        state = ex.probe_unit(host, self.product['controller'])
        problem_if(state['active_state'] != 'active', 'controller is %s' % state['active_state'], problems)
        problem_if(unit_digest(state) != controller['unit_sha256'], 'controller unit changed from the pinned bytes', problems)
        for path, digest in sorted(controller['files'].items()):
            problem_if(ex.sha256(host, path) != digest, 'controller file %s differs from its pinned digest' % path, problems)
        if 'config' in self.product:
            text = ex.read(host, self.product['config'])
            config = json.loads(text) if text else {}
            for key, value in self.product['config_equals'].items():
                problem_if(config.get(key) != value, 'controller %s is %r, expected %r' % (key, config.get(key), value), problems)
            for key, value in self.product['config_absent_or'].items():
                problem_if(config.get(key, value) != value, 'controller %s must stay %r' % (key, value), problems)
            for key, value in self.request['publication'].items():
                problem_if(config.get(key) != value, 'publication %s is %r, request pins %r' % (key, config.get(key), value), problems)
            for key in ('fleet_config', 'fleet_command'):
                problem_if(config.get(key) not in controller['files'], 'controller %s is not a pinned file' % key, problems)
        else:
            words = units.split_exec(units.exec_start(units.effective(
                [state['fragment_text'] or ''] + [d['text'] for d in state['drop_ins']])))[2].split()
            root = self.request['publication']['root']
            problem_if('--geometry' in words, 'controller passes --geometry, which run mode does not take', problems)
            problem_if(not any(words[i:i + 2] == ['--root', root] for i in range(len(words))),
                       'controller does not run --root %s' % root, problems)
            problem_if('%s/%s' % (root, self.product['controller_root_file']) not in controller['files'],
                       'controller.files must pin %s/%s' % (root, self.product['controller_root_file']), problems)
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
        for index in range(0, len(pinned['args']), 2):
            pair = pinned['args'][index:index + 2]
            present = any(words[i:i + len(pair)] == pair for i in range(len(words)))
            problem_if(not present, '%s does not run %s' % (target.key, ' '.join(pair)), problems)
        problems.extend(self.ready_problems(ex, target.key, target.host, target.url(target.role.ready_url or target.role.health)))
        return problems

    def authority_problems(self, ex):
        """The controller's current map and its health; the map advances during a rollout."""
        controller = self.request['controller']
        status, body = ex.http_get(controller['host'], self.controller_url)
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
        after, _ = self.authority_problems(ex)
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
            problem_if(document.get('warm_runtimes') != document.get('target_runtimes'),
                       '%s warm_runtimes %r of target %r' % (key, document.get('warm_runtimes'),
                                                             document.get('target_runtimes')), problems)
            problem_if(bool(document.get('prewarm_failed')), '%s prewarm_failed %r' % (key, document.get('prewarm_failed')),
                       problems)
        return problems

    def problems(self, ex, targets):
        problems = self.controller_problems(ex)
        for target in targets:
            problems.extend(self.worker_problems(ex, target))
        return problems


class UpgradeDeployer(Deployer):
    """The shared engine with the product refusals under its lock and after each restart."""

    def __init__(self, upgrade, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.upgrade = upgrade

    def schema_fence(self):
        super().schema_fence()
        problems = self.upgrade.problems(self.ex, self.targets)
        if problems:
            raise DeployError('product state refused under the lock; nothing was changed:\n  ' + '\n  '.join(problems))

    def activate(self, journal, index, sha):
        super().activate(journal, index, sha)
        record = journal.hosts[index]
        # A restarted worker must come back with the same publication and assignment.
        problems = self.upgrade.ready_problems(self.ex, record['key'], record['host'],
                                               record['verify']['ready_url'] or record['verify']['health'])
        problems += self.upgrade.controller_problems(self.ex)
        if problems:
            raise DeployError('publication identity changed after restarting %s: %s' % (record['key'], '; '.join(problems)))

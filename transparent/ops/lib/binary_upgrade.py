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
    },
    'transparent-display': {
        'workers': ('transparent-txid-display-worker.service',),
        'controller': 'transparent-txid-display-k8-controller.service',
        'controller_args': ('--geometry', 'txid-4k8'),
        'publication_keys': ('geometry', 'publication_root'),
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
        require(isinstance(pinned['ready_identity'], dict) and pinned['ready_identity'],
                '%s.ready_identity must pin at least one ready field (publication, assignment)' % key)
        require(isinstance(pinned['limits'], dict) and isinstance(pinned['args'], list), '%s limits/args' % key)
    return request


def unit_digest(state):
    texts = [state['fragment_text'] or ''] + [d['text'] for d in state['drop_ins']]
    return hashlib.sha256('\0'.join(texts).encode()).hexdigest()


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
            exec_start = units.effective([state['fragment_text'] or ''] + [d['text'] for d in state['drop_ins']]
                                         )['Service'].get('ExecStart', [''])[-1]
            flag, value = self.product['controller_args']
            problem_if('%s %s' % (flag, value) not in exec_start, 'controller does not run %s %s' % (flag, value), problems)
            problem_if('--geometry %s' % self.request['publication']['geometry'] not in exec_start,
                       'publication geometry differs from the controller', problems)
            problem_if(self.request['publication']['publication_root'] not in exec_start,
                       'publication root differs from the controller', problems)
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

    def ready_problems(self, ex, key, host, url):
        pinned = self.request['workers'][key]['ready_identity']
        status, body = ex.http_get(host, url)
        try:
            document = json.loads(body) if status == 200 else None
        except ValueError:
            document = None
        if not isinstance(document, dict):
            return ['%s ready answered %s' % (key, status)]
        return ['%s ready %s is %r, pinned %r' % (key, field, document.get(field), value)
                for field, value in sorted(pinned.items()) if document.get(field) != value]

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

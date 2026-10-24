"""An installed unified fleet for publisher update and recovery tests."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'tools/lib'))
from wallet_pir_ops import durable
from wallet_pir_ops.deploy import publisher, publisher_serving
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.remote import RemoteError
from fake_fleet import FakeFleet, sha256

BINARY = b'unified publisher executable'
HISTORY = {'data_dir': '/srv/transparent-activity/full-v3/journal',
           'publication_root': '/srv/transparent-activity/full-v12/publications',
           'initial_publication': '/srv/transparent-activity/full-v12/publications/initial',
           'recent_from': 1, 'recent_geometry': 'decoupled-2k-8k', 'archive_geometry': 'decoupled-32k-32k',
           'listen': '10.0.0.1:8096', 'source_sha': 'retained-source', 'follow_journal': False}
DISPLAY = {'root': '/srv/txid-display-genesis/v4/root', 'journal': '/srv/txid-display-genesis/journal-v3',
           'status_listen': '127.0.0.1:8098', 'max_recent_records': None}


class Coordinator(FakeFleet):
    def __init__(self):
        super().__init__()
        self.memory = 64 << 30
        self.crash_on = None
        self.fail_probe = False
        self.refuse_configuration = False
        self.clock_seconds = 0
        self.effects = []
        self.lock_held = False

    def publisher_unlocked(self, host, paths):
        if self.lock_held:
            raise DeployError('another writer retains its lock')
        assert all(self.probe_unit(host, name)['main_pid'] == 0 for name in (*publisher.OLD, publisher.UNIT))

    def probe_unit(self, host, name):
        value = super().probe_unit(host, name)
        state = self.host(host).units.get(name, {})
        value['unit_file_state'] = 'enabled' if state.get('enabled', True) else 'disabled'
        return value

    def publisher_resources(self, host, paths):
        files = self.host(host).files
        if any(path in self.host(host).dirs for path in paths):
            raise DeployError('publisher managed file must be a root-owned regular file')
        return {'machine_id': 'a' * 32, 'memory_available': self.memory, 'memory_total': 64 << 30,
                'files': {p: {'mode': 0o600} if p in files else None for p in paths},
                'disks': {p: {'available': 80 << 30, 'total': 100 << 30, 'device': 1} for p in paths}}

    def systemctl(self, host, *args):
        self.effects.append(args)
        if args[:2] == ('reload', 'caddy.service'):
            return
        if args[0] == 'start':
            args = ('restart', *args[1:])
        super().systemctl(host, *args)
        if self.crash_on == args:
            self.crash_on = None
            raise RemoteError('reply lost after completed service mutation')

    def http_get(self, host, url, timeout=5):
        if (host, url) in self.endpoints:
            status, body = FakeFleet.http_get(self, host, url, timeout)
            value = json.loads(body)
            executable = self.host(host).units[self.endpoints[(host, url)]]['exe']
            new_publisher = self.probe_unit('coordinator', publisher.UNIT)['exe_sha256'] == self.new_publisher
            value['ready'] = value['ready'] and ((executable == self.new_serving) == new_publisher)
            return status, json.dumps(value)
        active = lambda name: self.host(host).units.get(name, {}).get('active') == 'active'
        if ':18080/' in url:
            return (200, json.dumps({'ready': True})) if active(publisher.UNIT) else (503, '{}')
        if ':8098/' in url:
            return 200, json.dumps({'phase': 'running', 'mode': 'live', 'halted': None, 'stalled': False})
        return 200, json.dumps({'phase': 'serving'})

    def run(self, host, argv, timeout):
        if argv[:2] == ['python3', '-c']:
            from tools.enhance.lib.deploy_prerequisites import PROGRAM
            if argv[2] == PROGRAM:
                return super().run(host, argv, timeout)
        self.runs.append((host, argv))
        if argv[:2] == ['stat', '-c']:
            return 0, '644:0:0\n'
        if '--check' in argv:
            configuration = json.loads(self.read(host, argv[argv.index('--config') + 1]))
            expected = {'management_listen', *publisher.PRODUCTS} if argv[0].endswith('/pir-publisher') else {'management_listen', 'enhance'}
            if (not argv[0].endswith('/pir-publisher')
                    and configuration.get('enhance', {}).get('settings', {}).get('role') in ('query-ingress', 'combined')):
                expected.add('public_listen')
            if self.refuse_configuration or set(configuration) - {'chain'} != expected:
                return 1, 'unknown product configuration'
        elif self.fail_probe and argv[0].startswith('/bin/check-'):
            return 1, 'encrypted answer mismatch'
        return 0, 'independent exact answer verified'

    def clock(self):
        return self.clock_seconds

    def sleep(self, seconds):
        self.clock_seconds += seconds


class PublisherFixture:
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.ex = Coordinator()
        self.binary = self.root / 'pir-publisher'
        self.binary.write_bytes(BINARY)
        self.serving_binary = self.root / 'pir-query-server'
        self.serving_binary.write_bytes(b'new serving executable')
        self.old_publisher = sha256(b'installed publisher')
        config = {'management_listen': '127.0.0.1:18080',
            'chain': {'rpc_url': 'http://127.0.0.1:8232',
                      'rpc_cookie': '/run/credentials/pir-publisher.service/chain-cookie', 'poll_ms': 1000},
            'transparent_history': {'enabled': True, 'settings': copy.deepcopy(HISTORY)},
            'transaction_details': {'enabled': True, 'settings': copy.deepcopy(DISPLAY)},
            'enhance': {'enabled': True, 'settings': {'artifact_listen': '127.0.0.1:8084',
                'listen': '127.0.0.1:8080', 'data_dir': '/srv/enhance', 'prevout_cache_outputs': 10}},
            'status': {'enabled': True, 'settings': {'publisher': {'state_dir': '/srv/status'}}}}
        self.ex.install('coordinator', publisher.UNIT,
            '[Service]\nEnvironmentFile=/etc/pir/credentials.env\nExecStart=/bin/publisher --config '
            + publisher.CONFIG + '\nMemoryMax=40G\nMemorySwapMax=0\n[Install]\nWantedBy=multi-user.target\n',
            files={'/bin/publisher': b'installed publisher', '/etc/pir/credentials.env': b''})
        self.ex.install('coordinator', publisher.LOAD,
            '[Service]\nExecStart=/bin/load --controller-unit pir-publisher\n', files={'/bin/load': b'load'})
        self.ex.put('coordinator', publisher.CONFIG, json.dumps(config))
        self.ex.put('coordinator', publisher.SAMPLER, json.dumps({'targets': [
            {'sources': ['transparent-publisher', 'txid-controller'], 'unit': publisher.UNIT}]}))
        inventory = json.loads((ROOT / 'tools/enhance/deploy/deploy-inventory.example.json').read_text())
        inventory['services'].pop('receiver', None)
        inventory['ancillary'] = {k: v for k, v in inventory['ancillary'].items()
                                  if set(v['consumers']) <= set(inventory['services'])}
        # Start with the supported ownership graph, never replay retired deployments.
        for name, role in [('enhance', 'coordinator'), ('status', 'controller'),
                           ('transparent-history', 'controller'), ('transparent-display', 'controller')]:
            inventory['services'][name]['roles'].pop(role, None)
        inventory['services']['publisher'] = {
            'roles': {'server': [{'host': 'coordinator', 'vars': {'management_listen': '127.0.0.1:18080'}}]},
            'managed_products': publisher.PRODUCTS}
        checks = {name: {'host': 'coordinator', 'argv': ['/bin/check-' + name], 'timeout': 20}
                  for name in publisher.PRODUCTS}
        inventory['services']['publisher']['exact_check'] = {'products': checks}
        inventory['lock'] = {'type': 'remote', 'host': 'coordinator'}
        edge = 'example.test {\n' + publisher_serving.WALLET + '\thandle {\n\t\trespond 404\n\t}\n}\n'
        self.ex.put('coordinator', publisher_serving.CADDY, edge)
        self.ex.install('coordinator', 'caddy.service',
            '[Service]\nExecStart=/bin/caddy run --config /etc/caddy/Caddyfile\n', files={'/bin/caddy': b'caddy'})
        self.request = {'schema': publisher.SERVING_SCHEMA, 'host': 'coordinator', 'machine_id': 'a' * 32,
            'release': {'git_sha': 'b' * 40, 'binary_sha256': sha256(BINARY), 'binary_bytes': len(BINARY)},
            'configuration': config, 'inventory': inventory, 'checks': checks,
            'serving': {'git_sha': 'b' * 40, 'binary_sha256': sha256(self.serving_binary.read_bytes()),
                'binary_bytes': self.serving_binary.stat().st_size, 'caddy_sha256': publisher.sha(edge)}}
        self.ex.new_publisher = self.request['release']['binary_sha256']
        self.ex.new_serving = self.request['serving']['binary_sha256']
        for role in publisher_serving.ROLES:
            entry = inventory['services']['enhance']['roles'][role][0]
            self.ex.install(entry['host'], 'enhance-pir-' + role + '.service',
                '[Service]\nExecStart=/bin/old-serving ' + role + '\n', files={'/bin/old-serving': b'old serving'})
            for path in entry['settings']['credentials']:
                self.ex.put(entry['host'], path, '')
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        for target in self.target.serving.deployer.targets:
            self.ex.endpoints[(target.host, target.url(target.role.health))] = target.unit
        self.ex.effects.clear()

    def make_target(self):
        path = self.root / 'request.json'
        path.write_text(json.dumps(self.request))
        request, inventory = publisher.load_request(path, publisher.sha(path.read_text()))
        return publisher.Cutover(request, inventory, self.ex, self.root, clock=self.ex.clock, sleep=self.ex.sleep)

    def deploy(self):
        return self.target.deploy(self.binary, self.target.plan()['plan_sha256'], self.serving_binary)

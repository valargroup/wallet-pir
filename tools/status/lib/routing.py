"""Cut over only the installed Status wallet route under the production lock.

Plan pins the complete Caddyfile, host identity and query-router executable.
Deploy validates the candidate, saves both versions in durable journals, reloads
Caddy and runs independent encrypted probes. Rollback refuses foreign changes.
"""
import base64
import hashlib
import json
import re
from urllib.parse import urlsplit

from wallet_pir_ops.paths import ROOT
from wallet_pir_ops.deploy import coordinator_edge, descriptors, units
from wallet_pir_ops.deploy.engine import Deployer, DeployError, mutation_fence
from wallet_pir_ops.deploy.remote import production_lock
from wallet_pir_ops.deploy.transaction import FINAL, Journal

SERVICE = 'status-routing'
CONFIG = '/etc/caddy/Caddyfile'
# The legacy `/v1/status/*` route is required; its canonical `/status/v1/*`
# alias, when present, moves with it so the two never reach different routers.
ROUTE = re.compile(r'(?m)^([ \t]*handle /(v1/status|status/v1)/\* \{\n[ \t]*reverse_proxy )127\.0\.0\.1:(8489|8492)(\n[ \t]*\})')


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def require(condition, message):
    if not condition:
        raise DeployError('Status routing: ' + message)


def candidate(text):
    # The legacy route lives on one site; each site (the Enhance host and the
    # unified wallet host) may carry one canonical alias, and every alias moves
    # with the legacy route so no site reaches a different router.
    prefixes = [m[2] for m in ROUTE.finditer(text)]
    require(prefixes.count('v1/status') == 1 and text.count('handle /status/v1/') == prefixes.count('status/v1'),
            'expected exactly one simple Status wallet route and simple canonical aliases')
    for start, end in coordinator_edge.sites(text).values():
        require(text[start:end].count('handle /status/v1/') <= 1, 'expected at most one canonical alias per site')
    return ROUTE.sub(lambda m: m[1] + '127.0.0.1:8492' + m[4], text)


def load_request(path, expected):
    data = path.read_bytes()
    require(hashlib.sha256(data).hexdigest() == expected, 'request checksum differs')
    request = json.loads(data)
    require(set(request) == {'schema', 'host', 'machine_id', 'router_host', 'router_sha256', 'public_origin'},
            'unknown or missing request fields')
    require(request['schema'] == 'status-wallet-route/1', 'unsupported request schema')
    require(re.fullmatch('[0-9a-f]{32}', request['machine_id']) is not None, 'invalid machine identity')
    require(re.fullmatch('[0-9a-f]{64}', request['router_sha256']) is not None, 'invalid router digest')
    url = urlsplit(request['public_origin'])
    require(url.scheme == 'https' and url.hostname and not url.username and not url.password
            and url.path in ('', '/') and not url.query and not url.fragment and url.port in (None, 443),
            'public origin must be an existing HTTPS wallet origin')
    return request


class Routing:
    def __init__(self, inventory, executor, state_dir, request, *, lock=None):
        self.inventory, self.ex, self.state_dir, self.request = inventory, executor, state_dir, request
        self.lock_factory = lock or (lambda: production_lock(inventory, executor))
        self.lock = None
        self.host = request['host']
        self.service = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')['status']
        require(self.host in inventory.hosts and request['router_host'] in inventory.hosts,
                'request hosts must belong to the inventory')

    def run(self, argv, timeout=30):
        code, output = self.ex.run(self.host, argv, timeout)
        require(code == 0, 'command failed: ' + output[-1500:])
        return output.strip()

    def validate(self, text):
        payload = base64.b64encode(text.encode()).decode()
        program = ('import base64,subprocess,sys;'
                   'r=subprocess.run(["caddy","validate","--config","/dev/stdin","--adapter","caddyfile"],'
                   'input=base64.b64decode(sys.argv[1]),capture_output=True);'
                   'print((r.stdout+r.stderr).decode()[-1500:]);raise SystemExit(r.returncode)')
        self.run(['python3', '-c', program, payload])

    def plan(self, ready=True):
        for host in (self.host, self.request['router_host']):
            self.ex.check_identity(host)
        require(self.run(['cat', '/etc/machine-id']) == self.request['machine_id'], 'coordinator identity changed')
        require(self.run(['stat', '-c', '%a:%u:%g', CONFIG]) == '644:0:0', 'unexpected Caddyfile ownership or mode')
        caddy = self.ex.probe_unit(self.host, 'caddy.service')
        command = units.exec_start(units.effective([caddy['fragment_text']] +
                                                  [item['text'] for item in caddy['drop_ins']]))
        require(caddy['active_state'] == 'active' and '--config /etc/caddy/Caddyfile' in command,
                'Caddy must be active and use the installed Caddyfile')
        router = self.ex.probe_unit(self.request['router_host'], 'status-router.service')
        if ready:
            require(router['active_state'] == 'active' and router['exe_sha256'] == self.request['router_sha256'],
                    'the requested query-router executable is not active')
            status, _body = self.ex.http_get(self.host, 'http://127.0.0.1:8492/v1/status/init')
            require(status == 200, 'query-router wallet listener is not ready')
        before = self.ex.read(self.host, CONFIG)
        require(before is not None, 'Caddyfile is absent')
        after = candidate(before)
        self.validate(after)
        plan = {'request': self.request, 'before_sha256': digest(before), 'after_sha256': digest(after),
                'action': 'skip' if before == after else 'reload',
                'exact_check_sha256': digest(json.dumps(descriptors.exact_check(self.service, self.inventory), sort_keys=True)),
                'probe_client': self.inventory.services['status']['route_probe_client']}
        return plan, before, after

    def write(self, path, data, mode):
        self.lock.verify()
        self.ex.write(self.host, path, data, mode)

    def reload(self):
        self.lock.verify()
        self.ex.systemctl(self.host, 'reload', 'caddy.service')

    def persist(self, journal):
        journal.save()
        directory = '/opt/status-pir/routing-transactions/' + journal.id
        self.lock.verify()
        self.ex.mkdir(self.host, directory, 0o700)
        self.write(directory + '/journal.json', json.dumps(journal.data).encode(), 0o600)

    def public_probe(self):
        # The independent diagnostic client is pinned explicitly by the operator.
        client = self.inventory.services['status']['route_probe_client']
        match = re.fullmatch('/opt/status-pir/releases/([0-9a-f]{64})/status-pir', client)
        require(match is not None and self.ex.sha256(self.host, client) == match[1],
                'independent diagnostic client checksum differs')
        return self.run([client, 'probe-live', '--origin', self.request['public_origin'],
                         '--cookie', '/root/.cache/zakura/.cookie', '--queries', '4'], 90)

    def restore(self, journal):
        require(self.run(['cat', '/etc/machine-id']) == self.request['machine_id'], 'coordinator identity changed')
        before, after = journal.data['before'], journal.data['after']
        require(self.ex.read(self.host, CONFIG) in (before, after), 'Caddyfile changed outside this transaction')
        if journal.status == 'rolled-back':
            require(self.ex.read(self.host, CONFIG) == before, 'restored Caddyfile changed')
            return
        self.validate(before)
        journal.set_status('rolling-back')
        self.persist(journal)
        self.write(CONFIG, before.encode(), 0o644)
        self.reload()
        self.public_probe()
        journal.set_status('rolled-back')
        self.persist(journal)

    def deploy(self, expected):
        with self.lock_factory() as lock:
            self.lock = lock
            mutation_fence(self.inventory, self.ex, lock)
            latest = Journal.load(self.state_dir, SERVICE)
            require(latest is None or latest.status in FINAL, 'finish the interrupted routing transaction first')
            plan, before, after = self.plan()
            require(digest(json.dumps(plan, sort_keys=True)) == expected, 'plan changed since review')
            if before == after:
                self.public_probe()
                return {'phase': 'unchanged', 'plan': plan}
            journal = Journal.create(self.state_dir, SERVICE, digest(after), plan, [], {})
            journal.data.update(before=before, after=after, request=self.request)
            self.persist(journal)
            try:
                lock.verify()
                require(self.ex.read(self.host, CONFIG) == before, 'Caddyfile changed before installation')
                journal.set_status('installing')
                self.persist(journal)
                self.write(CONFIG, after.encode(), 0o644)
                self.reload()
                require(self.ex.read(self.host, CONFIG) == after, 'installed Caddyfile differs')
                self.public_probe()
                deployer = Deployer(self.service, self.inventory, self.ex, self.state_dir)
                deployer.run_exact_check(descriptors.exact_check(self.service, self.inventory), journal,
                                         self.request['router_sha256'])
                journal.set_status('committed')
                self.persist(journal)
            except Exception:
                from wallet_pir_ops.deploy.deployment_alerts import failed
                failed()
                try:
                    self.restore(journal)
                except Exception as error:
                    journal.set_status('rollback-failed', error=str(error))
                    self.persist(journal)
                    raise
                raise
            return {'phase': journal.status, 'transaction': journal.id, 'plan': plan}

    def rollback(self, identifier=None):
        journal = Journal.load(self.state_dir, SERVICE, identifier)
        require(journal is not None and journal.data['request'] == self.request, 'routing transaction/request mismatch')
        latest = Journal.load(self.state_dir, SERVICE)
        require(latest.id == journal.id or latest.status == 'rolled-back', 'roll back the later routing transaction first')
        with self.lock_factory() as lock:
            self.lock = lock
            mutation_fence(self.inventory, self.ex, lock)
            self.restore(journal)
        return {'phase': journal.status, 'transaction': journal.id}

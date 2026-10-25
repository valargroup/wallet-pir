"""Prepare Receiver HTTPS distribution and switch wallet routing under the fleet lock.

Each transaction changes one installed Caddyfile. Its request and plan pin the
host, complete file and candidate; the journal retains both files for rollback.
Distribution and query ingress are prepared while the old wallet runtime serves.
Only the final wallet phase requires the new query executable and encrypted probe.
"""
import base64
import hashlib
import ipaddress
import json
import re
from urllib.parse import urlsplit

from wallet_pir_ops.paths import ROOT
from wallet_pir_ops.deploy import coordinator_edge, descriptors, units
from wallet_pir_ops.deploy.engine import Deployer, DeployError, mutation_fence
from wallet_pir_ops.deploy.remote import production_lock
from wallet_pir_ops.deploy.transaction import FINAL, Journal

SERVICE = 'receiver-routing'
CONFIG = '/etc/caddy/Caddyfile'
RECEIVER = 'receiver-pir.valargroup.dev'
WALLET = re.compile(r'(?m)^([ \t]*handle @wallet \{\n[ \t]*reverse_proxy )([^\s{}]+)(\n[ \t]*\})')


def digest(text):
    return hashlib.sha256(text.encode()).hexdigest()


def require(condition, message):
    if not condition:
        raise DeployError('Receiver routing: ' + message)


def candidate(text, request):
    """Change only the selected Receiver routes, preserving other wallet products."""
    phase = request['phase']
    host = urlsplit(request['query_origin']).hostname if phase == 'query' else RECEIVER
    start, end = coordinator_edge.site_span(text, host)
    site = text[start:end]
    if phase == 'distribution':
        block = ('\t@provider path /.well-known/pir-service-discovery /provider/receiver/v1/*\n'
                 '\thandle @provider {\n\t\treverse_proxy ' + request['publisher_listen'] + '\n\t}\n\n')
        if '@provider' in site:
            require(site.count(block) == 1 and site.count('@provider') == 2,
                    'existing provider routes differ from the requested distribution')
            return text
        matches = list(re.finditer(r'(?m)^[ \t]*handle \{', site))
        require(len(matches) == 1 and len(WALLET.findall(site)) == 1,
                'expected one wallet handler and one fallback')
        require(WALLET.search(site)[2] == request['publisher_listen'], 'wallet predecessor differs')
        offset = matches[0].start()
        site = site[:offset] + block + site[offset:]
    elif phase == 'query':
        matches = list(re.finditer(r'(?m)^([ \t]*@wallet path )([^\n]+)$', site))
        require(len(matches) == 1, 'expected one query wallet matcher')
        match = matches[0]
        paths = match[2].split()
        for path in ('/receiver/v1/*', '/v1/receiver/*'):
            if path not in paths:
                paths.append(path)
        site = site[:match.start()] + match[1] + ' '.join(paths) + site[match.end():]
        stub = re.compile(r'(?m)^[ \t]*handle /receiver/v1/\* \{\n[ \t]*respond "Receiver serving integration is not installed" 503\n[ \t]*\}\n')
        require(site.count('handle /receiver/v1/') == len(stub.findall(site)),
                'unrecognized Receiver query handler')
        site = stub.sub('', site)
    else:
        require(phase == 'wallet', 'unknown phase')
        matches = list(WALLET.finditer(site))
        require(len(matches) == 1 and matches[0][2] in (request['publisher_listen'], request['query_origin']),
                'wallet upstream differs from the predecessor and candidate')
        require('/provider/receiver/v1/*' in site, 'prepare HTTPS distribution before switching wallets')
        site = WALLET.sub(lambda match: match[1] + request['query_origin'] + match[3], site)
    after = text[:start] + site + text[end:]
    if phase == 'query':
        proxy = 'trusted_proxies static ' + request['receiver_address'] + '/32'
        if text.lstrip().startswith('{'):
            require(proxy in text[:start], 'existing global proxy trust must name the Receiver edge')
        else:
            after = '{\n\tservers {\n\t\t' + proxy + '\n\t}\n}\n\n' + after
    return after


def load_request(path, expected):
    data = path.read_bytes()
    require(hashlib.sha256(data).hexdigest() == expected, 'request checksum differs')
    request = json.loads(data)
    require(set(request) == {'schema', 'phase', 'host', 'machine_id', 'query_host', 'query_origin',
                             'query_sha256', 'publisher_listen', 'receiver_address'},
            'unknown or missing request fields')
    require(request['schema'] == 'receiver-routing/1' and request['phase'] in ('distribution', 'query', 'wallet'),
            'unsupported routing request')
    require(re.fullmatch('[0-9a-f]{32}', request['machine_id']) is not None, 'invalid machine identity')
    require(re.fullmatch('[0-9a-f]{64}', request['query_sha256']) is not None, 'invalid query digest')
    url = urlsplit(request['query_origin'])
    require(url.scheme == 'https' and url.hostname and not url.username and not url.password
            and url.path == '' and not url.query and not url.fragment and url.port in (None, 443),
            'query origin must be a credential-free HTTPS origin')
    address, port = request['publisher_listen'].rsplit(':', 1)
    require(ipaddress.ip_address(address).is_private and port == '18380', 'expected the private publisher listener')
    ipaddress.IPv4Address(request['receiver_address'])
    return request


class Routing:
    def __init__(self, inventory, executor, state_dir, request, *, lock=None):
        self.inventory, self.ex, self.state_dir, self.request = inventory, executor, state_dir, request
        self.host = request['host']
        self.service_name = SERVICE + '-' + request['phase']
        self.service = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')['receiver']
        self.lock_factory = lock or (lambda: production_lock(inventory, executor))
        self.lock = None
        require(self.host in inventory.hosts and request['query_host'] in inventory.hosts, 'unknown routing host')

    def run(self, argv, timeout=30):
        code, output = self.ex.run(self.host, argv, timeout)
        require(code == 0, 'command failed: ' + output[-1500:])
        return output.strip()

    def validate(self, text):
        program = ('import base64,subprocess,sys;'
                   'r=subprocess.run(["caddy","validate","--config","/dev/stdin","--adapter","caddyfile"],'
                   'input=base64.b64decode(sys.argv[1]),capture_output=True);'
                   'print((r.stdout+r.stderr).decode()[-1500:]);raise SystemExit(r.returncode)')
        self.run(['python3', '-c', program, base64.b64encode(text.encode()).decode()])

    def plan(self, ready=True):
        self.ex.check_identity(self.host)
        require(self.run(['cat', '/etc/machine-id']) == self.request['machine_id'], 'host identity changed')
        require(self.run(['stat', '-c', '%a:%u:%g', CONFIG]) == '644:0:0', 'unexpected Caddyfile ownership or mode')
        caddy = self.ex.probe_unit(self.host, 'caddy.service')
        command = units.exec_start(units.effective([caddy['fragment_text']] + [x['text'] for x in caddy['drop_ins']]))
        require(caddy['active_state'] == 'active' and '--config /etc/caddy/Caddyfile' in command,
                'Caddy must be active with the installed Caddyfile')
        if ready and self.request['phase'] == 'wallet':
            query = self.request['query_host']
            self.ex.check_identity(query)
            unit = self.ex.probe_unit(query, 'wallet-pir-query.service')
            require(unit['active_state'] == 'active' and unit['exe_sha256'] == self.request['query_sha256'],
                    'requested query executable is not active')
            status, _body = self.ex.http_get(query, 'http://127.0.0.1:18081/ready')
            require(status == 200, 'query server is not ready')
        before = self.ex.read(self.host, CONFIG)
        require(before is not None, 'Caddyfile is absent')
        after = candidate(before, self.request)
        self.validate(after)
        plan = {'request': self.request, 'before_sha256': digest(before), 'after_sha256': digest(after),
                'action': 'skip' if before == after else 'reload'}
        if self.request['phase'] == 'wallet':
            check = descriptors.exact_check(self.service, self.inventory)
            require(check is not None, 'configure the encrypted Receiver probe before switching wallets')
            plan['exact_check_sha256'] = digest(json.dumps(check, sort_keys=True))
        return plan, before, after

    def persist(self, journal):
        journal.save()
        directory = '/opt/receiver-pir/routing-transactions/' + journal.id
        self.lock.verify()
        self.ex.mkdir(self.host, directory, 0o700)
        self.ex.write(self.host, directory + '/journal.json', json.dumps(journal.data).encode(), 0o600)

    def install(self, text):
        self.lock.verify()
        self.ex.write(self.host, CONFIG, text.encode(), 0o644)
        self.lock.verify()
        self.ex.systemctl(self.host, 'reload', 'caddy.service')

    def restore(self, journal):
        self.ex.check_identity(self.host)
        require(self.run(['cat', '/etc/machine-id']) == self.request['machine_id'], 'host identity changed')
        before, after = journal.data['before'], journal.data['after']
        require(self.ex.read(self.host, CONFIG) in (before, after), 'Caddyfile changed outside this transaction')
        self.validate(before)
        journal.set_status('rolling-back')
        self.persist(journal)
        self.install(before)
        require(self.ex.read(self.host, CONFIG) == before, 'restored Caddyfile differs')
        journal.set_status('rolled-back')
        self.persist(journal)

    def deploy(self, expected):
        with self.lock_factory() as self.lock:
            mutation_fence(self.inventory, self.ex, self.lock)
            latest = Journal.load(self.state_dir, self.service_name)
            require(latest is None or latest.status in FINAL, 'finish the interrupted routing transaction first')
            plan, before, after = self.plan()
            require(digest(json.dumps(plan, sort_keys=True)) == expected, 'plan changed since review')
            if before == after and self.request['phase'] != 'wallet':
                return {'phase': 'unchanged', 'plan': plan}
            journal = Journal.create(self.state_dir, self.service_name, digest(after), plan, [], {})
            journal.data.update(before=before, after=after, request=self.request)
            self.persist(journal)
            try:
                self.lock.verify()
                require(self.ex.read(self.host, CONFIG) == before, 'Caddyfile changed before installation')
                journal.set_status('installing')
                self.persist(journal)
                if before != after:
                    self.install(after)
                require(self.ex.read(self.host, CONFIG) == after, 'installed Caddyfile differs')
                if self.request['phase'] == 'wallet':
                    deployer = Deployer(self.service, self.inventory, self.ex, self.state_dir)
                    deployer.run_exact_check(descriptors.exact_check(self.service, self.inventory), journal,
                                             self.request['query_sha256'])
                journal.set_status('committed')
                self.persist(journal)
            except Exception as error:
                try:
                    self.restore(journal)
                except Exception as rollback_error:
                    journal.set_status('rollback-failed', deploy_error=str(error), rollback_error=str(rollback_error))
                    self.persist(journal)
                    raise DeployError('routing failed: %s; rollback failed: %s' % (error, rollback_error)) from error
                raise
        return {'phase': journal.status, 'transaction': journal.id, 'plan': plan}

    def rollback(self, identifier=None):
        journal = Journal.load(self.state_dir, self.service_name, identifier)
        require(journal is not None and journal.data['request'] == self.request, 'routing transaction/request mismatch')
        latest = Journal.load(self.state_dir, self.service_name)
        require(latest.id == journal.id, 'roll back the later routing transaction first')
        with self.lock_factory() as self.lock:
            mutation_fence(self.inventory, self.ex, self.lock)
            self.restore(journal)
        return {'phase': journal.status, 'transaction': journal.id}

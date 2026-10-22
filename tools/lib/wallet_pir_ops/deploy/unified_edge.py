"""Install the unified wallet site on the live coordinator Caddy under the production lock.

The coordinator's live Caddyfile is edited in place by product cutovers, so it
is never replaced with the rendered template. This operation adds the
`wallet-pir.valargroup.dev` site rendered from the template, or replaces that
one site when it differs, and leaves every other byte of the live file alone.
Plan pins the complete file and host identity. Deploy validates the candidate,
saves both versions in durable journals, reloads Caddy and probes the unified
host on the coordinator: canonical routes answer 200, legacy and operator
paths 404. Any failure restores the exact previous file. Rollback refuses
foreign changes.
"""
import base64
import hashlib
import json
import re
import time
from urllib.parse import urlsplit

from . import coordinator_edge
from .engine import DeployError, mutation_fence
from .remote import production_lock
from .transaction import FINAL, Journal

SERVICE = 'coordinator-edge'
CONFIG = '/etc/caddy/Caddyfile'
HOST = coordinator_edge.UNIFIED_HOST
# GET operations each product answers without a session, and paths the unified
# host must refuse: legacy aliases stay on the product hosts, operator routes
# are never public.
SERVED = ('/enhance/v1/init', '/status/v1/init', '/transparent/v1/shards', '/receiver/v1/inventory')
REFUSED = ('/metrics', '/v1/enhance/init', '/v1/status/init', '/v2/shards', '/v1/receiver/inventory',
           '/receiver/v1/health')
PROBE_ATTEMPTS = 12
PROBE_INTERVAL = 10


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def require(condition, message):
    if not condition:
        raise DeployError('coordinator edge: ' + message)


def unified_block(template, enhance_url):
    rendered = coordinator_edge.render(template, enhance_url)
    return rendered[slice(*coordinator_edge.site_span(rendered, HOST))]


def candidate(live, template, enhance_url):
    """The live file with the rendered unified site added or replaced.

    The live file must already serve the Enhance host named by `enhance_url`,
    which identifies it as the coordinator edge. Every other byte, including
    other sites and global options, is kept.
    """
    block = unified_block(template, enhance_url)
    found = coordinator_edge.sites(live)
    host = urlsplit(enhance_url).hostname
    require(host in found, 'live Caddyfile does not serve the Enhance host ' + str(host))
    if HOST in found:
        return coordinator_edge.replace_in_site(live, found[HOST], lambda _old: block)
    separator = '' if live.endswith('\n\n') else '\n' if live.endswith('\n') else '\n\n'
    after = live + separator + block
    require(set(coordinator_edge.sites(after)) == set(found) | {HOST}, 'adding the site changed other sites')
    return after


def load_request(path, expected):
    data = path.read_bytes()
    require(hashlib.sha256(data).hexdigest() == expected, 'request checksum differs')
    request = json.loads(data)
    require(set(request) == {'schema', 'host', 'machine_id', 'enhance_public_url'}, 'unknown or missing request fields')
    require(request['schema'] == 'coordinator-unified-edge/1', 'unsupported request schema')
    require(re.fullmatch('[0-9a-f]{32}', request['machine_id']) is not None, 'invalid machine identity')
    url = urlsplit(request['enhance_public_url'])
    require(url.scheme == 'https' and url.hostname and url.path in ('', '/') and not url.query
            and not url.username and url.port in (None, 443), 'Enhance public URL must be an HTTPS origin')
    return request


class UnifiedEdge:
    def __init__(self, inventory, executor, state_dir, request, *, lock=None, template=None, sleep=time.sleep):
        self.inventory, self.ex, self.state_dir, self.request = inventory, executor, state_dir, request
        self.lock_factory = lock or (lambda: production_lock(inventory, executor))
        self.lock = None
        self.host = request['host']
        self.template = template if template is not None else coordinator_edge.TEMPLATE.read_text()
        self.sleep = sleep
        require(self.host in inventory.hosts, 'request host must belong to the inventory')

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

    def plan(self):
        self.ex.check_identity(self.host)
        require(self.run(['cat', '/etc/machine-id']) == self.request['machine_id'], 'coordinator identity changed')
        require(self.run(['stat', '-c', '%a:%u:%g', CONFIG]) == '644:0:0', 'unexpected Caddyfile ownership or mode')
        caddy = self.ex.probe_unit(self.host, 'caddy.service')
        require(caddy['active_state'] == 'active'
                and '--config /etc/caddy/Caddyfile' in caddy['fragment_text'] + ''.join(
                    item['text'] for item in caddy['drop_ins']),
                'Caddy must be active and use the installed Caddyfile')
        before = self.ex.read(self.host, CONFIG)
        require(before is not None, 'Caddyfile is absent')
        after = candidate(before, self.template, self.request['enhance_public_url'])
        self.validate(after)
        plan = {'request': self.request, 'before_sha256': digest(before), 'after_sha256': digest(after),
                'template_sha256': digest(self.template), 'action': 'skip' if before == after else 'reload'}
        return plan, before, after

    def status_code(self, path):
        return self.run(['curl', '-sS', '-o', '/dev/null', '-w', '%{http_code}', '--max-time', '20',
                         '--resolve', HOST + ':443:127.0.0.1', 'https://' + HOST + path], 40)

    def probe(self):
        """Every canonical route answers 200 and every refused path 404.

        Retries cover certificate issuance after the first install.
        """
        expected = {path: '200' for path in SERVED} | {path: '404' for path in REFUSED}
        for attempt in range(PROBE_ATTEMPTS):
            try:
                observed = {path: self.status_code(path) for path in expected}
            except DeployError:
                observed = None
            if observed == expected:
                return observed
            if attempt + 1 < PROBE_ATTEMPTS:
                self.sleep(PROBE_INTERVAL)
        raise DeployError('coordinator edge: unified host probes failed: ' + json.dumps(observed, sort_keys=True))

    def write(self, path, data, mode):
        self.lock.verify()
        self.ex.write(self.host, path, data, mode)

    def reload(self):
        self.lock.verify()
        self.ex.systemctl(self.host, 'reload', 'caddy.service')

    def persist(self, journal):
        journal.save()
        directory = '/var/lib/wallet-pir-edge/transactions/' + journal.id
        self.lock.verify()
        self.ex.mkdir(self.host, directory, 0o700)
        self.write(directory + '/journal.json', json.dumps(journal.data).encode(), 0o600)

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
        require(self.ex.read(self.host, CONFIG) == before, 'restored Caddyfile differs')
        journal.set_status('rolled-back')
        self.persist(journal)

    def deploy(self, expected):
        with self.lock_factory() as lock:
            self.lock = lock
            mutation_fence(self.inventory, self.ex, lock)
            latest = Journal.load(self.state_dir, SERVICE)
            require(latest is None or latest.status in FINAL, 'finish the interrupted edge transaction first')
            plan, before, after = self.plan()
            require(digest(json.dumps(plan, sort_keys=True)) == expected, 'plan changed since review')
            if before == after:
                self.probe()
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
                self.probe()
                journal.set_status('committed')
                self.persist(journal)
            except Exception:
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
        require(journal is not None and journal.data['request'] == self.request, 'edge transaction/request mismatch')
        latest = Journal.load(self.state_dir, SERVICE)
        require(latest.id == journal.id or latest.status == 'rolled-back', 'roll back the later edge transaction first')
        with self.lock_factory() as lock:
            self.lock = lock
            mutation_fence(self.inventory, self.ex, lock)
            self.restore(journal)
        return {'phase': journal.status, 'transaction': journal.id}

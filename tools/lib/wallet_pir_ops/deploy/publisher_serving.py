"""Cut over Enhance's private control version with its publisher and wallet edge.

Only the packing router and ingress participate. Workers keep their wallet wire
protocol. Each role's normal file journal is embedded in the publisher journal,
including before every effect, so recovery works after loss of the runner. Roles
start before their matching publisher and are verified only after it is ready.
"""
import base64
import copy
from pathlib import Path
import re

from ..paths import ROOT
from . import descriptors
from .engine import Deployer, file_sha256, mutation_fence
from .publisher import require, sha, stable_unit
from .transaction import Journal

CADDY = '/etc/caddy/Caddyfile'
ROLES = ('packing-router', 'query-ingress')
FALLBACK = '\thandle {\n\t\treverse_proxy 127.0.0.1:8080\n\t}'
WALLET = '\thandle /v1/enhance/* {\n\t\treverse_proxy 127.0.0.1:8082\n\t}\n'


def candidate(text):
    require(text.count(WALLET) == 1 and FALLBACK not in text
            and text.count('\thandle {\n\t\trespond 404\n\t}') == 1,
            'expected the deployed wallet ingress and private publisher routes')
    return text


def validate_request(request, inventory):
    serving = request['serving']
    require(isinstance(serving, dict) and set(serving) == {'git_sha', 'binary_sha256', 'binary_bytes', 'caddy_sha256'},
            'serving release fields must be explicit')
    require(serving['git_sha'] == request['release']['git_sha'], 'publisher and serving source revisions differ')
    require(all(isinstance(serving[k], str) and re.fullmatch('[a-f0-9]{64}', serving[k])
                for k in ('binary_sha256', 'caddy_sha256')), 'invalid serving executable or edge digest')
    require(type(serving['binary_bytes']) is int and serving['binary_bytes'] > 0, 'invalid serving executable size')
    roles = inventory.services['enhance']['roles']
    require(all(len(roles.get(role, [])) == 1 for role in ROLES), 'cutover needs one packing router and one ingress')
    ingress = roles['query-ingress'][0]
    require(ingress['host'] == request['host'] and ingress['settings']['listen'] == '127.0.0.1:8082',
            'wallet ingress must use the existing coordinator loopback listener')


class ServingJournal(Journal):
    """Persist nested role intent to both the runner and coordinator."""
    def __init__(self, cutover, parent):
        self.cutover, self.parent = cutover, parent
        super().__init__(cutover.state_dir / ('serving-' + parent['id'] + '.json'), parent['serving_journal'])

    def save(self):
        super().save()
        self.parent['serving_journal'] = self.data
        self.cutover.save(self.parent)


class Serving:
    def __init__(self, cutover):
        self.cutover = cutover
        self.release = cutover.request['serving']
        service = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')['enhance']
        self.deployer = Deployer(service, cutover.inventory, cutover.ex, cutover.state_dir,
                                 only=ROLES, clock=cutover.clock, sleep=cutover.sleep, out=lambda _message: None)
        self.binary = None

    def validate_edge(self, text):
        payload = base64.b64encode(text.encode()).decode()
        program = ('import base64,subprocess,sys;'
                   'r=subprocess.run(["caddy","validate","--config","/dev/stdin","--adapter","caddyfile"],'
                   'input=base64.b64decode(sys.argv[1]),capture_output=True);'
                   'print((r.stdout+r.stderr).decode()[-1500:]);raise SystemExit(r.returncode)')
        code, output = self.cutover.ex.run(self.cutover.host, ['python3', '-c', program, payload], 30)
        require(code == 0, 'candidate wallet edge is invalid: ' + output[-1500:])

    def capture(self):
        owner, deployer = self.cutover, self.deployer
        mutation_fence(owner.inventory, owner.ex)
        deployer.check_identities()
        if self.binary:
            require(file_sha256(self.binary) == self.release['binary_sha256']
                    and Path(self.binary).stat().st_size == self.release['binary_bytes'], 'serving executable differs from request')
        plans, problems = deployer.assess(self.release['binary_sha256'], binary=self.binary,
                                         allow_drift=True, retire_historical=True, require_baseline=False)
        require(not problems, 'serving preflight refused: ' + '; '.join(problems))
        before = owner.read(CADDY)
        require(before is not None and sha(before) == self.release['caddy_sha256'], 'wallet edge differs from request')
        code, output = owner.ex.run(owner.host, ['stat', '-c', '%a:%u:%g', CADDY], 10)
        require(code == 0 and output.strip() == '644:0:0', 'unexpected wallet edge ownership or mode')
        edge = stable_unit(owner.ex.probe_unit(owner.host, 'caddy.service'))
        require(edge['active_state'] == 'active' and not edge['need_daemon_reload']
                and '--config /etc/caddy/Caddyfile' in edge['fragment_text'], 'Caddy is not active on the installed configuration')
        after = candidate(before)
        self.validate_edge(after)
        return {'roles': [deployer.record(p) for p in plans],
                'units': [stable_unit(p.state) for p in plans], 'edge': edge,
                'edge_before': before, 'edge_after': after}

    def journal(self, parent):
        return ServingJournal(self.cutover, parent)

    def unchanged_during_staging(self, plan):
        owner = self.cutover
        require(all(stable_unit(owner.ex.probe_unit(role['host'], role['unit'])) == before
                    for role, before in zip(plan['roles'], plan['units'])),
                'query service changed during staging')
        require(owner.read(CADDY) == plan['edge_before'] and
                stable_unit(owner.ex.probe_unit(owner.host, 'caddy.service')) == plan['edge'],
                'wallet edge changed during staging')

    def stage(self, parent, binary):
        owner, deployer = self.cutover, self.deployer
        mutation_fence(owner.inventory, owner.ex, owner.lock)
        require(binary is not None and file_sha256(binary) == self.release['binary_sha256'], 'serving executable is required')
        parent['serving_journal'] = {'id': parent['id'], 'service': 'enhance', 'hosts': copy.deepcopy(parent['plan']['serving']['roles']),
                                     'status': 'staging', 'events': []}
        journal = self.journal(parent)
        journal.save()
        deployer.lock = owner.lock
        deployer.stage([r['host'] for r in journal.hosts], self.release['binary_sha256'], binary, journal)
        deployer.product_checks(journal, self.release['binary_sha256'])

    def activate(self, parent):
        deployer = self.deployer
        deployer.lock = self.cutover.lock
        journal = self.journal(parent)
        for index, record in enumerate(journal.hosts):
            if record['action'] != 'skip':
                deployer.activate(journal, index, self.release['binary_sha256'], verify=False)

    def verify(self, parent, restored=False):
        if 'serving_journal' not in parent:
            return
        journal = self.journal(parent)
        self.deployer.lock = self.cutover.lock
        for index, record in enumerate(journal.hosts):
            digest = record['previous']['exe_sha256'] if restored else self.release['binary_sha256']
            self.deployer.wait_verified(record, digest)
            journal.set_phase(index, 'restored' if restored else 'verified')
        journal.set_status('rolled-back' if restored else 'verified')

    def route(self, parent):
        owner = self.cutover
        edge = parent['plan']['serving']
        require(owner.read(CADDY) == edge['edge_before'], 'wallet edge changed before installation')
        owner.effect(parent, 'install Enhance wallet edge', lambda: owner.write(CADDY, edge['edge_after'], 0o644))
        owner.effect(parent, 'reload Enhance wallet edge', lambda: owner.ex.systemctl(owner.host, 'reload', 'caddy.service'))
        require(owner.read(CADDY) == edge['edge_after'], 'installed wallet edge differs')

    def restore_guard(self, parent):
        owner = self.cutover
        mutation_fence(owner.inventory, owner.ex, owner.lock)
        edge = parent['plan']['serving']
        require(owner.read(CADDY) in (edge['edge_before'], edge['edge_after']), 'wallet edge changed outside the transaction')
        self.validate_edge(edge['edge_before'])
        if 'serving_journal' in parent:
            for record in self.journal(parent).hosts:
                require(not self.deployer.restore_conflicts(record), 'serving files changed outside the transaction: ' + record['key'])
                require(owner.ex.sha256(record['host'], record['previous']['exec_binary']) == record['previous']['exe_sha256'],
                        'serving rollback executable changed')

    def restore(self, parent):
        owner = self.cutover
        edge = parent['plan']['serving']
        owner.effect(parent, 'restore Enhance wallet edge', lambda: owner.write(CADDY, edge['edge_before'], 0o644))
        owner.effect(parent, 'reload restored Enhance wallet edge', lambda: owner.ex.systemctl(owner.host, 'reload', 'caddy.service'))
        if 'serving_journal' in parent:
            journal = self.journal(parent)
            self.deployer.lock = owner.lock
            for index in reversed(journal.touched()):
                self.deployer.restore(journal, index, verify=False)

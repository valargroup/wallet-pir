"""Direct Enhance HTTPS cutover under the unified publisher transaction.

Every existing worker, router and ingress is captured, including separate CPU
and CUDA executable identities. The source's offline Rust handoff owns recovery
state conversion; this adapter never reconstructs cryptographic session IDs.
"""
import copy
import json
from pathlib import Path
from urllib.parse import urlsplit

from ..paths import ROOT
from . import descriptors
from .engine import Deployer, file_sha256, mutation_fence
from .publisher import require, sha, stable_unit
from .publisher_serving import CADDY, Serving, ServingJournal, candidate

SCHEMA = 'enhance-https-cutover/1'
ROLES = ('worker', 'external-worker', 'packing-router', 'query-ingress')
PROVIDER = '\thandle /provider/enhance/v1/* {\n\t\treverse_proxy 127.0.0.1:8084\n\t}\n'


def prune_qualification(args, executor=None):
    """Delete only the explicitly approved, unused historical CPU fixture tree.

    The normal fleet lock excludes deployments. Each host retains a durable
    intent and result before/after this irreversible, explicitly named cleanup.
    Serving state and release directories cannot be selected by this command.
    """
    from .descriptors import load_inventory
    from .remote import SSHExecutor, production_lock
    from .transaction import new_id
    inventory = load_inventory(args.inventory)
    allowed = {r['host'] for r in inventory.services['enhance']['roles']['worker']
               if inventory.hosts[r['host']].get('accelerator') != 'cuda'
               and r.get('settings', {}).get('matvec_backend') != 'cuda'}
    require(args.host and len(set(args.host)) == len(args.host) and set(args.host) <= allowed,
            'qualification cleanup requires distinct existing CPU worker hosts')
    executor = executor or SSHExecutor(inventory)
    identifier = new_id('qualification-prune', sha(json.dumps(sorted(args.host))))
    program = '''import json,os,shutil,sys,time
from pathlib import Path
root=Path('/srv/enhance-pir-v7/qualification-worker')
assert str(root)==sys.argv[1] and root.is_dir() and not root.is_symlink() and root.resolve()==root
assert not any(line.split()[4]==str(root) or line.split()[4].startswith(str(root)+'/')
               for line in Path('/proc/self/mountinfo').read_text().splitlines())
for p in Path('/proc').iterdir():
    if not p.name.isdecimal() or p.name==str(os.getpid()): continue
    try:
        command=(p/'cmdline').read_bytes()
        assert not any(arg==str(root).encode() or arg.startswith(str(root).encode()+b'/')
                       for arg in command.split(bytes([0]))[1:]), 'qualification directory is in a running command'
        for f in [p/'cwd',*(p/'fd').iterdir()]:
            try: target=os.readlink(f)
            except OSError: continue
            assert target!=str(root) and not target.startswith(str(root)+'/'), 'qualification directory is open'
    except (FileNotFoundError,ProcessLookupError): pass
receipt=Path('/opt/enhance-pir/maintenance')/sys.argv[2]
receipt.mkdir(mode=0o700,parents=True,exist_ok=False)
state={'path':str(root),'phase':'deleting','available_before':os.statvfs(root).f_bavail*os.statvfs(root).f_frsize}
def save():
    temporary=receipt/'state.tmp'
    with temporary.open('w') as f:
        json.dump(state,f);f.flush();os.fsync(f.fileno())
    temporary.chmod(0o600);os.replace(temporary,receipt/'state.json')
    fd=os.open(receipt,os.O_RDONLY);os.fsync(fd);os.close(fd)
save()
shutil.rmtree(root)
state.update(phase='complete',available_after=os.statvfs(root.parent).f_bavail*os.statvfs(root.parent).f_frsize)
save();print(json.dumps(state))
'''
    result = {'transaction': identifier, 'hosts': {}}
    with production_lock(inventory, executor) as lock:
        for host in args.host:
            lock.verify()
            code, output = executor.run(host, ['python3', '-c', program, args.confirm_path, identifier], 90)
            require(code == 0, 'qualification cleanup refused on ' + host + ': ' + output[-1000:])
            result['hosts'][host] = json.loads(output)
    return result


def validate(request, inventory):
    from .publisher_serving import validate_request
    validate_request(request, inventory)
    options = request['enhance_https']
    require(isinstance(options, dict) and set(options) ==
            {'origin', 'publisher_budget_bytes', 'cuda_release', 'progress'},
            'Enhance HTTPS cutover needs explicit source, disk, CUDA and progress inputs')
    origin = urlsplit(options['origin'])
    require(origin.scheme == 'https' and origin.hostname and origin.port in (None, 443)
            and not origin.username and not origin.password and origin.path in ('', '/')
            and not origin.query and not origin.fragment, 'Enhance publisher requires a trusted HTTPS origin')
    require(type(options['publisher_budget_bytes']) is int and options['publisher_budget_bytes'] > 0,
            'Enhance publication disk budget must be explicit')
    settings = request['configuration']['enhance']['settings']
    require({'artifact_listen', 'listen', 'data_dir'} <= set(settings)
            <= {'artifact_listen', 'listen', 'data_dir', 'prevout_cache_outputs'}
            and settings['artifact_listen'] == '127.0.0.1:8084',
            'Enhance publisher must contain only canonical source settings and private listeners')
    # The node endpoint belongs to the root chain follower, never to a product.
    chain = request['configuration'].get('chain')
    require(isinstance(chain, dict) and {'rpc_url', 'rpc_cookie'} <= set(chain)
            <= {'rpc_url', 'rpc_cookie', 'poll_ms'},
            'Enhance publisher needs the root chain settings')
    service = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')['enhance']
    targets = descriptors.select(descriptors.targets(service, inventory), ROLES[:1] + ROLES[2:])
    # External workers are optional; include every one actually in the inventory.
    all_targets = [t for t in descriptors.targets(service, inventory) if t.role.name in ROLES]
    require(targets and len({(t.host, t.unit) for t in all_targets}) == len(all_targets),
            'Enhance cutover needs distinct existing serving targets')
    has_cuda = False
    for target in all_targets:
        sync = target.settings.get('synchronization', {})
        require(sync.get('publisher_origin') == options['origin'], 'consumer publisher origin differs')
        has_cuda |= target.settings.get('matvec_backend') == 'cuda'
    cuda = options['cuda_release']
    require((cuda is not None) == has_cuda, 'CUDA executable must match the selected workers')
    if cuda is not None:
        require(isinstance(cuda, dict) and set(cuda) == {'binary_sha256', 'binary_bytes'},
                'CUDA executable fields must be explicit')
        require(isinstance(cuda['binary_sha256'], str) and len(cuda['binary_sha256']) == 64
                and all(c in '0123456789abcdef' for c in cuda['binary_sha256'])
                and type(cuda['binary_bytes']) is int and cuda['binary_bytes'] > 0,
                'invalid CUDA executable identity')
    progress = options['progress']
    require(isinstance(progress, dict) and set(progress) == set(request['checks']),
            'every enabled publisher product needs a fresh-advancement observation')
    for probe in progress.values():
        require(isinstance(probe, dict) and set(probe) == {'host', 'url', 'field'}
                and probe['host'] in inventory.hosts and isinstance(probe['field'], str)
                and probe['field'] and all(p for p in probe['field'].split('.')),
                'invalid product progress observation')
        parsed = urlsplit(probe['url'])
        require(parsed.scheme in ('http', 'https') and parsed.hostname
                and not parsed.username and not parsed.password and not parsed.fragment,
                'invalid product progress URL')
    require(progress['enhance'] == {'host': request['host'],
            'url': options['origin'].rstrip('/') + '/v1/enhance/init', 'field': 'anchor_height'},
            'Enhance progress must observe the wallet manifest anchor')
    status = urlsplit(progress['status']['url'])
    require(status.scheme == 'https' and status.path == '/v1/status/init'
            and progress['status']['field'] == 'anchor_height',
            'Status progress must observe the wallet manifest anchor')
    for product, setting, field in [('transparent_history', 'listen', 'public_height'),
                                     ('transaction_details', 'status_listen', 'published.tip')]:
        listen = request['configuration'][product]['settings'][setting]
        require(progress[product] == {'host': request['host'], 'url': 'http://' + listen + '/v1/status', 'field': field},
                product + ' progress must observe the actual published height')


class GroupJournal(ServingJournal):
    def __init__(self, cutover, parent, group):
        self.group = group
        # Reuse the existing nested journal persistence contract for each
        # executable group without allowing one digest to qualify the other.
        proxy = dict(parent, serving_journal=parent['enhance_journals'][group])
        super().__init__(cutover, proxy)
        self.parent = parent
        self.path = cutover.state_dir / ('serving-' + parent['id'] + '-' + group + '.json')

    def save(self):
        from .transaction import Journal
        Journal.save(self)
        self.parent['enhance_journals'][self.group] = self.data
        self.cutover.save(self.parent)


class EnhanceServing(Serving):
    def __init__(self, cutover):
        self.cutover, self.release = cutover, cutover.request['serving']
        service = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')['enhance']
        self.deployer = Deployer(service, cutover.inventory, cutover.ex, cutover.state_dir,
                                 out=lambda _: None, clock=cutover.clock, sleep=cutover.sleep)
        self.deployer.targets = [t for t in self.deployer.targets if t.role.name in ROLES]
        self.groups = {}
        self.binary, self.cuda_binary = None, None
        for kind in ('cpu', 'cuda'):
            targets = [t.key for t in self.deployer.targets
                       if (t.settings.get('matvec_backend') == 'cuda') == (kind == 'cuda')]
            if targets:
                self.groups[kind] = Deployer(service, cutover.inventory, cutover.ex, cutover.state_dir,
                    only=targets, out=lambda _: None, clock=cutover.clock, sleep=cutover.sleep)

    def release_for(self, group):
        return self.release if group == 'cpu' else self.cutover.request['enhance_https']['cuda_release']

    def capture(self):
        owner = self.cutover
        mutation_fence(owner.inventory, owner.ex)
        budgets = {}
        for target in self.deployer.targets:
            kind = 'cuda' if target.settings.get('matvec_backend') == 'cuda' else 'cpu'
            allowance = (target.settings['synchronization']['cache_limit_bytes']
                         + self.release_for(kind)['binary_bytes'])
            budgets[target.host] = budgets.get(target.host, 0) + allowance
        for target in self.deployer.targets:
            # The safety file identifies the actual cache filesystem even when
            # the new state directory has not been created. This probe is read-only.
            path = str(Path(target.settings['data_dir']) / 'sync.json')
            resources = owner.ex.publisher_resources(target.host, [path])
            require(all(disk['available'] - budgets[target.host] >= disk['total'] * .2
                        for disk in resources['disks'].values()),
                    'Enhance consumer cache staging would leave less than 20 percent disk headroom')
        roles, states = {}, {}
        for kind, deployer in self.groups.items():
            deployer.check_identities()
            release = self.release_for(kind)
            binary = self.binary if kind == 'cpu' else self.cuda_binary
            if binary:
                require(file_sha256(binary) == release['binary_sha256']
                        and Path(binary).stat().st_size == release['binary_bytes'],
                        'serving executable differs from its reviewed identity')
            plans, problems = deployer.assess(release['binary_sha256'], binary=binary,
                allow_drift=True, retire_historical=True, require_baseline=False)
            require(not problems, 'Enhance serving preflight refused: ' + '; '.join(problems))
            roles[kind], states[kind] = [deployer.record(p) for p in plans], [stable_unit(p.state) for p in plans]
            require(all(p.state['active_state'] == 'active' and p.state['main_pid'] for p in plans),
                    'every Enhance predecessor must be running before cutover')
        before = owner.read(CADDY)
        require(before is not None and sha(before) == self.release['caddy_sha256'], 'wallet edge differs')
        candidate(before)
        require('/provider/enhance/v1' not in before and before.count('\thandle /v1/enhance/* {') == 1,
                'Enhance provider route must be a fresh direct cutover')
        code, output = owner.ex.run(owner.host, ['stat', '-c', '%a:%u:%g', CADDY], 10)
        require(code == 0 and output.strip() == '644:0:0', 'unexpected wallet edge ownership or mode')
        after = before.replace('\thandle /v1/enhance/* {', PROVIDER + '\thandle /v1/enhance/* {', 1)
        self.validate_edge(after)
        edge = stable_unit(owner.ex.probe_unit(owner.host, 'caddy.service'))
        require(edge['active_state'] == 'active' and not edge['need_daemon_reload']
                and '--config /etc/caddy/Caddyfile' in edge['fragment_text'], 'wallet edge is not stable')
        return {'groups': roles, 'group_units': states, 'edge': edge, 'edge_before': before, 'edge_after': after}

    def unchanged_during_staging(self, plan):
        owner = self.cutover
        for kind, records in plan['groups'].items():
            require(all(stable_unit(owner.ex.probe_unit(r['host'], r['unit'])) == state
                        for r, state in zip(records, plan['group_units'][kind])),
                    'Enhance serving target changed during staging')
        require(owner.read(CADDY) == plan['edge_before'] and
                stable_unit(owner.ex.probe_unit(owner.host, 'caddy.service')) == plan['edge'],
                'wallet edge changed during staging')

    def stage(self, parent, binary):
        owner = self.cutover
        parent['enhance_journals'] = {}
        for kind, deployer in self.groups.items():
            executable = binary if kind == 'cpu' else self.cuda_binary
            release = self.release_for(kind)
            require(executable is not None and file_sha256(executable) == release['binary_sha256'],
                    'matching CPU and CUDA serving executables are required')
            parent['enhance_journals'][kind] = {'id': parent['id'] + '-' + kind, 'service': 'enhance',
                'hosts': copy.deepcopy(parent['plan']['serving']['groups'][kind]), 'status': 'staging', 'events': []}
            journal = GroupJournal(owner, parent, kind)
            journal.save()
            deployer.lock = owner.lock
            deployer.stage([r['host'] for r in journal.hosts], release['binary_sha256'], executable, journal)
            deployer.product_checks(journal, release['binary_sha256'])

    def handoff(self, parent, rollback=False):
        owner = self.cutover
        data = owner.request['configuration']['enhance']['settings']['data_dir']
        argv = [parent['plan']['binary'], 'enhance', 'handoff-https-state', '--data-dir', data,
                '--transaction', parent['id']]
        if rollback:
            argv.append('--rollback')
        code, output = owner.ex.run(owner.host, argv, 60)
        require(code == 0, 'Enhance recovery handoff refused: ' + output[-1000:])
        receipt = json.loads(output)
        require(receipt.get('transaction') == parent['id'] and receipt.get('rollback') is rollback
                and type(receipt.get('recovery_epoch')) is int,
                'Enhance recovery handoff returned an invalid receipt')
        parent['enhance_handoff_rollback' if rollback else 'enhance_handoff'] = receipt
        owner.save(parent)

    def prepare_handoff(self, parent):
        owner = self.cutover
        parent['enhance_handoff_started'] = True
        owner.save(parent)
        owner.effect(parent, 'preserve Enhance recovery floors', lambda: self.handoff(parent))

    def publication_ready(self):
        """Require eligible HTTPS material before interrupting any consumer."""
        owner = self.cutover
        origin = owner.request['enhance_https']['origin'].rstrip('/')
        deadline = owner.clock() + 900
        while True:
            owner.supervision.guard()
            discovery, _ = owner.ex.http_get(owner.host, origin + '/.well-known/pir-service-discovery')
            code, body = owner.ex.http_get(owner.host, origin + '/provider/enhance/v1/current')
            try:
                current = json.loads(body)['current']
                if (discovery == 200 and code == 200 and current['eligible'] is True
                        and current.get('manifest') and current.get('inventory')):
                    return
            except (ValueError, KeyError, TypeError):
                pass
            require(owner.clock() < deadline, 'Enhance HTTPS publication readiness deadline exceeded')
            owner.sleep(3)

    def activate(self, parent):
        owner = self.cutover
        for kind, deployer in self.groups.items():
            deployer.lock = owner.lock
            journal = GroupJournal(owner, parent, kind)
            for index, record in enumerate(journal.hosts):
                if record['action'] != 'skip':
                    deployer.activate(journal, index, self.release_for(kind)['binary_sha256'], verify=False)
        self.route(parent)

    def route(self, parent):
        if self.cutover.read(CADDY) == parent['plan']['serving']['edge_after']:
            return
        super().route(parent)

    def verify(self, parent, restored=False):
        for kind, deployer in self.groups.items():
            if kind not in parent.get('enhance_journals', {}):
                continue
            deployer.lock = self.cutover.lock
            journal = GroupJournal(self.cutover, parent, kind)
            for index, record in enumerate(journal.hosts):
                expected = record['previous']['exe_sha256'] if restored else self.release_for(kind)['binary_sha256']
                check = copy.deepcopy(record)
                if restored and record['role'] in ('worker', 'external-worker'):
                    check['verify']['ready'] = {'field': 'published', 'nonempty': True}
                deployer.wait_verified(check, expected)
                journal.set_phase(index, 'restored' if restored else 'verified')
            journal.set_status('rolled-back' if restored else 'verified')

    def progress(self):
        observed = {}
        for product, probe in self.cutover.request['enhance_https']['progress'].items():
            status, body = self.cutover.ex.http_get(probe['host'], probe['url'])
            require(status == 200, product + ' progress endpoint is unavailable')
            value = json.loads(body)
            for part in probe['field'].split('.'):
                require(isinstance(value, dict) and part in value, product + ' progress field is missing')
                value = value[part]
            require(type(value) is int and value >= 0, product + ' progress is not an unsigned height')
            observed[product] = value
        return observed

    def verify_advancement(self, parent):
        owner = self.cutover
        before = self.progress()
        parent['product_progress'] = {'before': before}
        owner.save(parent)
        while True:
            owner.supervision.guard()
            after = self.progress()
            if all(after[name] > height for name, height in before.items()):
                parent['product_progress']['after'] = after
                owner.save(parent)
                return
            owner.sleep(3)

    def restore_guard(self, parent):
        owner = self.cutover
        mutation_fence(owner.inventory, owner.ex, owner.lock)
        edge = parent['plan']['serving']
        require(owner.read(CADDY) in (edge['edge_before'], edge['edge_after']), 'wallet edge changed outside transaction')
        self.validate_edge(edge['edge_before'])
        for kind, deployer in self.groups.items():
            if kind not in parent.get('enhance_journals', {}):
                continue
            for record in GroupJournal(owner, parent, kind).hosts:
                require(not deployer.restore_conflicts(record), 'Enhance serving files changed outside transaction')
                require(owner.ex.sha256(record['host'], record['previous']['exec_binary']) == record['previous']['exe_sha256'],
                        'Enhance rollback executable changed')

    def restore(self, parent):
        owner = self.cutover
        for kind, deployer in self.groups.items():
            if kind not in parent.get('enhance_journals', {}):
                continue
            for record in GroupJournal(owner, parent, kind).hosts:
                current = owner.ex.probe_unit(record['host'], record['unit'])
                require(not current['main_pid'] or current['exe_sha256'] in
                        (self.release_for(kind)['binary_sha256'], record['previous']['exe_sha256']),
                        'Enhance serving owner changed before rollback')
                owner.effect(parent, 'stop Enhance serving before floor transfer ' + record['key'],
                    lambda r=record: owner.ex.systemctl(r['host'], 'stop', r['unit']))
                stopped = owner.ex.probe_unit(record['host'], record['unit'])
                require(not stopped['main_pid'], 'Enhance serving owner did not stop')
        if parent.get('enhance_handoff_started'):
            owner.effect(parent, 'fence Enhance sessions for rollback', lambda: self.handoff(parent, True))
        for kind, deployer in reversed(list(self.groups.items())):
            if kind not in parent.get('enhance_journals', {}):
                continue
            deployer.lock = owner.lock
            journal = GroupJournal(owner, parent, kind)
            for index in reversed(range(len(journal.hosts))):
                # Every predecessor was stopped above, even when failure came
                # before its ordinary activation record was touched.
                deployer.restore(journal, index, verify=False)
        edge = parent['plan']['serving']
        owner.effect(parent, 'restore Enhance wallet edge', lambda: owner.write(CADDY, edge['edge_before'], 0o644))
        owner.effect(parent, 'reload restored Enhance edge', lambda: owner.ex.systemctl(owner.host, 'reload', 'caddy.service'))

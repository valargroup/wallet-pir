"""Complete ordered service phases for a prepared, reviewed v11 fleet.

Run exclusively through the schema wrapper. Preparation must already have
staged immutable native/unit/publication inputs on every pinned host; preflight
refuses otherwise. This coordinates concrete Host/Routing transitions, seeds
restart-safe v11 activation state, and keeps rollback within the outer budget.
It does not replace publication/oracle/certificate or capacity qualification.
"""
import asyncio
import importlib.util
from pathlib import Path

from wallet_pir_ops import inherited_lock
from wallet_pir_ops.deploy import descriptors

HERE = Path(__file__).parent


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


T = module('product_templates', HERE/'activity_schema_templates.py')
D = module('product_dispatch', HERE/'activity_schema_dispatch.py')
O = module('product_operation', HERE/'activity_schema_operation.py')
I = module('product_inputs', HERE/'activity_input_stage.py')
H, R = T.H, T.R
NATIVE = '12ce12918446eaa56e2d766ec2f43d82c531abb9'
STATE = Path('/srv/transparent-activity/ops/schema')
PUBLICATION = Path('/srv/transparent-activity/full-v11/publications/initial')
LOAD_ROOT = Path('/srv/transparent-activity/canonical-load/v11')
LOAD_BINARY = Path('/srv/transparent-activity/build/evidence')/('release-'+NATIVE)/'artifacts/examples/rate-query'
GATES = {'artifact-verification', 'native-certificates', 'independent-chain-oracle', 'comprehensive-ci'}


def input_(value):
    H.require(isinstance(value, dict) and set(value) == {'path', 'sha256'} and
              Path(value['path']).is_absolute() and H.HEX.fullmatch(value['sha256']), 'invalid product input')
    return value


def checked(value):
    input_(value)
    O.verify_inputs([value])
    return Path(value['path'])


def validate(spec):
    H.require(isinstance(spec, dict) and set(spec) == {'version', 'source_sha', 'inventory', 'publication_sha256',
              'assignment', 'recent_from', 'hosts', 'routing', 'load', 'gates'}, 'invalid product specification')
    H.require(type(spec['version']) is int and spec['version'] == 1 and
              isinstance(spec['source_sha'], str) and H.re.fullmatch('[0-9a-f]{40}', spec['source_sha']) and
              H.HEX.fullmatch(spec['publication_sha256']), 'invalid product identity')
    input_(spec['inventory']); input_(spec['assignment'])
    H.require(type(spec['recent_from']) is int and spec['recent_from'] >= 0, 'invalid recent cutoff')
    H.require(isinstance(spec['hosts'], list) and 5 <= len(spec['hosts']) <= 32, 'product fleet requires coordinator, router and all workers')
    names, machines, workers, roles = set(), set(), set(), []
    for entry in spec['hosts']:
        H.require(isinstance(entry, dict) and set(entry) == {'host', 'plan'} and
                  isinstance(entry['host'], str) and H.re.fullmatch('[a-zA-Z0-9-]{1,64}', entry['host']), 'invalid product host')
        T.validate(entry['plan'], 'host')
        plan = entry['plan']
        H.require(plan['source_sha'] == spec['source_sha'] and entry['host'] not in names and
                  plan['machine_id'] not in machines, 'duplicate host or source disagreement')
        names.add(entry['host']); machines.add(plan['machine_id']); roles.append(plan['role'])
        if plan['role'] == 'worker':
            w = plan['worker']
            H.require(w['id'] not in workers and w['map_file_sha256'] == spec['publication_sha256'] and
                      w['assignment_sha256'] == spec['assignment']['sha256'], 'worker publication/assignment disagreement')
            workers.add(w['id'])
    H.require(roles.count('coordinator') == 1 and roles.count('router') == 1 and roles.count('worker') >= 3, 'incomplete role inventory')
    T.validate(spec['routing'], 'routing')
    coordinator = next(e['plan'] for e in spec['hosts'] if e['plan']['role'] == 'coordinator')
    H.require(spec['routing']['machine_id'] == coordinator['machine_id'] and
              spec['routing']['source_sha'] == spec['source_sha'], 'routing coordinator disagreement')
    load = spec['load']
    H.require(isinstance(load, dict) and set(load) == {'binary', 'fixture', 'pins', 'policy'} and
              all(input_(v) for v in load.values()), 'invalid load/scaler inputs')
    H.require(load['binary']['path'] == str(LOAD_BINARY),
              'continuous load must use the retained fat-LTO artifact')
    H.require(isinstance(spec['gates'], dict) and set(spec['gates']) == GATES and
              all(input_(v) for v in spec['gates'].values()), 'missing full-publication release gates')
    return spec


class Product:
    def __init__(self, spec, *, spec_sha256=None, dispatch_factory=D.Dispatch, host_factory=H.Host, routing_factory=R.Routing):
        self.spec = validate(spec)
        self.spec_sha256 = spec_sha256
        self.inventory = descriptors.load_inventory(checked(spec['inventory']))
        self.dispatch = dispatch_factory(self.inventory)
        self.host_factory, self.routing_factory = host_factory, routing_factory

    def bound(self, transaction):
        self.hosts = [{**e, 'plan':T.bind(e['plan'], 'host', transaction)} for e in self.spec['hosts']]
        self.coordinator = next(e for e in self.hosts if e['plan']['role'] == 'coordinator')
        self.router = next(e for e in self.hosts if e['plan']['role'] == 'router')
        self.workers = [e for e in self.hosts if e['plan']['role'] == 'worker']
        self.local = self.host_factory(self.coordinator['plan'])
        self.routing = self.routing_factory(T.bind(self.spec['routing'], 'routing', transaction))
        self.root = STATE/transaction/'product'

    def remote(self, entry, action, attempt):
        request = {'version':1, 'request_id':str(attempt)+'-'+action, 'action':action,
                   'plan':entry['plan'], 'plan_sha256':D.digest(entry['plan'])}
        return self.dispatch.call(entry['host'], request, timeout=330 if action in ('preflight','stage') else 90)

    async def all_workers(self, action, attempt):
        # Distinct pinned hosts own distinct locks/results. Always join every
        # outcome: a failed first worker cannot hide another live remote owner.
        replies = await asyncio.gather(*(asyncio.to_thread(self.remote, e, action, attempt) for e in self.workers), return_exceptions=True)
        unknown = next((r for r in replies if isinstance(r, D.subprocess.TimeoutExpired)), None)
        if unknown: raise unknown
        failure = next((r for r in replies if isinstance(r, BaseException)), None)
        if failure: raise failure
        return replies

    def inputs(self):
        source=self.spec['source_sha']
        receipt=H.load(Path('/srv/transparent-activity/ops/staging')/(source+'.json'))
        D.S.verify_receipt(receipt, Path('/srv/transparent-activity/ops/sources')/source, source, receipt['archive_sha256'])
        map_path = PUBLICATION/'shards.json'
        H.require(map_path.is_file() and not map_path.is_symlink() and H.checksum(map_path) == self.spec['publication_sha256'],
                  'full publication identity changed')
        mapping = H.load(map_path)
        H.require(mapping.get('start_height') == 0 and mapping.get('shards') and
                  mapping['shards'][-1]['end_height'] == 3500738, 'initial publication is not the complete approved range')
        assignment = H.load(checked(self.spec['assignment']))
        R.assignment_digest(assignment)
        H.require(assignment['set']['map_sha256'] == H.transparent_map.served_sha256(mapping) and
                  assignment['set']['shard_schema'] == 'transparent-shard-v11' and not assignment['unassigned'] and
                  assignment['generated_by']['source_sha'] == NATIVE, 'assignment does not bind frozen native inputs')
        H.require(all(e['plan']['worker']['map_sha256'] == assignment['set']['map_sha256'] for e in self.workers),
                  'worker protocol identity disagrees with native assignment')
        rows = assignment['workers']
        H.require({w['id'] for w in rows} == {e['plan']['worker']['id'] for e in self.workers} and
                  sum(w['role'] == 'recent-replica' for w in rows) >= 2 and
                  any(w['role'] == 'archive-owner' for w in rows), 'assignment omits reviewed workers')
        for name, entry in self.spec['gates'].items():
            result = H.load(checked(entry))
            H.require(result.get('status') == 'passed' and result.get('gate') == name and
                      result.get('native_source_sha') == NATIVE and
                      result.get('publication_sha256') == self.spec['publication_sha256'], 'release gate identity does not match: '+name)
        for entry in self.spec['load'].values(): checked(entry)
        pins = H.load(self.spec['load']['pins']['path'])
        H.require(pins == {e['plan']['worker']['id']:e['plan']['worker']['binary_sha256'] for e in self.workers}, 'load pins omit/change a worker')
        fixture = H.load(self.spec['load']['fixture']['path'])
        H.require(fixture.get('schema') == 'transparent-shard-v11' and isinstance(fixture.get('tables'),list) and fixture['tables'],
                  'continuous load fixture is not v11')
        entries = {entry['shard_id']:entry for entry in mapping['shards']}
        groups=set()
        for target in fixture['tables']:
            entry=entries.get(target.get('shard_id'))
            H.require(entry is not None and target.get('revision') == entry['manifest_digest'] and
                      target.get('geometry') == entry['geometry'], 'load fixture names an unverified revision')
            groups.add((target['geometry'],target.get('table')))
        H.require(groups == {(g,t) for g in ('recent-4k-8k','archive-wide') for t in ('directory','pages')},
                  'load fixture omits the required 80/20 recent/archive directory/page mix')
        release=Path('/srv/transparent-activity/build/evidence')/('release-'+NATIVE)/'artifacts'
        for entry in self.hosts:
            for install in entry['plan']['installs']:
                if install['target'].startswith('/usr/local/bin/'):
                    name = Path(install['target']).name
                    binary = I.worker_binary(name) if entry['plan']['role'] == 'worker' and name in I.WORKER_HASHES else release/name
                    H.require(H.checksum(binary) == install['sha256'], 'host native binary is not the selected release')
        policy = H.load(self.spec['load']['policy']['path'])
        H.require(policy.get('mode') == 'observe', 'schema switch requires observe-only scaler policy')
        self.mapping, self.assignment, self.rows = mapping, assignment, rows

    def preflight(self):
        self.bound(T.VALIDATION_ID)
        self.local.identity()
        H.require(self.inventory.lock == {'type':'pinned_host', 'machine_id':self.coordinator['plan']['machine_id']}, 'product inventory must pin the root coordinator')
        for entry in self.hosts:
            H.require(self.inventory.hosts[entry['host']].get('machine_id') == entry['plan']['machine_id'], 'host inventory pin changed')
        self.inputs()
        self.units()
        self.local.preflight()
        for entry in [self.router, *self.workers]: self.remote(entry, 'preflight', 0)
        # Every actual candidate byte must exist before maintenance starts.
        H.require(not (PUBLICATION.parent/'active.json').exists(), 'initial candidate already activated; reconcile')
        return {'status':'passed', 'publication_sha256':self.spec['publication_sha256']}

    def units(self):
        installs = {i['target']:Path(i['source']) for i in self.coordinator['plan']['installs']}
        source = Path('/srv/transparent-activity/ops/sources')/self.spec['source_sha']
        expected = {
            H.LOAD:['/usr/bin/python3','-B',str(source/'transparent/ops/scripts/transparent-quality-load.py'),
                    '--root',str(LOAD_ROOT),'--fleet',str(H.ROOT/'v11/fleet.json'),
                    '--expected-worker-sha256-file',str(LOAD_ROOT/'pins.json')],
            H.SCALER:['/usr/bin/python3','-B',str(source/'transparent/ops/scripts/transparent-fleet-scaler.py'),
                      '--state-dir',str(H.ROOT/'v11/state'),'--scaler-dir',str(H.ROOT/'v11/scaler')]}
        for unit, argv in expected.items():
            path = '/etc/systemd/system/'+unit
            H.require(path in installs, 'product omits schema-bound load/scaler unit')
            starts = [H.shlex.split(line[10:]) for line in installs[path].read_text().splitlines() if line.startswith('ExecStart=')]
            H.require(starts == [argv], 'load/scaler unit disagrees with reviewed v11 source, fixture or state')
        filter_ = installs['/etc/systemd/system/'+H.FILTER].read_text()
        starts = [H.shlex.split(line[10:]) for line in filter_.splitlines() if line.startswith('ExecStart=')]
        H.require(len(starts) == 1 and starts[0][0] == '/usr/local/bin/transparent-filter-server', 'filter executable is not pinned')
        args = starts[0][1:]
        H.require(args.count('--shard-dir') == 1 and args[args.index('--shard-dir')+1] == str(PUBLICATION) and
                  not any(a.startswith('--shard-dir=') for a in args), 'filter still serves another schema publication')

    def seed(self):
        self.local.withdrawn()
        self.local.quiet(H.UNITS['coordinator'])
        self.inputs()
        self.units()
        state = H.ROOT/'v11/state'
        H.require(state.is_dir() and not any(state.iterdir()), 'candidate fleet state already exists; reconcile')
        digest = H.transparent_map.served_sha256(self.mapping)
        assignment_path = state/(digest+'.assignment.json')
        H.B.atomic(assignment_path, checked(self.spec['assignment']).read_bytes())
        roster = H.load(H.ROOT/'v11/roster.json')
        H.require({w['id'] for w in roster} == {w['id'] for w in self.rows}, 'installed roster omits planned fleet')
        H.B.atomic(state/(digest+'.roster.json'), H.encode(roster))
        prepared = {e['plan']['worker']['id']:{'expected':digest, 'publication':{k:e['plan']['worker'][k] for k in ('directory','assignment','map_sha256')}} for e in self.workers}
        req = {'directory':str(PUBLICATION), 'map_sha256':digest, 'recent_from':self.spec['recent_from'],
               'source_sha':NATIVE, 'assignment':str(assignment_path),
               'prepared':{'ok':True, 'workers':prepared, 'assignment':str(assignment_path)}}
        for name, value in ((digest+'.request.json',req), ('desired.json',req), (digest+'.prepared.json',prepared),
                            ('active.json',{'map_sha256':digest,'workers':sorted(prepared),'assignment':str(assignment_path)}),
                            ('maintenance.json',{'enabled':True})):
            H.B.atomic(state/name, H.encode(value))
        terminal = self.mapping['shards'][-1]
        H.B.atomic(PUBLICATION.parent/'active.json', H.encode({'directory':str(PUBLICATION),'map_sha256':digest,
                   'height':terminal['end_height'],'hash':terminal['terminal_block_hash'], 'upstreams':[w['upstream'] for w in self.rows]}))
        scaler = H.ROOT/'v11/scaler'
        scaler.mkdir(mode=0o700)
        H.B.atomic(scaler/'policy.json', checked(self.spec['load']['policy']).read_bytes())
        LOAD_ROOT.mkdir(parents=True, mode=0o700)
        for name, key, mode in (('rate-query','binary',0o755), ('fixture.json','fixture',0o600), ('pins.json','pins',0o600)):
            H.B.atomic(LOAD_ROOT/name, checked(self.spec['load'][key]).read_bytes(), mode)
            (LOAD_ROOT/name).chmod(mode)

    def sandbox(self):
        unit = self.local.commands.state(H.AUTHORITY[0])
        pid = unit.get('MainPID','0')
        H.require(unit.get('ActiveState') == 'active' and pid.isdigit() and int(pid)>0, 'installed publisher is not running')
        expected = next(i['sha256'] for i in self.coordinator['plan']['installs'] if i['target']=='/usr/local/bin/transparent-publish-controller')
        H.require(H.checksum('/proc/'+pid+'/exe') == expected, 'installed sandbox publisher executable differs')
        uid = next(line for line in Path('/proc/'+pid+'/status').read_text().splitlines() if line.startswith('Uid:'))
        H.require(all(value == '0' for value in uid.split()[1:]), 'sandbox probe requires the installed root publisher identity')
        script = Path('/srv/transparent-activity/ops/sources')/self.spec['source_sha']/'transparent/ops/scripts/transparent-activity-link-probe.py'
        self.local.commands.run(['/usr/bin/nsenter','--target',pid,'--mount','--','/usr/bin/python3','-B',str(script)], timeout=15)

    def owned(self, transaction, phase, journal):
        inherited_lock.descriptors(required=True, path=H.LOCK)
        H.require(Path(journal) == STATE/(transaction+'.json'), 'phase journal is outside owning schema state')
        record = H.load(journal)
        H.require(record['id'] == transaction and record['status'] in ('applying','rolling-back') and record['events'] and
                  record['events'][-1]['name'] == phase and record['events'][-1]['status'] == 'running', 'phase has no current durable schema intent')
        command = next(c for c in record['recipe'][record['events'][-1]['group']] if c['name'] == phase)
        H.require(self.spec_sha256 is not None and '--spec-sha256' in command['argv'] and
                  command['argv'][command['argv'].index('--spec-sha256')+1] == self.spec_sha256,
                  'product spec is not bound by the owning recipe')
        return record

    async def phase(self, transaction, phase, journal):
        record = self.owned(transaction, phase, journal)
        self.bound(transaction)
        self.local.identity(mutation=True)
        self.routing.identity(mutate=True)
        # Rollback must retain and verify operation code even if native
        # publication qualification has failed; never rerun forward gates here.
        source=self.spec['source_sha']
        receipt=H.load(Path('/srv/transparent-activity/ops/staging')/(source+'.json'))
        D.S.verify_receipt(receipt, Path('/srv/transparent-activity/ops/sources')/source, source, receipt['archive_sha256'])
        attempt = len(record['events'])
        group = record['events'][-1]['group']
        if phase == 'preserve-v10':
            self.local.capture()
            self.remote(self.router, 'capture', attempt)
            await self.all_workers('capture', attempt)
        elif phase == 'maintenance':
            await self.routing.withdraw('v10')
        elif phase == 'stage-v11':
            await self.all_workers('stage', attempt)
            self.local.stage()
            self.seed()
        elif phase == 'activate-prewarm':
            await self.all_workers('activate', attempt)
            await self.all_workers('verify-worker', attempt)
            self.local.activate()
        elif phase == 'align-origins':
            await self.routing.route_private('v11')
        elif phase == 'verify-canonical':
            self.sandbox()
            await self.routing.verify('v11')
        elif phase == 'resume-load':
            # Reopen includes a fresh real HTTPS recovery before it returns.
            await self.routing.reopen('v11')
            self.local.commands.unit('start', H.SCALER, H.LOAD)
        elif phase == 'withdraw-origins':
            self.local.commands.unit('stop', H.SCALER, H.LOAD, *H.AUTHORITY)
            self.local.quiet(H.WRITERS['coordinator'])
            await self.routing.withdraw('v11' if (H.ROOT/'v11/fleet.json').exists() else 'v10')
        elif phase == 'restore-v10':
            # Captured-but-untouched hosts still have a full baseline. A host
            # without a complete capture refuses rather than guessing state.
            await self.all_workers('restore', attempt)
            self.local.restore()
        elif phase == 'verify-rollback':
            await self.all_workers('verify-rollback-worker', attempt)
            await self.routing.route_private('v10')
            await self.routing.verify('v10')
        elif phase == 'reopen-v10':
            await self.routing.reopen('v10', restore_router=lambda:self.remote(self.router, 'restore-routing', attempt))
            _, state = self.local.saved()
            # The predecessor load tree and scaler were never overwritten.
            self.local.commands.unit('start', *[u for u in (H.LOAD,H.SCALER) if state['units'][u]['ActiveState']=='active'])
        elif phase == 'verify-service':
            kind = 'v10' if group == 'rollback' else 'v11'
            await self.routing.public(kind)
            self.local.quiet((H.QUALITY,))
        else:
            raise ValueError('unsupported product phase')
        return {'phase':phase, 'transaction':transaction, 'status':'passed'}


def recipe(spec_path, expected):
    path = checked({'path':str(spec_path), 'sha256':expected})
    spec = validate(H.load(path))
    source = Path('/srv/transparent-activity/ops/sources')/spec['source_sha']
    wrapper = source/'ops/scripts/wallet-pir-deploy.py'
    dependencies = [wrapper, source/'ops/lib/wallet_pir_ops/deploy/cli.py',
                    *(source/'transparent/ops/lib'/name for name in ('activity_schema_product.py','activity_schema_dispatch.py',
                      'activity_schema_templates.py','activity_schema_operation.py','activity_schema_host.py','activity_schema_baseline.py',
                      'activity_schema_routing.py','activity_recovery_proof.py'))]
    dependencies.append(source/'transparent/ops/scripts/transparent-activity-link-probe.py')
    dependencies.append(Path('/srv/transparent-activity/ops/staging')/(spec['source_sha']+'.json'))
    entries = [{'path':str(p),'sha256':H.checksum(p)} for p in [path,*dependencies]]
    base = ['/usr/bin/python3',str(wrapper),'--inventory',spec['inventory']['path'],'--state-dir',str(STATE)]
    def command(name, group):
        return {'name':name, 'argv':[*base,'schema-product-phase','--spec',str(path),'--spec-sha256',expected,
                '--phase',name,'--transaction','{transaction}','--journal','{journal}'],
                'read_only':name.startswith('verify-'), 'timeout':(160 if name in ('verify-rollback','verify-service') else 140) if group=='rollback' else 1800}
    result = {'version':1,'source_sha':spec['source_sha'],'publication_sha256':spec['publication_sha256'],
              'inputs':entries,'rollback_inputs':entries,
              'preflight':[{'name':'product-preflight','argv':[*base,'schema-product-preflight','--spec',str(path),'--spec-sha256',expected],
                            'timeout':1800,'read_only':True}],
              'steps':[command(n,'steps') for n in O.FORWARD], 'rollback':[command(n,'rollback') for n in O.ROLLBACK]}
    return O.validate(result)

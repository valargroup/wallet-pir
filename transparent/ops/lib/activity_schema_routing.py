"""Concrete withdrawal, canonical recovery and guarded reopening schema phases.

Invoke from the coordinator schema recipe under its inherited production lock.
The private loopback relay permits the reference HTTP client to exercise the
whole assigned fleet while both public metadata origins remain withdrawn. All
workers, including both recent replicas, and every advertised anchor must agree.
This does not substitute for full publication certificates or sustained load.
"""
import hashlib
import ipaddress
import importlib.util
import json
import os
from pathlib import Path
import re
import time
import urllib.request

from wallet_pir_ops import inherited_lock

HERE = Path(__file__).parents[1]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


H = module('routing_host', HERE/'lib/activity_schema_host.py')
L = module('routing_fleet', HERE/'scripts/transparent-live-fleet.py')
U = module('routing_upgrade', HERE/'scripts/upgrade-transparent-fleet.py')
P = module('routing_recovery_proof', HERE/'lib/activity_recovery_proof.py')
LOOPBACK = 'http://127.0.0.1:18193'
MAX_REPLY = 16 * 1024 * 1024


def require(ok, message):
    if not ok:
        raise ValueError(message)


def read_json(url, expected_digest=None):
    with urllib.request.urlopen(urllib.request.Request(url, headers={'Cache-Control':'no-cache'}), timeout=10) as response:
        data = response.read(MAX_REPLY+1)
    require(len(data) <= MAX_REPLY, 'service metadata exceeds bound')
    if expected_digest is not None:
        require(hashlib.sha256(data).hexdigest() == expected_digest, 'canonical manifest bytes differ from their map digest')
    return json.loads(data, object_pairs_hook=H.unique)


def warm_active(status, digest, *, continuous=False):
    """Future preparation does not invalidate an attested current warm map."""
    return (status.get('warm') is True and status.get('invalidated') is False and
            status.get('active',{}).get('map_sha256') == digest and
            (continuous or status.get('candidate') is None and status.get('preparing') is None))


def relay(router):
    require(isinstance(router, str) and re.fullmatch(r'10\.142\.\d{1,3}\.\d{1,3}:(?:8080|8093)', router), 'invalid private verification router')
    require(ipaddress.ip_address(router.rsplit(':', 1)[0]) in ipaddress.ip_network('10.142.0.0/16'),
            'verification router is outside the reviewed private network')
    return '''\nhttp://127.0.0.1:18193 {
    @metadata path /v1/shards /v1/shards/init /v1/shards/*/revisions/*/manifest /v1/filters/shards /v1/filters/shards/*
    handle @metadata {
        reverse_proxy 127.0.0.1:8094
    }
    handle {
        reverse_proxy '''+router+'''
    }
}\n'''


def validate(plan):
    require(isinstance(plan, dict) and set(plan) == {'version', 'source_sha', 'machine_id', 'transaction',
            'old_fleet', 'new_fleet', 'coordinator_baseline', 'original_coordinator_sha256', 'private_router', 'recovery'}, 'invalid routing plan')
    require(type(plan['version']) is int and plan['version'] == 1, 'unsupported routing plan')
    require(re.fullmatch('[0-9a-f]{40}', plan['source_sha']) and re.fullmatch('[0-9a-f]{32}', plan['machine_id']) and
            H.TXN.fullmatch(plan['transaction']), 'invalid routing identities')
    for name in ('old_fleet', 'new_fleet'):
        value = plan[name]
        require(isinstance(value, dict) and set(value) == {'path', 'sha256'} and Path(value['path']).is_absolute() and
                H.HEX.fullmatch(value['sha256']), 'invalid fleet input')
    require(plan['old_fleet']['path'] == '/opt/transparent-publisher/fleet.json' and
            plan['new_fleet']['path'] == '/opt/transparent-publisher/v11/fleet.json', 'fleet config namespace disagrees')
    require(plan['coordinator_baseline'] == '/opt/transparent-publisher/schema-rollback/'+plan['transaction'],
            'routing baseline is not transaction-bound')
    require(H.HEX.fullmatch(plan['original_coordinator_sha256']), 'invalid original routing identity')
    relay(plan['private_router'])
    require(isinstance(plan['recovery'], dict) and set(plan['recovery']) == {'v10', 'v11'}, 'both recovery readers must be defined')
    for kind, entry in plan['recovery'].items():
        require(isinstance(entry, dict) and set(entry) == {'binary', 'binary_sha256', 'sample', 'sample_sha256'}, 'invalid recovery input')
        require(Path(entry['binary']).is_absolute() and Path(entry['sample']).is_absolute() and
                H.HEX.fullmatch(entry['binary_sha256']) and H.HEX.fullmatch(entry['sample_sha256']), 'invalid recovery identities')
        # The current reader handles migrated stores and legacy unavailable
        # metadata. Never invoke an obsolete store reader for client rollback.
        require(entry['binary'] == '/srv/transparent-activity/build/evidence/release-12ce12918446eaa56e2d766ec2f43d82c531abb9/artifacts/transparent-loadtest',
                'recovery must use the retained compatible fat-LTO reader')
    return plan


def continuation(before, after):
    """Only a growing tail may change while the bounded recovery runs."""
    require(before['lineage'] == after['lineage'], 'reader lineage changed')
    old, new = before['history'], after['history']
    require(len(new) >= len(old), 'publication history was withdrawn')
    for index, entry in enumerate(old):
        current = new[index]
        if entry['sealed'] or entry['end_height'] == current['end_height']:
            require(entry == current, 'verified publication history changed')
        else:
            require(index == len(old)-1 and not entry['sealed'] and
                    all(entry[k] == current[k] for k in ('shard_id', 'start_height', 'parent_block_hash', 'geometry')) and
                    current['end_height'] > entry['end_height'] and current['revision'] > entry['revision'],
                    'publication tail did not advance monotonically')


def assignment_digest(assignment):
    """Match the native Assignment declaration order, independent of JSON formatting."""
    def ordered(value, keys):
        require(isinstance(value, dict) and set(value) == set(keys), 'assignment fields differ from native version 1')
        return {k:value[k] for k in keys}
    canonical = ordered(assignment, ('schema','set','generated_by','workers','unassigned'))
    canonical['set'] = ordered(assignment['set'], ('shard_schema','map_sha256','network','genesis_hash','shards',
                               'start_height','covered_through','recent_from_shard'))
    canonical['generated_by'] = ordered(assignment['generated_by'], ('tool','source_sha','generated_at'))
    canonical['workers'] = [ordered(w, ('id','role','replica_group','upstream','cache_bytes','shards',
                                      'estimated_resident_bytes')) for w in assignment['workers']]
    require(canonical['schema'] == 'transparent-assignment-v1', 'unsupported native assignment version')
    return hashlib.sha256(json.dumps(canonical, ensure_ascii=False, separators=(',',':')).encode()).hexdigest()


class Routing:
    def __init__(self, plan, *, commands=None, fleet_factory=None, fetch=read_json, proof=P.run):
        self.plan = validate(plan)
        self.root = Path('/srv/transparent-activity/ops/schema')/plan['transaction']/'routing'
        self.commands = commands or H.Commands()
        self.fleet_factory = fleet_factory or self.direct_fleet
        self.fetch, self.proof = fetch, proof
        self.worker_pins = {}

    def identity(self, mutate=False):
        require(os.geteuid() == 0 and Path('/etc/machine-id').read_text().strip() == self.plan['machine_id'], 'routing requires pinned root coordinator')
        self.source_identity(Path(__file__).resolve().parents[3])
        if mutate:
            inherited_lock.descriptors(required=True, path=H.LOCK)

    def source_identity(self, source):
        original = Path('/srv/transparent-activity/ops/sources')/self.plan['source_sha']
        if source == original:
            return
        inherited_lock.descriptors(required=True, path=H.LOCK)
        repair = getattr(self, 'recovery_program', None)
        record = H.load(self.root.parent.with_suffix('.json'))
        require(repair is not None and record.get('status') == 'rolling-back' and
                record.get('id') == self.plan['transaction'] and
                record.get('recipe', {}).get('source_sha') == self.plan['source_sha'] and
                record.get('recovery_programs', [])[-1:] == [repair] and
                record.get('events') and record['events'][-1].get('group') == 'rollback' and
                record['events'][-1].get('status') == 'running',
                'routing requires current rollback repair intent')
        expected = Path('/srv/transparent-activity/ops/sources')/repair['source_sha']
        require(source == expected and repair['wrapper'] == str(expected/'ops/scripts/wallet-pir-deploy.py'),
                'routing repair source differs')
        receipt = H.load(Path('/srv/transparent-activity/ops/staging')/(repair['source_sha']+'.json'))
        require(receipt['archive_sha256'] == repair['archive_sha256'], 'routing repair receipt differs')
        stage = module('routing_repair_source', HERE/'lib/activity_source_stage_host.py')
        stage.verify_receipt(receipt, source, repair['source_sha'], repair['archive_sha256'])

    @staticmethod
    def direct_fleet(path, read_only=False):
        config = H.load(path)
        # One-shot phases must not create a persistent master that outlives the
        # global lock owner. Long-running daemon control sessions remain separate.
        config = {**config, 'control_sessions':False, 'status_socket_forwarding':False}
        fleet = L.Fleet(config, read_only=read_only)
        fleet.ssh_args = fleet.direct_ssh_args + ['-oControlMaster=no', '-oControlPath=none']
        return fleet

    def fleet(self, kind, read_only=False):
        value = self.plan['new_fleet' if kind == 'v11' else 'old_fleet']
        require(H.checksum(value['path']) == value['sha256'], 'fleet configuration changed')
        return self.fleet_factory(Path(value['path']), read_only=read_only)

    def original(self):
        baseline = Path(self.plan['coordinator_baseline'])
        if not (baseline/'complete.json').exists():
            original = Path('/etc/caddy/Caddyfile').read_bytes()
            require(hashlib.sha256(original).hexdigest() == self.plan['original_coordinator_sha256'],
                    'missing baseline and routing no longer matches original; reconcile')
            return original
        record = H.B.verify(baseline)
        index = next(n for n, item in enumerate(record['plan']['files']) if item['path'] == '/etc/caddy/Caddyfile')
        original = (baseline/'files'/str(index)).read_bytes()
        require(hashlib.sha256(original).hexdigest() == self.plan['original_coordinator_sha256'], 'original routing identity changed')
        return original

    def private_router(self):
        value = self.plan['old_fleet']
        require(H.checksum(value['path']) == value['sha256'], 'fleet configuration changed')
        config = H.load(value['path'])
        endpoint = config.get('internal_listen')
        relay(endpoint)
        require(endpoint.rsplit(':',1)[0] == self.plan['private_router'].rsplit(':',1)[0] ==
                config.get('router_host'), 'captured private router identity changed')
        return endpoint

    def guarded(self, original_endpoint=False):
        endpoint = self.plan['private_router']
        if getattr(self,'predecessor_continuous',False) and not original_endpoint:
            endpoint = self.private_router()
        return (U.guard_coordinator(self.original().decode())+relay(endpoint)).encode()

    def check_guard(self, allow_original=False):
        allowed = [self.guarded()]
        if allow_original and getattr(self,'predecessor_continuous',False):
            allowed.append(self.guarded(original_endpoint=True))
        require(Path('/etc/caddy/Caddyfile').read_bytes() in allowed, 'coordinator routing differs from owned maintenance state')
        require(all(self.commands.metadata_status(url) == 503 for url in H.PUBLIC_METADATA), 'both public metadata origins must remain withdrawn')

    async def withdraw(self, kind):
        fleet = self.fleet(kind)
        original, guarded = self.original(), self.guarded()
        current = Path('/etc/caddy/Caddyfile').read_bytes()
        require(current in (original, guarded), 'unrelated coordinator routing update requires reconciliation')
        # Guard the authority first. A router reload cannot expose metadata
        # until the only metadata authority has passed independent verification.
        U.apply_coordinator(guarded)
        async with fleet.lock('routing'):
            L.atomic_json(fleet.root/'maintenance.json', {'enabled':True})
            await fleet.route([])
        self.check_guard()

    async def route_private(self, kind):
        self.check_guard()
        fleet = self.fleet(kind)
        async with fleet.lock('routing'):
            L.atomic_json(fleet.root/'maintenance.json', {'enabled':True})
            target = fleet.reconciliation_target()
            require(target is not None, 'no coherent fleet activation record')
            active, _ = target
            assignment = H.load(active['assignment'])
            workers = [w for w in fleet.roster if w['id'] in active['workers']]
            require({w['id'] for w in workers} == {w['id'] for w in fleet.roster}, 'private verification requires the entire reviewed fleet')
            await fleet.route(workers, assignment)
        self.check_guard()

    async def live(self, kind, fleet, retained=False):
        require(not retained or kind == 'v10' and getattr(self,'predecessor_continuous',False),
                'retained proof is only for explicit predecessor preparation')
        target = fleet.reconciliation_target() if retained else None
        require(not retained or target is not None, 'retained fleet activation record is absent')
        mapping = H.load(Path(target[1]['directory'])/'shards.json') if retained else self.fetch('http://127.0.0.1:8094/v1/shards')
        require(mapping.get('start_height') == 0 and mapping.get('shards'), 'authority is not a complete genesis publication')
        fleet.canonical.clear()
        require(await fleet.canonical_hash(0) == mapping.get('genesis_hash'), 'authority genesis differs from accepted node')
        target = fleet.reconciliation_target()
        require(target is not None, 'fleet activation record is absent')
        active, request = target
        raw_map = Path(request['directory'])/'shards.json'
        require(H.transparent_map.served_sha256(H.load(raw_map)) == active['map_sha256'] and H.load(raw_map) == mapping,
                'controller and fleet active publications disagree')
        assignment = H.load(active['assignment'])
        assignment_sha = assignment_digest(assignment)
        require(assignment['set']['map_sha256'] == active['map_sha256'] and
                assignment['set']['shard_schema'] == 'transparent-shard-'+kind and not assignment['unassigned'] and
                all(assignment['set'][k] == mapping[k] for k in ('genesis_hash','network','start_height')) and
                assignment['set']['shards'] == len(mapping['shards']) and
                assignment['set']['covered_through'] == mapping['shards'][-1]['end_height'], 'fleet assignment omits or names another publication')
        rows = {row['id']:row for row in assignment['workers']}
        workers = fleet.roster
        require(len(workers) >= 3 and sum(w['role'] == 'recent-replica' for w in workers) >= 2 and
                any(w['role'] == 'archive-owner' for w in workers) and
                {w['id'] for w in workers} == set(active['workers']), 'both recent replicas and all archive owners must be active')
        require(len(rows) == len(assignment['workers']) and set(rows) == {w['id'] for w in workers} and
                {s for row in rows.values() for s in row['shards']} == set(range(len(mapping['shards']))),
                'reviewed fleet assignment is incomplete')
        observations = []
        manifests = set()
        for worker in workers:
            status = await fleet.control(worker, {'operation':'status'})
            require(warm_active(status, active['map_sha256'], continuous=kind == 'v10' and
                    getattr(self,'predecessor_continuous',False)), 'worker does not attest the complete warm publication')
            ready = self.fetch('http://'+worker['upstream']+'/v1/ready')
            require(ready.get('ready') is True and ready.get('mode') == 'warm' and ready.get('map_sha256') == active['map_sha256'],
                    'HTTP readiness does not agree with native control')
            require(rows[worker['id']]['role'] == worker['role'] and rows[worker['id']]['upstream'] == worker['upstream'] and
                    ready.get('assignment_sha256') == assignment_sha and ready.get('worker_id') == worker['id'] and
                    ready.get('role') == worker['role'] and ready.get('assigned_shards') == len(rows[worker['id']]['shards']),
                    'running worker scope differs from the complete reviewed assignment')
            # Compare the running service to its frozen release pin. For rollback
            # the private baseline proof supplies that worker's predecessor hash.
            expected = self.worker_pins.get(kind, {}).get(worker['id'])
            require(expected is not None and ready.get('binary_sha256') == expected, 'worker running binary is not the reviewed release')
            revisions = status.get('revisions')
            require(isinstance(revisions, list) and revisions, 'worker omitted retained advertised anchors')
            for revision in revisions:
                height, anchor = revision.get('end_height'), revision.get('terminal_block_hash')
                require(isinstance(revision.get('digest'), str) and H.HEX.fullmatch(revision['digest']) and
                        type(height) is int and height >= 0 and isinstance(anchor, str) and H.HEX.fullmatch(anchor) and
                        await fleet.canonical_hash(height) == anchor, 'advertised revision is not independently canonical')
            observations.append({'id':worker['id'], 'binary_sha256':expected, 'map_sha256':active['map_sha256'], 'revisions':revisions})
        previous = None
        for index, entry in enumerate(mapping['shards']):
            require(type(entry.get('shard_id')) is int and entry['shard_id'] == index and type(entry.get('start_height')) is int and
                    type(entry.get('end_height')) is int and 0 <= entry['start_height'] <= entry['end_height'] and
                    type(entry.get('revision')) is int and entry['revision'] >= 0 and
                    type(entry.get('sealed')) is bool and (entry['sealed'] or index == len(mapping['shards'])-1) and
                    isinstance(entry.get('geometry'), str) and isinstance(entry.get('parent_block_hash'), str) and
                    H.HEX.fullmatch(entry['parent_block_hash']) and
                    (entry['start_height'] == 0 if previous is None else
                     entry['start_height'] == previous['end_height']+1 and entry['parent_block_hash'] == previous['terminal_block_hash']),
                    'authority publication ranges are malformed or discontinuous')
            digest = entry['manifest_digest']
            require(isinstance(digest, str) and H.HEX.fullmatch(digest), 'invalid manifest digest')
            if retained:
                path = Path(request['directory'])/digest/'manifest.json'
                require(path.is_file() and not path.is_symlink() and H.checksum(path) == digest,
                        'retained manifest identity changed')
                manifest = H.load(path)
            else:
                manifest = self.fetch('http://127.0.0.1:8094/v1/shards/'+str(entry['shard_id'])+'/revisions/'+digest+'/manifest', digest)
            require(manifest.get('schema') == 'transparent-shard-'+kind and not manifest.get('txid_display') and
                    all(manifest.get(k) == entry[k] for k in ('shard_id','start_height','end_height','geometry',
                                                             'parent_block_hash','terminal_block_hash','revision','sealed')) and
                    all(manifest.get(k) == mapping[k] for k in ('genesis_hash','network','profile')) and
                    manifest.get('parent_manifest_digest') == ('' if previous is None else previous['manifest_digest']),
                    'manifest schema/anchor/capabilities disagree')
            require(await fleet.canonical_hash(entry['end_height']) == entry['terminal_block_hash'], 'authority shard is not canonical')
            manifests.add(digest)
            previous = entry
        lineage = {k:mapping.get(k) for k in ('genesis_hash','network','profile','range_envelope_version','start_height','seal')}
        return {'kind':kind, 'lineage':lineage, 'history':mapping['shards'], 'map_sha256':active['map_sha256'], 'assignment_sha256':assignment_sha,
                'manifests':sorted(manifests), 'workers':observations}

    def pins(self, kind):
        # Public binary identities only; derive v10 pins from independently
        # retained private baselines, never from the service being verified.
        input_ = self.plan['recovery'][kind]
        require(H.checksum(input_['sample']) == input_['sample_sha256'], 'reviewed recovery sample changed')
        config = H.load(input_['sample'])
        pins = config.get('cutover_worker_pins')
        require(isinstance(pins, dict) and pins and all(isinstance(v, str) and H.HEX.fullmatch(v) for v in pins.values()),
                'reviewed recovery sample must bind every running worker binary')
        self.worker_pins[kind] = pins

    async def verify(self, kind):
        self.check_guard()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        previous = self.root/('verified-'+kind+'.json')
        if previous.exists():
            # A failed new verification must not leave an earlier proof usable.
            previous.rename(self.root/('superseded-'+kind+'-'+str(time.time_ns())+'.json'))
            H.B.sync_dir(self.root)
        self.pins(kind)
        fleet = self.fleet(kind, read_only=True)
        before = await self.live(kind, fleet)
        input_ = self.plan['recovery'][kind]
        sample = H.load(input_['sample'])
        require(await fleet.canonical_hash(sample['anchor_height']) == sample['anchor_hash'], 'reference sample anchor is no longer accepted')
        output = self.root/(kind+'-recovery-'+str(time.time_ns()))
        recovery = self.proof(input_['binary'], input_['binary_sha256'], input_['sample'], input_['sample_sha256'],
                              'transparent-shard-'+kind, LOOPBACK, LOOPBACK, output, self.plan['source_sha'])
        after = await self.live(kind, fleet)
        continuation(before, after)
        require(await fleet.canonical_hash(sample['anchor_height']) == sample['anchor_hash'],
                'reference anchor changed during recovery')
        require(recovery.get('status') == 'passed' and recovery.get('observations'), 'no nonempty independently reopened recovery')
        record = {'source_sha':self.plan['source_sha'], 'transaction':self.plan['transaction'], 'kind':kind,
                  'verified_unix':time.time(), 'live':after, 'recovery_result':str(output/'result.json'),
                  'recovery_sha256':H.checksum(output/'result.json'), 'sample_sha256':input_['sample_sha256']}
        H.B.atomic(self.root/('verified-'+kind+'.json'), H.encode(record))
        self.check_guard()
        return record

    async def reopen(self, kind, restore_router=None):
        self.check_guard()
        H.B.verify(Path(self.plan['coordinator_baseline']))  # No reopen from a partial snapshot.
        record = H.load(self.root/('verified-'+kind+'.json'))
        input_ = self.plan['recovery'][kind]
        require(record.get('source_sha') == self.plan['source_sha'] and record.get('transaction') == self.plan['transaction'] and
                record.get('kind') == kind and 0 <= time.time()-record['verified_unix'] <= 300 and
                record.get('sample_sha256') == input_['sample_sha256'] and
                H.checksum(record['recovery_result']) == record['recovery_sha256'], 'recovery proof is missing, stale or changed')
        proof = H.load(record['recovery_result'])
        require(proof.get('status') == 'passed' and proof.get('observations'), 'recovery proof is not passing')
        self.pins(kind)
        fleet = self.fleet(kind)
        async with fleet.lock('routing'):
            live = await self.live(kind, fleet)
            # Canonical tail advancement may continue. It cannot change the
            # reader lineage or invalidate the independently accepted sample.
            continuation(record['live'], live)
            sample = H.load(input_['sample'])
            require(await fleet.canonical_hash(sample['anchor_height']) == sample['anchor_hash'], 'recovery anchor changed before reopening')
            target = fleet.reconciliation_target()
            active, _ = target
            assignment = H.load(active['assignment'])
            try:
                L.atomic_json(fleet.root/'maintenance.json', {'enabled':False})
                await fleet.route(fleet.roster, assignment)
                if restore_router:
                    require(kind == 'v10', 'original router restoration is rollback-only')
                    restore_router()
                # Keep the authority guarded through the router handoff.
                U.apply_coordinator(self.original())
                await self.public(kind, fleet)
            except BaseException:
                U.apply_coordinator(self.guarded())
                L.atomic_json(fleet.root/'maintenance.json', {'enabled':True})
                await fleet.route([])
                raise

    async def public(self, kind, fleet=None):
        fleet = fleet or self.fleet(kind, read_only=True)
        first, second = (self.fetch(url) for url in H.PUBLIC_METADATA)
        require(first == second, 'canonical public origins disagree')
        self.pins(kind)
        live = await self.live(kind, fleet)
        require(first == self.fetch('http://127.0.0.1:8094/v1/shards'), 'public origins do not serve the verified authority')
        input_ = self.plan['recovery'][kind]
        sample = H.load(input_['sample'])
        require(await fleet.canonical_hash(sample['anchor_height']) == sample['anchor_hash'], 'public recovery anchor changed')
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        output = self.root/(kind+'-public-recovery-'+str(time.time_ns()))
        recovery = self.proof(input_['binary'], input_['binary_sha256'], input_['sample'], input_['sample_sha256'],
                              'transparent-shard-'+kind, 'https://transparent-pir.valargroup.dev',
                              'https://enhance-pir.valargroup.dev', output, self.plan['source_sha'])
        require(recovery.get('status') == 'passed' and recovery.get('observations'), 'public HTTP recovery did not reopen nonempty stores')
        after = await self.live(kind, fleet)
        continuation(live, after)
        checked_map = {**after['lineage'], 'shards':after['history']}
        require(await fleet.canonical_hash(sample['anchor_height']) == sample['anchor_hash'] and
                self.fetch(H.PUBLIC_METADATA[0]) == self.fetch(H.PUBLIC_METADATA[1]) ==
                self.fetch('http://127.0.0.1:8094/v1/shards') == checked_map,
                'canonical origins or accepted anchor changed during public recovery')
        record = {'source_sha':self.plan['source_sha'], 'transaction':self.plan['transaction'], 'kind':kind,
                  'verified_unix':time.time(), 'live':after, 'recovery_result':str(output/'result.json'),
                  'recovery_sha256':H.checksum(output/'result.json'), 'sample_sha256':input_['sample_sha256']}
        H.B.atomic(self.root/('verified-public-'+kind+'.json'), H.encode(record))
        return record

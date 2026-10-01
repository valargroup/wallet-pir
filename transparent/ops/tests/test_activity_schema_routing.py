"""No public reopen before complete worker/anchor and real recovery proof gates."""
from contextlib import asynccontextmanager
import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[3]/'ops/lib'))
SPEC = importlib.util.spec_from_file_location('schema_routing', Path(__file__).parents[1]/'lib/activity_schema_routing.py')
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)
BINARY = '/srv/transparent-activity/build/evidence/release-12ce12918446eaa56e2d766ec2f43d82c531abb9/artifacts/transparent-loadtest'


class RepairIdentity(unittest.TestCase):
    def test_repair_routing_requires_owning_journal_and_checksum_bound_source(self):
        p=object.__new__(M.Routing);p.plan={'source_sha':'a'*40,'transaction':'tx'}
        p.root=Path('/srv/transparent-activity/ops/schema/tx/routing')
        source=Path('/srv/transparent-activity/ops/sources')/('b'*40)
        repair={'source_sha':'b'*40,'archive_sha256':'c'*64,'wrapper':str(source/'ops/scripts/wallet-pir-deploy.py')}
        p.recovery_program=repair
        record={'status':'rolling-back','id':'tx','recipe':{'source_sha':'a'*40},'recovery_programs':[repair],
                'events':[{'group':'rollback','status':'running'}]}
        from types import SimpleNamespace
        seen=[]
        stage=SimpleNamespace(verify_receipt=lambda *args:seen.append(args))
        with patch.object(M.inherited_lock,'descriptors'),patch.object(M.H,'load',side_effect=lambda path:
                record if str(path).endswith('/tx.json') else {'archive_sha256':'c'*64}),patch.object(M,'module',return_value=stage):
            p.source_identity(source);self.assertEqual(len(seen),1)
            record['status']='applying'
            with self.assertRaisesRegex(ValueError,'rollback repair intent'):p.source_identity(source)
            record['status']='rolling-back';repair['wrapper']='wrong'
            with self.assertRaisesRegex(ValueError,'source differs'):p.source_identity(source)


class Fleet:
    def __init__(self, root, mapping):
        self.root=root
        self.roster=[{'id':'recent1','role':'recent-replica','upstream':'10.142.0.1:8093'},
                     {'id':'recent2','role':'recent-replica','upstream':'10.142.0.2:8093'},
                     {'id':'archive1','role':'archive-owner','upstream':'10.142.0.3:8093'}]
        self.canonical={};self.mapping=mapping;self.digest=M.H.transparent_map.served_sha256(mapping)
        self.actions=[];self.fail_route=False;self.bad_anchor=False;self.bad_worker=None
        self.status={'warm':True,'invalidated':False,'candidate':None,'preparing':None,
                     'active':{'map_sha256':self.digest},
                     'revisions':[{'digest':'d'*64,'end_height':10,'terminal_block_hash':'c'*64}]}
        self.assignment=root/'assignment.json';self.write_assignment()
    def write_assignment(self):
        self.assignment.write_text(json.dumps({'schema':'transparent-assignment-v1',
            'set':{'shard_schema':'transparent-shard-v11','map_sha256':self.digest,'network':self.mapping['network'],
                   'genesis_hash':self.mapping['genesis_hash'],'shards':len(self.mapping['shards']),
                   'start_height':0,'covered_through':self.mapping['shards'][-1]['end_height'],'recent_from_shard':0},
            'generated_by':{'tool':'shard-assign','source_sha':'e'*40,'generated_at':'2026-10-01'},
            'workers':[{**w,'replica_group':None,'cache_bytes':1024,'shards':list(range(len(self.mapping['shards']))),
                        'estimated_resident_bytes':1024} for w in self.roster],'unassigned':[]}))
    @asynccontextmanager
    async def lock(self,*_):yield
    def reconciliation_target(self):
        return ({'map_sha256':self.digest,'workers':[w['id'] for w in self.roster],
                 'assignment':str(self.assignment)}, {'directory':str(self.root/'publication')})
    async def canonical_hash(self,height):
        return 'b'*64 if height==0 else '9'*64 if self.bad_anchor else 'c'*64
    async def control(self,worker,_):
        result=copy.deepcopy(self.status)
        if worker['id']==self.bad_worker:result['warm']=False
        return result
    async def route(self,workers,*_):
        self.actions.append([w['id'] for w in workers])
        maintenance=json.loads((self.root/'maintenance.json').read_text())['enabled']
        if self.fail_route and workers and not maintenance:raise RuntimeError('injected router application failure')


class RoutingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        (self.root/'publication').mkdir()
        self.mapping={'start_height':0,'genesis_hash':'b'*64,'network':'mainnet','profile':'test',
                      'range_envelope_version':2,'seal':{},'shards':[{'shard_id':0,'manifest_digest':'d'*64,
                      'start_height':0,'end_height':10,'terminal_block_hash':'c'*64,
                      'parent_block_hash':'0'*64,'geometry':'test','revision':0,'sealed':False,
                      'filter_hash':'8'*64,'scripts':1,'page_rows':1,'txids':1,'directory_segments':1,'page_segments':1}]}
        (self.root/'publication/shards.json').write_text(json.dumps(self.mapping))
        self.fleet=Fleet(self.root,self.mapping)
        self.sample=self.root/'sample.json';self.sample.write_text(json.dumps({'anchor_height':10,'anchor_hash':'c'*64,
                         'cutover_worker_pins':{w['id']:'f'*64 for w in self.fleet.roster}}))
        self.original=b'handle @transparent_publication { reverse_proxy 127.0.0.1:8094 }\nhandle @legacy_transparent_filters { reverse_proxy 127.0.0.1:8090 }\n'
        txn='transparent-schema-routing-test'
        inp={'binary':BINARY,'binary_sha256':'a'*64,'sample':str(self.sample),'sample_sha256':M.H.checksum(self.sample)}
        self.plan={'version':1,'source_sha':'e'*40,'machine_id':'a'*32,'transaction':txn,
            'old_fleet':{'path':'/opt/transparent-publisher/fleet.json','sha256':'1'*64},
            'new_fleet':{'path':'/opt/transparent-publisher/v11/fleet.json','sha256':'2'*64},
            'coordinator_baseline':'/opt/transparent-publisher/schema-rollback/'+txn,
            'original_coordinator_sha256':hashlib.sha256(self.original).hexdigest(),
            'private_router':'10.142.0.11:8093','recovery':{'v10':copy.deepcopy(inp),'v11':inp}}
        self.current=(M.U.guard_coordinator(self.original.decode())+M.relay(self.plan['private_router'])).encode()
        self.schema='transparent-shard-v11';self.bad_binary=False;self.other_origin=False;self.bad_assignment=False
        commands=type('Commands',(),{'metadata_status':lambda _,url:503 if self.current!=self.original else 200})()
        def proof(*args):
            output=Path(args[7]);output.mkdir(parents=True)
            result={'status':'passed','observations':[{'events':1}]}
            (output/'result.json').write_text(json.dumps(result))
            return result
        self.routing=M.Routing(self.plan,commands=commands,fetch=self.fetch,proof=proof)
        self.routing.root=self.root/'evidence'
        self.routing.original=lambda:self.original
        self.routing.fleet=lambda *_,**__:self.fleet
        real_read=Path.read_bytes
        def read(path):return self.current if str(path)=='/etc/caddy/Caddyfile' else real_read(path)
        p=patch.object(Path,'read_bytes',read);p.start();self.addCleanup(p.stop)
        p=patch.object(M.U,'apply_coordinator',side_effect=lambda data:setattr(self,'current',data));p.start();self.addCleanup(p.stop)
        p=patch.object(M.H.B,'verify',return_value={});p.start();self.addCleanup(p.stop)

    def fetch(self,url,expected_digest=None):
        if url.endswith('/v1/ready'):
            worker=next(w for w in self.fleet.roster if url=='http://'+w['upstream']+'/v1/ready')
            return {'ready':True,'mode':'warm','map_sha256':self.fleet.digest,
                    'worker_id':worker['id'],'role':worker['role'],'assigned_shards':len(self.mapping['shards']),
                    'assignment_sha256':'0'*64 if self.bad_assignment else M.assignment_digest(M.H.load(self.fleet.assignment)),
                    'binary_sha256':'0'*64 if self.bad_binary else 'f'*64}
        if url.endswith('/manifest'):
            index=int(url.split('/v1/shards/')[1].split('/')[0])
            entry=self.mapping['shards'][index]
            return {**entry,'schema':self.schema,**{k:self.mapping[k] for k in ('genesis_hash','network','profile')},
                    'parent_manifest_digest':'' if index==0 else self.mapping['shards'][index-1]['manifest_digest']}
        result=copy.deepcopy(self.mapping)
        if self.other_origin and url==M.H.PUBLIC_METADATA[1]:result['profile']='foreign'
        return result

    async def test_withdraw_guard_authority_then_router_and_keep_private_relay(self):
        self.current=self.original
        await self.routing.withdraw('v11')
        self.assertIn(b'http://127.0.0.1:18193',self.current)
        self.assertEqual(self.fleet.actions,[[]])
        self.assertTrue(json.loads((self.root/'maintenance.json').read_text())['enabled'])

    async def test_private_routes_do_not_reopen_public_metadata(self):
        await self.routing.route_private('v11')
        self.assertEqual(self.fleet.actions,[['recent1','recent2','archive1']])
        self.assertNotEqual(self.current,self.original)
        self.routing.check_guard()

    async def test_complete_private_recovery_proof_then_reopen_both_origins(self):
        await self.routing.verify('v11')
        self.assertNotEqual(self.current,self.original)
        await self.routing.reopen('v11')
        self.assertEqual(self.current,self.original)
        self.assertEqual(self.fleet.actions,[['recent1','recent2','archive1']])
        self.assertFalse(json.loads((self.root/'maintenance.json').read_text())['enabled'])
        self.assertTrue((self.routing.root/'verified-public-v11.json').exists())

    async def test_one_recent_quorum_is_insufficient(self):
        self.fleet.roster.pop(1)
        with self.assertRaisesRegex(ValueError,'both recent'):
            await self.routing.verify('v11')
        self.assertFalse((self.routing.root/'verified-v11.json').exists())
        self.assertNotEqual(self.current,self.original)

    async def test_unwarm_replica_binary_drift_wrong_schema_and_retired_fork_refuse(self):
        for field,value in [('bad_worker','recent2'),('bad_anchor',True)]:
            setattr(self.fleet,field,value)
            with self.assertRaises(ValueError):await self.routing.verify('v11')
            setattr(self.fleet,field,None if field=='bad_worker' else False)
        self.bad_binary=True
        with self.assertRaisesRegex(ValueError,'binary'):await self.routing.verify('v11')
        self.bad_binary=False;self.schema='transparent-shard-v10'
        with self.assertRaisesRegex(ValueError,'schema'):await self.routing.verify('v11')
        self.assertFalse((self.routing.root/'verified-v11.json').exists())

    async def test_proof_cannot_be_missing_stale_or_modified(self):
        with self.assertRaises(FileNotFoundError):await self.routing.reopen('v11')
        await self.routing.verify('v11')
        path=self.routing.root/'verified-v11.json';record=json.loads(path.read_text())
        record['verified_unix']=time.time()-301;path.write_text(json.dumps(record))
        with self.assertRaisesRegex(ValueError,'stale'):await self.routing.reopen('v11')
        record['verified_unix']=time.time();path.write_text(json.dumps(record))
        Path(record['recovery_result']).write_text('{"status":"passed","observations":[]}')
        with self.assertRaisesRegex(ValueError,'changed'):await self.routing.reopen('v11')
        self.assertNotEqual(self.current,self.original)

    async def test_router_failure_and_public_origin_mismatch_rewithdraw_both(self):
        await self.routing.verify('v11')
        self.fleet.fail_route=True
        with self.assertRaises(RuntimeError):await self.routing.reopen('v11')
        self.routing.check_guard();self.assertEqual(self.fleet.actions[-1],[])
        self.fleet.fail_route=False;self.other_origin=True
        with self.assertRaisesRegex(ValueError,'origins disagree'):await self.routing.reopen('v11')
        self.routing.check_guard();self.assertEqual(self.fleet.actions[-1],[])

    async def test_public_http_recovery_failure_rewithdraws_both_origins(self):
        await self.routing.verify('v11')
        def public_failure(*args):
            self.assertEqual(args[5:7],('https://transparent-pir.valargroup.dev','https://enhance-pir.valargroup.dev'))
            raise ValueError('injected canonical HTTPS recovery failure')
        self.routing.proof=public_failure
        with self.assertRaisesRegex(ValueError,'canonical HTTPS'):
            await self.routing.reopen('v11')
        self.routing.check_guard();self.assertEqual(self.fleet.actions[-1],[])
        self.assertFalse((self.routing.root/'verified-public-v11.json').exists())

    async def test_each_public_origin_queries_and_reopens_its_own_store(self):
        calls=[]
        proof=self.routing.proof
        def track(*args):
            calls.append((args[5],args[6],args[7]))
            return proof(*args)
        self.routing.proof=track
        record=await self.routing.public('v11')
        self.assertEqual([c[:2] for c in calls],[
            ('https://transparent-pir.valargroup.dev','https://enhance-pir.valargroup.dev'),
            ('https://enhance-pir.valargroup.dev','https://transparent-pir.valargroup.dev')])
        self.assertNotEqual(calls[0][2],calls[1][2])
        self.assertEqual(len(record['recoveries']),2)
        for result in record['recoveries']:
            self.assertEqual(M.H.checksum(result['result']),result['sha256'])

    async def test_second_public_query_failure_rewithdraws_even_if_first_passed(self):
        await self.routing.verify('v11')
        proof=self.routing.proof
        def fail_second(*args):
            if args[5]=='https://enhance-pir.valargroup.dev':
                raise ValueError('second encrypted query origin failed')
            return proof(*args)
        self.routing.proof=fail_second
        with self.assertRaisesRegex(ValueError,'second encrypted'):
            await self.routing.reopen('v11')
        self.routing.check_guard()
        self.assertFalse((self.routing.root/'verified-public-v11.json').exists())

    async def test_failed_new_verification_cannot_reuse_previous_passing_proof(self):
        await self.routing.verify('v11')
        self.fleet.bad_worker='recent2'
        with self.assertRaisesRegex(ValueError,'warm'):
            await self.routing.verify('v11')
        self.fleet.bad_worker=None
        with self.assertRaises(FileNotFoundError):await self.routing.reopen('v11')
        self.assertEqual(len(list(self.routing.root.glob('superseded-v11-*.json'))),1)

    async def test_new_public_map_after_last_worker_check_rewithdraws(self):
        await self.routing.verify('v11')
        proof=self.routing.proof
        fetch=self.routing.fetch
        recovered=False
        moved=False
        def recover(*args):
            nonlocal recovered
            result=proof(*args);recovered=True;return result
        def changing(url,*args):
            nonlocal moved
            if recovered and not moved and url==M.H.PUBLIC_METADATA[0]:
                moved=True
                self.mapping['shards'][0].update(end_height=11,revision=1,manifest_digest='1'*64)
                self.publish()
            return fetch(url,*args)
        self.routing.proof=recover;self.routing.fetch=changing
        with self.assertRaisesRegex(ValueError,'canonical origins'):
            await self.routing.reopen('v11')
        self.routing.check_guard();self.assertTrue(moved)

    async def test_reader_lineage_change_before_reopen_refuses(self):
        await self.routing.verify('v11')
        self.mapping['profile']='changed'
        (self.root/'publication/shards.json').write_text(json.dumps(self.mapping))
        self.fleet.digest=M.H.transparent_map.served_sha256(self.mapping)
        self.fleet.status['active']['map_sha256']=self.fleet.digest
        self.fleet.write_assignment()
        with self.assertRaisesRegex(ValueError,'lineage'):await self.routing.reopen('v11')
        self.assertNotEqual(self.current,self.original)

    async def test_changed_sample_cannot_supply_new_worker_pins(self):
        self.sample.write_text('{}')
        with self.assertRaisesRegex(ValueError,'sample changed'):await self.routing.verify('v11')

    async def test_wrong_running_assignment_or_unassigned_history_refuses(self):
        self.bad_assignment=True
        with self.assertRaisesRegex(ValueError,'scope'):await self.routing.verify('v11')
        self.bad_assignment=False
        assignment=M.H.load(self.fleet.assignment);assignment['unassigned']=[0]
        self.fleet.assignment.write_text(json.dumps(assignment))
        with self.assertRaisesRegex(ValueError,'assignment'):await self.routing.verify('v11')
        self.routing.check_guard()

    def test_native_assignment_digest_ignores_file_formatting_and_key_order(self):
        assignment=M.H.load(self.fleet.assignment)
        expected=M.assignment_digest(assignment)
        def reverse(value):
            if isinstance(value,dict):return {k:reverse(value[k]) for k in reversed(value)}
            if isinstance(value,list):return [reverse(v) for v in value]
            return value
        self.assertEqual(M.assignment_digest(reverse(assignment)),expected)
        assignment['workers'][0]['shards']=[]
        self.assertNotEqual(M.assignment_digest(assignment),expected)

    def publish(self):
        (self.root/'publication/shards.json').write_text(json.dumps(self.mapping))
        self.fleet.digest=M.H.transparent_map.served_sha256(self.mapping)
        self.fleet.status['active']['map_sha256']=self.fleet.digest
        self.fleet.write_assignment()

    async def test_same_chain_sealed_history_rewrite_refuses_reopen(self):
        first=self.mapping['shards'][0]
        first.update(end_height=9,sealed=True)
        self.mapping['shards'].append({**first,'shard_id':1,'start_height':10,'end_height':10,
                                      'parent_block_hash':'c'*64,'sealed':False})
        self.publish()
        await self.routing.verify('v11')
        first['manifest_digest']='1'*64
        self.publish()
        with self.assertRaisesRegex(ValueError,'history changed'):
            await self.routing.reopen('v11')
        self.routing.check_guard()

    async def test_tail_advancement_can_reopen_but_same_height_rewrite_cannot(self):
        await self.routing.verify('v11')
        tail=self.mapping['shards'][0]
        tail['manifest_digest']='1'*64
        self.publish()
        with self.assertRaisesRegex(ValueError,'history changed'):
            await self.routing.reopen('v11')
        tail.update(end_height=11,revision=1)
        self.publish()
        await self.routing.reopen('v11')
        self.assertEqual(self.current,self.original)

    async def test_recovery_rewrite_does_not_create_passing_proof(self):
        original=self.routing.proof
        def rewrite(*args):
            result=original(*args)
            self.mapping['shards'][0]['manifest_digest']='1'*64
            self.publish()
            return result
        self.routing.proof=rewrite
        with self.assertRaisesRegex(ValueError,'history changed'):
            await self.routing.verify('v11')
        self.assertFalse((self.routing.root/'verified-v11.json').exists())
        self.routing.check_guard()

    async def test_malformed_range_or_digest_cannot_reopen(self):
        for field,value in [('start_height',1),('manifest_digest','g'*64),('sealed',None)]:
            old=self.mapping['shards'][0][field]
            self.mapping['shards'][0][field]=value
            if field=='sealed':
                (self.root/'publication/shards.json').write_text(json.dumps(self.mapping))
                self.fleet.digest='0'*64
            else:self.publish()
            with self.assertRaises(ValueError):await self.routing.verify('v11')
            self.mapping['shards'][0][field]=old;self.publish()

    def test_closed_plan_and_loopback_relay_cannot_select_other_hosts_or_old_reader(self):
        M.validate(copy.deepcopy(self.plan))
        for change in (lambda p:p.update(private_router='example.com:8093'),
                       lambda p:p['recovery']['v10'].update(binary='/usr/local/bin/old-reader'),
                       lambda p:p.update(new_fleet={'path':'/opt/transparent-publisher/fleet.json','sha256':'2'*64}),
                       lambda p:p.update(extra=True)):
            p=copy.deepcopy(self.plan);change(p)
            with self.assertRaises(ValueError):M.validate(p)
        self.assertIn('http://127.0.0.1:18193',M.relay(self.plan['private_router']))
        self.assertNotIn('0.0.0.0',M.relay(self.plan['private_router']))
        with self.assertRaises(ValueError):M.relay('10.142.999.11:8093')

    def test_raw_manifest_digest_and_duplicate_fields_are_checked(self):
        raw=b'{"schema":"transparent-shard-v11"}'
        with patch.object(M.urllib.request,'urlopen',return_value=io.BytesIO(raw)):
            self.assertEqual(M.read_json('http://localhost',hashlib.sha256(raw).hexdigest())['schema'],self.schema)
        with patch.object(M.urllib.request,'urlopen',return_value=io.BytesIO(raw)),self.assertRaisesRegex(ValueError,'digest'):
            M.read_json('http://localhost','d'*64)
        with patch.object(M.urllib.request,'urlopen',return_value=io.BytesIO(b'{"a":1,"a":2}')),self.assertRaisesRegex(ValueError,'duplicate'):
            M.read_json('http://localhost')


if __name__=='__main__':unittest.main()

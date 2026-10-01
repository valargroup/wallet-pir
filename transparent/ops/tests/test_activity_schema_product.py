"""Product ordering, remote ownership, interpolation and uncertain SSH failures."""
import asyncio
import base64
import hashlib
import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

HERE = Path(__file__).parent
sys.path.insert(0,str(HERE.parents[2]/'ops/lib'))
sys.path.insert(0,str(HERE))
from wallet_pir_ops import inherited_lock, schema_fence
from test_activity_schema_host import worker_plan


def module(name, path):
    spec = importlib.util.spec_from_file_location(name,path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


M = module('product_test',HERE.parent/'lib/activity_schema_product.py')
D, T = M.D, M.T


def request(action='capture'):
    p=worker_plan()
    return {'version':1,'request_id':'1-'+action,'action':action,'plan':p,'plan_sha256':D.digest(p)}


class Templates(unittest.TestCase):
    def test_preserved_fleet_must_share_assignment_map_and_canonical_anchors(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);publication=root/'protected/publication';publication.mkdir(parents=True)
            row={'shard_id':0,'manifest_digest':'b'*64,'start_height':0,'end_height':1,
                 'terminal_block_hash':'c'*64,'parent_block_hash':'0'*64,'geometry':'test','revision':0,
                 'sealed':False,'filter_hash':'d'*64,'scripts':1,'page_rows':1,'txids':1,
                 'directory_segments':1,'page_segments':1}
            mapping={'genesis_hash':'a'*64,'network':'Main','profile':'test','range_envelope_version':1,
                     'start_height':0,'seal':{},'shards':[row]}
            (publication/'shards.json').write_text(json.dumps(mapping))
            digest=M.H.transparent_map.served_sha256(mapping)
            assignment=root/'assignment.json';assignment.write_text('captured complete assignment')
            ids=['recent1','recent2','archive1'];item={'sentinel':str(publication/'shards.json')}
            baseline={'captured_publications':[item],'protected_publications':[{'item':item,'protected':str(publication.parent)}],
                      'plan_sha256':'e'*64}
            p=object.__new__(M.Product);p.root=root/'proof';p.local=SimpleNamespace(saved=lambda:(baseline,None))
            p.workers=[{'plan':{'worker':{'id':name}}} for name in ids]
            async def canonical(height):return 'a'*64 if height==0 else 'c'*64
            fleet=SimpleNamespace(reconciliation_target=lambda:({'map_sha256':digest,'assignment':str(assignment),'workers':ids},{}),
                                  canonical_hash=canonical)
            p.routing=SimpleNamespace(fleet=lambda *_,**__:fleet)
            replies=[{'result':{'worker_id':name,'active':{'map_sha256':digest},'assignment_sha256':M.H.checksum(assignment),
                       'revisions':[{'digest':'b'*64,'end_height':1,'terminal_block_hash':'c'*64}]}} for name in ids]
            replies[1]['result']['active']['map_sha256']='f'*64
            with self.assertRaisesRegex(ValueError,'incoherent'):asyncio.run(p.verify_preserved(replies))
            self.assertFalse((p.root/'preserved-v10.json').exists())
            replies[1]['result']['active']['map_sha256']=digest
            async def foreign(_):return 'f'*64
            fleet.canonical_hash=foreign
            with self.assertRaisesRegex(ValueError,'genesis'):asyncio.run(p.verify_preserved(replies))
            fleet.canonical_hash=canonical;asyncio.run(p.verify_preserved(replies))
            self.assertEqual(json.loads((p.root/'preserved-v10.json').read_text())['map_sha256'],digest)

    def test_generated_rollback_budget_allows_cold_cache_wait_within_total_limit(self):
        spec={'source_sha':'a'*40,'publication_sha256':'b'*64,'inventory':{'path':'/inventory.json'}}
        with patch.object(M,'checked',return_value=Path('/spec.json')),patch.object(M,'validate',return_value=spec), \
                patch.object(M.H,'load',return_value=spec),patch.object(M.H,'checksum',return_value='c'*64):
            recipe=M.recipe('/spec.json','c'*64)
        budgets={command['name']:command['timeout'] for command in recipe['rollback']}
        self.assertEqual(budgets,{'withdraw-origins':60,'restore-v10':140,'verify-rollback':300,
                                  'reopen-v10':100,'verify-service':140})
        self.assertGreater(budgets['verify-rollback'],250)
        self.assertEqual(sum(budgets.values()),740)

    def test_repair_source_requires_current_rollback_intent_and_retained_receipt(self):
        p=object.__new__(M.Product);p.spec={'source_sha':'a'*40}
        root=Path('/srv/transparent-activity/ops/sources')/('b'*40)
        repair={'source_sha':'b'*40,'archive_sha256':'c'*64,'wrapper':str(root/'ops/scripts/wallet-pir-deploy.py')}
        record={'events':[{'group':'rollback'}],'recovery_programs':[repair]}
        p.owned=lambda *args:record
        with self.assertRaisesRegex(ValueError,'pinned immutable'):
            p.source_identity(root)
        with patch.object(M.H,'load',return_value={'archive_sha256':'c'*64}),patch.object(M.D.S,'verify_receipt') as verify:
            p.source_identity(root,'tx','withdraw-origins','journal');verify.assert_called_once()
            record['events'][-1]['group']='forward'
            with self.assertRaisesRegex(ValueError,'rollback repair'):
                p.source_identity(root,'tx','withdraw-origins','journal')
            record['events'][-1]['group']='rollback'
            repair['wrapper']='wrong'
            with self.assertRaisesRegex(ValueError,'owning journal'):
                p.source_identity(root,'tx','withdraw-origins','journal')
        repair['wrapper']=str(root/'ops/scripts/wallet-pir-deploy.py')
        with patch.object(M.H,'load',return_value={'archive_sha256':'d'*64}):
            with self.assertRaisesRegex(ValueError,'receipt differs'):
                p.source_identity(root,'tx','withdraw-origins','journal')

    def test_full_publication_fixture_exceeds_plan_bound_but_keeps_its_own_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'fixture.json'
            fixture={'schema':'transparent-shard-v11','tables':[{'query': 'a'*4096} for _ in range(180)]}
            path.write_text(json.dumps(fixture))
            self.assertGreater(path.stat().st_size,256*1024)
            self.assertEqual(M.read_fixture(path),fixture)
            with self.assertRaisesRegex(ValueError,'bound'):
                M.H.load(path)
            path.write_text(json.dumps({'tables':['a'*(2*1024*1024)]}))
            with self.assertRaisesRegex(ValueError,'bound'):
                M.read_fixture(path)

    def test_load_executable_path_matches_the_frozen_release_receipt(self):
        receipt=json.loads((HERE.parents[1]/'evidence/activity-metadata-2026-09-30/release-12ce1291.json').read_text())
        self.assertEqual(str(M.LOAD_BINARY),receipt['binaries']['examples/rate-query']['retained_path'])

    def template(self):
        plan=worker_plan()
        plan['transaction']=T.TOKEN;plan['baseline_root']=T.ROOT+T.TOKEN
        return plan

    def test_only_transaction_and_rollback_root_bind(self):
        plan=self.template();original=copy.deepcopy(plan)
        result=T.bind(plan,'host','transparent-schema-real')
        self.assertEqual(result['transaction'],'transparent-schema-real')
        self.assertEqual(result['baseline_root'],T.ROOT+'transparent-schema-real')
        self.assertEqual(plan,original)
        self.assertEqual(result['installs'],plan['installs'])

    def test_no_interpolation_in_sources_targets_commands_or_paths(self):
        for key in ('source','target'):
            plan=self.template();plan['installs'][0][key]+='/{transaction}'
            with self.assertRaisesRegex(ValueError,'outside defined'):
                T.bind(plan,'host','transparent-schema-real')
        for txn in ('../escape','transparent-schema-x/../escape','arbitrary'):
            with self.assertRaises(ValueError):T.bind(self.template(),'host',txn)

    def test_partial_template_or_unsupported_fields_refuse(self):
        for key in ('transaction','baseline_root'):
            plan=self.template();plan[key]='fixed'
            with self.assertRaises(ValueError):T.validate(plan,'host')
        plan=self.template();plan['shell']='true'
        with self.assertRaises(ValueError):T.validate(plan,'host')


class Lock:
    def __init__(self,path):self.path=path;self.fd=None
    def __enter__(self):self.fd=os.open(self.path,os.O_CREAT|os.O_RDWR,0o600);return self
    def __exit__(self,*_):os.close(self.fd);self.fd=None
    def verify(self):assert self.fd is not None
    def descriptors(self):self.verify();return (self.fd,)


class Actors(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.lock=Lock(self.root/'lock')
        self.events=[];self.error=None
        owner=self
        class Host:
            def __init__(self,plan):self.plan=plan
            def identity(self,mutation=False):
                if mutation:inherited_lock.descriptors(required=True,path=owner.lock.path)
            def capture(self):
                record=json.loads(owner.actor.path.read_text())
                owner.events.append(record['status'])
                if owner.error:raise owner.error
                return 'captured'
            def preflight(self):owner.events.append('preflight');return None
        self.host_factory=Host
        self.actor=D.Actor(request(),root=self.root/'owners',host_factory=Host,lock_factory=lambda:self.lock)

    def test_intent_before_effect_and_private_result_with_inherited_lock(self):
        result=self.actor.run()
        self.assertEqual(self.events,['running'])
        self.assertEqual(result['status'],'passed')
        self.assertEqual(result['result'],'captured')
        self.assertEqual(self.actor.path.stat().st_mode&0o777,0o600)
        self.assertIsNone(self.lock.fd)
        with self.assertRaisesRegex(ValueError,'already exists'):self.actor.run()

    def test_timeout_blocks_new_action_until_explicit_locked_reconciliation(self):
        self.error=subprocess.TimeoutExpired('fixture',1)
        with self.assertRaises(subprocess.TimeoutExpired):self.actor.run()
        self.assertEqual(self.actor.status()['status'],'interrupted')
        second=request();second['request_id']='2-capture'
        other=D.Actor(second,root=self.actor.root,host_factory=self.host_factory,lock_factory=lambda:self.lock)
        with self.assertRaisesRegex(ValueError,'unfinished'):other.run()
        self.actor.reconcile();self.error=None
        self.assertEqual(other.run()['status'],'passed')

    def test_failed_checksum_refuses_before_owner_or_effect(self):
        bad=request();bad['plan']['worker']['map_sha256']='1'*64
        with self.assertRaises(ValueError):D.Actor(bad,root=self.actor.root)
        self.assertFalse(self.actor.root.exists());self.assertEqual(self.events,[])

    def test_candidate_cold_start_can_exceed_one_minute_but_rollback_probe_cannot(self):
        from unittest.mock import Mock
        for action in ('verify-worker','verify-rollback-worker'):
            actor=D.Actor.__new__(D.Actor);actor.request={'action':action}
            actor.host=Mock();actor.host.commands.control.side_effect=[
                {'warm':False,'invalidated':False,'candidate':None,'preparing':None}, {'warm':True,'invalidated':False,'candidate':None,'preparing':None}]
            actor.host.verify_worker.return_value='verified'
            with patch.object(D.time,'monotonic',side_effect=[0,70]),patch.object(D.time,'sleep'):
                if action=='verify-worker':self.assertEqual(actor.execute(),'verified')
                else:
                    with self.assertRaisesRegex(ValueError,'warm deadline'):actor.execute()

    def test_candidate_transport_outlives_its_three_hundred_second_warm_bound(self):
        from unittest.mock import Mock
        product=M.Product.__new__(M.Product);product.dispatch=Mock()
        entry={'host':'worker','plan':request()['plan']}
        product.remote(entry,'verify-worker',1)
        self.assertEqual(product.dispatch.call.call_args.kwargs['timeout'],330)
        product.remote(entry,'verify-rollback-worker',2)
        self.assertEqual(product.dispatch.call.call_args.kwargs['timeout'],90)

    def test_read_only_preflight_creates_neither_lock_nor_owner(self):
        actor=D.Actor(request('preflight'),root=self.actor.root,host_factory=self.host_factory,lock_factory=lambda:self.lock)
        self.assertIsNone(actor.run());self.assertFalse(self.lock.path.exists());self.assertFalse(actor.root.exists())

    def test_request_id_binding_and_duplicate_json_fields(self):
        import io
        req=request()
        self.assertEqual(D.read_request(io.BytesIO(D.H.encode(req)),D.digest(req)),req)
        with self.assertRaises(ValueError):D.read_request(io.BytesIO(D.H.encode(req)),'f'*64)
        with self.assertRaises(ValueError):D.read_request(io.BytesIO(b'{"version":1,"version":1}'),'f'*64)

    def test_ssh_disconnect_is_uncertain_and_structured_failure_is_definite(self):
        inventory=SimpleNamespace(hosts={'worker':{'machine_id':'a'*32}},ssh={'mode':'config'})
        replies=[SimpleNamespace(returncode=255,stdout=b''),SimpleNamespace(returncode=1,stdout=D.H.encode({'request_sha256':D.digest(request()),'status':'failed'}))]
        dispatcher=D.Dispatch(inventory,run=lambda *_,**__:replies.pop(0))
        with patch.object(D.inherited_lock,'descriptors',return_value=()),patch.object(D.inherited_lock,'options',return_value={}):
            with self.assertRaises(subprocess.TimeoutExpired):dispatcher.call('worker',request())
            with self.assertRaisesRegex(ValueError,'phase failed'):dispatcher.call('worker',request())


class Fences(unittest.TestCase):
    def test_source_repair_binds_failed_recipe_and_keeps_other_owner_fences(self):
        root=Path('/fixture');identifier='transparent-schema-owner';recipe={'pinned':'original'}
        digest=M.O.digest(recipe);recovery={'transaction':identifier,'recipe_sha256':digest}
        record={'id':identifier,'journal_version':1,'status':'rollback-failed','recipe':recipe,'recipe_sha256':digest,'events':[{'status':'failed'}]}
        files={str(root/schema_fence.SCHEMA_POINTER):json.dumps({'id':identifier}),str(root/(identifier+'.json')):json.dumps(record)}
        schema_fence.schema_mutation_fence(files.get,root,recovery=recovery)
        for changed in ({'transaction':'transparent-schema-other','recipe_sha256':digest},{'transaction':identifier,'recipe_sha256':'f'*64}):
            with self.assertRaises(ValueError):schema_fence.schema_mutation_fence(files.get,root,recovery=changed)
        for status in ('applying','rolling-back','committed'):
            record['status']=status;files[str(root/(identifier+'.json'))]=json.dumps(record)
            with self.assertRaises(ValueError):schema_fence.schema_mutation_fence(files.get,root,recovery=recovery)
        record['status']='rollback-failed';record['events']=[{'status':'running'}];files[str(root/(identifier+'.json'))]=json.dumps(record)
        with self.assertRaises(ValueError):schema_fence.schema_mutation_fence(files.get,root,recovery=recovery)
        with self.assertRaises(ValueError):schema_fence.schema_mutation_fence(lambda _:None,root,recovery=recovery)

    def test_repair_actor_cannot_run_forward_programs(self):
        for action in ('capture','stage','activate','restore'):
            req=request(action);req['recovery_source_sha']='b'*40
            with self.assertRaisesRegex(ValueError,'forward'):D.validate(req)
        req=request('repair-restore')
        with self.assertRaises(ValueError):D.validate(req)
        req['recovery_source_sha']='b'*40;D.validate(req)
    def test_unfinished_schema_blocks_other_services_and_corrupt_pointer_refuses(self):
        root=Path('/fixture');identifier='transparent-schema-owner'
        files={str(root/schema_fence.SCHEMA_POINTER):json.dumps({'id':identifier}),
               str(root/(identifier+'.json')):json.dumps({'id':identifier,'journal_version':1,'status':'interrupted'})}
        with self.assertRaisesRegex(ValueError,'unfinished'):schema_fence.schema_mutation_fence(files.get,root)
        for status in ('committed','rolled-back'):
            files[str(root/(identifier+'.json'))]=json.dumps({'id':identifier,'journal_version':1,'status':status})
            schema_fence.schema_mutation_fence(files.get,root)
        files[str(root/schema_fence.SCHEMA_POINTER)]=json.dumps({'id':'../../unsafe'})
        with self.assertRaises(ValueError):schema_fence.schema_mutation_fence(files.get,root)

    def test_missing_or_oversized_owner_is_not_absence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/schema_fence.SCHEMA_POINTER).write_text(json.dumps({'id':'transparent-schema-missing'}))
            with self.assertRaises(ValueError):schema_fence.local_schema_fence(root)

    def test_source_staging_preserves_claimed_global_fd_and_environment(self):
        source=module('source_dispatch_fixture',HERE.parent/'lib/activity_source_stage.py')
        inventory=SimpleNamespace(hosts={'coordinator':{'machine_id':'a'*32}},ssh={'mode':'config'},lock={'type':'remote','host':'coordinator'})
        with tempfile.TemporaryDirectory() as tmp:
            lock=Lock(Path(tmp)/'lock')
            with lock,patch.dict(os.environ,{inherited_lock.VARIABLE:str(lock.fd)}):
                calls=[]
                def run(argv,**kwargs):
                    calls.append(kwargs)
                    return SimpleNamespace(returncode=0,stdout=b'{"ok":true,"result":{"status":"staged"}}')
                with patch.object(source.subprocess,'run',side_effect=run):
                    source.SourceStage(inventory,out=lambda _:None).call({'mode':'status'})
                self.assertEqual(calls[0]['pass_fds'],(lock.fd,))
                self.assertEqual(calls[0]['env']['PYTHONDONTWRITEBYTECODE'],'1')


class ProductSeed(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name).resolve()
        self.product=object.__new__(M.Product)
        self.product.mapping={'shards':[{'end_height':3500738,'terminal_block_hash':'c'*64}]}
        p=patch.object(M.H.transparent_map,'served_sha256',return_value='2'*64);p.start();self.addCleanup(p.stop)
        self.product.rows=[{'id':name,'upstream':'10.142.0.'+str(n)+':8093'} for n,name in enumerate(('r1','r2','a1'),1)]
        self.product.workers=[{'plan':{'worker':{'id':w['id'],'directory':'/srv/transparent-pir/v11/publications/'+'a'*64,
                               'assignment':'/srv/transparent-pir/v11/publications/'+'a'*64+'/assignment.json','map_sha256':'2'*64,'map_file_sha256':'a'*64}}} for w in self.product.rows]
        self.product.local=SimpleNamespace(withdrawn=lambda:None,quiet=lambda _:None)
        self.product.inputs=lambda:None;self.product.units=lambda:None
        def entry(name,data):
            path=self.root/name;path.write_bytes(data)
            return {'path':str(path),'sha256':M.H.checksum(path)}
        self.product.spec={'source_sha':'b'*40,'publication_sha256':'a'*64,'recent_from':3000000,
                           'assignment':entry('assignment.json',b'{"schema":"fixture"}'),
                           'load':{'binary':entry('rate-query',b'fixture executable'),'fixture':entry('fixture.json',b'{}'),
                                   'pins':entry('pins.json',b'{}'),'policy':entry('policy.json',b'{"mode":"observe"}')}}
        publisher=self.root/'publisher';(publisher/'v11/state').mkdir(parents=True)
        (publisher/'v11/roster.json').write_text(json.dumps(self.product.rows))
        publication=self.root/'publications/initial';publication.mkdir(parents=True)
        self.publisher,self.publication=publisher,publication
        self.product.root=self.root/'proof'
        self.product.coordinator={'plan':{'baseline':{'files':[{'path':str(publication.parent/'active.json'),'required':False}]}}}
        self.record={'plan_sha256':'f'*64,'files':{str(publication.parent/'active.json'):None}}
        self.product.local.saved=lambda:(self.record,None)
        patches=[patch.object(M.H,'ROOT',publisher),patch.object(M,'PUBLICATION',publication),patch.object(M,'LOAD_ROOT',self.root/'load')]
        for p in patches:p.start();self.addCleanup(p.stop)

    def test_restart_records_are_complete_and_authority_starts_in_maintenance(self):
        previous=os.umask(0o077)
        try:self.product.seed()
        finally:os.umask(previous)
        state=self.publisher/'v11/state'
        active=json.loads((state/'active.json').read_text())
        desired=json.loads((state/'desired.json').read_text())
        req=json.loads((state/('2'*64+'.request.json')).read_text())
        self.assertEqual(desired,req)
        self.assertEqual(active['map_sha256'],desired['map_sha256'])
        self.assertEqual(active['assignment'],desired['prepared']['assignment'])
        self.assertEqual(set(desired['prepared']['workers']),{'r1','r2','a1'})
        self.assertEqual(json.loads((state/'maintenance.json').read_text()),{'enabled':True})
        self.assertEqual(json.loads((self.publication.parent/'active.json').read_text())['height'],3500738)
        self.assertEqual((M.LOAD_ROOT/'rate-query').stat().st_mode&0o777,0o755)
        self.assertEqual(json.loads((self.publisher/'v11/scaler/policy.json').read_text())['mode'],'observe')
        with self.assertRaisesRegex(ValueError,'already exists'):self.product.seed()

    def test_changed_assignment_refuses_before_first_fleet_state_write(self):
        Path(self.product.spec['assignment']['path']).write_bytes(b'changed')
        with self.assertRaises(ValueError):self.product.seed()
        self.assertEqual(list((self.publisher/'v11/state').iterdir()),[])

    def test_exact_captured_stale_activation_is_adopted_with_receipt(self):
        path=self.publication.parent/'active.json'
        path.write_text(json.dumps(self.product.expected_activation()))
        self.record['files'][str(path)]=M.H.B.entries(path)
        self.product.candidate_activation()
        self.product.seed()
        receipt=json.loads((self.product.root/'candidate-publication-adoption.json').read_text())
        self.assertEqual(receipt['captured'],self.record['files'][str(path)])
        self.assertEqual(receipt['target'],self.product.expected_activation())

    def test_uncaptured_drift_foreign_activation_and_symlink_refuse(self):
        path=self.publication.parent/'active.json'
        path.write_text(json.dumps(self.product.expected_activation()))
        with self.assertRaisesRegex(ValueError,'changed after capture'):self.product.seed()
        self.record['files'][str(path)]=M.H.B.entries(path)
        path.write_text('{}')
        with self.assertRaisesRegex(ValueError,'reviewed target'):self.product.candidate_activation()
        path.unlink();path.symlink_to(self.root/'assignment.json')
        with self.assertRaisesRegex(ValueError,'aliases'):self.product.candidate_activation()
        self.assertEqual(list((self.publisher/'v11/state').iterdir()),[])


class RecoveryPinPreflight(unittest.TestCase):
    def test_missing_or_changed_candidate_pins_refuse_before_host_preflight(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);paths={kind:root/(kind+'.json') for kind in ('v10','v11')}
            for p in paths.values():p.write_text('{}')
            product=object.__new__(M.Product);product.bound=lambda _:None;product.inputs=lambda:None
            effects=[];product.local=SimpleNamespace(identity=lambda:None,preflight=lambda:effects.append('host'))
            product.coordinator={'host':'coordinator','plan':{'machine_id':'a'*32}}
            product.workers=[{'host':'w','plan':{'worker':{'id':'w','binary_sha256':'b'*64}}}]
            product.hosts=[product.coordinator]
            product.inventory=SimpleNamespace(lock={'type':'pinned_host','machine_id':'a'*32},hosts={'coordinator':{'machine_id':'a'*32}})
            product.spec={'routing':{'recovery':{kind:{'sample':str(path),'sample_sha256':'a'*64} for kind,path in paths.items()}}}
            with patch.object(M,'checked',side_effect=lambda e:Path(e['path'])):
                with self.assertRaisesRegex(ValueError,'pins'):product.preflight()
                for p in paths.values():p.write_text(json.dumps({'cutover_worker_pins':{'w':'c'*64}}))
                with self.assertRaisesRegex(ValueError,'candidate recovery pins'):product.preflight()
            self.assertFalse(effects)


class InstalledSetups(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup);self.root=Path(tmp.name)
        public=b'measured setup bytes';self.public=public;self.sha=hashlib.sha256(public).hexdigest()
        manifest={'directory_segments':[{'sha256':'d'*64}], 'page_segments':[{'sha256':'e'*64}]}
        raw=json.dumps(manifest).encode();self.digest=hashlib.sha256(raw).hexdigest()
        (self.root/self.digest).mkdir();(self.root/self.digest/'manifest.json').write_bytes(raw)
        self.bindings=[{'shard_id':0,'manifest_digest':self.digest,'geometry':'archive-wide','table':t,
                       'segment':0,'table_sha256':h,'public_sha256':self.sha} for t,h in [('directory','d'*64),('pages','e'*64)]]
        self.report=self.root/'report.json';self.save()
        self.product=object.__new__(M.Product);self.product.root=self.root/'proof'
        self.product.spec={'gates':{'native-certificates':{'path':str(self.report),'sha256':M.H.checksum(self.report)}}}
        self.product.inputs=lambda:None;self.product.mapping={'shards':[{'shard_id':0,'manifest_digest':self.digest,'geometry':'archive-wide'}]}
        self.product.rows=[{'id':'w1','upstream':'127.0.0.1:8080','shards':[0]}, {'id':'w2','upstream':'127.0.0.1:8081','shards':[0]}]
        self.replies=[];self.change=lambda reply:None
        def fetch(url):
            self.replies.append(url);table=url.split('/')[-2]
            reply={'shard_id':0,'manifest_digest':self.digest,'geometry':'archive-wide','table':table,
                   'segment':0,'segments':1,'public_params_sha256':self.sha,'public_params':base64.b64encode(public).decode()}
            self.change(reply);return reply
        self.product.routing=SimpleNamespace(fetch=fetch)
        patches=[patch.object(M,'PUBLICATION',self.root),patch.object(M,'checked',side_effect=lambda e:Path(e['path']))]
        for p in patches:p.start();self.addCleanup(p.stop)

    def save(self):self.report.write_text(json.dumps({'setup_bindings':self.bindings}))

    def test_every_segment_on_every_assigned_replica_matches_actual_public_bytes(self):
        self.product.verify_setups()
        self.assertEqual(len(self.replies),4)
        proof=json.loads((self.product.root/'installed-setups.json').read_text())
        self.assertEqual(proof['status'],'passed');self.assertEqual(len(proof['setups']),4)

    def test_missing_and_duplicate_certificate_segments_refuse_before_http(self):
        original=copy.deepcopy(self.bindings)
        for bindings in (original[:-1],original+[original[0]]):
            self.bindings=bindings;self.save()
            with self.assertRaises(ValueError):self.product.verify_setups()
            self.assertFalse(self.replies)

    def test_advertised_hash_cannot_hide_changed_bytes_or_response_scope(self):
        for key,value in [('public_params',base64.b64encode(b'changed').decode()),('manifest_digest','f'*64),('segments',2)]:
            self.change=lambda reply,key=key,value=value:reply.update({key:value})
            with self.assertRaises(ValueError):self.product.verify_setups()
            self.assertFalse((self.product.root/'installed-setups.json').exists())


class ProductPhases(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.events=[];self.failed=None
        self.product=object.__new__(M.Product)
        self.product.spec={'source_sha':'b'*40};self.product.workers=[{'host':'w1'},{'host':'w2'},{'host':'a1'}]
        self.product.router={'host':'router'};self.product.local=SimpleNamespace()
        for action in ('capture','stage','activate','restore'):
            setattr(self.product.local,action,lambda action=action:self.events.append('local-'+action))
        self.product.local.identity=lambda **_:None
        self.product.local.quiet=lambda _:None
        self.product.local.commands=SimpleNamespace(unit=lambda action,*units:self.events.append((action,units)))
        self.product.local.saved=lambda:(None,{'units':{u:{'ActiveState':'active'} for u in (M.H.LOAD,M.H.SCALER,*M.H.AUTHORITY)}})
        async def warm():self.events.append('restored-workers-warm')
        self.product.wait_restored_workers=warm
        async def preserved(_):self.events.append('coherent-v10-baseline')
        self.product.verify_preserved=preserved
        owner=self
        class Routing:
            def identity(self,**_):pass
            def fetch(self,_):return {'start_height':0,'shards':[{}]}
            async def run(self,action,kind,**kwargs):
                owner.events.append(action+'-'+kind)
                if owner.failed==action:raise ValueError('injected proof failure')
                if 'restore_router' in kwargs:kwargs['restore_router']()
            async def withdraw(self,kind):await self.run('withdraw',kind)
            async def route_private(self,kind):await self.run('private',kind)
            async def verify(self,kind):await self.run('verify',kind)
            async def reopen(self,kind,**kwargs):await self.run('reopen',kind,**kwargs)
            async def public(self,kind):await self.run('public',kind)
        self.product.routing=Routing()
        self.product.bound=lambda _:None
        self.product.owned=lambda _,phase,__:{'events':[{'group':self.group}]}
        self.product.remote=lambda entry,action,attempt:self.events.append(entry['host']+'-'+action)
        self.product.seed=lambda:self.events.append('seed-complete-state')
        self.product.verify_setups=lambda:self.events.append('measured-installed-setups')
        self.product.sandbox=lambda:self.events.append('installed-sandbox')
        self.group='steps'
        source=patch.object(M.D.S,'verify_receipt',return_value=None);source.start();self.addCleanup(source.stop)
        load=patch.object(M.H,'load',return_value={'archive_sha256':'c'*64});load.start();self.addCleanup(load.stop)

    async def phase(self,name):await self.product.phase('transparent-schema-fixture',name,'fixture')

    async def test_all_forward_phases_keep_capture_before_withdrawal_and_proof_before_load(self):
        for phase in M.O.FORWARD:await self.phase(phase)
        self.assertLess(self.events.index('local-capture'),self.events.index('router-capture'))
        self.assertLess(self.events.index('a1-capture'),self.events.index('withdraw-v10'))
        self.assertLess(self.events.index('coherent-v10-baseline'),self.events.index('withdraw-v10'))
        self.assertLess(self.events.index('a1-stage'),self.events.index('seed-complete-state'))
        self.assertLess(self.events.index('a1-verify-worker'),self.events.index('measured-installed-setups'))
        self.assertLess(self.events.index('measured-installed-setups'),self.events.index('local-activate'))
        self.assertLess(self.events.index('installed-sandbox'),self.events.index('verify-v11'))
        self.assertLess(self.events.index('reopen-v11'),self.events.index(('start',(M.H.SCALER,M.H.LOAD))))

    async def test_failed_setup_binding_never_starts_authority(self):
        def fail():raise ValueError('setup certificate mismatch')
        self.product.verify_setups=fail
        with self.assertRaises(ValueError):await self.phase('activate-prewarm')
        self.assertNotIn('local-activate',self.events)

    async def test_failed_public_recovery_never_resumes_load(self):
        self.failed='reopen'
        with self.assertRaises(ValueError):await self.phase('resume-load')
        self.assertFalse(any(isinstance(e,tuple) and e[0]=='start' for e in self.events))

    async def test_all_workers_finish_and_unknown_reply_prevents_local_stage(self):
        def remote(entry,action,attempt):
            self.events.append(entry['host'])
            if entry['host']=='w1':raise ValueError('definite failure')
            if entry['host']=='w2':raise subprocess.TimeoutExpired('lost reply',1)
        self.product.remote=remote
        with self.assertRaises(subprocess.TimeoutExpired):await self.phase('stage-v11')
        self.assertEqual(set(self.events),{'w1','w2','a1'})
        self.assertNotIn('local-stage',self.events)

    async def test_fresh_reopen_process_retains_protected_predecessor_policy(self):
        self.group='rollback'
        original_saved=self.product.local.saved
        self.product.local.saved=lambda:({'protected_publications':[{'item':'bound'}]},original_saved()[1])
        for phase in ('reopen-v10','verify-service'):
            self.product.routing.predecessor_continuous=False
            await self.phase(phase)
            self.assertTrue(self.product.routing.predecessor_continuous)
        self.group='steps';self.product.routing.predecessor_continuous=False
        await self.phase('verify-service')
        self.assertFalse(self.product.routing.predecessor_continuous)
        self.group='rollback';self.product.local.saved=original_saved
        await self.phase('reopen-v10')
        self.assertFalse(self.product.routing.predecessor_continuous)

    async def test_rollback_restores_original_router_and_uses_compatible_old_proof(self):
        self.group='rollback'
        for phase in ('restore-v10','verify-rollback','reopen-v10','verify-service'):await self.phase(phase)
        self.assertLess(self.events.index('a1-restore'),self.events.index('verify-v10'))
        self.assertIn('router-restore-routing',self.events)
        self.assertIn('public-v10',self.events)
        self.assertLess(self.events.index('local-restore'),self.events.index(('stop',M.H.WRITERS['coordinator'])))
        self.assertLess(self.events.index(('stop',M.H.WRITERS['coordinator'])),self.events.index('a1-restore'))
        self.assertLess(self.events.index('restored-workers-warm'),self.events.index('a1-verify-rollback-worker'))
        self.assertLess(self.events.index('a1-verify-rollback-worker'),self.events.index(('start',M.H.AUTHORITY)))
        self.assertLess(self.events.index(('start',M.H.AUTHORITY)),self.events.index('verify-v10'))


if __name__=='__main__':unittest.main()

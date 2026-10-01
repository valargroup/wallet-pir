"""Concrete adoption preserves prior bytes and refuses changed/partial inputs."""
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
import asyncio
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT/'ops/lib'))
from wallet_pir_ops import schema_fence
spec = importlib.util.spec_from_file_location('adopt', ROOT/'transparent/ops/lib/activity_schema_reconcile.py')
M = importlib.util.module_from_spec(spec); spec.loader.exec_module(M)


class AdoptionTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.publisher = self.root/'publisher'; (self.publisher/'state').mkdir(parents=True)
        self.source = self.root/'displaced'; self.source.mkdir()
        self.baseline = self.root/'baseline'; self.baseline.mkdir()
        (self.baseline/'complete.json').write_text('independently captured')
        self.publication = self.root/'publication'; self.publication.mkdir()
        (self.publication/'shards.json').write_text('retained immutable map')
        self.native = self.root/'native'; self.native.mkdir()
        self.target = 'a'*64
        files = {}
        for name in ('active.json', self.target+'.request.json', self.target+'.assignment.json',
                     self.target+'.prepared.json', self.target+'.roster.json','native-active'):
            path = (self.native/'active.json.schema-displaced-fixture') if name == 'native-active' else self.source/name
            path.write_bytes(('retained '+name).encode())
            s = path.stat()
            files[name] = {'path':str(path),'sha256':M.H.checksum(path),'mode':s.st_mode & 0o777,'uid':s.st_uid,'gid':s.st_gid}
        self.plan = {'transaction':'fixture', 'recipe_sha256':'b'*64,'map_sha256':self.target,
                     'baseline_sha256':M.H.checksum(self.baseline/'complete.json'), 'files':files,
                     'publication':{'path':str(self.publication),'sha256':M.H.checksum(self.publication/'shards.json')}}
        self.record = {'id':'fixture','recipe_sha256':'b'*64,'v10_reconciliation':{'plan':self.plan}}
        self.product = SimpleNamespace(root=self.root/'product', local=SimpleNamespace(root=self.baseline,
            quiet=lambda units:None, withdrawn=lambda:None, saved=lambda:None))
        self.product.root.mkdir()
        self.addCleanup(patch.stopall)
        patch.object(M.H,'ROOT',self.publisher).start()

    def test_exact_adoption_preserves_prior_and_uses_request_for_desired(self):
        current = self.publisher/'state/active.json'; current.write_bytes(b'old missing map')
        (self.publisher/'state/desired.json').write_bytes(b'pending future map')
        M.install(self.product,self.record)
        self.assertEqual(current.read_bytes(), (self.source/'active.json').read_bytes())
        self.assertEqual((self.publisher/'state/desired.json').read_bytes(),
                         (self.source/(self.target+'.request.json')).read_bytes())
        before = self.product.root/'reconciliation-before'
        self.assertEqual((before/'active.json').read_bytes(),b'old missing map')
        self.assertEqual((before/'desired.json').read_bytes(),b'pending future map')
        self.assertEqual(json.loads((before/'complete.json').read_text())['map_sha256'],self.target)
        with self.assertRaisesRegex(ValueError,'partial adoption'):
            M.install(self.product,self.record)

    def test_changed_source_refuses_before_destination_effects(self):
        (self.source/'active.json').write_text('changed')
        with self.assertRaisesRegex(ValueError,'retained record changed'):
            M.install(self.product,self.record)
        self.assertFalse((self.product.root/'reconciliation-before').exists())
        self.assertEqual(list((self.publisher/'state').iterdir()),[])

    def test_symlink_source_refuses_even_with_same_bytes(self):
        path=self.source/'active.json'; raw=path.read_bytes(); path.unlink()
        other=self.source/'other'; other.write_bytes(raw); path.symlink_to(other)
        with self.assertRaisesRegex(ValueError,'retained record changed'):
            M.install(self.product,self.record)

    def test_lost_publication_refuses_before_any_write(self):
        (self.publication/'shards.json').unlink()
        with self.assertRaises(FileNotFoundError): M.install(self.product,self.record)
        self.assertFalse((self.product.root/'reconciliation-before').exists())

    def test_forward_effects_disallow_adoption(self):
        with self.assertRaisesRegex(ValueError,'unstaged cutover'):
            M.inspect(None,{'events':[{'group':'steps','name':'stage-v11','status':'failed'}]},'a'*64)

    def test_unproved_reconciled_label_does_not_release_fence(self):
        record={'journal_version':1,'id':'transparent-schema-fixture','status':'reconciled-v10'}
        def read(path):
            if path.endswith(schema_fence.SCHEMA_POINTER): return json.dumps({'id':record['id']})
            if path.endswith(record['id']+'.json'): return json.dumps(record)
            return None
        with self.assertRaisesRegex(ValueError,'unfinished schema'):
            schema_fence.schema_mutation_fence(read)

    def test_resume_refuses_restore_failure_or_unknown_descendant(self):
        for name, code in [('restore-v10',1),('verify-rollback',75)]:
            record={'v10_reconciliation':{'status':'running'}, 'events':[{'name':name,'status':'failed','exit_code':code}]}
            with self.assertRaisesRegex(ValueError,'ordinary post-adoption'):
                M.inspect_resume(None,record)

    def test_bare_assignment_upstreams_use_real_http_urls(self):
        spec=importlib.util.spec_from_file_location('adopt_product',ROOT/'transparent/ops/lib/activity_schema_product.py')
        product=importlib.util.module_from_spec(spec);spec.loader.exec_module(product)
        fake=SimpleNamespace(spec={'assignment':{}},workers=[{'plan':{'worker':{'id':'w'}}}])
        with patch.object(product,'checked',return_value=self.root/'unused'), \
             patch.object(product.H,'load',return_value={'workers':[{'id':'w','upstream':'10.142.0.10:8093'}]}), \
             patch.object(product.R,'read_json',return_value={'ready':True,'mode':'warm'}) as read:
            asyncio.run(product.Product.wait_restored_workers(fake))
        read.assert_called_once_with('http://10.142.0.10:8093/v1/ready')

    def test_continuous_future_preparation_keeps_current_proof_strict(self):
        spec=importlib.util.spec_from_file_location('adopt_routing',ROOT/'transparent/ops/lib/activity_schema_routing.py')
        routing=importlib.util.module_from_spec(spec);spec.loader.exec_module(routing)
        status={'warm':True,'invalidated':False,'active':{'map_sha256':'a'*64},'preparing':{'map_sha256':'b'*64}}
        self.assertFalse(routing.warm_active(status,'a'*64))
        self.assertTrue(routing.warm_active(status,'a'*64,continuous=True))
        for field, value in [('warm',False),('invalidated',True),('active',{'map_sha256':'b'*64})]:
            self.assertFalse(routing.warm_active(dict(status,**{field:value}),'a'*64,continuous=True))

    def test_private_endpoint_is_derived_from_pinned_producer_config(self):
        spec=importlib.util.spec_from_file_location('endpoint_routing',ROOT/'transparent/ops/lib/activity_schema_routing.py')
        routing=importlib.util.module_from_spec(spec);spec.loader.exec_module(routing)
        config=self.root/'fleet.json'
        config.write_text(json.dumps({'internal_listen':'10.142.0.11:8080','router_host':'10.142.0.11'}))
        fake=SimpleNamespace(plan={'old_fleet':{'path':str(config),'sha256':routing.H.checksum(config)},
                                   'private_router':'10.142.0.11:8093'})
        self.assertEqual(routing.Routing.private_router(fake),'10.142.0.11:8080')
        for endpoint in ('10.142.0.12:8080','127.0.0.1:8080','10.142.0.11:22','example.com:8080'):
            config.write_text(json.dumps({'internal_listen':endpoint,'router_host':'10.142.0.11'}))
            fake.plan['old_fleet']['sha256']=routing.H.checksum(config)
            with self.assertRaises(ValueError):routing.Routing.private_router(fake)
        config.write_text(json.dumps({'internal_listen':'10.142.0.11:8080','router_host':'10.142.0.11'}))
        with self.assertRaisesRegex(ValueError,'configuration changed'):routing.Routing.private_router(fake)

    def test_prepare_refuses_unowned_proof_failure(self):
        for name,code in [('restore-v10',1),('verify-rollback',75)]:
            record={'v10_reconciliation':{'status':'running'},'events':[{'name':name,'status':'failed','exit_code':code}]}
            with self.assertRaisesRegex(ValueError,'ordinary post-adoption'):M.inspect_resume(None,record,prepare=True)

    def test_preparation_starts_only_captured_authority_and_retains_guard(self):
        caddy=self.root/'Caddyfile'; caddy.write_bytes(b'old guard')
        actions=[]
        import urllib.error
        probes=[urllib.error.URLError(ConnectionRefusedError()),200]
        def readiness(_):
            result=probes.pop(0)
            if isinstance(result,Exception):raise result
            return result
        commands=SimpleNamespace(run=lambda argv,**kw:actions.append((argv,kw)),
            unit=lambda action,*units:actions.append((action,units)),metadata_status=readiness)
        self.product.local.commands=commands
        self.product.local.saved=lambda:({'files':{'/etc/caddy/Caddyfile':{'.':{'kind':'file','mode':0o644,'uid':caddy.stat().st_uid,'gid':caddy.stat().st_gid}}}}, {'units':{u:{'ActiveState':'active'} for u in (M.H.FILTER,*M.H.AUTHORITY)}})
        self.product.routing=SimpleNamespace(check_guard=lambda **_:None,guarded=lambda:b'correct guard')
        self.record['v10_reconciliation']['preparations']=[{'status':'running','plan_sha256':'c'*64}]
        real=Path
        def mapped(value):
            if str(value).startswith('/etc/caddy/'):
                return self.root/real(value).name
            return real(value)
        with patch.object(M,'Path',side_effect=mapped):M.prepare_resume(self.product,self.record,lambda _:None)
        self.assertEqual(caddy.read_bytes(),b'correct guard')
        self.assertEqual(caddy.stat().st_mode & 0o777,0o644)
        self.assertEqual(actions[-1],('start',(M.H.FILTER,*M.H.AUTHORITY)))
        self.assertEqual(self.record['v10_reconciliation']['preparations'][-1]['status'],'passed')
        self.assertEqual((self.product.root/'resume-prepare-1/Caddyfile').read_bytes(),b'old guard')
        attempts=json.loads((self.product.root/'resume-prepare-1/readiness-attempts.json').read_text())
        self.assertEqual(attempts,[{'status':None,'error_type':'ConnectionRefusedError'},{'status':200}])
        self.assertNotIn(M.H.LOAD,actions[-1][1]);self.assertNotIn(M.H.SCALER,actions[-1][1])
        self.assertNotIn(M.H.QUALITY,actions[-1][1])

    def test_reconciliation_refuses_running_preparation_owner(self):
        record={'v10_reconciliation':{'preparations':[{'status':'running','pid':1}]}}
        with patch.object(M.os,'kill',return_value=None):
            with self.assertRaisesRegex(ValueError,'owner is still present'):M.reconcile_preparation(None,record,lambda _:None)


if __name__ == '__main__': unittest.main()

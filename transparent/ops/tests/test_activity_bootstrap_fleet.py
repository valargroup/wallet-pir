"""Fictional bootstrap fleet contracts; kernel lease fixtures run on Linux."""
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
HERE=Path(__file__).resolve()
sys.path[:0]=[str(HERE.parents[3]/'ops/lib'),str(HERE.parents[1]/'lib')]
import activity_bootstrap_fleet as G
import activity_source_stage as B
from wallet_pir_ops import durable


class Fleet(unittest.TestCase):
    def response(self,binding,host,holder=None):
        return dict(bootstrap=G.KIND,kind=G.S.KIND,binding=binding,machine_id=G.PINS[host],euid=0,
            observed_unix=time.time(),inventory_sha256=G.INVENTORY_SHA,bounds=G.S.BOUNDS,classes=G.CLASSES,
            classes_sha256=hashlib.sha256(G.S.canonical(G.CLASSES)).hexdigest(),baseline=list(G.BASELINE),
            namespaces=[[n,str(p)] for n,p in (('schema',G.ROOT/'schema'),('host-actions',G.ROOT/'host-actions'),
                        ('input-staging',G.INPUTS),('source-staging',G.ROOT/'staging'))],
            inventory={'sha256':'1'*64},selected=[],selected_count=0,selected_sha256='2'*64,
            blocked=[],blocked_count=0,associated=[],associated_count=0,unattributed=[],unattributed_count=0,
            processes=[],lock={'holders':[holder] if holder else []},ancillary={
                'kind':G.A.KIND,'pins_sha256':G.A.PINS_SHA256,'machine_id':G.PINS[host],
                'boot_id':'00000000-0000-0000-0000-000000000001' if host in ('coordinator','router') else None,
                'observed_unix':time.time(),'units':{u:{'status':'absent'} for u in G.A.UNITS} if host=='coordinator' else {},
                'route':{'sha256':'1'*64,'bytes':10,'prototype_port_excluded':True} if host in ('coordinator','router') else None})

    def test_partial_duplicate_or_substituted_inventory_refuses(self):
        inventory=SimpleNamespace(hosts={h:{'machine_id':p} for h,p in G.PINS.items()},ssh={'mode':'pinned'})
        G.pinned(inventory)
        for hosts in ({'coordinator':inventory.hosts['coordinator']},
                      {**inventory.hosts,'worker-3':inventory.hosts['worker-2']},
                      {**inventory.hosts,'worker-3':{'machine_id':'f'*32}}):
            with self.assertRaisesRegex(ValueError,'complete five-host'):G.pinned(SimpleNamespace(hosts=hosts,ssh=inventory.ssh))

    def test_missing_stale_foreign_or_blocked_survey_is_not_a_pass(self):
        binding={'request_sha256':'a'*64,'nonce':'b'*48,'host':'coordinator','skip':None}
        value=self.response(binding,'coordinator')
        G.verify(value,binding,'coordinator')
        changes=({'binding':{**binding,'nonce':'c'*48}}, {'machine_id':'f'*32},{'blocked_count':1},
                 {'unattributed_count':1},{'associated_count':1},{'processes':[{}]}, {'namespaces':[]},
                 {'inventory':{}},{'observed_unix':time.time()-301},{'selected_count':1},
                 {'lock':{'holders':[{'pid':100}]}})
        for change in changes:
            with self.assertRaises(ValueError):G.verify({**value,**change},binding,'coordinator')

    def test_complete_fleet_is_nonce_bound_and_failed_raw_reply_is_retained(self):
        request={'source_sha':'a'*40};raws=[];commands=[]
        inventory=SimpleNamespace(hosts={h:{} for h in G.PINS})
        transport=SimpleNamespace(transport=lambda h:['ssh',h])
        fixture=self
        class Child:
            def __init__(self,argv,**kw):
                commands.append(argv);self.returncode=0
            def communicate(self,raw,timeout):
                envelope=json.loads(raw);host=envelope['host']
                binding=envelope['binding']
                if fixture.foreign and host=='worker-2':binding={**binding,'nonce':'f'*48}
                return json.dumps(fixture.response(binding,host)).encode(),None
        self.foreign=False
        def local(binding,host,**kw):return self.response(binding,host,kw['holder'])
        with patch.object(G,'runtime',return_value=(inventory,transport)),patch.object(G,'local',side_effect=local), \
             patch.object(G.subprocess,'Popen',Child),patch.object(G.S,'process',return_value={'pid':123,'start_ticks':456}), \
             patch.object(G.S,'boot_id',return_value='00000000-0000-0000-0000-000000000001'):
            proof=G.fleet(request,SimpleNamespace(verify=lambda:None),None,'fixed readonly code',retain=lambda h,r:raws.append((h,r)))
            self.assertEqual(proof['hosts'],sorted(G.PINS))
            self.assertEqual([h for h,_ in raws],sorted(G.PINS))
            self.assertTrue(all('-oControlPath=none' in command for command in commands))
            self.assertTrue(all(' -B ' in command[-1] for command in commands))
            self.foreign=True;raws.clear()
            with self.assertRaisesRegex(ValueError,'stale or foreign'):
                G.fleet(request,SimpleNamespace(verify=lambda:None),None,'fixed readonly code',retain=lambda h,r:raws.append((h,r)))
            self.assertEqual(raws[-1][0],'worker-2')
            self.assertEqual(json.loads(raws[-1][1])['binding']['nonce'],'f'*48)

    def test_every_candidate_tool_is_an_operational_class_and_prototype_is_never_baseline(self):
        import activity_candidate as C
        self.assertTrue({n.rsplit('/',1)[-1] for n in C.ARTIFACTS}<=set(G.NAMES))
        self.assertFalse(any('prototype' in name for name in G.BASELINE))

    def test_embedded_components_keep_distinct_globals_and_fixed_survey_has_no_mutation_entry(self):
        namespace={};exec(B.EMBEDDED_FLEET,namespace)
        self.assertIsNot(namespace['_SURVEY'].__dict__,namespace['_BOOTSTRAP_FLEET'].__dict__)
        self.assertIs(namespace['_BOOTSTRAP_FLEET'].S,namespace['_SURVEY'])
        self.assertEqual(namespace['_SURVEY'].KIND,G.S.KIND)
        self.assertEqual(namespace['_BOOTSTRAP_FLEET'].KIND,G.KIND)
        compile(B.SURVEY_HELPER,'fictional-readonly-survey','exec')
        compile(B.HELPER,'fictional-source-receiver','exec')


class Transport(unittest.TestCase):
    def client(self):
        client=object.__new__(B.SourceStage)
        client.host='worker-1';client.machine=G.PINS['worker-1'];client.target='worker-1'
        client.attempt=1;client.recovery=None;client.out=lambda value:None
        client.inventory=SimpleNamespace(hosts={'worker-1':{}})
        client.executor=SimpleNamespace(transport=lambda host:['ssh',host])
        return client

    def test_launcher_executes_exact_reviewed_bytes_with_bounded_full_argument(self):
        compiled=[]
        def compiler(raw,name,mode):
            compiled.append(raw);return compile('',name,mode)
        exec(B.LAUNCHER,{'compile':compiler})
        self.assertEqual(compiled,[B.HELPER.encode()])
        command=B.command_for(['sudo','-n','--'],{'mode':'stage','source_sha':'a'*40})
        self.assertLess(len(command.encode()),131071)
        self.assertLess(len(command.encode()),B.MAX_COMMAND)
        with self.assertRaisesRegex(ValueError,'bounded header'):
            B.command_for([],{'surveys':'x'*(B.MAX_REQUEST+1)})
        # A substituted digest must refuse before compilation/execution.
        with self.assertRaises(AssertionError):
            exec(B.LAUNCHER.replace(B.HELPER_SHA256,'0'*64),{'compile':compiler})
        self.assertEqual(len(compiled),1)

    def test_transport_loss_is_unknown_without_exposing_command_or_stderr(self):
        import subprocess
        client=self.client()
        cases=[subprocess.TimeoutExpired('fictional-sensitive-command',1800),OSError('fictional-sensitive-path')]
        for error in cases:
            with patch.object(B.subprocess,'run',side_effect=error):
                with self.assertRaises(B.Unknown) as caught:client.call({'mode':'stage'})
            self.assertNotIn('fictional-sensitive',str(caught.exception))
        for result in (SimpleNamespace(returncode=255,stdout=b'',stderr=b'fictional-sensitive'),
                       SimpleNamespace(returncode=0,stdout=b'[]',stderr=b''),
                       SimpleNamespace(returncode=0,stdout=b'bad json',stderr=b'')):
            with patch.object(B.subprocess,'run',return_value=result):
                with self.assertRaises(B.Unknown):client.call({'mode':'stage'})

    def test_target_preflight_retains_raw_surveys_but_forwards_only_bounded_proof(self):
        client=self.client();seen=[];output=[];client.out=output.append
        proof={'schema':G.KIND,'surveys':{'worker-1':'x'*200000},'nonce':'b'*48}
        def call(request):
            seen.append(request);B.command_for([],request);return {'ok':True}
        with tempfile.TemporaryDirectory() as directory:
            archive=Path(directory)/'fixture.tar';archive.write_bytes(b'fictional archive')
            checksum=hashlib.sha256(archive.read_bytes()).hexdigest()
            with patch.object(B.FLEET,'fleet',return_value=proof),patch.object(client,'call',side_effect=call):
                result=client.run('preflight','a'*40,checksum,archive)
        self.assertNotIn('surveys',seen[0]['coordinator_fleet'])
        self.assertEqual(result['coordinator_fleet'],proof)
        self.assertEqual(json.loads(output[-1])['coordinator_fleet'],proof)


@unittest.skipUnless(Path('/proc/sys/kernel/random/boot_id').is_file(),'Linux kernel identity fixture')
class Lease(unittest.TestCase):
    def setUp(self):
        scratch=tempfile.TemporaryDirectory();self.addCleanup(scratch.cleanup)
        self.root=Path(scratch.name);self.inputs=self.root/'input';self.calls=[]
        for name,value in (('INPUTS',self.inputs),('PINS',{'coordinator':Path('/etc/machine-id').read_text().strip()}),
                           ('FENCE',lambda **kw:self.calls.append(('fence',kw)))):
            p=patch.object(G,name,value);p.start();self.addCleanup(p.stop)
        self.request={'mode':'stage','machine_id':G.PINS['coordinator'],'source_sha':'a'*40,'sha256':'b'*64,'guard_attempt':1}
        self.lock=SimpleNamespace(verify=lambda:self.calls.append('lock'))
        self.proof={'schema':G.KIND,'nonce':'c'*48}
        p=patch.object(G,'fleet',return_value=self.proof);p.start();self.addCleanup(p.stop)

    def leased(self,operation):return G.leased(self.request,self.lock,None,'fixed-readonly-code',operation,durable.atomic_json)

    def test_fence_and_real_kernel_owner_are_durable_before_effect(self):
        def effect(proof):
            identifier=G.lease_id(self.request)
            record=json.loads((self.inputs/(identifier+'.json')).read_text())
            self.assertEqual(record['status'],'running');self.assertEqual(record['pid'],os.getpid())
            self.assertEqual(record['start_ticks'],G.S.process(os.getpid())['start_ticks'])
            self.assertEqual(record['boot_id'],G.S.boot_id())
            self.assertEqual(json.loads((self.inputs/'latest.json').read_text()),{'request_sha256':identifier})
            self.assertEqual(proof['lease_request_sha256'],identifier)
            return {'status':'staged'}
        self.leased(effect)
        self.assertEqual(G.status(self.request)['status'],'staged')
        with self.assertRaisesRegex(ValueError,'exists'):self.leased(effect)

    def test_fleet_failure_keeps_failed_owner_and_never_dispatches(self):
        G.fleet.side_effect=ValueError('fictional stale fleet')
        with self.assertRaisesRegex(ValueError,'stale fleet'):
            self.leased(lambda p:(_ for _ in ()).throw(AssertionError('no effects')))
        self.assertEqual(G.status(self.request)['status'],'failed')
        self.assertTrue((self.inputs/(G.lease_id(self.request)+'.request.json')).exists())

    def test_reconcile_refuses_live_owner_and_invalid_boot_then_preserves_failure(self):
        with self.assertRaises(ValueError):self.leased(lambda p:(_ for _ in ()).throw(ValueError('fictional transfer failure')))
        callback=lambda p:{'status':'reconciled'}
        with self.assertRaisesRegex(ValueError,'remains alive'):
            G.reconciled(self.request,self.lock,None,'code',callback,durable.atomic_json)
        path=self.inputs/(G.lease_id(self.request)+'.json');record=json.loads(path.read_text())
        record['boot_id']=None;durable.atomic_json(path,record)
        with self.assertRaisesRegex(ValueError,'identity is incomplete'):
            G.reconciled(self.request,self.lock,None,'code',callback,durable.atomic_json)
        record['boot_id']=G.S.boot_id();record['start_ticks']-=1;durable.atomic_json(path,record)
        G.reconciled(self.request,self.lock,None,'code',callback,durable.atomic_json)
        self.assertEqual(G.status(self.request)['status'],'reconciled')
        failed=json.loads((self.inputs/(G.lease_id(self.request)+'.failure.json')).read_text())
        self.assertEqual(failed['status'],'failed');self.assertEqual(failed['error_type'],'ValueError')
        with self.assertRaisesRegex(ValueError,'exists'):self.leased(callback)
        self.request['guard_attempt']=2;self.leased(callback)
        self.assertEqual(G.status(self.request)['status'],'staged')

if __name__=='__main__':unittest.main()

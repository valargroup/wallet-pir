"""Fictional orchestration/refusal contracts; no production qualification."""
import json
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve()
sys.path[:0] = [str(HERE.parents[3]/'ops/lib'), str(HERE.parents[1]/'lib')]
import activity_candidate_execution as E


class OracleWorkflow(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory(); self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name).resolve()
        self.events = []
        self.receiver = object.__new__(E.Receiver)
        self.receiver.evidence = self.root
        self.refs = {k:{'path':str(self.root/k),'sha256':'a'*64} for k in
                     ('snapshot_request','snapshot_owner','snapshot_manifest')}
        self.receiver.snapshot_inputs = lambda:self.refs
        self.receiver.save = lambda r:self.events.append('save')
        self.record = {'completed':0}
        self.plan = {'binary':'/fixed/candidate/event-spotcheck', 'snapshot':self.refs}
        self.inputs = {'mapping':{'path':str(self.root/'mapping'),'sha256':'b'*64}}
        self.lock = SimpleNamespace(verify=lambda:self.events.append('lock'))
        self.sampler = SimpleNamespace(deadline=time.monotonic()+30, samples=[{}],
                                       tick=lambda:self.events.append('sample'),
                                       sample=lambda:self.events.append('sample'))
        self.prepared = {'root':self.root/'immutable', 'sample':{'canonical_hashes':{'0':'0'*64}}}
        def prepare(evidence,budget):
            budget.check(); self.events.append('full-verify'); return self.prepared
        def produce(evidence,budget,path):
            budget.check(); self.events.append('report'); path.write_text('{}')
        self.oracle = SimpleNamespace(prepare_snapshot=prepare, produce_and_write=produce,
                                       S=SimpleNamespace(EXPLICIT=(0,200000,419200,903000,1687104,2726400,3500738)))
        def read_cookie(*_):self.events.append('cookie'); return b'fictional:test'
        self.snapshot = SimpleNamespace(COOKIE=Path('/fixed/runtime/cookie'),read_small=read_cookie)
        fixture = self
        class Capture:
            def __init__(self,directory,authorization,deadline,check):
                fixture.events.append('capture'); self.directory=directory
                directory.mkdir(); self.url='http://127.0.0.1:12345'
            def canonical(self,expected,phase):fixture.events.append(phase)
            def close(self):
                fixture.events.append('close')
                for name in ('attempts.jsonl','timing.jsonl','result.json'):
                    (self.directory/name).write_text('{}\n')
        self.capture = SimpleNamespace(Capture=Capture)
        self.modules = {'activity_oracle_report.py':self.oracle,'activity_oracle_capture.py':self.capture,
                        'activity_journal_snapshot.py':self.snapshot}
        def execute(lock,plan,item,record,sampler):
            self.events.append('native'); self.item=item; return {'fictional':'execution'}
        self.receiver.execute=execute

    def run_workflow(self):
        with patch.object(E,'module',side_effect=lambda name,path:self.modules[path.name]):
            self.receiver.oracle_workflow(self.lock,self.plan,self.record,self.sampler,self.inputs)

    def test_full_verify_precedes_cookie_capture_native_and_report(self):
        self.run_workflow()
        sequence=[v for v in self.events if v in ('full-verify','cookie','capture','before','native','after','close','report')]
        self.assertEqual(sequence,['full-verify','cookie','capture','before','native','after','close','report'])
        args=self.item['argv']; flags=dict(zip(args[1::2],args[2::2]))
        self.assertEqual(flags['--data-dir'],str(self.prepared['root']/'journal'))
        self.assertEqual(flags['--zakura-rpc-url'],'http://127.0.0.1:12345')
        self.assertEqual(flags['--zakura-cookie'],'/fixed/runtime/cookie')
        self.assertEqual(flags['--batch'],'256'); self.assertEqual(flags['--source-sha'],E.C.SOURCE_SHA)
        self.assertNotIn('fictional:test',json.dumps(self.record)+json.dumps(args))
        self.assertEqual(self.record['workflow_status'],'passed')
        self.assertEqual(self.record['completed'],1)

    def test_snapshot_refusal_opens_no_cookie_listener_or_native(self):
        def refuse(*_):raise ValueError('full snapshot refused')
        self.oracle.prepare_snapshot=refuse
        with self.assertRaisesRegex(ValueError,'full snapshot refused'):self.run_workflow()
        self.assertEqual(self.events,[])
        self.assertFalse((self.root/'rpc').exists())

    def test_changed_reviewed_snapshot_refuses_before_full_reader(self):
        self.plan['snapshot']={**self.refs,'snapshot_owner':{'path':'/wrong','sha256':'c'*64}}
        with self.assertRaisesRegex(ValueError,'changed since reviewed'):self.run_workflow()
        self.assertNotIn('full-verify',self.events)
        self.assertNotIn('native',self.events)

    def test_native_failure_closes_capture_and_never_reports_or_passes(self):
        def refuse(*_):self.events.append('native'); raise ValueError('native original failure')
        self.receiver.execute=refuse
        with self.assertRaisesRegex(ValueError,'native original failure'):self.run_workflow()
        self.assertIn('close',self.events)
        self.assertNotIn('after',self.events); self.assertNotIn('report',self.events)
        self.assertNotIn('workflow_status',self.record)

    def test_bad_cookie_error_is_sanitized_and_opens_no_listener(self):
        def refuse(*_):raise ValueError('fictional-secret-byte')
        self.snapshot.read_small=refuse
        with self.assertRaisesRegex(ValueError,'runtime authentication unavailable') as error:self.run_workflow()
        self.assertNotIn('fictional-secret-byte',str(error.exception))
        self.assertNotIn('capture',self.events); self.assertNotIn('native',self.events)

    def test_plan_exposes_finite_budget_and_closed_recipe_without_dispatching(self):
        receiver=self.receiver
        receiver.mode='independent-chain-oracle'; receiver.executable='event-spotcheck'
        receiver.budget=E.BUDGETS[receiver.mode]; receiver.request={'hosts':[]}
        receiver.identifier='d'*64; receiver.root=self.root/'publication'
        receiver.candidate=lambda:Path('/fixed/candidate/event-spotcheck')
        with patch.object(E,'publication',return_value={'manifests':{}}), \
             patch.object(E,'host_abi',return_value={}), patch.object(E,'verify_host',return_value={}):
            plan=receiver.plan()
        self.assertEqual(plan['snapshot'],self.refs)
        self.assertEqual(plan['dispatches'],[])
        self.assertEqual(plan['budgets']['aggregate_seconds'],5400)
        self.assertEqual(plan['oracle_recipe']['cookie_path'],'/root/.cache/zakura/.cookie')
        self.assertEqual(plan['oracle_recipe']['batch'],256)
        self.assertEqual(plan['oracle_recipe']['rpc_upstream'],'http://127.0.0.1:8232')
        self.assertEqual(self.events,[])

    def test_partial_or_foreign_snapshot_pins_refuse_request(self):
        request={'version':E.VERSION,'kind':E.KIND,'mode':'independent-chain-oracle','source_sha':'f'*40,
                 'candidate_sha':E.C.SOURCE_SHA,'candidate_identity':E.C.identity(),
                 'preparation_request_sha256':'1'*64,'publication_sha256':E.MAP_SHA256,
                 'coordinator':'coordinator','machine_id':format(1,'032x'),
                 'hosts':[{'host':h,'machine_id':format(i+1,'032x')} for i,h in enumerate(E.FLEET_HOSTS)],
                 'attempt':1,'snapshot':{'request_sha256':'2'*64,'owner_sha256':'3'*64,'manifest_sha256':'4'*64}}
        E.validate(request)
        for pins in ({'request_sha256':'2'*64}, {**request['snapshot'],'manifest_sha256':'bad'},
                     {**request['snapshot'],'path':'/live/journal'}):
            with self.assertRaisesRegex(ValueError,'snapshot pins'):E.validate({**request,'snapshot':pins})
        with self.assertRaisesRegex(ValueError,'fully verified snapshot'):E.dispatches('independent-chain-oracle',Path('/fixed'),{})

if __name__ == '__main__':unittest.main()

"""Real process survival and fail-closed ownership tests; live SSH is separate."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'ops/lib'))
from wallet_pir_ops import schema_fence
SPEC = importlib.util.spec_from_file_location('qualification',ROOT/'transparent/ops/lib/activity_lock_qualification.py')
Q = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(Q)


class QualificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.machine = self.root/'machine'; self.machine.write_text('b'*32)
        self.request = {'version':1,'source_sha':'a'*40,'host':'worker-1','machine_id':'b'*32,
                        'coordinator_machine_id':'c'*32,'attempt':1}
        self.patchers = [patch.object(Q,'OWNERS',self.root/'owners'),
                        patch.object(schema_fence,'INPUT_STAGING',self.root/'owners'),
                        patch.object(Q.ProductionLock,'PATH',self.root/'lock'),
                        patch.object(Q.ProductionLock,'MACHINE_ID',self.machine),
                        patch.object(Q.ProductionLock,'ROOT_UID',os.geteuid())]
        for item in self.patchers: item.start(); self.addCleanup(item.stop)
        self.prefix = f"""import sys,json,os,time,importlib.util
from pathlib import Path
sys.path.insert(0,{str(ROOT/'ops/lib')!r})
spec=importlib.util.spec_from_file_location('q',{str(ROOT/'transparent/ops/lib/activity_lock_qualification.py')!r})
Q=importlib.util.module_from_spec(spec);spec.loader.exec_module(Q)
Q.OWNERS=Path({str(self.root/'owners')!r})
Q.schema_fence.INPUT_STAGING=Q.OWNERS
Q.ProductionLock.PATH=Path({str(self.root/'lock')!r})
Q.ProductionLock.MACHINE_ID=Path({str(self.machine)!r})
Q.ProductionLock.ROOT_UID=os.geteuid()
Q.HOLD_SECONDS=2
request=json.loads({json.dumps(self.request)!r})
"""

    def test_remote_parent_exits_child_locks_and_fences_until_reconcile(self):
        child = self.prefix+"print(json.dumps(Q.Owner(request,remote=True).child()))"
        parent = self.prefix+f"Q.command=lambda request,action: [sys.executable,'-c',{child!r}]\nprint(json.dumps(Q.Owner(request,remote=True).remote_parent()))"
        process = subprocess.Popen([sys.executable,'-c',parent],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        self.addCleanup(lambda: process.wait(timeout=10))
        self.assertEqual(process.wait(timeout=5),0)
        owner = Q.Owner(self.request,remote=True)
        deadline=time.monotonic()+3
        while not owner.status().get('parent_exited'):
            self.assertLess(time.monotonic(),deadline)
            time.sleep(.025)
        proof=owner.probe()
        self.assertTrue(proof['host_lock_blocked'])
        with self.assertRaises(BlockingIOError): owner.reconcile()
        output,error=process.communicate(timeout=5)
        self.assertEqual(error,b'')
        self.assertEqual(len(output.splitlines()),2)
        self.assertTrue(owner.status()['parent_exited'])
        self.assertTrue(owner.status()['child_finished_unix'])
        self.assertEqual(owner.reconcile()['status'],'reconciled')
        with owner.lock(): schema_fence.local_schema_fence()

    def test_repeated_or_mismatched_owner_refuses(self):
        owner=Q.Owner(self.request,remote=True)
        with owner.lock(): owner.begin()
        with owner.lock(), self.assertRaises(ValueError): owner.begin()
        record=owner.status(); record['request']['attempt']=2; owner.save(record)
        with self.assertRaises(ValueError): owner.status()

    def test_relay_exits_transport_descendant_keeps_coordinator_fd(self):
        self.machine.write_text('c'*32)
        parent=self.prefix+'''from types import SimpleNamespace
owner=Q.Owner(request)
with owner.lock() as lock:
    owner.begin()
    os.environ[Q.inherited_lock.VARIABLE]=str(lock.fd)
    client=object.__new__(Q.Client)
    client.request=request;client.owner=owner
    client.executor=SimpleNamespace(transport=lambda host:[sys.executable,'-c','import time; time.sleep(2)'])
    print(json.dumps(client.relay()))
'''
        process=subprocess.Popen([sys.executable,'-c',parent],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        output,error=process.communicate(timeout=5)
        self.assertEqual(process.returncode,0,error.decode())
        receipt=json.loads(output)
        self.assertEqual(receipt['relay_pid'],process.pid)
        owner=Q.Owner(self.request)
        with self.assertRaises(BlockingIOError): owner.reconcile()
        deadline=time.monotonic()+4
        while True:
            try:
                self.assertEqual(owner.reconcile()['status'],'reconciled')
                break
            except BlockingIOError:
                self.assertLess(time.monotonic(),deadline)
                time.sleep(.025)

    def test_unfinished_owner_blocks_reconciliation_of_other_owner(self):
        owner=Q.Owner(self.request,remote=True)
        with owner.lock(): owner.begin()
        Q.durable.atomic_json(Q.OWNERS/'latest.json',{'request_sha256':'d'*64},mode=0o600)
        Q.durable.atomic_json(Q.OWNERS/('d'*64+'.json'),{'status':'interrupted'},mode=0o600)
        with self.assertRaises(ValueError): owner.reconcile()

    def test_closed_request_bounds(self):
        for changes in ({'attempt':True},{'attempt':101},{'host':'../worker'},{'source_sha':'x'},
                        {'machine_id':'c'*32},{'command':'systemctl'}):
            with self.subTest(changes=changes),self.assertRaises(ValueError):
                Q.validate(dict(self.request,**changes))

    def test_missing_child_owner_and_released_probe_refuse(self):
        owner=Q.Owner(self.request,remote=True)
        with self.assertRaises(ValueError): owner.child()
        with owner.lock(): record=owner.begin()
        record.update(parent_exited=True); owner.save(record)
        with self.assertRaises(ValueError): owner.probe()


if __name__=='__main__': unittest.main()

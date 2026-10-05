"""Fictional files/units; no fixture authorizes a production baseline."""
import copy
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'lib'))
from wallet_pir_ops import ancillary_baseline as A, owner_survey as S

class Retained(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name).resolve();self.proc=self.root/'proc';self.props={}
        boot=self.proc/'sys/kernel/random/boot_id';boot.parent.mkdir(parents=True);boot.write_text(A.PINS[A.PROTOTYPE]['boot_id'])
        self.pins=copy.deepcopy(A.PINS)
        for unit,pid in ((A.PROTOTYPE,A.PINS[A.PROTOTYPE]['pid']),(A.LOAD,333)):
            pin=self.pins[unit];start=pin.get('start_ticks',987)
            base=self.proc/str(pid);base.mkdir(parents=True)
            fields=['0']*23;fields[0]='S';fields[19]=str(start)
            (base/'stat').write_text(str(pid)+' (fixture) '+' '.join(fields))
            (base/'cgroup').write_text('0::/system.slice/'+unit+'\n')
            command=(unit+'\0--fictional\0').encode();(base/'cmdline').write_bytes(command)
            binary=self.root/(unit+'.exe');binary.write_bytes(b'fictional executable '+unit.encode());(base/'exe').symlink_to(binary)
            fragment=self.root/(unit+'.unit');fragment.write_bytes(b'fictional unit '+unit.encode())
            pin.update(fragment=str(fragment),fragment_sha256=hashlib.sha256(fragment.read_bytes()).hexdigest(),
                       exe_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),command_sha256=hashlib.sha256(command).hexdigest())
            self.props[unit]=dict(Id=unit,ActiveState='active',SubState='running',MainPID=str(pid),NRestarts='0',
                ControlGroup='/system.slice/'+unit,FragmentPath=str(fragment),DropInPaths='',NeedDaemonReload='no')
            (base/'fd').mkdir()
        pid=self.pins[A.PROTOTYPE]['pid'];(self.proc/str(pid)/'fd/0').symlink_to('socket:[123]')
        net=self.proc/'net';net.mkdir()
        (net/'tcp').write_text('header\n 0: 0100007F:2000 00000000:0000 0A 0 0 0 0 0 123\n');(net/'tcp6').write_text('header\n')
        for item in (patch.object(A,'PINS',self.pins),patch.object(A,'PINS_SHA256',hashlib.sha256(json.dumps(self.pins,sort_keys=True,separators=(',',':')).encode()).hexdigest()),
                     patch.object(A,'properties',side_effect=lambda unit:dict(self.props[unit])),patch.object(A.os,'geteuid',return_value=0),
                     patch.object(A,'route',return_value={'sha256':'f'*64,'bytes':50,'prototype_port_excluded':True})):
            item.start();self.addCleanup(item.stop)

    def observe(self):return A.observe(A.COORDINATOR,proc=self.proc)

    def test_exact_files_process_command_socket_and_route_proof_then_allow_only_main_process(self):
        value=self.observe();A.verify(value,A.COORDINATOR)
        cap=value['units'][A.PROTOTYPE];item=dict(cap,argv=[],cgroup='0::'+cap['cgroup'])
        classes={'names':[],'roots':[str(self.root)+'/']}
        allowed=A.authorities(value,A.COORDINATOR)
        self.assertTrue(A.authorized(value,A.COORDINATOR,item))
        bound,foreign,errors=S.operational([item],set(),classes,(),ancillary=allowed)
        self.assertEqual((len(bound),foreign,errors),(1,[],[]))
        for change in ({'pid':cap['pid']+1},{'start_ticks':cap['start_ticks']+1},{'command_sha256':'0'*64},
                       {'cgroup':item['cgroup']+'/descendant'},{'exe':'/other/transparent-shard-server'}):
            test={**item,**change};test['argv']=[str(self.root/'operation')]
            bound,foreign,errors=S.operational([test],set(),classes,(),ancillary=allowed)
            self.assertFalse(bound);self.assertEqual(len(foreign),1)
            self.assertFalse(A.authorized(value,A.COORDINATOR,test))
        self.assertEqual(set(A.UNITS),set(S.ANCILLARY_UNITS))
        self.assertEqual(len(S.operational([item],set(),classes,(A.PROTOTYPE,))[1]),1)
        # A unit name without the verified authority remains unclassified.
        self.assertEqual(len(S.operational([item],set(),classes,())[1]),1)

    def test_changed_pid_start_command_binary_unit_or_public_listener_refuses(self):
        pid=self.pins[A.PROTOTYPE]['pid'];base=self.proc/str(pid)
        for path,raw in ((base/'cmdline',b'changed'),(self.root/(A.PROTOTYPE+'.exe'),b'changed'),
                         (Path(self.pins[A.PROTOTYPE]['fragment']),b'changed'),
                         (self.proc/'net/tcp',b'header\n 0: 00000000:2000 00000000:0000 0A 0 0 0 0 0 123\n')):
            before=path.read_bytes();path.write_bytes(raw)
            with self.assertRaises(ValueError):self.observe()
            path.write_bytes(before)
        before=self.props[A.PROTOTYPE]['MainPID'];self.props[A.PROTOTYPE]['MainPID']='333'
        with self.assertRaisesRegex(ValueError,'process identity'):self.observe()
        self.props[A.PROTOTYPE]['MainPID']=before
        self.props[A.LOAD]['DropInPaths']='/unexpected'
        with self.assertRaises(ValueError):self.observe()

    def test_proof_cannot_be_stale_substituted_or_omit_router_route(self):
        value=self.observe()
        for change in ({'observed_unix':time.time()-301},{'machine_id':A.ROUTER},{'pins_sha256':'0'*64},
                       {'route':None},{'route':{'sha256':'f'*64,'bytes':50,'prototype_port_excluded':False}},{'units':{}}):
            with self.assertRaises(ValueError):A.verify({**value,**change},A.COORDINATOR)
        router=A.observe(A.ROUTER,proc=self.proc);A.verify(router,A.ROUTER)
        with self.assertRaises(ValueError):A.verify(dict(router,route=None),A.ROUTER)

    def test_inactive_unit_grants_no_authority_and_other_hosts_never_probe(self):
        self.props[A.PROTOTYPE].update(ActiveState='inactive',MainPID='0')
        value=self.observe();self.assertNotIn(self.pins[A.PROTOTYPE]['pid'],A.authorities(value,A.COORDINATOR))
        with patch.object(A,'properties',side_effect=AssertionError('not a coordinator')):
            value=A.observe('a'*32,proc=self.root/'absent');A.verify(value,'a'*32)
        self.assertEqual(A.authorities(value,'a'*32),{})

    def test_loaded_route_never_redirects_or_allows_port_reference(self):
        class Response:
            status=200
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def read(self,n):return b'{"dial":"127.0.0.1:8192"}'
        # Test original implementation, not the setUp route stand-in.
        original=self._original_route
        with patch.object(A.urllib.request,'build_opener',return_value=type('Opener',(),{'open':lambda *a,**k:Response()})()):
            with self.assertRaises(ValueError):original()
            with patch.object(Response,'read',lambda self,n: b'{"dial":"127.0.0.1:\\u0038192"}'):
                with self.assertRaisesRegex(ValueError,'historical prototype'):original()
        with self.assertRaises(ValueError):A.NoRedirect().redirect_request(None,None,None,None,None,None)

    _original_route=staticmethod(A.route)

if __name__=='__main__':unittest.main()

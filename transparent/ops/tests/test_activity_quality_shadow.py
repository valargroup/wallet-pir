"""Synthetic APM files exercise refusal; they are not production shadow evidence."""
import hashlib
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path[:0]=[str(Path(__file__).resolve().parents[1]/'lib'),str(Path(__file__).resolve().parents[3]/'ops/lib')]
import activity_quality_shadow as Q

class Shadow(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup);self.root=Path(t.name).resolve()
        self.proc=self.root/'proc';base=self.proc/'777';base.mkdir(parents=True);self.base=base
        self.machine=self.root/'machine';self.machine.write_text(Q.A.COORDINATOR)
        boot=self.proc/'sys/kernel/random/boot_id';boot.parent.mkdir(parents=True);boot.write_text('38feb427-561c-4b5f-b164-aca94f4e310a')
        fields=['0']*23;fields[0]='S';fields[19]='123';(base/'stat').write_text('777 (fixture) '+' '.join(fields))
        self.binary=self.root/'apm';self.binary.write_bytes(b'fictional code');(base/'exe').symlink_to(self.binary)
        (base/'cgroup').write_text('0::/system.slice/'+Q.UNIT)
        (base/'environ').write_bytes(b'UNRELATED_SECRET=fixture-sensitive-value\0'+Q.KEY+b'=shadow\0')
        self.state={'ActiveState':'active','SubState':'running','MainPID':'777','ControlGroup':'/system.slice/'+Q.UNIT}
        self.commands=type('Commands',(),{'state':lambda obj,unit:dict(self.state)})()
        self.timeouts=[]
        def run(argv,timeout):
            self.timeouts.append(timeout)
            return '\n'.join(k+'='+v for k,v in self.commands.state(argv[2]).items()).encode()
        self.commands.run=run
        for p in (patch.object(Q.os,'geteuid',return_value=0),patch.object(Q,'EXECUTABLE',str(self.binary)),
                  patch.object(Q,'BINARY_SHA256',hashlib.sha256(self.binary.read_bytes()).hexdigest())):
            p.start();self.addCleanup(p.stop)

    def observe(self):return Q.observe(self.commands,proc=self.proc,machine_path=self.machine)

    def test_loaded_shadow_retains_only_identity_and_enum(self):
        value=self.observe();self.assertEqual(value['quality_alert_mode'],'shadow')
        self.assertEqual(len(self.timeouts),2);self.assertTrue(all(0<t<=5 for t in self.timeouts))
        self.assertNotIn('fixture-sensitive-value',repr(value));self.assertNotIn('UNRELATED_SECRET',repr(value))
        self.assertTrue(Q.same_process(value,dict(value,observed_unix=0)))
        for k in ('pid','start_ticks','boot_id','binary_sha256','unit','quality_alert_mode'):
            self.assertFalse(Q.same_process(value,dict(value,**{k:'changed'})))

    def test_absent_active_duplicate_invalid_and_oversized_mode_refuse_without_secret(self):
        for raw in (b'',Q.KEY+b'=active\0',Q.KEY+b'=shadow\0'+Q.KEY+b'=shadow\0',
                    Q.KEY+b'=fixture-sensitive-value\0',b'x'*((1<<20)+1)):
            (self.base/'environ').write_bytes(raw)
            with self.assertRaises(ValueError) as e:self.observe()
            self.assertNotIn('fixture-sensitive-value',str(e.exception))

    def test_foreign_machine_unreviewed_binary_cgroup_unit_or_process_refuse(self):
        for path,data in ((self.machine,b'f'*32),(self.binary,b'changed'),(self.base/'cgroup',b'0::/foreign')):
            before=path.read_bytes();path.write_bytes(data)
            with self.assertRaises(ValueError):self.observe()
            path.write_bytes(before)
        for change in ({'MainPID':'0'},{'ActiveState':'inactive'},{'SubState':'reloading'}):
            before=dict(self.state);self.state.update(change)
            with self.assertRaises(ValueError):self.observe()
            self.state=before

    def test_identity_drift_and_unreadable_environment_refuse(self):
        calls=0
        def state(unit):
            nonlocal calls
            calls+=1
            return dict(self.state,MainPID='778' if calls>1 else '777')
        self.commands.state=state
        with self.assertRaisesRegex(ValueError,'changed'):self.observe()
        self.commands.state=lambda unit:dict(self.state)
        (self.base/'environ').unlink()
        with self.assertRaises(FileNotFoundError):self.observe()

if __name__=='__main__':unittest.main()

"""Fictional mapped cache files and real local lock/rename/crash recovery tests."""
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'lib'))
import activity_cache_fault as C

class Cache(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);self.root=Path(temp.name).resolve()
        self.cache=self.root/'runtime-cache';self.cache.mkdir();(self.cache/'.lock').touch()
        self.proc=self.root/'proc';(self.proc/'123').mkdir(parents=True)
        self.manifest='a'*64;self.prefix=hashlib.sha256(self.manifest.encode()).hexdigest();self.name=self.prefix+'-'+'b'*64+'.runtime'
        self.target=self.cache/self.name;body=b'fictional public preprocessing'*32
        self.original=bytes.fromhex('b'*64)+hashlib.sha256(body).digest()+body;self.target.write_bytes(self.original)
        info=self.target.stat();line='1000-2000 r--s 00000000 %x:%x %d %s\n'%(os.major(info.st_dev),os.minor(info.st_dev),info.st_ino,self.target)
        (self.proc/'123/maps').write_text(line)
        self.record={};self.snapshots=[];self.fail_phase=None;self.stopped=True
        self.deadline=SimpleNamespace(need=lambda *args:None);self.lock=SimpleNamespace(verify=lambda:None)
        for item in (patch.object(C,'CACHE',self.cache),patch.object(C,'RECOVERY',self.root/'cache-faults')):
            item.start();self.addCleanup(item.stop)
        self.fault=C.CacheFault('c'*64,self.record,self.save,self.deadline,self.lock,lambda:self.stopped,proc=self.proc)

    def save(self):
        self.snapshots.append(copy.deepcopy(self.record))
        if self.record['cache']['phase']==self.fail_phase:
            self.fail_phase=None
            raise RuntimeError('fictional transport/owner interruption')

    def capture(self):self.fault.capture(123,[self.manifest])

    def test_corruption_uses_new_inode_and_original_is_exactly_restored(self):
        self.capture();inode=self.target.stat().st_ino
        with self.target.open('rb') as held:
            self.fault.corrupt()
            self.assertEqual(held.read(),self.original)
            self.assertNotEqual(self.target.stat().st_ino,inode)
            self.assertEqual(self.target.stat().st_size,64)
            self.fault.restore()
        self.assertEqual(self.target.read_bytes(),self.original);self.assertEqual(self.target.stat().st_ino,inode)
        self.assertEqual(self.record['cache']['phase'],'restored')
        self.assertEqual((self.fault.directory/'displaced').stat().st_size,64)
        self.fault.restore()  # explicit recovery is idempotent; corruption is not
        with self.assertRaisesRegex(ValueError,'not replayable'):self.fault.corrupt()

    def recover_phase(self):
        self.capture();self.fail_phase=self.selected_phase
        with self.assertRaises(RuntimeError):self.fault.corrupt()
        self.fault.restore();self.assertEqual(self.target.read_bytes(),self.original)

    def test_unknown_replacement_stays_fenced_with_original_retained(self):
        self.capture();self.fault.corrupt();self.target.write_bytes(b'x'*128)
        with self.assertRaisesRegex(ValueError,'unexpected replacement'):self.fault.restore()
        self.assertEqual((self.fault.directory/'original').read_bytes(),self.original)
        self.assertEqual(self.target.read_bytes(),b'x'*128)

    def test_regenerated_exact_cache_is_retained_without_overwrite(self):
        self.capture();self.fault.corrupt();self.target.write_bytes(self.original)
        self.fault.restore();self.assertEqual(self.target.read_bytes(),self.original)
        self.assertEqual((self.fault.directory/'displaced').read_bytes(),self.original)

    def test_missing_stop_or_foreign_native_lock_prevents_any_cache_mutation(self):
        self.capture();self.stopped=False
        with self.assertRaisesRegex(ValueError,'stopped'):self.fault.corrupt()
        self.stopped=True
        with (self.cache/'.lock').open('rb') as held:
            fcntl.flock(held,fcntl.LOCK_EX|fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):self.fault.corrupt()
        self.assertEqual(self.target.read_bytes(),self.original);self.assertFalse(self.fault.directory.exists())

    def test_changed_captured_file_refuses_before_rename(self):
        self.capture();self.target.write_bytes(self.original+b'x')
        with self.assertRaisesRegex(ValueError,'changed since capture'):self.fault.corrupt()
        self.assertFalse(self.fault.directory.exists())

    def test_unknown_mapping_checksum_identity_and_links_refuse(self):
        with self.assertRaisesRegex(ValueError,'no current sealed'):self.fault.capture(123,['d'*64])
        self.target.write_bytes(self.original[:-1]+b'x')
        with self.assertRaisesRegex(ValueError,'checksum'):self.capture()
        self.target.write_bytes(self.original)
        original=self.target.read_bytes();self.target.unlink();outside=self.root/'outside';outside.write_bytes(original)
        self.target.symlink_to(outside)
        with self.assertRaisesRegex(ValueError,'link'):self.capture()

    def test_insufficient_disk_headroom_refuses_before_rename(self):
        self.capture()
        disk=SimpleNamespace(f_bavail=1,f_frsize=4096,f_blocks=1000000)
        with patch.object(C.os,'statvfs',return_value=disk):
            with self.assertRaisesRegex(ValueError,'headroom'):self.fault.corrupt()
        self.assertEqual(self.target.read_bytes(),self.original);self.assertFalse(self.fault.directory.exists())

    def test_tampered_retained_original_remains_fenced(self):
        self.capture();self.fault.corrupt()
        retained=self.fault.directory/'original';retained.write_bytes(b'x'*128)
        with self.assertRaisesRegex(ValueError,'checksum'):self.fault.restore()
        self.assertEqual(self.target.stat().st_size,64);self.assertTrue(retained.exists())

    def test_mapped_inode_and_header_identity_must_match(self):
        mapped=self.proc/'123/maps';raw=mapped.read_text();fields=raw.split(maxsplit=5)
        fields[4]=str(int(fields[4])+1);mapped.write_text(' '.join(fields))
        with self.assertRaisesRegex(ValueError,'inode differs'):self.capture()
        mapped.write_text(raw)
        self.target.write_bytes(bytes.fromhex('d'*64)+self.original[32:])
        with self.assertRaisesRegex(ValueError,'identity differs'):self.capture()

    def test_linked_native_lock_refuses_before_any_rename(self):
        self.capture();os.link(self.cache/'.lock',self.root/'lock-alias')
        with self.assertRaisesRegex(ValueError,'exclusive'):self.fault.corrupt()
        self.assertFalse(self.fault.directory.exists());self.assertEqual(self.target.read_bytes(),self.original)

    def recover_restore_phase(self):
        self.capture();self.fault.corrupt();self.fail_phase=self.selected_phase
        with self.assertRaises(RuntimeError):self.fault.restore()
        self.fault.restore();self.assertEqual(self.target.read_bytes(),self.original)
        self.assertEqual(self.target.stat().st_ino,self.record['cache']['original']['inode'])

    def test_expired_deadline_does_not_act(self):
        def expired(*args):raise ValueError('expired')
        self.fault.deadline=SimpleNamespace(need=expired)
        with self.assertRaisesRegex(ValueError,'expired'):self.capture()
        self.assertEqual(self.target.read_bytes(),self.original)

# Each case gets an independent filesystem/owner; no failed run is re-used.
for phase in ('retain-intent','retained','install-intent','corrupted'):
    def trial(self, phase=phase):
        self.selected_phase=phase;self.recover_phase()
    setattr(Cache,'test_interruption_'+phase.replace('-','_'),trial)

for phase in ('displace-intent','restore-intent','restored'):
    def trial(self, phase=phase):
        self.selected_phase=phase;self.recover_restore_phase()
    setattr(Cache,'test_restore_interruption_'+phase.replace('-','_'),trial)

if __name__=='__main__':unittest.main()

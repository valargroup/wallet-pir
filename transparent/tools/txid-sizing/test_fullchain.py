"""Bounded gateway ordering, rate guards and immutable input negative cases."""
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
import fullchain
from aggregate import Aggregate

class FullChainTests(unittest.TestCase):
    def test_one_transport_matches_out_of_order_ids(self):
        script="""import json,sys
requests=[json.loads(sys.stdin.readline()) for _ in range(3)]
for r in reversed(requests):
 assert r['method']=='getblock' and r['params'][1]==0
 print(json.dumps(dict(id=r['id'],result=bytes([int(r['params'][0])]).hex(),error=None)),flush=True)
"""
        process=subprocess.Popen([sys.executable,'-c',script],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        gateway=SimpleNamespace(process=process,next_id=10,last=0)
        try:
            rows=list(fullchain.Pipeline(gateway,rate=50,window=3).blocks(1,4))
            self.assertEqual(sorted(rows),[(1,b'\x01'),(2,b'\x02'),(3,b'\x03')])
        finally:
            process.wait(timeout=5)
            for stream in (process.stdin,process.stdout,process.stderr):stream.close()
    def test_limits_and_raw_checksum_failure(self):
        for rate,window in ((51,200),(0,200),(45,201)):
            with self.assertRaisesRegex(ValueError,'rate/window'):fullchain.Pipeline(None,rate,window)
        with tempfile.TemporaryDirectory() as directory:
            cache=Path(directory);(cache/'raw').mkdir()
            raw=b'public';(cache/'raw/0.bin').write_bytes(raw)
            pin=dict(height=0,raw_bytes=len(raw),raw_sha256=hashlib.sha256(raw).hexdigest())
            (cache/'raw-manifest.jsonl').write_text(json.dumps(pin)+'\n')
            self.assertEqual(fullchain.verify_raw_cache(cache),[pin])
            (cache/'raw/0.bin').write_bytes(b'tamper')
            with self.assertRaisesRegex(ValueError,'checksum'):fullchain.verify_raw_cache(cache)
    def test_bundle_corruption_and_duplicate_heights(self):
        with tempfile.TemporaryDirectory() as directory:
            cache=Path(directory);raw=b'public'
            (cache/'chain-0-0.raw').write_bytes(raw)
            pin=dict(height=0,offset=0,raw_bytes=len(raw),raw_sha256=hashlib.sha256(raw).hexdigest())
            path=cache/'chain-0-0.jsonl';path.write_text(json.dumps(pin)+'\n')
            self.assertEqual(len(list(fullchain.bundles(cache))),1)
            path.write_text((json.dumps(pin)+'\n')*2)
            with self.assertRaisesRegex(ValueError,'duplicate'):list(fullchain.bundles(cache))
            path.write_text(json.dumps(dict(pin,raw_sha256='00'*32))+'\n')
            with self.assertRaisesRegex(ValueError,'checksum'):list(fullchain.bundles(cache))

class AggregateTests(unittest.TestCase):
    @staticmethod
    def block(height=0):
        records=[]
        for i,size in enumerate([10]*8+[20,30]):
            records.append(dict(txid_internal=f'{i:064x}',coinbase=i==0,input_count=0 if i==0 else 1,output_count=1,
                shielded_components=False,fee='not_applicable' if i==0 else (0 if i==1 else 'unknown'),missing_prevouts=0 if i<2 else 1,
                empty_scripts=0,op_return_scripts=0,scripts={'raw_escape':1},sizes=[size,size-1,size-2,size-3]))
        return dict(height=height,hash=f'block-{height}',transactions=11,eligible=10,shielded_only=1,raw_bytes=123,records=records)
    def test_strict_boundary_unknown_zero_and_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'aggregate.sqlite'
            aggregate=Aggregate(path);block=self.block();aggregate.add(block);aggregate.close()
            aggregate=Aggregate(path);aggregate.add(block)
            report=aggregate.report()
            self.assertEqual(report['totals']['eligible'],10)
            self.assertEqual(report['totals']['fee_exact_zero'],1)
            self.assertEqual(report['totals']['fee_unknown'],8)
            self.assertEqual(report['codecs']['display-v1']['all']['frontiers']['0.8'],20)
            self.assertEqual(report['codecs']['display-v1']['all']['frontiers']['0.85'],20)
            self.assertEqual(report['codecs']['display-v1']['all']['frontiers']['0.99'],30)
            aggregate.close()
    def test_failed_batch_rolls_back_and_does_not_hide_eligible_records(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'aggregate.sqlite'
            aggregate=Aggregate(path);aggregate.add(self.block());aggregate.flush()
            bad=self.block(1)
            with self.assertRaises(Exception):aggregate.add(bad)
            aggregate.close();aggregate=Aggregate(path)
            self.assertEqual(aggregate.db.execute('SELECT count(*) FROM blocks').fetchone()[0],1)
            self.assertEqual(aggregate.report()['totals']['eligible'],10)
            aggregate.close()

if __name__=='__main__':unittest.main()

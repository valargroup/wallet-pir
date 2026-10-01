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

if __name__=='__main__':unittest.main()

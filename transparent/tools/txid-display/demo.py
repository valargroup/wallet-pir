#!/usr/bin/env python3
"""Run the native confirmed-vector acceptance gate in one leased Cargo lane."""
import argparse
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'shared/dev'))
from target_lease import local_target,inherited_fds

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--report',default='/tmp/transparent-txid-demo.json')
p.add_argument('--offline',action='store_true')
args=p.parse_args()
subprocess.run([sys.executable,str(ROOT/'transparent/services/transparent-shard-server/examples/fixtures/verify_fixture.py')],cwd=ROOT,check=True)
base=['cargo','run','--locked','--profile','release-fast','-p','transparent-shard-server','--example','transparent-txid-demo']
if args.offline:base.append('--offline')
with local_target(ROOT):
 subprocess.run(base+['--','--report',args.report],cwd=ROOT,**inherited_fds(),check=True)
 with tempfile.TemporaryDirectory(prefix='txid-oracle-negative-') as temp:
  altered=Path(temp)/'must-not-exist.json'
  result=subprocess.run(base+['--','--corrupt-oracle','--report',str(altered)],cwd=ROOT,**inherited_fds())
  if result.returncode==0 or altered.exists():raise SystemExit('FAIL: corrupted independent oracle was accepted')
print('confirmed native gate and independent-oracle exit control passed')

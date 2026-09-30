#!/usr/bin/env python3
"""Independent, offline verification of frozen legacy transaction facts.

Only Python's standard library. This does not import candidate Rust extraction,
regenerate expectations, verify consensus proofs, or retrieve public txids.
"""
from pathlib import Path
import json, hashlib, struct
def cs(b,p):
 x=b[p];p+=1
 if x<253:return x,p
 n={253:2,254:4,255:8}[x];return int.from_bytes(b[p:p+n],'little'),p+n
def parse(b,p=0):
 start=p;v=int.from_bytes(b[p:p+4],'little');p+=4;ver=v&0x7fffffff
 assert ver<5
 if v&0x80000000:p+=4
 n,p=cs(b,p);ins=[]
 for _ in range(n):
  h=b[p:p+32].hex();idx=int.from_bytes(b[p+32:p+36],'little');p+=36;l,p=cs(b,p);p+=l+4;ins.append([h,idx])
 n,p=cs(b,p);outs=[]
 for _ in range(n):
  val=int.from_bytes(b[p:p+8],'little');p+=8;l,p=cs(b,p);sc=b[p:p+l].hex();p+=l;outs.append({'value':val,'script':sc})
 p+=4
 if ver>=3:p+=4
 balance=0;components=False
 if ver==4:
  balance=int.from_bytes(b[p:p+8],'little',signed=True);p+=8;ns,p=cs(b,p);p+=ns*384;no,p=cs(b,p);p+=no*948;components=ns+no>0
 if ver>=2:
  nj,p=cs(b,p);components|=nj>0
  for _ in range(nj):
   old,new=struct.unpack_from('<QQ',b,p);balance+=new-old;p+=1698 if ver==4 else 1802
  if nj:p+=96
 if ver==4 and ns+no:p+=64
 raw=b[start:p];tid=hashlib.sha256(hashlib.sha256(raw).digest()).digest().hex()
 coinbase=len(ins)==1 and ins[0]==['00'*32,0xffffffff]
 return p,{'txid':tid,'coinbase':coinbase,'input_count':0 if coinbase else len(ins),'shielded_components':components,'outputs':outs,'inputs':ins,'shielded_balance':balance,'raw':raw.hex()}

fixture_path=Path(__file__).with_name('txid-confirmed.json')
fixture=json.loads(fixture_path.read_text())
assert fixture['format']=='transparent-txid-demo-confirmed-v1'
parents={}
for parent in fixture['parents']:
 b=bytes.fromhex(parent['raw']);assert hashlib.sha256(b).hexdigest()==parent['sha256']
 p,t=parse(b);assert p==len(b) and t['txid']==parent['txid'];parents[t['txid']]=t
alltx=dict(parents);blocks=[]
for frozen in fixture['blocks']:
 b=bytes.fromhex(frozen['raw']);assert hashlib.sha256(b).hexdigest()==frozen['sha256']
 p=140;size,p=cs(b,p);p+=size
 assert hashlib.sha256(hashlib.sha256(b[:p]).digest()).digest().hex()==frozen['hash']
 count,p=cs(b,p);items=[]
 for _ in range(count):p,t=parse(b,p);items.append(t);alltx[t['txid']]=t
 assert p==len(b) and len(items)==len(frozen['transactions']);blocks.append((frozen,items))
for frozen,items in blocks:
 for actual,expected in zip(items,frozen['transactions']):
  fee='not_applicable' if actual['coinbase'] else sum(alltx[h]['outputs'][i]['value'] for h,i in actual['inputs'])-sum(o['value'] for o in actual['outputs'])+actual['shielded_balance']
  for key in ['txid','coinbase','input_count','shielded_components','outputs']:assert actual[key]==expected[key],(frozen['height'],key)
  assert fee==expected['fee'],(frozen['height'],'fee')
print('independent frozen fixture verification passed: four blocks, sixteen transactions, fifteen prevouts')

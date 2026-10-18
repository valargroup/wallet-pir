#!/usr/bin/env python3
"""Independent, offline verification of frozen legacy transaction facts.

Only Python's standard library. This does not import candidate Rust extraction,
regenerate expectations, verify consensus proofs, or retrieve public txids.
"""
from pathlib import Path
import json, hashlib, struct, sys
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

TAG_DOMAIN=b'transparent-txid-display/tag/v2'
def address(script_hex):
 """Kind and hash of a locking script: 1 P2PKH, 2 P2SH, 3 no address."""
 b=bytes.fromhex(script_hex)
 if len(b)==25 and b[:3]==bytes([0x76,0xa9,0x14]) and b[23:]==bytes([0x88,0xac]):return 1,b[3:23]
 if len(b)==23 and b[:2]==bytes([0xa9,0x14]) and b[22:]==bytes([0x87]):return 2,b[2:22]
 return 3,bytes(20)
def entry(t,alltx,fee):
 """The 113-byte display v2 entry, from the parsed transaction and its parents."""
 spent=[] if t['coinbase'] else [alltx[h]['outputs'][i]['script'] for h,i in t['inputs']]
 standard=[address(s) for s in spent if address(s)[0] in (1,2)]
 source=standard[0] if standard else (3,bytes(20)) if spent else (0,bytes(20))
 multiple=len(set(spent))>1
 mixed=bool(spent) and t['shielded_components'] and t['shielded_balance']>0
 outs=t['outputs']
 flags=(t['coinbase'])|(t['shielded_components']<<1)|(multiple<<2)|((len(outs)>2)<<3)|(mixed<<4)
 tag=hashlib.sha256(TAG_DOMAIN+bytes.fromhex(t['txid'])).digest()[:16]
 b=tag+struct.pack('<HQII',flags,0 if t['coinbase'] else fee,len(spent),len(outs))+bytes([source[0]])+source[1]
 for o in outs[:2]:
  kind,h=address(o['script']);b+=struct.pack('<Q',o['value'])+bytes([kind])+h
 b+=bytes(29*(2-min(2,len(outs))))
 assert len(b)==113
 return b.hex()

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
write='--write-entries' in sys.argv[1:]
for frozen,items in blocks:
 for actual,expected in zip(items,frozen['transactions']):
  fee='not_applicable' if actual['coinbase'] else sum(alltx[h]['outputs'][i]['value'] for h,i in actual['inputs'])-sum(o['value'] for o in actual['outputs'])+actual['shielded_balance']
  for key in ['txid','coinbase','input_count','shielded_components','outputs']:assert actual[key]==expected[key],(frozen['height'],key)
  assert fee==expected['fee'],(frozen['height'],'fee')
  # Only transactions with a transparent input or output have an entry.
  eligible=actual['coinbase'] or bool(actual['inputs']) or bool(actual['outputs'])
  derived=entry(actual,alltx,0 if actual['coinbase'] else fee) if eligible else None
  if write:
   expected.pop('entry',None)
   if derived:expected['entry']=derived
  else:assert expected.get('entry')==derived,(frozen['height'],'entry')
if write:
 fixture_path.write_text(json.dumps(fixture,indent=2)+'\n')
 print('wrote independent display v2 entries into',fixture_path.name)
else:
 print('independent frozen fixture verification passed: four blocks, sixteen transactions, fifteen prevouts, display v2 entries')

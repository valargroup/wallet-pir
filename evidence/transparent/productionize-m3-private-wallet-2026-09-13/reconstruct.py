import base64,datetime,gzip,hashlib,json,pathlib,sqlite3,subprocess
root=pathlib.Path('/Users/roman/Library/Application Support/transparent-m3-validation')
profile=root/'shadow-20260913'; conn=sqlite3.connect((profile/'cache.db').as_uri()+'?mode=ro&immutable=1',uri=True)
scripts={bytes(r[0]) for r in conn.execute('SELECT script FROM transparent_scripts')}
height,anchor,completion=conn.execute('SELECT anchor_height,anchor_hash,completion FROM transparent_set WHERE id=1').fetchone()
start=3480000
proto='/Users/roman/projects/wallet-libraries-m3-correctness/zakura/wallet-lwd/proto'
def rpc(method,data):
 r=subprocess.run(['grpcurl','-max-time','60','-max-msg-sz','67108864','-import-path',proto,'-proto','service.proto','-d',json.dumps(data),'us.zec.stardust.rest:443','cash.z.wallet.sdk.rpc.CompactTxStreamer/'+method],capture_output=True,text=True,timeout=70)
 if r.returncode:raise RuntimeError('block RPC failed')
 return r.stdout
def messages(s):
 decoder=json.JSONDecoder()
 while s.strip():
  s=s.lstrip();v,end=decoder.raw_decode(s);yield v;s=s[end:]
def raw(s):return base64.b64decode(s)
def txid(s):return raw(s)[::-1].hex()
accepted=json.loads(rpc('GetTreeState',{'height':height}))
assert accepted['hash']==anchor and int(accepted['height'])==height
expected={'anchor_height':height,'anchor_hash':anchor,'completion':'complete','receives':{},'spends':{}}
prev=None;blocks=0;outputs=0;inputs=0;records=0
with gzip.open(root/'independent-blocks.jsonl.gz','wt') as capture:
 for lo in range(start,height+1,100):
  hi=min(lo+99,height)
  batch=list(messages(rpc('GetBlockRange',{'start':{'height':lo},'end':{'height':hi},'poolTypes':[1]})))
  assert len(batch)==hi-lo+1
  for offset,b in enumerate(batch):
   h=int(b['height']);assert h==lo+offset
   if prev is not None:assert b['prevHash']==prev
   prev=b['hash'];blocks+=1
   capture.write(json.dumps(b)+'\n')
   for t in b.get('vtx',[]):
    records+=1;tid=txid(t['txid']);index=int(t.get('index',0))
    for i,v in enumerate(t.get('vin',[])):
     inputs+=1;op=f"{txid(v['prevoutTxid'])}:{int(v.get('prevoutIndex',0))}"
     if op in expected['receives']:
      expected['spends'][op]={'script_sha256':expected['receives'][op]['script_sha256'],'spending_txid':tid,'transaction_index':index,'input_index':i,'height':h}
    for i,v in enumerate(t.get('vout',[])):
     outputs+=1;s=raw(v.get('scriptPubKey',''))
     if s in scripts:
      expected['receives'][f'{tid}:{i}']={'script_sha256':hashlib.sha256(s).hexdigest(),'value':int(v.get('value',0)),'height':h,'transaction_index':index,'coinbase':index==0}
  (root/'oracle-progress.json').write_text(json.dumps({'blocks':blocks,'through':hi,'target':height})+'\n')
assert txid(prev)==anchor
assert outputs>0 and inputs>0
(root/'expected.json').write_text(json.dumps(expected,indent=2)+'\n')
summary={'blocks':blocks,'start':start,'anchor':height,'chain_contiguous':True,'anchor_corroborated':True,'compact_transactions':records,'transparent_outputs':outputs,'transparent_inputs':inputs,'expected_receives':len(expected['receives']),'expected_spends':len(expected['spends']),'watched_scripts':len(scripts),'scope':'block-based independent reducer; watched script inputs from wallet derivation; birthday supplied by operator; no wallet events read'}
(root/'oracle-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary))

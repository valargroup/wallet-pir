#!/usr/bin/env python3
"""Bind live public setup to certified sealed tables and check canonical service."""
import base64,concurrent.futures,datetime,hashlib,json,pathlib,sys,urllib.request
root=pathlib.Path('/opt/transparent-v10-8e69ea75')
destination=root/(sys.argv[1] if len(sys.argv)>1 else 'public-check.json')
assert not destination.exists(), 'preserve earlier public checks'
def get(url):
 with urllib.request.urlopen(url,timeout=30) as response: return response.read()
def read(url): return json.loads(get(url))
init=read('https://transparent-pir.valargroup.dev/v1/shards/init')
assert init['schema']=='transparent-shard-v10'
maps=[get(url) for url in ['https://transparent-pir.valargroup.dev/v1/shards','https://enhance-pir.valargroup.dev/v1/filters/shards']]
assert maps[0]==maps[1], 'public origins differ; repeat to distinguish a concurrent tip update'
published=json.loads(maps[0]);candidate=json.loads(pathlib.Path('/srv/zakura/transparent-shards-v10-full/shards.json').read_text())
sealed={e['shard_id']:e for e in candidate['shards'] if e['sealed']}
assert len(sealed)==85
current={e['shard_id']:e for e in published['shards']}
for sid,expected in sealed.items(): assert current[sid]==expected, ('sealed entry changed',sid)
certs=json.loads((root/'qualification/certificates/summary.json').read_text())
items=[e for e in certs['segments'] if e['shard_id'] in sealed]
assert len(items)==170
def setup(item):
 url='https://transparent-pir.valargroup.dev/v1/shards/{}/revisions/{}/setup/{}/0'.format(item['shard_id'],item['manifest_digest'],item['table'])
 result=read(url)
 actual=hashlib.sha256(base64.b64decode(result['public_params'],validate=True)).hexdigest()
 assert result['shard_id']==item['shard_id'] and result['manifest_digest']==item['manifest_digest']
 assert actual==result['public_params_sha256']==item['public_sha256']
 return {'shard_id':item['shard_id'],'table':item['table'],'manifest_digest':item['manifest_digest'],'public_sha256':actual,'certified_failure_bits':item['failure_bits']}
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool: checks=list(pool.map(setup,items))
roster=json.loads((root/'pre-roster.json').read_text())
workers=[]
for w in roster:
 r=read('http://'+w['upstream']+'/v1/ready')
 assert r['ready'] and r['mode']=='warm' and r['warm_runtimes']==r['target_runtimes'] and r['prewarm_failed']==0
 assert r['binary_sha256']=='0ece0ae1ac7f526c8471b01bb06c35c0138c80eb2c46cbaff992fa237c45f36b'
 workers.append(r)
status=read('http://127.0.0.1:8094/v1/status')
assert status['phase']=='serving'
cookie=pathlib.Path('/root/.cache/zakura/.cookie').read_bytes().strip()
request=urllib.request.Request('http://127.0.0.1:8232',data=json.dumps({'jsonrpc':'1.0','id':'v10-public-check','method':'getblockhash','params':[status['public_height']]}).encode(),headers={'Authorization':'Basic '+base64.b64encode(cookie).decode(),'Content-Type':'application/json'})
with urllib.request.urlopen(request,timeout=10) as response: node=json.load(response)
assert not node.get('error') and node['result']==status['public_hash']
result={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_sha':'8e69ea75b1e0071e3b978b0c78cc0487377f9e82','init':init,'public_map_sha256':hashlib.sha256(maps[0]).hexdigest(),'public_origins_identical':True,'sealed_entries_unchanged':85,'certified_public_setups_checked':checks,'initial_tail_certificate_binding':'Initial tail had two passing certificates at publication. It is mutable; this check binds the 170 sealed segments. Recent geometry meets the data-independent 128-bit bound.','workers':workers,'controller':status,'public_hash_matches_local_node':True}
destination.write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'output':str(destination),'sealed_setups_checked':len(checks),'warm_workers':len(workers),'canonical_public_height':status['public_height']}))

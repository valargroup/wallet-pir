#!/usr/bin/env python3
"""Real public-path regression and bounded staged load after v10 activation."""
import datetime,hashlib,json,pathlib,subprocess,urllib.request
root=pathlib.Path('/root/wallet-pir-v10-validation-8e69ea75')
sha='8e69ea75b1e0071e3b978b0c78cc0487377f9e82'
bins=pathlib.Path('/tmp/wallet-pir-v10-release-8e69ea75')
fixture=root/'mainnet-v10.json'
sample=pathlib.Path('/root/wallet-pir-8e69ea75/transparent/evidence/v9-cutover-2026-09-28/load/sample-v2codec.json')
assert hashlib.sha256(sample.read_bytes()).hexdigest()=='d9ce4f84249f9559ba0adaee0bae88f13b3f97f86aa79d92119ae04141e5f0a1'
assert hashlib.sha256((bins/'transparent-loadtest').read_bytes()).hexdigest()=='368b57a56908e258e8f1aec43c792b6cefc5a92d4640adeb2c01f2b5a5ad503a'
assert hashlib.sha256((bins/'transparent-regression').read_bytes()).hexdigest()=='19ff67b2cb854bf631f368647c48060ae3ada24cf0a0a7b5838f88ad43b6f2a6'
for name in ['regression','smoke.json','load-4.json','commands.jsonl']:
 assert not (root/name).exists(), 'preserve earlier attempts: '+name
with urllib.request.urlopen('https://transparent-pir.valargroup.dev/v1/shards/init',timeout=20) as f: init=json.load(f)
assert init['schema']=='transparent-shard-v10' and init['shards']>=86
maps=[]
for url in ['https://transparent-pir.valargroup.dev/v1/shards','https://enhance-pir.valargroup.dev/v1/filters/shards']:
 with urllib.request.urlopen(url,timeout=20) as f: maps.append(f.read())
assert maps[0]==maps[1], 'origins disagree before validation'
(root/'before-init.json').write_text(json.dumps(init,indent=2)+'\n')
(root/'before-map.json').write_bytes(maps[0])
(root/'input-hashes.json').write_text(json.dumps({str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in [fixture,sample,bins/'transparent-regression',bins/'transparent-loadtest']},indent=2)+'\n')
def run(name,args):
 args=list(map(str,args));start=datetime.datetime.now(datetime.timezone.utc)
 with (root/'commands.jsonl').open('a') as stream: stream.write(json.dumps({'start':name,'utc':start.isoformat(),'args':args})+'\n')
 print('START '+name,flush=True)
 with (root/(name+'.log')).open('w') as log:
  result=subprocess.run(args,stdout=log,stderr=subprocess.STDOUT)
 with (root/'commands.jsonl').open('a') as stream: stream.write(json.dumps({'complete':name,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'exit_code':result.returncode})+'\n')
 result.check_returncode()
 print('COMPLETE '+name,flush=True)
run('regression',[bins/'transparent-regression','--fixture',fixture,'--shard-url','https://transparent-pir.valargroup.dev','--filter-url','https://enhance-pir.valargroup.dev','--out-dir',root/'regression','--source-sha',sha,'--http-attempts','3'])
assert json.loads((root/'regression/report.json').read_text())['passed']
common=[bins/'transparent-loadtest','--shard-url','https://transparent-pir.valargroup.dev','--filter-url','https://enhance-pir.valargroup.dev','--sample',sample,'--max-queries','6000','--timeout','600s','--http-attempts','3','--source-sha',sha,'--host-sku','g-8vcpu-32gb-intel','--host-region','ams3','--client-label','public HTTPS from roman-ipir-bench-8vcpu']
# Worker operator ports admit the coordinator, not the bench. The coordinator
# records metrics and host headroom separately; do not open another ingress.
run('smoke',common+['--steps','1','--step-duration','120s','--min-completed-per-class','1','--classes','catch-up-1d,catch-up-7d,catch-up-30d,small-active,restore-6m,restore-old,multi-script,unused','--run-id','v10-smoke-1','--json-out',root/'smoke.json'])
smoke=json.loads((root/'smoke.json').read_text())
assert all(s['syncs']>0 and s['syncs']==s['exact'] and s['failed']==0 for s in smoke['steps']), 'smoke must be complete and exact before applying four-client load'
run('load-4',common+['--steps','4','--step-duration','300s','--min-completed-per-class','3','--run-id','v10-pinned-live-1','--json-out',root/'load-4.json'])
load=json.loads((root/'load-4.json').read_text())
assert all(s['completed']==s['exact'] and s['failed']==0 for s in load['steps']), 'production load returned a failure or incorrect ledger'
(root/'complete.json').write_text(json.dumps({'source_sha':sha,'completed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2)+'\n')

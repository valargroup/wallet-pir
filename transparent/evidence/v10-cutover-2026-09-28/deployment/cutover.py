"""Sequential SSH cutover; stop at any failure and preserve all output."""
import datetime,json,pathlib,subprocess,urllib.request
root=pathlib.Path('/opt/transparent-v10-8e69ea75')
sha='8e69ea75b1e0071e3b978b0c78cc0487377f9e82'
assert (root/'staging.complete').read_text().strip()==sha
assert (root/'candidate-storage.complete').read_text().strip()==sha
assert json.loads((root/'qualification/complete.json').read_text())['source_sha']==sha
assert not (root/'cutover-commands.jsonl').exists()
def read(url):
 with urllib.request.urlopen(url,timeout=15) as r:return json.load(r)
before=read('http://127.0.0.1:8094/v1/status')
assert before['phase']=='serving'
assert read('https://transparent-pir.valargroup.dev/v1/shards/init')['schema']=='transparent-shard-v9'
for w in json.loads((root/'pre-roster.json').read_text()):
 r=read('http://'+w['upstream']+'/v1/ready');assert r['ready'] and r['mode']=='warm' and r['prewarm_failed']==0
(root/'pre-cutover-controller.json').write_text(json.dumps(before,indent=2)+'\n')
def run(name,args):
 args=list(map(str,args))
 start=datetime.datetime.now(datetime.timezone.utc).isoformat()
 with (root/'cutover-commands.jsonl').open('a') as f:f.write(json.dumps({'start':name,'utc':start,'args':args})+'\n')
 print('START '+name,flush=True)
 with (root/(name+'.log')).open('x') as f:r=subprocess.run(args,stdout=f,stderr=subprocess.STDOUT)
 with (root/'cutover-commands.jsonl').open('a') as f:f.write(json.dumps({'end':name,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'returncode':r.returncode})+'\n')
 r.check_returncode();print('COMPLETE '+name,flush=True)
run('pre-cutover-fleet',['python3',root/'fleet-observe.py'])
run('backup-v9',['python3',root/'backup-v9.py'])
run('fleet-deploy',['python3',root/'fleet.py','fleet-deploy'])
run('cutover-paths',['python3',root/'cutover-paths.py'])
run('publisher-shadow',['python3',root/'fleet.py','publisher-shadow'])
run('publisher-activate',['python3',root/'fleet.py','publisher-activate'])
run('control-sessions',['systemctl','enable','--now','transparent-control-sessions'])
run('public-check',['python3',root/'public-check.py'])
(root/'cutover-complete.json').write_text(json.dumps({'source_sha':sha,'ops_sha':(root/'ops-revision').read_text().strip(),'completed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'controller':read('http://127.0.0.1:8094/v1/status')},indent=2)+'\n')
print('PUBLIC V10 CUTOVER VERIFIED',flush=True)

#!/usr/bin/env python3
"""Stage qualified candidate and prewarm privately; never activate production."""
import datetime,json,pathlib,subprocess,time
root=pathlib.Path('/opt/transparent-v10-8e69ea75')
sha='8e69ea75b1e0071e3b978b0c78cc0487377f9e82'
while not (root/'qualification/complete.json').exists():
 if subprocess.run(['systemctl','is-active','--quiet','wallet-pir-v10-qualification']).returncode!=0:
  raise RuntimeError('qualification stopped without completion marker; inspect its logs')
 time.sleep(10)
assert json.loads((root/'qualification/complete.json').read_text())['source_sha']==sha
while not (root/'candidate-storage.complete').exists(): time.sleep(5)
def run(name,args):
 print(json.dumps({'start':name,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}),flush=True)
 with (root/(name+'.log')).open('w') as log:
  subprocess.run(list(map(str,args)),stdout=log,stderr=subprocess.STDOUT,check=True)
 print(json.dumps({'done':name,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}),flush=True)
run('filter-check',[root/'bin/transparent-filter-server','--check-shard-dir','--shard-dir','/srv/zakura/transparent-shards-v10-full','--zakura-cookie','/root/.cache/zakura/.cookie'])
run('assignment',[root/'bin/shard-assign','plan','--shard-dir','/srv/zakura/transparent-shards-v10-full','--roster',root/'pre-roster.json','--recent-from-height','3262749','--out-assignment',root/'assignment.json','--source-sha',sha])
run('fleet-preflight',['python3',root/'fleet.py','fleet-preflight'])
run('prewarm',['python3',root/'prewarm.py'])
(root/'staging.complete').write_text(sha+'\n')
print('Candidate staged and runtimes persisted; live v9 unchanged',flush=True)

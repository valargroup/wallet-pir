import collections,datetime,hashlib,json,shutil,subprocess,tarfile
from pathlib import Path
p=Path('/opt/transparent-publisher-build/sessions-20260910');out=p/'start-capture';out.mkdir()
shutil.copytree(p/'rollout',out/'rollout')
for unit in ['transparent-m1-sessions-rollout.service','transparent-control-sessions.service','transparent-replica-reconciler.service']:
 (out/(unit+'.txt')).write_bytes(subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','-p','SubState','-p','MainPID','-p','ExecMainStartTimestamp','-p','NRestarts']))
counts=collections.Counter()
for file in (out/'rollout/canary').glob('query-*.ndjson'):
 for line in file.read_text().splitlines():
  try:r=json.loads(line)
  except ValueError:continue
  counts['exact' if r.get('exact') else r.get('event','unknown')]+=1
facts={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'query_counts':dict(counts),'sha256':{}}
for name in ['transparent-live-fleet.py','fleet.json','roster.json']:
 f=Path('/opt/transparent-publisher')/name;facts['sha256'][name]=hashlib.sha256(f.read_bytes()).hexdigest()
(out/'capture.json').write_text(json.dumps(facts,indent=2)+'\n')
with tarfile.open(p/'start-capture.tar.gz','w:gz') as tar:tar.add(out,arcname=out.name)
print(json.dumps(facts))

import collections,datetime,json,subprocess
from pathlib import Path
root=Path('/opt/transparent-publisher-build/headless-20260911/rollout')
rows=[json.loads(x) for x in (root/'canary/samples.ndjson').read_text().splitlines()]
counts=[]
for i in range(2):
 q=[json.loads(x) for x in (root/f'canary/query-{i}.ndjson').read_text().splitlines()]
 counts.append({'exact':sum(r.get('exact') is True for r in q),'retries':sum(r.get('event')=='retry' for r in q),'mismatches':sum(r.get('event')=='query' and r.get('exact') is not True for r in q)})
c=json.loads(Path('/opt/transparent-publisher/fleet.json').read_text())
summary={'worker_samples':len([r for r in rows if r['event']=='worker'])}
w=[r for r in rows if r['event']=='worker']
summary.update(min_available_percent=min(100*r['facts']['MemAvailable']/r['facts']['MemTotal'] for r in w), max_restarts=max(r['facts']['NRestarts'] for r in w), max_oom_kills=max(r['facts']['oom_kill'] for r in w))
for event in ['block_visible','canary_block_visible']:
 vals=[r['seconds'] for r in rows if r['event']==event];summary[event]={'count':len(vals),'max_seconds':max(vals,default=None)}
start=next(r for r in rows if r['event']=='start')
summary['elapsed_seconds']=(datetime.datetime.now(datetime.timezone.utc)-datetime.datetime.fromisoformat(start['utc'])).total_seconds()
print(json.dumps({'partial':True,'start':start,'summary':summary,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'unit':subprocess.check_output(['systemctl','show','transparent-m1-headless-rollout.service','-p','MainPID','-p','ActiveState','-p','ExecMainStatus'],text=True),'status':json.loads((root/'status.json').read_text()),'events':dict(collections.Counter(r['event'] for r in rows)),'queries':counts,'routing':json.loads((Path(c['state_dir'])/'routing-availability.json').read_text()),'result':json.loads((root/'canary/result.json').read_text()) if (root/'canary/result.json').exists() else None},indent=2))

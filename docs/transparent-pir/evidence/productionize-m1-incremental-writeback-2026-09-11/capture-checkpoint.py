import collections,datetime,json,subprocess
from pathlib import Path
root=Path('/opt/transparent-publisher-build/incremental-writeback-20260911/rollout')
rows=[json.loads(x) for x in (root/'canary/samples.ndjson').read_text().splitlines()]
counts=[]
for i in range(2):
 q=[json.loads(x) for x in (root/f'canary/query-{i}.ndjson').read_text().splitlines()]
 counts.append({'exact':sum(r.get('exact') is True for r in q),'retries':sum(r.get('event')=='retry' for r in q),'mismatches':sum(r.get('event')=='query' and r.get('exact') is not True for r in q)})
c=json.loads(Path('/opt/transparent-publisher/fleet.json').read_text())
print(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'unit':subprocess.check_output(['systemctl','show','transparent-m1-incremental-writeback-rollout.service','-p','MainPID','-p','ActiveState','-p','ExecMainStatus'],text=True),'status':json.loads((root/'status.json').read_text()),'events':dict(collections.Counter(r['event'] for r in rows)),'elapsed_seconds':(datetime.datetime.now(datetime.timezone.utc)-datetime.datetime.fromisoformat(next(r['utc'] for r in rows if r['event']=='start'))).total_seconds(),'max_public_seconds':max((r['seconds'] for r in rows if r['event']=='block_visible'),default=0),'max_replica_seconds':max((r['seconds'] for r in rows if r['event']=='canary_block_visible'),default=0),'minimum_available_memory_fraction':min((r['facts']['MemAvailable']/r['facts']['MemTotal'] for r in rows if r['event']=='worker'),default=None),'maximum_pending_saves':max((r['ready']['runtime_cache']['pending_saves'] for r in rows if r['event']=='worker'),default=0),'latest_runtime_cache':next((r['ready']['runtime_cache'] for r in reversed(rows) if r['event']=='worker'),None),'queries':counts,'routing':json.loads((Path(c['state_dir'])/'routing-availability.json').read_text()),'result':json.loads((root/'canary/result.json').read_text()) if (root/'canary/result.json').exists() else None},indent=2))

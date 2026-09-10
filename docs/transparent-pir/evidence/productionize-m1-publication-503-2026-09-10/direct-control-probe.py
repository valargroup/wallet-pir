import asyncio,importlib.util,json,time,statistics
from pathlib import Path
s=importlib.util.spec_from_file_location('fleet','/opt/transparent-publisher-build/querywait-20260909/transparent-live-fleet.corrected.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
async def main():
 f=m.Fleet(json.loads(Path('/opt/transparent-publisher/fleet.json').read_text()))
 rows=[]
 for round in range(2):
  for w in f.roster:
   t=time.monotonic();d=await f.control(w,{'operation':'status'})
   rows.append(dict(worker=w['id'],seconds=time.monotonic()-t,warm=d.get('warm'),invalidated=d.get('invalidated',False)))
 out=Path('/opt/transparent-publisher-build/querywait-20260909/failure-investigation/direct-control-probe.json');out.write_text(json.dumps(rows,indent=2)+'\n')
 print(json.dumps(dict(samples=len(rows),maximum_seconds=max(r['seconds'] for r in rows),all_warm=all(r['warm'] and not r['invalidated'] for r in rows))))
asyncio.run(main())
